import utils
import os
import uuid
import etcd
import gevent
import greenclock
import exc
from build import tt_pb2 as proto
from retry import RetryWithBackoff, RETRY_CONTINUE, RETRY_BREAK
from tt import Clock
from tt_offset import ClockMonitor
from tt_rpc import NewRPCServer, NewRPCClient, Bundle
import conf as _cf

pj = os.path.join


LEADER_LEASE = 60
CONNECTION_TIMEOUT = 150
# CONFIG_RESOURCE = 'http://178.62.168.162:4001/v2/'
ETCD_HOST = '178.62.168.162'
ETCD_PORT = 4001
SERVICE_NAME = '_db'
DEFAULT_MAX_OFFSET = 250 * 1000  # micro

HEARTBEAT_TASK = 'hb'


class SimpleDiscoveryProtocol(object):

    def __init__(self):
        self._etcd = etcd.Client(host=ETCD_HOST,
                                 port=ETCD_PORT,
                                 allow_reconnect=True,
                                 protocol='http')
        self._base_dir = SERVICE_NAME

    def join(self, cluster_uuid, name=None, host=None, **kwargs):
        assert None not in (name, host), "both must be provided"
        dir_name = pj(cluster_uuid, name)
        self._exec_write(dir_name, value=host, **kwargs)

    def leave(self, cluster_uuid, name=None):
        dir_name = pj(cluster_uuid, name)
        self._exec_delete(dir_name)

    def has_peers(self, cluster_id):
        dir_name = pj(cluster_id, 'state')
        try:
            self._exec_write(dir_name, 'colonized', prevExist=False)
        except KeyError:
            return True
        else:
            return False

    def get_peers(self, cluster_id):
        result = self._exec_read(cluster_id, recursive=True)
        return ((peer.key, peer.value) for peer in result.children
                if not peer.key.endswith('state'))

    def cleanup(self):
        key = '/' + self._base_dir
        self._etcd.delete(key, recursive=True)

    def _exec_write(self, dir_name, value, **kwargs):
        key = '/' + pj(self._base_dir, dir_name)
        return self._etcd.write(key, value, **kwargs)

    def _exec_read(self, dir_name, **kwargs):
        key = '/' + pj(self._base_dir, dir_name)
        return self._etcd.read(key, **kwargs)

    def _exec_delete(self, dir_name):
        key = '/' + pj(self._base_dir, dir_name)
        return self._etcd.delete(key)


class Server(object):

    def __init__(self, host, cluster_uuid, max_offset):
        self.uuid = str(uuid.uuid4())
        self.host = host
        self.addr, self.port = utils.parse_host(host)
        self.cluster_uuid = cluster_uuid
        self.clock = Clock()
        self.clock_monitor = ClockMonitor(self.clock)
        self.clock.SetMaxOffset(max_offset)
        self.bundle = Bundle(host=self.host,
                             node_id=self.uuid,
                             local_clock=self.clock,
                             cluster_uuid=self.cluster_uuid,
                             remote_clock_monitor=self.clock_monitor)
        self.rpc_server = NewRPCServer(
            (self.addr, self.port),
            bundle=self.bundle,
            rpc_callback=self.on_rpc_request)

        self.neighbors = {}
        self._discovery_proto = SimpleDiscoveryProtocol()
        self._network_leader = False
        self._scheduler = greenclock.Scheduler(
            logger_name='%s_scheduler' % self.uuid)

    def start(self, cleanup=False):
        """Starter function:
            * join itself to internal network.
            * polls other nodes in the system for status/time/..."""
        if cleanup:
            self._discovery_proto.cleanup()
        peers = self.discover_network(self.cluster_uuid)
        jobs = []
        for node_uuid, node_host in peers:
            if utils.thats_me(node_uuid, self.uuid):
                continue
            jobs.append(gevent.spawn(self.establish_conn, node_host))
        # gevent.joinall(jobs)   # todo(xepa4ep): might be timeout it
        gevent.spawn(self.clock_monitor.MonitorOffset)
        gevent.spawn(self._scheduler.run_forever, 'once')
        self.rpc_server.serve_forever()

    def establish_conn(self, host):
        """Manually establish tcp connection with other nodes
        in the system."""
        addr, port = utils.parse_host(host)
        rpc_client = NewRPCClient((addr, port), bundle=self.bundle)
        try:
            remote_node_id = rpc_client.auth()
        except exc.NodeAuthError:
            return
        rpc_client.remote_node_id = remote_node_id
        _cli = self.neighbors[remote_node_id] = rpc_client
        self.start_heartbeat(_cli, remote_node_id)

    def on_rpc_request(self, request_type, request):
        if request_type == _cf.AUTH_REQUEST:
            _cli = self.neighbors[request.node_uuid] = NewRPCClient(
                utils.parse_host(request.host), bundle=self.bundle)
            _node_id = _cli.remote_node_id = request.node_uuid
            print "NOTE %s connected the cluster" % _node_id
            self.start_heartbeat(_cli, _node_id)

    def start_heartbeat(self, remote_rpc_client, remote_node_id):
        job = self.heartbeat
        run_every = greenclock.every_second(_cf.HEARTBEAT_INTERVAL)
        task_name = "%s_%s" % (HEARTBEAT_TASK, remote_node_id)
        print "RESCHEDULE %s" % remote_node_id
        self._reschedule_task(
            task_name, run_every, job, remote_rpc_client, remote_node_id)
        print 'START HEARTBEATING'

    def heartbeat(self, remote_rpc_client, remote_node_id):
        """Christian's algorithm for maintain offset also known
        as probabilistic clock sync algorithm"""
        send_time = self.clock.WallTime()
        offset = proto.RemoteOffset()
        retry_options = {
            'tag': '',
            'backoff': 1,  # default retry backoff interval
            'max_backoff': 20,  # default max retry backoff interval
            'constant_factor': 2,  # default backoff multiplier
            'max_attempts': 3,  # default max of attempts
        }
        try:
            response = RetryWithBackoff(
                retry_options, self._heartbeat, args=(remote_rpc_client,))
        except Exception as exc:
            print "UNSCHEDULING due to %s" % exc.reason   # log exception
            self.cleanup_remote_peer(remote_node_id)
            return
        if response is None:
            offset = utils.max_offset()
            offset.measured_at = self.clock.WallTime()
        else:
            recv_time = self.clock.WallTime()
            if recv_time - send_time > _cf.MAX_CLOCK_READING_DELAY:
                offset = utils.max_offset
                offset.measured_at = recv_time
            else:
                rr_delay = recv_time - send_time - (
                    response.send_time - response.recv_time)
                offset.offset = response.send_time + (rr_delay / 2) - recv_time
                offset.error = rr_delay / 2
                offset.measured_at = recv_time
        self.clock_monitor.UpdateRemoteOffset(self.uuid, offset)
        return response

    def cleanup_remote_peer(self, remote_node_id):
        self._scheduler.stop_task('%s_%s' % (HEARTBEAT_TASK, remote_node_id))
        self.neighbors.pop(remote_node_id, None)
        with gevent.Timeout(5):
            self._discovery_proto.leave(
                self.cluster_uuid, name=remote_node_id)
        print "NODE %s left cluster" % remote_node_id

    def discover_network(self, cluster_id):
        self._discovery_proto.join(
            cluster_id, name=self.uuid, host=self.host)
        if not self._discovery_proto.has_peers(cluster_id):
            self._network_leader = True
        return self._discovery_proto.get_peers(cluster_id)

    def stop(self):
        for _sock in self.neighbors.values():
            _sock.close()
        self.rpc_server.stop()
        self._scheduler.stop()

    def _heartbeat(self, rpc_client):
        try:
            response = rpc_client.heartbeat()
        except IOError as e:
            return RETRY_CONTINUE, e
        else:
            return RETRY_BREAK, response

    def _reschedule_task(self, task_name, run_every, job, *args):
        try:
            self._scheduler.stop_task(task_name)  # unschedule old
        except KeyError:
            pass   # if no task registered then silent
        self._scheduler.schedule(task_name, run_every, job, *args)
        self._scheduler.stop()
        gevent.sleep(1.5)
        self._scheduler.run_forever()


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Start a node',)
    parser.add_argument('host', type=str, help='127.0.0.1:4001')
    parser.add_argument('--cluster_id', type=str, help='UUID of cluster')
    parser.add_argument('--max_offset', type=int, default=DEFAULT_MAX_OFFSET,
                        help='max offset for internal wall time')
    parser.add_argument('--cleanup', type=bool, default=False,
                        help='max offset for internal wall time')
    args = parser.parse_args()

    # start a server
    server = Server(args.host, args.cluster_id, args.max_offset)
    server.start(cleanup=args.cleanup)


if __name__ == '__main__':
    main()
