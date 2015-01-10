import utils
import os
pj = os.path.join
import uuid
from functools import partial
import etcd
import gevent
from gevent.server import StreamServer
from build import tt_pb2 as proto
from tt import Clock
from tt_offset import ClockMonitor


LEADER_LEASE = 60
CONNECTION_TIMEOUT = 150
# CONFIG_RESOURCE = 'http://178.62.168.162:4001/v2/'
ETCD_HOST = '178.62.168.162'
ETCD_PORT = 4001
SERVICE_NAME = '_db'
DEFAULT_MAX_OFFSET = 250  # micro
DEFAULT_BUFF_SIZE = 1024


def send_rpc_request(socket, request, response):
    socket.send(request.SerializeToString())
    response.ParseFromString(socket.recv(DEFAULT_BUFF_SIZE))
    return response


class SimpleDiscoveryProtocol(object):

    def __init__(self):
        self._etcd = etcd.Client(host=ETCD_HOST,
                                 port=ETCD_PORT,
                                 allow_reconnect=True,
                                 protocol='http')
        self._base_dir = SERVICE_NAME

    def join(self, cluster_uuid, name=None, host=None, **kwargs):
        assert not None in (name, host), "both must be provided"
        dir_name = pj(cluster_uuid, name)
        self._exec_write(dir_name, value=host, **kwargs)

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


class Server(object):

    def __init__(self, host, cluster_uuid, max_offset):
        self.uuid = str(uuid.uuid4())
        self.host = host
        self.cluster_uuid = cluster_uuid
        self.clock = Clock()
        self.clock.SetMaxOffset(max_offset)
        self.server = StreamServer(utils.parse_host(host),
                                   self._handle_connection)
        self.neighbors = {}
        self._discovery_proto = SimpleDiscoveryProtocol()
        self._network_leader = False

    def _handle_connection(self, socket, address):
        """Handle incoming connection."""
        remote_node_uuid = self.authenticate(socket)
        print 'INCOMING CONN: %s' % remote_node_uuid
        self.neighbors[remote_node_uuid] = socket
        gevent.joinall(
            [gevent.spawn(self.serve_rpc_request, socket),
             gevent.spawn(self.serve_rpc_response, socket)])

    def _handle_outgoing_connection(self, node_uuid, socket):
        """Handle outgoing connection."""
        self.neighbors[node_uuid] = socket

    def authenticate(self, socket):
        request = proto.AuthRequest(ping='auth')
        response = proto.AuthResponse()
        response = send_rpc_request(socket, request, response)
        assert response.cluster_uuid == self.cluster_uuid, "cluster is bad"
        return response.node_uuid

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
            jobs.append(gevent.spawn(
                self.establish_tcp_conn, node_host,
                callback=partial(
                    self._handle_outgoing_connection, node_uuid)))
        gevent.joinall(jobs)   # todo(xepa4ep): might be timeout it
        gevent.spawn(ClockMonitor(self.clock).MonitorOffset())
        self.server.serve_forever()

        # self.monitor_clock_offset()

    def establish_tcp_conn(self, peer, callback=None):
        """Manually establish tcp connection with other nodes
        in the system."""
        addr, port = utils.parse_host(peer)
        new_sock = gevent.socket.create_connection(
            (addr, port), timeout=CONNECTION_TIMEOUT)
        request = proto.AuthRequest()
        with gevent.Timeout(CONNECTION_TIMEOUT):
            request.ParseFromString(new_sock.recv(DEFAULT_BUFF_SIZE))
            if request.HasField('ping'):  # auth
                response = proto.AuthResponse(
                    pong=request.ping,
                    cluster_uuid=self.cluster_uuid,
                    node_uuid=self.uuid)
                new_sock.send(response.SerializeToString())
        if callback:
            callback(new_sock)
        utils.reset_timeout(new_sock)
        gevent.joinall(
            [gevent.spawn(self.serve_rpc_request, new_sock),
             gevent.spawn(self.serve_rpc_response, new_sock)])

    def discover_network(self, cluster_id):
        self._discovery_proto.join(
            cluster_id, name=self.uuid, host=self.host)
        if not self._discovery_proto.has_peers(cluster_id):
            self._network_leader = True
        return self._discovery_proto.get_peers(cluster_id)

    def serve_rpc_response(self, socket):
        while True:
            print 'receiving...'
            data = socket.recv(DEFAULT_BUFF_SIZE)
            if not data:
                print "bye"
                return
            print data, "hi"

    def serve_rpc_request(self, socket):
        while True:
            gevent.sleep(3)
            socket.send('[%s]: hi' % self.uuid)

    def stop(self):
        for _sock in self.neighbors.values():
            _sock.close()
        self.server.stop()


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
