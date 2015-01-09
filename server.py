from tt import Clock
import etcd
import uuid
from gevent.server import StreamServer
from gevent import socket
import utils
import os
pj = os.path.join


LEADER_LEASE = 60
CONNECTION_TIMEOUT = 15
# CONFIG_RESOURCE = 'http://178.62.168.162:4001/v2/'
ETCD_HOST = '178.62.168.162'
ETCD_PORT = 4001
SERVICE_NAME = '_db'
DEFAULT_MAX_OFFSET = 250  # micro


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
        return ((peer.key, peer.value) for peer in result.children)

    def _exec_write(self, dir_name, value, **kwargs):
        key = pj(self._base_dir, dir_name)
        return self._etcd.write(key, value, **kwargs)

    def _exec_read(self, dir_name, **kwargs):
        key = pj(self._base_dir, dir_name)
        return self._etcd.read(key, **kwargs)


class Server(object):

    def __init__(self, host, cluster_uuid, max_offset):
        self.uuid = str(uuid.uuid4())
        self.host = host
        self.cluster_uuid = cluster_uuid
        self.clock = Clock()
        self.clock.SetMaxOffset(max_offset)
        self.server = StreamServer((utils.parse_host(host),),
                                   self._handle_connection)
        self.neighbors = {}
        self._discovery_proto = SimpleDiscoveryProtocol()
        self._network_leader = False

    def _handle_connection(self, socket, address):
        self.neighbors[address] = socket
        print 'New connection entered'

    def start(self):
        peers = self.discover_network(self.cluster_uuid)
        for node_uuid, node_host in peers:
            self.establish_tcp_conn(node_host,
                                    callback=self._handle_connection)
        self.server.start()
        # self.monitor_clock_offset()

    def establish_tcp_conn(self, peer, callback=None):
        addr, port = utils.parse_host(peer)
        new_sock = socket.create_connection((addr, port),
                                            timeout=CONNECTION_TIMEOUT)
        if callback:
            callback(new_sock)

    def discover_network(self, cluster_id):
        self._discovery_proto.join(
            cluster_id, name=self.uuid, host=self.host)
        if not self._discovery_proto.has_peers(cluster_id):
            self._network_leader = True
        return self._discovery_proto.get_peers(cluster_id)

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
    args = parser.parse_args()

    # start a server
    server = Server(args.host, args.cluster_id, args.max_offset)
    server.start()


if __name__ == '__main__':
    main()
