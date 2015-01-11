import gevent
import conf as _cf
import exc
import utils
from .base import rpc_response
from build import tt_pb2 as proto
from mprpc import RPCClient as BaseRPCClient


class RPCClient(BaseRPCClient):
    def __init__(self, *args, **kwargs):
        self.bundle = kwargs.pop('bundle')
        self.remote_node_id = None  # established after auth
        super(RPCClient, self).__init__(*args, **kwargs)

    @rpc_response
    def auth(self):
        req = proto.AuthRequest()
        req.ping = 'auth'
        req.node_uuid = self.bundle.node_id
        req.host = self.bundle.host
        return req

    def on_auth(self, response):
        try:
            assert self.bundle.cluster_uuid == response.cluster_uuid, "must be equal"
        except:
            raise exc.NodeAuthError('auth failed')
        return response.node_uuid

    @rpc_response
    def heartbeat(self):
        return proto.HeartbeatRequest(ping='hb')
