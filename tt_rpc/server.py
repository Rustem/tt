import conf as _cf
import gevent
from .base import rpc_request
from build import tt_pb2 as proto
from mprpc import RPCServer as BaseRPCServer


class RPCServer(BaseRPCServer):

    def __init__(self, bundle=None, rpc_callback=None, *args, **kwargs):
        self.bundle = bundle
        self.rpc_callback = rpc_callback
        super(RPCServer, self).__init__(*args, **kwargs)

    @rpc_request
    def auth(self, request):
        resp = proto.AuthResponse()
        resp.pong = request.ping
        resp.cluster_uuid = self.bundle.cluster_uuid
        resp.node_uuid = self.bundle.node_id
        try:
            return resp
        finally:
            if callable(self.rpc_callback):
                gevent.spawn(self.rpc_callback,
                             _cf.AUTH_REQUEST,
                             request)

    @rpc_request
    def heartbeat(self, request):
        recv_time = self.bundle.local_clock.WallTime()
        resp = proto.HeartbeatResponse()
        resp.pong = request.ping
        resp.recv_time = recv_time
        resp.send_time = self.bundle.local_clock.WallTime()
        try:
            return resp
        finally:
            if callable(self.rpc_callback):
                gevent.spawn(self.rpc_callback,
                             _cf.HEARTBEAT_REQUEST,
                             request)
