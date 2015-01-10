from tt import conf as _cf
from build import tt_pb2 as proto
from .base import BaseRPCServer


class RPCServer(BaseRPCServer):

    def __init__(self, bundle=None, rpc_callback=None, *args, **kwargs):
        self.bundle = bundle
        self.rpc_callback = rpc_callback
        super(RPCServer, self).__init__(*args, **kwargs)

    def auth(self, request):
        resp = proto.AuthResponse()
        resp.pong = request.ping
        resp.cluster_uuid = self.bundle.cluster_uuid
        resp.node_uuid = self.bundle.node_id
        if callable(self.rpc_callback):
            self.rpc_callback(_cf.AUTH_REQUEST, request)

    def heartbeat(self, request):
        recv_time = self.bundle.local_clock.WallTime()
        resp = proto.HeartbeatResponse()
        resp.pong = request.ping
        resp.recv_time = recv_time
        try:
            return resp
        finally:
            resp.send_time = self.bundle.local_clock.WallTime()
            if callable(self.rpc_callback):
                self.rpc_callback(_cf.HEARTBEAT_REQUEST, request)
