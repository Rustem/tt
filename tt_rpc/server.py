import conf as _cf
import utils
import gevent
from build import tt_pb2 as proto
from mprpc import RPCServer as BaseRPCServer


class RPCServer(BaseRPCServer):

    def __init__(self, bundle=None, rpc_callback=None, *args, **kwargs):
        self.bundle = bundle
        self.rpc_callback = rpc_callback
        super(RPCServer, self).__init__(*args, **kwargs)

    def auth(self, raw_request):
        request = proto.AuthRequest()
        utils.load(request, raw_request)
        resp = proto.AuthResponse()
        resp.pong = request.ping
        resp.cluster_uuid = self.bundle.cluster_uuid
        resp.node_uuid = self.bundle.node_id
        try:
            return utils.dump(resp)
        finally:
            if callable(self.rpc_callback):
                gevent.spawn(self.rpc_callback,
                             _cf.AUTH_REQUEST,
                             request)

    def heartbeat(self, raw_request):
        request = proto.HeartbeatRequest()
        utils.load(request, raw_request)
        recv_time = self.bundle.local_clock.WallTime()
        resp = proto.HeartbeatResponse()
        resp.pong = request.ping
        resp.recv_time = recv_time
        resp.send_time = self.bundle.local_clock.WallTime()
        try:
            return utils.dump(resp)
        finally:
            if callable(self.rpc_callback):
                gevent.spawn(self.rpc_callback,
                             _cf.HEARTBEAT_REQUEST,
                             request)
