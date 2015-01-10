import gevent
from tt import conf as _cf, exc, utils
from build import tt_pb2 as proto
from .base import BaseRPCClient


class RPCClient(BaseRPCClient):
    def __init__(self, bundle=None, *args, **kwargs):
        self.bundle = bundle
        self.remote_node_id = None  # established after auth
        super(RPCClient, self).__init__(*args, **kwargs)

    def auth(self):
        req = proto.AuthRequest()
        req.ping = 'auth'
        req.node_uuid = self.bundle.node_id
        response = self.call(_cf.AUTH_REQUEST, req)
        try:
            assert self.bundle.cluster_uuid == response.cluster_uuid, "must be equal"
        except:
            raise exc.NodeAuthError('auth failed')
        self.remote_node_id = response.node_uuid
        return response.node_uuid

    def heartbeat(self):
        """Christian's algorithm for maintain offset also known
        as probabilistic clock sync algorithm"""
        offset = proto.RemoteOffset()
        req = proto.HeartbeatRequest(ping='hb')
        send_time = self.bundle.local_clock.WallTime()
        with gevent.Timeout(_cf.HEARTBEAT_INTERVAL * 2):
            response = self.call(_cf.HEARTBEAT_REQUEST, req)
        if response is None:
            offset = utils.max_offset()
            offset.measured_at = self.bundle.local_clock.WallTime()
        else:
            recv_time = self.bundle.local_clock.WallTime()
            if recv_time - send_time > _cf.MAX_CLOCK_READING_DELAY:
                offset = utils.max_offset
                offset.measured_at = recv_time
            else:
                rr_delay = recv_time - send_time - (
                    response.send_time - response.recv_time)
                offset.offset = response.send_time + (rr_delay / 2) - recv_time
                offset.error = rr_delay / 2
                offset.measured_at = recv_time
        self.bundle.remote_clock_monitor.UpdateRemoteOffset(
            self.remote_node_id, offset)
        return response
