import gevent
import conf as _cf
import exc
import utils
from build import tt_pb2 as proto
from mprpc import RPCClient as BaseRPCClient


class RPCClient(BaseRPCClient):
    def __init__(self, *args, **kwargs):
        self.bundle = kwargs.pop('bundle')
        self.remote_node_id = None  # established after auth
        super(RPCClient, self).__init__(*args, **kwargs)

    def auth(self):
        req = proto.AuthRequest()
        req.ping = 'auth'
        req.node_uuid = self.bundle.node_id
        req.host = self.bundle.host
        response = proto.AuthResponse()
        utils.load(response, self.call(_cf.AUTH_REQUEST, utils.dump(req)))
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
        response = proto.HeartbeatResponse()

        send_time = self.bundle.local_clock.WallTime()
        with gevent.Timeout(_cf.HEARTBEAT_INTERVAL * 2):
            utils.load(response, self.call(
                _cf.HEARTBEAT_REQUEST, utils.dump(req)))
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
                print response.send_time, response.recv_time
                offset.offset = response.send_time + (rr_delay / 2) - recv_time
                offset.error = rr_delay / 2
                offset.measured_at = recv_time
        print 'heartbeating %s' % offset
        self.bundle.remote_clock_monitor.UpdateRemoteOffset(
            self.remote_node_id, offset)
        return response
