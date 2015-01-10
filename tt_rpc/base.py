from mprpc import RPCServer as GeventRPCServer, RPCClient as GeventRPCClient
from google.protobuf import message
from tt import conf as _cf

RPC_REQUEST = 'rpc://req'
RPC_RESPONSE = 'rpc://resp'


class UnPacker(object):

    def feed(self, raw):
        tp, msg_id, meth, body = raw.split(',')
        assert meth in _cf.RPC_SERVICES, 'not support rpc service %s' % meth
        req = _cf.RPC_SERVICES[meth][0]()
        req.ParseFromString(body)
        return iter(((tp, msg_id, meth, (req,)),))


class Packer(object):

    def pack(self, resp):
        resp = list(resp)
        assert isinstance(resp[3], message.Message), "Should support proto buf protocol"
        resp[3] = resp[3].SerializeToString()
        return ','.join(resp)


class BaseRPCServer(GeventRPCServer):

    def __init__(self, *args, **kwargs):
        super(BaseRPCServer, self).__init__(*args, **kwargs)
        self._packer = Packer()
        self._unpacker = UnPacker()


class BaseRPCClient(GeventRPCClient):

    def __init__(self, *args, **kwargs):
        super(BaseRPCClient, self).__init__(*args, **kwargs)
        self._packer = Packer()
        self._unpacker = UnPacker()
