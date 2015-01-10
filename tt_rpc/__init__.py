from gevent.server import StreamServer
from .server import RPCServer
from .client import RPCClient
from .bundle import Bundle
__all__ = ['Bundle', 'NewRPCServer']


def NewRPCServer(host, **options):
    return StreamServer(host, RPCServer(**options))


def NewRPCClient(host, **options):
    return RPCClient(*host, **options)
