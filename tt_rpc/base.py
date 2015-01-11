import functools
import conf as _cf
import utils


def rpc_request(fn):

    @functools.wraps(fn)
    def inner(rpc_server, raw):
        _meth = fn.func_name
        req = _cf.RPC_SERVICES[_meth][0]()
        utils.load(req, raw)
        resp = fn(rpc_server, req)
        return utils.dump(resp)

    return inner


def rpc_response(fn):

    @functools.wraps(fn)
    def inner(rpc_client):
        _meth = fn.func_name
        resp = _cf.RPC_SERVICES[_meth][1]()
        req = utils.dump(fn(rpc_client))
        utils.load(resp, rpc_client.call(_meth, req))
        if hasattr(rpc_client, 'on_{}'.format(_meth)):
            hook = getattr(rpc_client, 'on_{}'.format(_meth))
            return hook(resp)
        return resp

    return inner
