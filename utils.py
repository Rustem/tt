import base64
from build import tt_pb2 as proto
from copy import deepcopy


def parse_host(host):
    host, port = host.split(':')
    return str(host), int(port)


def thats_me(uuid_suffix, uuid):
    return uuid_suffix.endswith(uuid)


def reset_timeout(sock):
    sock.timeout = None


def max_offset():
    return deepcopy(proto.MAX_OFFSET)


def dump(protobuf_data):
    raw = protobuf_data.SerializeToString()
    return base64.b64encode(raw)


def load(protobuf_data, base64_data):
    raw = base64.b64decode(base64_data)
    protobuf_data.ParseFromString(raw)
