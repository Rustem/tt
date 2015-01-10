from build import tt_pb2 as proto

HEARTBEAT_INTERVAL = 5
MONITOR_INTERVAL = HEARTBEAT_INTERVAL * 7

DEFAULT_BUFF_SIZE = 1024
DELIM = '#'

HEARTBEAT_REQUEST = 'heartbeat'
AUTH_REQUEST = 'auth'

RPC_SERVICES = {
    HEARTBEAT_REQUEST: (proto.HeartbeatRequest, proto.HeartbeatResponse),
    AUTH_REQUEST: (proto.AuthRequest, proto.AuthResponse)
}

DEFAULT_PORT = 0
