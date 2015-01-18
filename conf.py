from build import tt_pb2 as proto

HEARTBEAT_INTERVAL = 3  # in seconds
MONITOR_INTERVAL = HEARTBEAT_INTERVAL * 7  # in seconds
MAX_CLOCK_READING_DELAY = 1500 * 1000   # in micro

DEFAULT_BUFF_SIZE = 1024
DELIM = '#'

HEARTBEAT_REQUEST = 'heartbeat'
AUTH_REQUEST = 'auth'

RPC_SERVICES = {
    HEARTBEAT_REQUEST: (proto.HeartbeatRequest, proto.HeartbeatResponse),
    AUTH_REQUEST: (proto.AuthRequest, proto.AuthResponse)
}

DEFAULT_PORT = 0
