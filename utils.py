def parse_host(host):
    host, port = host.split(':')
    return host, int(port)


def thats_me(uuid_suffix, uuid):
    return uuid_suffix.endswith(uuid)


def reset_timeout(sock):
    sock.timeout = None
