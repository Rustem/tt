def parse_host(host):
    host, port = host.split(':')
    return host, int(port)

