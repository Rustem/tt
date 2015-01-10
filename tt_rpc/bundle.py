class Bundle(object):

    def __init__(self, node_id=None, local_clock=None, cluster_uuid=None,
                 offset=None, remote_clock_monitor=None):
        self.node_id = node_id
        self.local_clock = local_clock
        self.cluster_uuid = cluster_uuid
        self.offset = offset or 0
        self.remote_clock_monitor = remote_clock_monitor
