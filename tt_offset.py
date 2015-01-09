import sys
import gevent
from collections import namedtuple
from exc import IntervalNotFoundError
import conf as cf

IntervalPoint = namedtuple('IntervalPoint', ['offset', 'type'])


def compare_pts(l, r):
    if l.offset == r.offset:
        if l.type < r.type:
            return -1
        else:
            return 1
    if l.offset < r.offset:
        return -1
    else:
        return 1


class ClockMonitor(object):
    def __init__(self, local_clock):
        self.local_clock = local_clock
        self.offsets = {}   # maps remote node uuid to proto.RemoteOffset
        self.last_monitored_at = 0

    def UpdateRemoteOffset(self, node_id, offset):
        old_offset = self.offsets.get(node_id, None)
        if old_offset is None:
            self.offsets[node_id] = offset
        elif old_offset.measured_at < self.last_monitored_at:
            self.offsets[node_id] = self.last_monitored_at
        elif (old_offset.measured_at >= self.last_monitored_at and
              old_offset.error > offset.error):
            self.offsets[node_id] = offset

    def MonitorOffset(self):
        """Periodically checks that offset is within
        [-maxOffset, +maxOffset]. If the offset exceeds, then
        this method calls a system.exit() causing the node to
        suicide."""
        max_offset = self.local_clock.max_offset
        while True:
            gevent.sleep(cf.MONITOR_INTERVAL)
            try:
                offset_interval = self.find_offset_interval()
            except IntervalNotFoundError:
                sys.exit("clock offset could not be determined.")

            if self.local_clock.max_offset != 0:
                if (offset_interval[0] > max_offset or
                        offset_interval[1] < -max_offset):
                    sys.exit("clock offset is greater than defined maximum:"
                             "%s %s" % (offset_interval, max_offset))
            self.last_monitored_at = self.local_clock.WallTime()

    def find_offset_interval(self):
        """Marzullo algorithm that helps to find true region that include
        true offset from the cluster time. Required majority."""
        pts = sorted(self.build_endpoint_list(), cmp=compare_pts)
        quorum = len(pts) / 4
        if not pts:
            return ((0, 0), None)
        best, cnt = 0, 0
        lower, upper = 0, 0
        for i, pt in enumerate(pts):
            cnt -= pt.type
            if cnt > best:
                best = cnt
                lower, upper = pt.offset, pts[i + 1].offset
        if best <= quorum:
            raise IntervalNotFoundError('')
        return ((lower, upper), None)

    def build_endpoint_list(self):
        pts = []
        del_keys = set([])
        for addr, o in self.offsets.iteritems():
            if o.measured_at < self.last_monitored_at:
                del_keys.add(addr)
                continue
            pts.append(IntervalPoint(
                offset=o.offset - o.error - self.local_clock.max_offset,
                type=-1))
            pts.append(IntervalPoint(
                offset=o.offset + o.error + self.local_clock.max_offset,
                type=+1))
        for key in del_keys:
            del self.offsets[key]
        return pts
