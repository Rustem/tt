import time
from copy import deepcopy
from exc import AheadOfMaxOffsetError
try:
    from build import tt_pb2 as proto
except Exception, e:
    print 'not generated'
    proto = None

""" Clock is a hybrid logical clock. Objects of this
type model causality while maintaining a relation
to physical time. Roughly speaking, timestamps
consist of the largest wall clock time among all
events, and a logical clock that ticks whenever
an event happens in the future of the local physical
clock. The data structure is thread safe and thus can safely
be shared by multiple goroutines."""


MICRO = 10 ** 6


def ts():
    """Returns timestamp with micro accuracy"""
    return int(round(time.time() * MICRO))


class Clock(object):

    """
    Parameters
    ----------
        state - last hlc timestamp of local/send/receive event
        max_offset - how far ahead of the physical clock the wall time can be.
    """

    def __init__(self, physical_clock):
        self.ts = proto.Timestamp()
        self.physical_time = physical_clock
        self.max_offset = 0

    def TS(self):
        return deepcopy(self.ts)

    def SetMaxOffset(self, delta):
        self.max_offset = delta

    def Now(self):
        """Now returns a timestamp associated with an event from
        the local machine that may be sent to other members
        of the distributed network. This is the counterpart
        of Update, which is passed a timestamp received from
        another member of the distributed network.
        """
        physical_now = self.physical_time()
        if self.ts.wall_time >= physical_now:
            self.ts.logical += 1
        else:
            self.ts.wall_time = physical_now
            self.ts.logical = 0
        return self.TS()

    def Update(self, receive_ts):
        """Update takes a hybrid timestamp, usually originating from
        an event received from another member of a distributed
        system. The clock is updated and the hybrid timestamp
        associated to the receipt of the event returned.
        An error may only occur if offset checking is active and
        the remote timestamp was rejected due to clock offset,
        in which case the state of the clock will not have been
        altered.
        To timestamp events of local origin, use Now instead."""
        assert isinstance(receive_ts, proto.Timestamp), ""
        physical_now = self.physical_time()
        if (physical_now > self.ts.wall_time) and (
                physical_now > receive_ts.wall_time):
            # physical clock is ahead of both wall times. It is used
            # as a new wall time and its logical clock is reset
            self.ts.wall_time = physical_now
            self.ts.logical = 0
            return self.TS(), None

        if receive_ts.wall_time > self.ts.wall_time:
            # received clock is larger => it remains but with tick
            # logical clock
            # TODO (xepa4er): consider max offset might be raise exception
            if self.max_offset > 0 and \
                    receive_ts.wall_time - physical_now > self.max_offset:
                return self.TS(), AheadOfMaxOffsetError("")
            self.ts.wall_time = receive_ts.wall_time
            self.ts.logical = receive_ts.logical + 1
        elif self.ts.wall_time > receive_ts.wall_time:
            # our wall time is ahead => we tick our clock
            self.ts.logical += 1
        else:
            # both clocks are equal => larger logical clock is used and ticked.
            if receive_ts.logical > self.ts.logical:
                self.ts.logical = receive_ts.logical
            self.ts.logical += 1
        return self.TS(), None

