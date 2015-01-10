import unittest
from tt_offset import IntervalPoint, compare_pts, ClockMonitor
import tt
from build import tt_pb2 as proto


class TTOffsetTest(unittest.TestCase):

    def test_endpointListSort(self):
        pts = [
            IntervalPoint(offset=5, type=+1),
            IntervalPoint(offset=3, type=-1),
            IntervalPoint(offset=3, type=+1),
            IntervalPoint(offset=1, type=-1),
            IntervalPoint(offset=4, type=+1)
        ]

        expected_pts = [
            IntervalPoint(offset=1, type=-1),
            IntervalPoint(offset=3, type=-1),
            IntervalPoint(offset=3, type=+1),
            IntervalPoint(offset=4, type=+1),
            IntervalPoint(offset=5, type=+1)
        ]

        actual_pts = sorted(pts, cmp=compare_pts)
        self.assertEqual(len(actual_pts), len(expected_pts))

        for i in xrange(len(actual_pts)):
            self.assertEqual(actual_pts[i], expected_pts[i])

    def test_buildEndpointList(self):
        offsets = {
            '0': proto.RemoteOffset(offset=0, error=10),
            '1': proto.RemoteOffset(offset=1, error=10),
            '2': proto.RemoteOffset(offset=2, error=10),
            '3': proto.RemoteOffset(offset=3, error=10)
        }

        m = tt.ManualClock(0)
        clock = tt.Clock(m.get_clock)
        clock.SetMaxOffset(5)

        remoteClocks = ClockMonitor(clock)
        remoteClocks._replace_offsets(offsets)

        expected = [
            IntervalPoint(offset=-15, type=-1),
            IntervalPoint(offset=-14, type=-1),
            IntervalPoint(offset=-13, type=-1),
            IntervalPoint(offset=-12, type=-1),
            IntervalPoint(offset=15, type=1),
            IntervalPoint(offset=16, type=1),
            IntervalPoint(offset=17, type=1),
            IntervalPoint(offset=18, type=1),
        ]

        actual = sorted(remoteClocks.build_endpoint_list(), cmp=compare_pts)
        self.assertEqual(len(actual), len(expected))
        for i in xrange(len(expected)):
            self.assertEqual(expected[i], actual[i])

    def test_buildEndpointListRemoveFalseSource(self):
        offsets = {
            '0': proto.RemoteOffset(offset=0, error=10, measured_at=11),
            '1': proto.RemoteOffset(offset=2, error=10, measured_at=12),
            'false_0': proto.RemoteOffset(offset=1, error=5, measured_at=0),
            'false_1': proto.RemoteOffset(offset=0, error=2, measured_at=9)
        }
        m = tt.ManualClock(0)
        clock = tt.Clock(m.get_clock)
        clock.SetMaxOffset(5)

        remoteClocks = ClockMonitor(clock)
        remoteClocks._replace_offsets(offsets)
        remoteClocks.last_monitored_at = 10
        remoteClocks.build_endpoint_list()
        self.assertRaises(KeyError,
                          remoteClocks.offsets.__getitem__,
                          'false_0')
        self.assertRaises(KeyError,
                          remoteClocks.offsets.__getitem__,
                          'false_1')

    def test_findOffsetInterval(self):
        offsets = {
            '0': proto.RemoteOffset(offset=0, error=10, measured_at=11),
            '1': proto.RemoteOffset(offset=2, error=10, measured_at=12),
            'false_0': proto.RemoteOffset(offset=1, error=5, measured_at=0),
            'false_1': proto.RemoteOffset(offset=0, error=2, measured_at=9)
        }

        m = tt.ManualClock(0)
        clock = tt.Clock(m.get_clock)
        clock.SetMaxOffset(0)

        remoteClocks = ClockMonitor(clock)
        remoteClocks._replace_offsets(offsets)
        remoteClocks.last_monitored_at = 10

        expected = (-8, 10)
        actual, err = remoteClocks.find_offset_interval()
        self.assertTrue(err is None)
        self.assertEqual(actual, expected)

    def test_findOffsetIntervalNoMajorityIntersection(self):
        offsets = {
            '0': proto.RemoteOffset(offset=0, error=1),
            '1': proto.RemoteOffset(offset=3, error=1),
            '2': proto.RemoteOffset(offset=8, error=1),
            '3': proto.RemoteOffset(offset=9, error=1)
        }

        m = tt.ManualClock(0)
        clock = tt.Clock(m.get_clock)
        clock.SetMaxOffset(0)

        remoteClocks = ClockMonitor(clock)
        remoteClocks._replace_offsets(offsets)
        self.assertRaises(Exception, remoteClocks.find_offset_interval)

    def test_findOffsetIntervalNoSources(self):
        m = tt.ManualClock(0)
        clock = tt.Clock(m.get_clock)
        clock.SetMaxOffset(0)

        remoteClocks = ClockMonitor(clock)
        expected = (0, 0)
        actual, _ = remoteClocks.find_offset_interval()
        self.assertEqual(actual, expected)

    def test_findOffsetIntervalOneSource(self):
        offsets = {
            '0': proto.RemoteOffset(offset=0, error=15)
        }

        m = tt.ManualClock(0)
        clock = tt.Clock(m.get_clock)
        clock.SetMaxOffset(20)

        remoteClocks = ClockMonitor(clock)
        remoteClocks._replace_offsets(offsets)
        expected = (-35, 35)
        actual, err = remoteClocks.find_offset_interval()
        self.assertEqual(actual, expected)
        self.assertTrue(err is None)
