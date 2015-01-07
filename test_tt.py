import unittest
from collections import namedtuple
from exc import AheadOfMaxOffsetError
import tt
from build import tt_pb2 as proto

SEND = 's'
RECV = 'r'


class ManualClock(object):

    def __init__(self, micros):
        self.micros = micros

    def set(self, micros):
        self.micros = micros

    def get_clock(self):
        return self.micros


ClockCase = namedtuple('ClockCase',
                       ['wall_time', 'event', 'input', 'expected'])


class TTTest(unittest.TestCase):

    def test_less(self):
        m = ManualClock(0)
        c = tt.Clock(m.get_clock)
        a = c.TS()
        b = c.TS()
        self.assertEqual(a, b)

        m.set(1)
        b = c.Now()
        self.assertLess(a, b)

        a = c.Now()  # tick logical clock from b by one
        self.assertGreater(a, b)

    def test_max_offset(self):
        m = ManualClock(123456)
        skewedTime = 123456 + 51
        c = tt.Clock(m.get_clock)
        self.assertTrue(c.max_offset == 0)

        c.SetMaxOffset(50)
        self.assertTrue(c.max_offset > 0)

        c.Now()
        self.assertEqual(c.TS().wall_time, m.micros)
        _, err = c.Update(proto.Timestamp(wall_time=skewedTime))
        self.assertTrue(not err is None)
        self.assertTrue(isinstance(err, AheadOfMaxOffsetError))

    def test_clock(self):
        m = ManualClock(0)
        c = tt.Clock(m.get_clock)
        c.SetMaxOffset(1000)
        expected_history = (
            ClockCase(5, SEND, None, proto.Timestamp(wall_time=5, logical=0)),
            ClockCase(6, SEND, None, proto.Timestamp(wall_time=6, logical=0)),
            ClockCase(10, RECV, proto.Timestamp(wall_time=10, logical=5), proto.Timestamp(wall_time=10, logical=6)),
            ClockCase(7, SEND, None, proto.Timestamp(wall_time=10, logical=7)),
            ClockCase(8, RECV, proto.Timestamp(wall_time=10, logical=4), proto.Timestamp(wall_time=10, logical=8)),
            # must be discarded
            ClockCase(9, RECV, proto.Timestamp(wall_time=1100, logical=888), proto.Timestamp(wall_time=10, logical=8)),
            ClockCase(10, RECV, proto.Timestamp(wall_time=10, logical=99), proto.Timestamp(wall_time=10, logical=100)))

        for i, step in enumerate(expected_history):
            m.set(step.wall_time)
            if step.event == SEND:
                current = c.Now()
            else:
                previous = c.TS()
                current, err = c.Update(step.input)
                self.assertFalse(current == previous and not err)
            self.assertEqual(current, step.expected)


