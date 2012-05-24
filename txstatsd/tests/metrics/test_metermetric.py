import random
from twisted.trial.unittest import TestCase

from txstatsd.metrics.metermetric import MeterMetricReporter


class TestDeriveMetricReporter(TestCase):

    def test_fastpoll(self):
        wall_time = 42
        reporter = MeterMetricReporter(
            "test", wall_time_func=lambda: wall_time)

        self.assertEquals([], reporter.report(wall_time))

    def test_interface(self):
        random.seed(1)

        wall_time = [0]
        reporter = MeterMetricReporter("test", prefix="some.prefix",
                                       wall_time_func=lambda: wall_time[0])
        reporter.mark(42)
        reporter.mark(60)
        reporter.mark(38)
        wall_time = [10]

        reported = reporter.report(10)
        self.assertEqual(2, len(reported))
        self.assertEqual(140, reported[0][1])
        self.assertEqual(14, reported[1][1])
        self.assertEquals(
            ['some.prefix.test.count', 'some.prefix.test.rate'],
            [reported[0][0], reported[1][0]])
