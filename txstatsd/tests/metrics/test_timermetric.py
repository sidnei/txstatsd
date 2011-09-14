
import math

from twisted.trial.unittest import TestCase

from txstatsd.metrics.timermetric import TimerMetricReporter


class TestBlankTimerMetric(TestCase):
    def setUp(self):
        self.timer = TimerMetricReporter('test')
        self.timer.tick()

    def test_max(self):
        self.assertEqual(
            self.timer.max(), 0,
            'Should have a max of zero')

    def test_min(self):
        self.assertEqual(
            self.timer.min(), 0,
            'Should have a min of zero')

    def test_mean(self):
        self.assertEqual(
            self.timer.max(), 0,
            'Should have a mean of zero')

    def test_count(self):
        self.assertEqual(
            self.timer.count(), 0,
            'Should have a count of zero')

    def test_std_dev(self):
        self.assertEqual(
            self.timer.std_dev(), 0,
            'Should have a standard deviation of zero')

    def test_percentiles(self):
        percentiles = self.timer.percentiles(0.5, 0.95, 0.98, 0.99, 0.999)
        self.assertEqual(
            percentiles[0], 0,
            'Should have median of zero')
        self.assertEqual(
            percentiles[1], 0,
            'Should have p95 of zero')
        self.assertEqual(
            percentiles[2], 0,
            'Should have p98 of zero')
        self.assertEqual(
            percentiles[3], 0,
            'Should have p99 of zero')
        self.assertEqual(
            percentiles[4], 0,
            'Should have p99.9 of zero')

    def test_mean_rate(self):
        self.assertEqual(
            self.timer.mean_rate(), 0,
            'Should have a mean rate of zero')

    def test_one_minute_rate(self):
        self.assertEqual(
            self.timer.one_minute_rate(), 0,
            'Should have a one-minute rate of zero`')

    def test_five_minute_rate(self):
        self.assertEqual(
            self.timer.five_minute_rate(), 0,
            'Should have a five-minute rate of zero')

    def test_fifteen_minute_rate(self):
        self.assertEqual(
            self.timer.fifteen_minute_rate(), 0,
            'Should have a fifteen-minute rate of zero')

    def test_no_values(self):
        self.assertEqual(
            len(self.timer.get_values()), 0,
            'Should have no values')


class TestTimingSeriesEvents(TestCase):
    def setUp(self):
        self.timer = TimerMetricReporter('test')
        self.timer.tick()
        self.timer.update(10)
        self.timer.update(20)
        self.timer.update(20)
        self.timer.update(30)
        self.timer.update(40)

    def test_count(self):
        self.assertEqual(
            self.timer.count(), 5,
            'Should record the count')

    def test_min(self):
        self.assertTrue(
            (math.fabs(self.timer.min() - 10.0) < 0.001),
            'Should calculate the minimum duration')

    def test_max(self):
        self.assertTrue(
            (math.fabs(self.timer.max() - 40.0) < 0.001),
            'Should calculate the maximum duration')

    def test_mean(self):
        self.assertTrue(
            (math.fabs(self.timer.mean() - 24.0) < 0.001),
            'Should calculate the mean duration')

    def test_std_dev(self):
        self.assertTrue(
            (math.fabs(self.timer.std_dev() - 11.401) < 0.001),
            'Should calculate the standard deviation')

    def test_percentiles(self):
        percentiles = self.timer.percentiles(0.5, 0.95, 0.98, 0.99, 0.999)
        self.assertTrue(
            (math.fabs(percentiles[0] - 20.0) < 0.001),
            'Should calculate the median')
        self.assertTrue(
            (math.fabs(percentiles[1] - 40.0) < 0.001),
            'Should calculate the p95')
        self.assertTrue(
            (math.fabs(percentiles[2] - 40.0) < 0.001),
            'Should calculate the p98')
        self.assertTrue(
            (math.fabs(percentiles[3] - 40.0) < 0.001),
            'Should calculate the p99')
        self.assertTrue(
            (math.fabs(percentiles[4] - 40.0) < 0.001),
            'Should calculate the p999')

    def test_values(self):
        self.assertEqual(
            set(self.timer.get_values()), set([10, 20, 20, 30, 40]),
            'Should have a series of values')
