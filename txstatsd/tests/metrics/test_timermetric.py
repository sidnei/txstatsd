# Copyright (C) 2011-2012 Canonical Services Ltd
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import math
import time

from twisted.trial.unittest import TestCase

from txstatsd.metrics.timermetric import TimerMetricReporter


class TestBlankTimerMetric(TestCase):
    def setUp(self):
        self.timer = TimerMetricReporter('test')

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
            self.timer.count, 0,
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

    def test_rate(self):
        self.assertEqual(
            self.timer.rate(time.time()), 0,
            'Should have a one-minute rate of zero`')

    def test_no_values(self):
        self.assertEqual(
            len(self.timer.get_values()), 0,
            'Should have no values')


class TestTimingSeriesEvents(TestCase):
    def setUp(self):
        self.timer = TimerMetricReporter('test')
        self.timer.update(10)
        self.timer.update(20)
        self.timer.update(20)
        self.timer.update(30)
        self.timer.update(40)

    def test_count(self):
        self.assertEqual(
            self.timer.count, 5,
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
