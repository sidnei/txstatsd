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
from unittest import TestCase

from txstatsd.metrics.histogrammetric import HistogramMetricReporter
from txstatsd.stats.uniformsample import UniformSample


class TestHistogramReporterMetric(TestCase):

    def test_histogram_with_zero_recorded_values(self):
        sample = UniformSample(100)
        histogram = HistogramMetricReporter(sample)

        self.assertEqual(histogram.count, 0, 'Should have a count of 0')
        self.assertEqual(histogram.max(), 0,
                         'Should have a max of 0')
        self.assertEqual(histogram.min(), 0,
                         'Should have a min of 0')

        self.assertEqual(histogram.mean(), 0,
                         'Should have a mean of 0')

        self.assertEqual(histogram.std_dev(), 0,
                         'Should have a standard deviation of 0')

        percentiles = histogram.percentiles(0.5, 0.75, 0.99)
        self.assertTrue(
            (math.fabs(percentiles[0] - 0) < 0.01),
            'Should calculate percentiles')
        self.assertTrue(
            (math.fabs(percentiles[1] - 0) < 0.01),
            'Should calculate percentiles')
        self.assertTrue(
            (math.fabs(percentiles[2] - 0) < 0.01),
            'Should calculate percentiles')

        self.assertEqual(len(histogram.get_values()), 0,
                         'Should have no values')

    def test_histogram_of_numbers_1_through_10000(self):
        sample = UniformSample(100000)
        histogram = HistogramMetricReporter(sample)
        for i in range(1, 10001):
            histogram.update(i)

        self.assertEqual(histogram.count, 10000,
                         'Should have a count of 10000')

        self.assertEqual(histogram.max(), 10000,
                         'Should have a max of 10000')
        self.assertEqual(histogram.min(), 1,
                         'Should have a min of 1')

        self.assertTrue(
            (math.fabs(histogram.mean() - 5000.5) < 0.01),
            'Should have a mean value of 5000.5')

        self.assertTrue(
            (math.fabs(histogram.std_dev() - 2886.89) < 0.01),
            'Should have a standard deviation of X')

        percentiles = histogram.percentiles(0.5, 0.75, 0.99)
        self.assertTrue(
            (math.fabs(percentiles[0] - 5000.5) < 0.01),
            'Should calculate percentiles')
        self.assertTrue(
            (math.fabs(percentiles[1] - 7500.75) < 0.01),
            'Should calculate percentiles')
        self.assertTrue(
            (math.fabs(percentiles[2] - 9900.99) < 0.01),
            'Should calculate percentiles')

        values = [i for i in range(1, 10001)]
        self.assertEqual(histogram.get_values(), values,
                         'Should have 10000 values')

    def test_histogram_histogram(self):
        sample = UniformSample(100000)
        histogram = HistogramMetricReporter(sample)
        for i in range(1001, 11001):
            histogram.update(i)

        hist = histogram.histogram()
        self.assertEquals(sum(hist), 10000)

        total = sum(hist)
        binsize = int(total / len(hist))
        for i in hist:
            self.assertTrue(abs(i - binsize) <= 1)

