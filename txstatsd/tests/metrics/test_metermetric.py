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
