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

import time

from txstatsd.metrics.metric import Metric


class MeterMetric(Metric):
    """
    A simplier meter metric which measures instant throughput rate for each
    interval.
    """

    def __init__(self, connection, name, sample_rate=1):
        """Construct a metric that reports samples to the supplied
        C{connection}.

        @param connection: The connection endpoint representing
            the StatsD server.
        @param name: Indicates what is being instrumented.
        @param sample_rate: Restrict the number of samples sent
            to the StatsD server based on the supplied C{sample_rate}.
        """
        Metric.__init__(self, connection, name, sample_rate=sample_rate)

    def mark(self, value=1):
        """Mark the occurrence of a given number (C{value}) of events."""
        self.send("%s|m" % value)


class MeterMetricReporter(object):
    """
    A simplier meter metric which measures instant throughput rate for each
    interval.
    """

    def __init__(self, name, wall_time_func=time.time, prefix=""):
        """Construct a metric we expect to be periodically updated.

        @param name: Indicates what is being instrumented.
        @param wall_time_func: Function for obtaining wall time.
        """
        self.name = name
        self.wall_time_func = wall_time_func

        if prefix:
            prefix += "."
        self.prefix = prefix

        self.value = self.count = 0
        self.poll_time = self.wall_time_func()

    def mark(self, value):
        """
        Process new data for this metric.

        @type value: C{float}
        @param value: The reported value, to be aggregate into the meter.
        """
        self.value += value

    def report(self, timestamp):
        """
        Returns a list of metrics to report.

        @type timestamp: C{float}
        @param timestamp: The timestamp for now.
        """
        poll_prev, self.poll_time = self.poll_time, timestamp

        if self.poll_time == poll_prev:
            return list()

        rate = float(self.value) / (self.poll_time - poll_prev)
        self.count, self.value = self.count + self.value, 0

        metrics = []
        items = {
            ".count": self.count,
            ".rate": rate
            }

        for item, value in sorted(items.iteritems()):
            metrics.append((self.prefix + self.name + item,
                            round(value, 6), timestamp))
        return metrics
