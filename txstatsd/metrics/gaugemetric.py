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

from txstatsd.metrics.metric import Metric


class GaugeMetric(Metric):
    """A gauge metric is an instantaneous reading of a particular value."""

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

    def mark(self, value):
        """Report the C{value} for this gauge."""
        self.send("%s|g" % value)


class GaugeMetricReporter(object):
    """A gauge metric is an instantaneous reading of a particular value."""

    def __init__(self, name, prefix=""):
        """Construct a metric we expect to be periodically updated.

        @param name: Indicates what is being instrumented.
        """
        self.name = name

        if prefix:
            prefix += "."
        self.prefix = prefix
        self.value = 0

    def mark(self, value):
        self.value = value

    def report(self, timestamp):
        return [(self.prefix + self.name + ".value", self.value, timestamp)]
