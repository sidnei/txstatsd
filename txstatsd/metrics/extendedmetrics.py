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

from txstatsd.metrics.countermetric import CounterMetric
from txstatsd.metrics.timermetric import TimerMetric
from txstatsd.metrics.metrics import Metrics


class ExtendedMetrics(Metrics):

    def __init__(self, connection=None, namespace=""):
        """A convenience class for reporting metric samples
        to a C{txstatsd} server configured with the
        L{ConfigurableProcessor<txstatsd.server.configurableprocessor>}
        processor.

        @param connection: The connection endpoint representing
            the C{txstatsd} server.
        @param namespace: The top-level namespace identifying the
            origin of the samples.
        """

        super(ExtendedMetrics, self).__init__(connection, namespace)

    def increment(self, name, value=1, sample_rate=1):
        """Report and increase in name by count."""
        name = self.fully_qualify_name(name)
        if not name in self._metrics:
            metric = CounterMetric(self.connection,
                                   name,
                                   sample_rate)
            self._metrics[name] = metric
        self._metrics[name].increment(value)

    def decrement(self, name, value=1, sample_rate=1):
        """Report and decrease in name by count."""
        name = self.fully_qualify_name(name)
        if not name in self._metrics:
            metric = CounterMetric(self.connection,
                                   name,
                                   sample_rate)
            self._metrics[name] = metric
        self._metrics[name].decrement(value)

    def timing(self, name, duration=None, sample_rate=1):
        """Report this sample performed in duration seconds."""
        if duration is None:
            duration = self.calculate_duration()
        name = self.fully_qualify_name(name)
        if not name in self._metrics:
            metric = TimerMetric(self.connection,
                                 name,
                                 sample_rate)
            self._metrics[name] = metric
        self._metrics[name].mark(duration)

