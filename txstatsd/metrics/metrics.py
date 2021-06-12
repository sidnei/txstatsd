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
from txstatsd.metrics.gaugemetric import GaugeMetric
from txstatsd.metrics.metermetric import MeterMetric
from txstatsd.metrics.distinctmetric import DistinctMetric
from txstatsd.metrics.metric import Metric


class GenericMetric(Metric):
    def __init__(self, connection, key, name, tags):
        super(GenericMetric, self).__init__(connection, name, tags=tags)
        self.key = key

    def mark(self, value, extra=None):
        if extra is None:
            self.send("%s|%s" % (value, self.key))
        else:
            self.send("%s|%s|%s" % (value, self.key, extra))


class Metrics(object):
    def __init__(self, connection=None, namespace=""):
        """A convenience class for reporting metric samples
        to a StatsD server (C{connection}).

        @param connection: The connection endpoint representing
            the StatsD server.
        @param namespace: The top-level namespace identifying the
            origin of the samples.
        """

        self.connection = connection
        self.namespace = namespace
        self._metrics = {}
        self.last_time = 0

    def report(self, name, value, metric_type, extra=None, tags=None):
        """Report a generic metric.

        Used for server side plugins without client support.
        """
        key, name = self.key_and_fqn(name, tags)
        if key not in self._metrics:
            metric = GenericMetric(self.connection,
                                   metric_type,
                                   name,
                                   tags)
            self._metrics[key] = metric
        self._metrics[key].mark(value, extra)

    def sli(self, name, duration, size=None):
        """Report a service level metric.

        The optional size parameter is used with linear thereshold slis.
        So for example, to report a download you could use size and the size in
        bytes of the file.
        """
        self.report(name, duration, "sli", size)

    def sli_error(self, name):
        """Report an error for a service level metric.

        When something that is measures for service level errs (no time or size
        are required/present) you can use this method to inform it.
        """
        self.report(name, "error", "sli")

    def gauge(self, name, value, sample_rate=1, tags=None):
        """Report an instantaneous reading of a particular value."""
        key, name = self.key_and_fqn(name, tags)
        if key not in self._metrics:
            gauge_metric = GaugeMetric(self.connection,
                                       name,
                                       sample_rate,
                                       tags)
            self._metrics[key] = gauge_metric
        self._metrics[key].mark(value)

    def meter(self, name, value=1, sample_rate=1, tags=None):
        """Mark the occurrence of a given number of events."""
        key, name = self.key_and_fqn(name, tags)
        if key not in self._metrics:
            meter_metric = MeterMetric(self.connection,
                                       name,
                                       sample_rate,
                                       tags)
            self._metrics[key] = meter_metric
        self._metrics[key].mark(value)

    def increment(self, name, value=1, sample_rate=1, tags=None):
        """Report and increase in name by count."""
        key, name = self.key_and_fqn(name, tags)
        if key not in self._metrics:
            metric = Metric(self.connection,
                            name,
                            sample_rate,
                            tags)
            self._metrics[key] = metric
        self._metrics[key].send("%s|c" % value)

    def decrement(self, name, value=1, sample_rate=1, tags=None):
        """Report and decrease in name by count."""
        key, name = self.key_and_fqn(name, tags)
        if key not in self._metrics:
            metric = Metric(self.connection,
                            name,
                            sample_rate,
                            tags)
            self._metrics[key] = metric
        self._metrics[key].send("%s|c" % -value)

    def reset_timing(self):
        """Resets the duration timer for the next call to timing()"""
        self.last_time = time.time()

    def calculate_duration(self):
        """Resets the duration timer and returns the elapsed duration"""
        current_time = time.time()
        duration = current_time - self.last_time
        self.last_time = current_time
        return duration

    def timing(self, name, duration=None, sample_rate=1, tags=None):
        """Report that this sample performed in duration seconds.
           Default duration is the actual elapsed time since
           the last call to this method or reset_timing()"""
        if duration is None:
            duration = self.calculate_duration()
        key, name = self.key_and_fqn(name, tags)
        if key not in self._metrics:
            metric = Metric(self.connection,
                            name,
                            sample_rate,
                            tags)
            self._metrics[key] = metric
        self._metrics[key].send("%s|ms" % (duration * 1000))

    def distinct(self, name, item, tags=None):
        key, name = self.key_and_fqn(name, tags)
        if key not in self._metrics:
            metric = DistinctMetric(self.connection, name, tags)
            self._metrics[key] = metric
        self._metrics[key].mark(item)

    def clear(self, name, tags=None):
        """Allow the metric to re-initialize its internal state."""
        key, _ = self.key_and_fqn(name, tags)
        if key in self._metrics:
            metric = self._metrics[key]
            if getattr(metric, 'clear', None) is not None:
                metric.clear()

    def key_and_fqn(self, name, tags):
        """Return the cache key and fully-qualified name."""
        name = self.fully_qualify_name(name)
        key = name, tuple(sorted(tags)) if tags else None
        return key, name

    def fully_qualify_name(self, name):
        """Compose the fully-qualified name: namespace and name."""
        fully_qualified_name = ""
        if self.namespace is not None:
            fully_qualified_name = self.namespace
        if name is not None:
            # prepend the separator should we have a namespace
            if self.namespace is not None and len(self.namespace) > 0:
                fully_qualified_name += "." + name
            else:
                fully_qualified_name = name
        return fully_qualified_name
