
from txstatsd.metrics.gaugemetric import GaugeMetric
from txstatsd.metrics.metermetric import MeterMetric
from txstatsd.metrics.metric import Metric


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

    def gauge(self, name, value, sample_rate=1):
        """Report an instantaneous reading of a particular value."""
        name = self.fully_qualify_name(name)
        if not name in self._metrics:
            gauge_metric = GaugeMetric(self.connection,
                                       name,
                                       sample_rate)
            self._metrics[name] = gauge_metric
        self._metrics[name].mark(value)

    def meter(self, name, value, sample_rate=1):
        """Mark the occurrence of a given number of events."""
        name = self.fully_qualify_name(name)
        if not name in self._metrics:
            meter_metric = MeterMetric(self.connection,
                                       name,
                                       sample_rate)
            self._metrics[name] = meter_metric
        self._metrics[name].mark(value)

    def increment(self, name, value=1, sample_rate=1):
        """Report and increase in name by count."""
        name = self.fully_qualify_name(name)
        if not name in self._metrics:
            metric = Metric(self.connection,
                            name,
                            sample_rate)
            self._metrics[name] = metric
        self._metrics[name].send("%s|c" % value)

    def decrement(self, name, value=1, sample_rate=1):
        """Report and decrease in name by count."""
        name = self.fully_qualify_name(name)
        if not name in self._metrics:
            metric = Metric(self.connection,
                            name,
                            sample_rate)
            self._metrics[name] = metric
        self._metrics[name].send("%s|c" % -value)

    def timing(self, name, duration, sample_rate=1):
        """Report this sample performed in duration ms."""
        name = self.fully_qualify_name(name)
        if not name in self._metrics:
            metric = Metric(self.connection,
                            name,
                            sample_rate)
            self._metrics[name] = metric
        self._metrics[name].send("%s|ms" % duration)

    def clear(self, name):
        """Allow the metric to re-initialize its internal state."""
        name = self.fully_qualify_name(name)
        if name in self._metrics:
            metric = self._metrics[name]
            if getattr(metric, 'clear', None) is not None:
                metric.clear()

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
