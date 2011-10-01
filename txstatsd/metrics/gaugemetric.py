
from string import Template

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

    MESSAGE = (
        "$prefix%(key)s.value %(value)s %(timestamp)s\n")

    def __init__(self, name, prefix=""):
        """Construct a metric we expect to be periodically updated.

        @param name: Indicates what is being instrumented.
        """
        self.name = name

        if prefix:
            prefix += '.'
        self.message = Template(GaugeMetricReporter.MESSAGE).substitute(
            prefix=prefix)

        self.value = 0

    def mark(self, value):
        self.value = value

    def report(self, timestamp):
        return self.message % {
            "key": self.name,
            "value": self.value,
            "timestamp": timestamp}
