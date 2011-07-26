
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
