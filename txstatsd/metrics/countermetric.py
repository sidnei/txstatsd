import math

from txstatsd.metrics.metric import Metric


class CounterMetric(Metric):
    """An incrementing and decrementing counter metric."""

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

        self._count = 0

    def increment(self, value):
        """Increment the counter by C{value}"""
        self._count += value
        self._update(self._count)

    def decrement(self, value):
        """Decrement the counter by C{value}"""
        self._count -= value
        self._update(self._count)

    def count(self):
        """Returns the counter's current value."""
        return self._count

    def clear(self):
        """Resets the counter to 0."""
        self._count = 0
        self._update(self._count)

    def _update(self, value):
        """Report the counter."""
        self.send("%s|c" % value)


class CounterMetricReporter(object):
    """An incrementing and decrementing counter metric."""

    def __init__(self, name, prefix=""):
        """Construct a metric we expect to be periodically updated.

        @param name: Indicates what is being instrumented.
        """
        self.name = name

        if prefix:
            prefix += "."
        self.prefix = prefix
        self.count = 0

    def mark(self, value):
        self.count = value

    def report(self, timestamp):
        return [(self.prefix + self.name + ".count",
                 math.trunc(self.count), timestamp)]
