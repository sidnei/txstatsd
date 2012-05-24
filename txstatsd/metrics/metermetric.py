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
        poll_prev, self.poll_time = self.poll_time, self.wall_time_func()

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
