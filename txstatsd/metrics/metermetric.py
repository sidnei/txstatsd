
import time

from txstatsd.metrics.metric import Metric
from txstatsd.stats.ewma import Ewma


class MeterMetric(Metric):
    """
    A meter metric which measures mean throughput and one-, five-, and
    fifteen-minute exponentially-weighted moving average throughputs.

    See:
    - U{EMA
    <http://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average>}
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
    A meter metric which measures mean throughput and one-, five-, and
    fifteen-minute exponentially-weighted moving average throughputs.

    See:
    - U{EMA
    <http://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average>}
    """

    MESSAGE = (
        "stats.meter.%(key)s.count %(count)s %(timestamp)s\n"
        "stats.meter.%(key)s.mean_rate %(mean_rate)s %(timestamp)s\n"
        "stats.meter.%(key)s.1min_rate %(rate_1min)s %(timestamp)s\n"
        "stats.meter.%(key)s.5min_rate %(rate_5min)s %(timestamp)s\n"
        "stats.meter.%(key)s.15min_rate %(rate_15min)s %(timestamp)s\n")

    def __init__(self, name, wall_time_func=time.time):
        """Construct a metric we expect to be periodically updated.

        @param name: Indicates what is being instrumented.
        @param wall_time_func: Function for obtaining wall time.
        """
        self.name = name
        self.wall_time_func = wall_time_func

        self.m1_rate = Ewma.one_minute_ewma()
        self.m5_rate = Ewma.five_minute_ewma()
        self.m15_rate = Ewma.fifteen_minute_ewma()
        self.count = 0
        self.start_time = self.wall_time_func()

    def mark(self, value=1):
        """Mark the occurrence of a given number of events."""
        self.count += value
        self.m1_rate.update(value)
        self.m5_rate.update(value)
        self.m15_rate.update(value)

    def tick(self):
        """Updates the moving averages."""
        self.m1_rate.tick()
        self.m5_rate.tick()
        self.m15_rate.tick()

    def report(self, timestamp):
        return MeterMetricReporter.MESSAGE % {
            "key": self.name,
            "count": self.count,
            "mean_rate": self.mean_rate(),
            "rate_1min": self.one_minute_rate(),
            "rate_5min": self.five_minute_rate(),
            "rate_15min": self.fifteen_minute_rate(),
            "timestamp": timestamp}

    def fifteen_minute_rate(self):
        return self.m15_rate.rate

    def five_minute_rate(self):
        return self.m5_rate.rate

    def one_minute_rate(self):
        return self.m1_rate.rate

    def mean_rate(self):
        if self.count == 0:
            return 0.0
        else:
            elapsed = self.wall_time_func() - self.start_time
            return float(self.count) / elapsed
