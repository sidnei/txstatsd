
import random


class Metric(object):
    """
    The foundation metric from which the specialized
    metrics are derived.
    """

    def __init__(self, connection, name, sample_rate=1):
        """Construct a metric that reports samples to the supplied
        C{connection}.

        @param connection: The connection endpoint representing
            the StatsD server.
        @param name: Specific description for this metric.
        @param sample_rate: Restrict the number of samples sent
            to the StatsD server based on the supplied C{sample_rate}.
        """
        self.connection = connection
        self.name = name
        self.sample_rate = sample_rate

    def clear(self):
        """Responsibility of the specialized metrics."""
        pass

    def send(self, data):
        """
        Message the C{data} to the C{StatsD} server according to the
        C{sample_rate}.
        """

        if self.sample_rate < 1:
            if random.random() > self.sample_rate:
                return
            data += "|@%s" % (self.sample_rate,)

        data = self.name + ":" + data

        self.write(data)

    def write(self, data):
        """Message the C{data} to the C{StatsD} server."""
        if self.connection is not None:
            self.connection.write(data)
