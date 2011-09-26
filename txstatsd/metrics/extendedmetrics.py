
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

    def timing(self, name, duration, sample_rate=1):
        """Report this sample performed in duration ms."""
        name = self.fully_qualify_name(name)
        if not name in self._metrics:
            metric = TimerMetric(self.connection,
                                 name,
                                 sample_rate)
            self._metrics[name] = metric
        self._metrics[name].mark(duration)
