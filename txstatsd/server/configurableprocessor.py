
import time

from txstatsd.metrics.countermetric import CounterMetricReporter
from txstatsd.metrics.gaugemetric import GaugeMetricReporter
from txstatsd.metrics.metermetric import MeterMetricReporter
from txstatsd.metrics.timermetric import TimerMetricReporter
from txstatsd.server.processor import MessageProcessor


class ConfigurableMessageProcessor(MessageProcessor):
    """
    This specialised C{MessageProcessor} supports behaviour
    that is not StatsD-compliant.

    Currently, this extends to:
    - Allow a prefix to be added to the composed messages sent
      to the Graphite server.
    - Report an instantaneous reading of a particular value.
    - Report an incrementing and decrementing counter metric.
    - Report a timer metric which aggregates timing durations and provides
      duration statistics, plus throughput statistics.
    """

    def __init__(self, time_function=time.time, message_prefix=""):
        super(ConfigurableMessageProcessor, self).__init__(
            time_function=time_function)

        self.message_prefix = message_prefix
        self.gauge_metrics = {}

    def compose_timer_metric(self, key, duration):
        if not key in self.timer_metrics:
            metric = TimerMetricReporter(key, prefix=self.message_prefix)
            self.timer_metrics[key] = metric
        self.timer_metrics[key].update(duration)

    def process_counter_metric(self, key, composite, message):
        try:
            value = float(composite[0])
        except (TypeError, ValueError):
            return self.fail(message)

        self.compose_counter_metric(key, value)

    def compose_counter_metric(self, key, value):
        if not key in self.counter_metrics:
            metric = CounterMetricReporter(key, prefix=self.message_prefix)
            self.counter_metrics[key] = metric
        self.counter_metrics[key].mark(value)

    def compose_gauge_metric(self, key, value):
        if not key in self.gauge_metrics:
            metric = GaugeMetricReporter(key, prefix=self.message_prefix)
            self.gauge_metrics[key] = metric
        self.gauge_metrics[key].mark(value)

    def compose_meter_metric(self, key, value):
        if not key in self.meter_metrics:
            metric = MeterMetricReporter(key, self.time_function,
                                         prefix=self.message_prefix)
            self.meter_metrics[key] = metric
        self.meter_metrics[key].mark(value)

    def flush_counter_metrics(self, interval, timestamp):
        metrics = []
        events = 0
        for metric in self.counter_metrics.itervalues():
            message = metric.report(timestamp)
            metrics.append(message)
            events += 1

        return (metrics, events)

    def flush_gauge_metrics(self, timestamp):
        metrics = []
        events = 0
        for metric in self.gauge_metrics.itervalues():
            message = metric.report(timestamp)
            metrics.append(message)
            events += 1

        return (metrics, events)

    def flush_timer_metrics(self, percent, timestamp):
        metrics = []
        events = 0
        for metric in self.timer_metrics.itervalues():
            message = metric.report(timestamp)
            metrics.append(message)
            events += 1

        return (metrics, events)

    def update_metrics(self):
        super(ConfigurableMessageProcessor, self).update_metrics()

        for metric in self.timer_metrics.itervalues():
            metric.tick()
