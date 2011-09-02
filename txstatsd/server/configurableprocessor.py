
from string import Template

import time

from txstatsd.metrics.metermetric import MeterMetricReporter
from txstatsd.server.processor import MessageProcessor


class ConfigurableMessageProcessor(MessageProcessor):
    """
    This specialised C{MessageProcessor} supports behaviour
    that is not StatsD-compliant.
    Currently, this extends to:
    - Allow a prefix to be added to the composed messages sent
      to the Graphite server.
    """

    # Notice: These messages replicate those seen in the
    # MessageProcessor (excepting the prefix identifier).
    # In a future release they will be placed in their
    # respective metric reporter class.
    # See MeterMetricReporter.
    COUNTERS_MESSAGE = (
        "$prefix%(key)s %(value)s %(timestamp)s\n"
        "$prefix%(key)s %(count)s %(timestamp)s\n")

    TIMERS_MESSAGE = (
        "$prefix%(key)s.mean %(mean)s %(timestamp)s\n"
        "$prefix%(key)s.upper %(upper)s %(timestamp)s\n"
        "$prefix%(key)s.upper_%(percent)s %(threshold_upper)s"
            " %(timestamp)s\n"
        "$prefix%(key)s.lower %(lower)s %(timestamp)s\n"
        "$prefix%(key)s.count %(count)s %(timestamp)s\n")

    GAUGE_METRIC_MESSAGE = (
        "$prefix%(key)s.value %(value)s %(timestamp)s\n")

    def __init__(self, time_function=time.time, message_prefix=""):
        super(ConfigurableMessageProcessor, self).__init__(time_function)

        self.message_prefix = message_prefix
        if message_prefix:
            message_prefix += '.'

        message = Template(ConfigurableMessageProcessor.COUNTERS_MESSAGE)
        self.counters_message = message.substitute(
            prefix=message_prefix)

        message = Template(ConfigurableMessageProcessor.TIMERS_MESSAGE)
        self.timers_message = message.substitute(
            prefix=message_prefix)

        message = Template(ConfigurableMessageProcessor.GAUGE_METRIC_MESSAGE)
        self.gauge_metric_message = message.substitute(
            prefix=message_prefix)

    def compose_meter_metric(self, key, value):
        if not key in self.meter_metrics:
            metric = MeterMetricReporter(key, self.time_function,
                                         prefix=self.message_prefix)
            self.meter_metrics[key] = metric
        self.meter_metrics[key].mark(value)
