# Copyright (C) 2011-2012 Canonical Services Ltd
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

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

    def __init__(self, time_function=time.time, message_prefix="",
                 internal_metrics_prefix="", plugins=None):
        super(ConfigurableMessageProcessor, self).__init__(
            time_function=time_function, plugins=plugins)

        if not internal_metrics_prefix and not message_prefix:
            internal_metrics_prefix = "statsd."
        elif message_prefix and not internal_metrics_prefix:
            internal_metrics_prefix = message_prefix + "." + "statsd."
        self.internal_metrics_prefix = internal_metrics_prefix
        self.message_prefix = message_prefix
        self.gauge_metrics = {}

    def get_message_prefix(self, kind):
        return self.message_prefix

    def compose_timer_metric(self, key, duration):
        if not key in self.timer_metrics:
            metric = TimerMetricReporter(key,
                wall_time_func=self.time_function, prefix=self.message_prefix)
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
            messages = metric.report(timestamp)
            metrics.extend(messages)
            events += 1

        return (metrics, events)

    def flush_gauge_metrics(self, timestamp):
        metrics = []
        events = 0
        for metric in self.gauge_metrics.itervalues():
            messages = metric.report(timestamp)
            metrics.extend(messages)
            events += 1

        return (metrics, events)

    def flush_timer_metrics(self, percent, timestamp):
        metrics = []
        events = 0
        for metric in self.timer_metrics.itervalues():
            messages = metric.report(timestamp)
            metrics.extend(messages)
            events += 1

        return (metrics, events)
