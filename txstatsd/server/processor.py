from collections import deque
import re
import time
import logging

from twisted.python import log

from txstatsd.metrics.metermetric import MeterMetricReporter


SPACES = re.compile("\s+")
SLASHES = re.compile("\/+")
NON_ALNUM = re.compile("[^a-zA-Z_\-0-9\.]")
RATE = re.compile("^@([\d\.]+)")


def normalize_key(key):
    """
    Normalize a key that might contain spaces, forward-slashes and other
    special characters into something that is acceptable by graphite.
    """
    key = SPACES.sub("_", key)
    key = SLASHES.sub("-", key)
    key = NON_ALNUM.sub("", key)
    return key


class MessageProcessor(object):
    """
    This C{MessageProcessor} produces StatsD-compliant messages
    for publishing to a Graphite server.
    Metrics behaviour that varies from StatsD should be placed in
    some specialised C{MessageProcessor} (see L{ConfigurableMessageProcessor
    <txstatsd.server.configurableprocessor.ConfigurableMessageProcessor>}).
    """

    COUNTERS_MESSAGE = (
        "stats.%(key)s %(value)s %(timestamp)s\n"
        "stats_counts.%(key)s %(count)s %(timestamp)s\n")

    TIMERS_MESSAGE = (
        "stats.timers.%(key)s.mean %(mean)s %(timestamp)s\n"
        "stats.timers.%(key)s.upper %(upper)s %(timestamp)s\n"
        "stats.timers.%(key)s.upper_%(percent)s %(threshold_upper)s"
            " %(timestamp)s\n"
        "stats.timers.%(key)s.lower %(lower)s %(timestamp)s\n"
        "stats.timers.%(key)s.count %(count)s %(timestamp)s\n")

    GAUGE_METRIC_MESSAGE = (
        "stats.gauge.%(key)s.value %(value)s %(timestamp)s\n")

    def __init__(self, time_function=time.time):
        self.time_function = time_function

        self.counters_message = MessageProcessor.COUNTERS_MESSAGE
        self.timers_message = MessageProcessor.TIMERS_MESSAGE
        self.gauge_metric_message = MessageProcessor.GAUGE_METRIC_MESSAGE

        self.timer_metrics = {}
        self.counter_metrics = {}
        self.gauge_metrics = deque()
        self.meter_metrics = {}

    def fail(self, message):
        """Log and discard malformed message."""
        log.msg("Bad line: %r" % message, logLevel=logging.DEBUG)

    def process(self, message):
        """
        Process a single entry, adding it to either C{counters}, C{timers},
        or C{gauge_metrics} depending on which kind of message it is.
        """
        if not ":" in message:
            return self.fail(message)

        key, data = message.strip().split(":", 1)
        if not "|" in data:
            return self.fail(message)

        fields = data.split("|")
        if len(fields) < 2 or len(fields) > 3:
            return self.fail(message)

        key = normalize_key(key)

        if fields[1] == "c":
            self.process_counter_metric(key, fields, message)
        elif fields[1] == "ms":
            self.process_timer_metric(key, fields[0], message)
        elif fields[1] == "g":
            self.process_gauge_metric(key, fields[0], message)
        elif fields[1] == "m":
            self.process_meter_metric(key, fields[0], message)
        else:
            return self.fail(message)

    def process_timer_metric(self, key, duration, message):
        try:
            duration = float(duration)
        except (TypeError, ValueError):
            return self.fail(message)

        self.compose_timer_metric(key, duration)

    def compose_timer_metric(self, key, duration):
        if key not in self.timer_metrics:
            self.timer_metrics[key] = []
        self.timer_metrics[key].append(duration)

    def process_counter_metric(self, key, composite, message):
        try:
            value = float(composite[0])
        except (TypeError, ValueError):
            return self.fail(message)
        rate = 1
        if len(composite) == 3:
            match = RATE.match(composite[2])
            if match is None:
                return self.fail(message)
            rate = match.group(1)

        self.compose_counter_metric(key, value, rate)

    def compose_counter_metric(self, key, value, rate):
        if key not in self.counter_metrics:
            self.counter_metrics[key] = 0
        self.counter_metrics[key] += value * (1 / float(rate))

    def process_gauge_metric(self, key, composite, message):
        values = composite.split(":")
        if not len(values) == 1:
            return self.fail(message)

        try:
            value = float(values[0])
        except (TypeError, ValueError):
            self.fail(message)

        self.compose_gauge_metric(key, value)

    def compose_gauge_metric(self, key, value):
        metric = [value, key]
        self.gauge_metrics.append(metric)

    def process_meter_metric(self, key, composite, message):
        values = composite.split(":")
        if not len(values) == 1:
            return self.fail(message)

        try:
            value = float(values[0])
        except (TypeError, ValueError):
            self.fail(message)

        self.compose_meter_metric(key, value)

    def compose_meter_metric(self, key, value):
        if not key in self.meter_metrics:
            metric = MeterMetricReporter(key, self.time_function,
                                         prefix="stats.meter")
            self.meter_metrics[key] = metric
        self.meter_metrics[key].mark(value)

    def flush(self, interval=10000, percent=90):
        """
        Flush all queued stats, computing a normalized count based on
        C{interval} and mean timings based on C{threshold}.
        """
        messages = []
        num_stats = 0
        interval = interval / 1000
        timestamp = int(self.time_function())

        counter_metrics, events = self.flush_counter_metrics(interval,
                                                             timestamp)
        if events > 0:
            messages.extend(counter_metrics)
            num_stats += events

        timer_metrics, events = self.flush_timer_metrics(percent, timestamp)
        if events > 0:
            messages.extend(timer_metrics)
            num_stats += events

        gauge_metrics, events = self.flush_gauge_metrics(timestamp)
        if events > 0:
            messages.extend(gauge_metrics)
            num_stats += events

        meter_metrics, events = self.flush_meter_metrics(timestamp)
        if events > 0:
            messages.extend(meter_metrics)
            num_stats += events

        self.flush_metrics_summary(messages, num_stats, timestamp)
        return messages

    def flush_counter_metrics(self, interval, timestamp):
        metrics = []
        events = 0
        for key, count in self.counter_metrics.iteritems():
            self.counter_metrics[key] = 0

            value = count / interval
            message = self.counters_message % {
                "key": key,
                "value": value,
                "count": count,
                "timestamp": timestamp}
            metrics.append(message)
            events += 1

        return (metrics, events)

    def flush_timer_metrics(self, percent, timestamp):
        metrics = []
        events = 0

        threshold_value = ((100 - percent) / 100.0)
        for key, timers in self.timer_metrics.iteritems():
            count = len(timers)
            if count > 0:
                self.timer_metrics[key] = []

                timers.sort()
                lower = timers[0]
                upper = timers[-1]
                count = len(timers)

                mean = lower
                threshold_upper = upper

                if count > 1:
                    index = count - int(round(threshold_value * count))
                    timers = timers[:index]
                    threshold_upper = timers[-1]
                    mean = sum(timers) / index

                message = self.timers_message % {
                    "key": key,
                    "mean": mean,
                    "upper": upper,
                    "percent": percent,
                    "threshold_upper": threshold_upper,
                    "lower": lower,
                    "count": count,
                    "timestamp": timestamp}
                metrics.append(message)
                events += 1

        return (metrics, events)

    def flush_gauge_metrics(self, timestamp):
        metrics = []
        events = 0
        for metric in self.gauge_metrics:
            value = metric[0]
            key = metric[1]

            message = self.gauge_metric_message % {
                "key": key,
                "value": value,
                "timestamp": timestamp}
            metrics.append(message)
            events += 1

        self.gauge_metrics.clear()

        return (metrics, events)

    def flush_meter_metrics(self, timestamp):
        metrics = []
        events = 0
        for metric in self.meter_metrics.itervalues():
            message = metric.report(timestamp)
            metrics.append(message)
            events += 1

        return (metrics, events)

    def flush_metrics_summary(self, messages, num_stats, timestamp):
        messages.append("statsd.numStats %s %s\n" % (num_stats, timestamp))

    def update_metrics(self):
        for metric in self.meter_metrics.itervalues():
            metric.tick()
