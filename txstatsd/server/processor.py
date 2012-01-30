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


class BaseMessageProcessor(object):

    def process(self, message):
        """
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
        metric_type = fields[1]
        return self.process_message(message, metric_type, key, fields)

    def rebuild_message(self, metric_type, key, fields):
        return key + ":" + "|".join(fields)

    def fail(self, message):
        """Log and discard malformed message."""
        log.msg("Bad line: %r" % message, logLevel=logging.DEBUG)


class MessageProcessor(BaseMessageProcessor):
    """
    This C{MessageProcessor} produces StatsD-compliant messages
    for publishing to a Graphite server.
    Metrics behaviour that varies from StatsD should be placed in
    some specialised C{MessageProcessor} (see L{ConfigurableMessageProcessor
    <txstatsd.server.configurableprocessor.ConfigurableMessageProcessor>}).
    """

    def __init__(self, time_function=time.time, plugins=None):
        self.time_function = time_function

        self.stats_prefix = "stats."
        self.internal_metrics_prefix = "statsd."
        self.count_prefix = "stats_counts."
        self.timer_prefix = self.stats_prefix + "timers."
        self.gauge_prefix = self.stats_prefix + "gauge."

        self.process_timings = {}
        self.by_type = {}
        self.timer_metrics = {}
        self.counter_metrics = {}
        self.gauge_metrics = deque()
        self.meter_metrics = {}
        self.distinct_metrics = {}

        self.plugins = {}
        self.plugin_metrics = {}

        if plugins is not None:
            for plugin in plugins:
                self.plugins[plugin.metric_type] = plugin

    def process_message(self, message, metric_type, key, fields):
        """
        Process a single entry, adding it to either C{counters}, C{timers},
        or C{gauge_metrics} depending on which kind of message it is.
        """
        start = self.time_function()
        if metric_type == "c":
            self.process_counter_metric(key, fields, message)
        elif metric_type == "ms":
            self.process_timer_metric(key, fields[0], message)
        elif metric_type == "g":
            self.process_gauge_metric(key, fields[0], message)
        elif metric_type == "m":
            self.process_meter_metric(key, fields[0], message)
        elif metric_type in self.plugins:
            self.process_plugin_metric(metric_type, key, fields, message)
        else:
            return self.fail(message)
        self.process_timings.setdefault(metric_type, 0)
        self.process_timings[metric_type] += self.time_function() - start
        self.by_type.setdefault(metric_type, 0)
        self.by_type[metric_type] += 1

    def get_message_prefix(self, kind):
        return "stats." + kind

    def process_plugin_metric(self, metric_type, key, items, message):
        if not key in self.plugin_metrics:
            factory = self.plugins[metric_type]
            metric = factory.build_metric(
                self.get_message_prefix(factory.name),
                name=key, wall_time_func=self.time_function)
            self.plugin_metrics[key] = metric
        self.plugin_metrics[key].process(items)

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
        per_metric = {}
        num_stats = 0
        interval = interval / 1000
        timestamp = int(self.time_function())

        start = self.time_function()
        counter_metrics, events = self.flush_counter_metrics(interval,
                                                             timestamp)
        duration = self.time_function() - start
        if events > 0:
            messages.extend(sorted(counter_metrics))
            num_stats += events
        per_metric["counter"] = (events, duration)

        start = self.time_function()
        timer_metrics, events = self.flush_timer_metrics(percent, timestamp)
        duration = self.time_function() - start
        if events > 0:
            messages.extend(sorted(timer_metrics))
            num_stats += events
        per_metric["timer"] = (events, duration)

        start = self.time_function()
        gauge_metrics, events = self.flush_gauge_metrics(timestamp)
        duration = self.time_function() - start
        if events > 0:
            messages.extend(sorted(gauge_metrics))
            num_stats += events
        per_metric["gauge"] = (events, duration)

        start = self.time_function()
        meter_metrics, events = self.flush_meter_metrics(timestamp)
        duration = self.time_function() - start
        if events > 0:
            messages.extend(sorted(meter_metrics))
            num_stats += events
        per_metric["meter"] = (events, duration)

        start = self.time_function()
        plugin_metrics, events = self.flush_plugin_metrics(interval, timestamp)
        duration = self.time_function() - start
        if events > 0:
            messages.extend(sorted(plugin_metrics))
            num_stats += events
        per_metric["plugin"] = (events, duration)

        self.flush_metrics_summary(messages, num_stats, per_metric, timestamp)
        return messages

    def flush_counter_metrics(self, interval, timestamp):
        metrics = []
        events = 0
        for key, count in self.counter_metrics.iteritems():
            self.counter_metrics[key] = 0

            value = count / interval
            metrics.append((self.stats_prefix + key, value, timestamp))
            metrics.append((self.count_prefix + key, count, timestamp))
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

                items = {".mean": mean,
                         ".upper": upper,
                         ".upper_%s" % percent: threshold_upper,
                         ".lower": lower,
                         ".count": count}
                for item, value in items.iteritems():
                    metrics.append((self.timer_prefix + key + item,
                                    value, timestamp))
                events += 1

        return (metrics, events)

    def flush_gauge_metrics(self, timestamp):
        metrics = []
        events = 0
        for metric in self.gauge_metrics:
            value = metric[0]
            key = metric[1]

            metrics.append((self.gauge_prefix + key + ".value",
                            value, timestamp))
            events += 1

        self.gauge_metrics.clear()

        return (metrics, events)

    def flush_meter_metrics(self, timestamp):
        metrics = []
        events = 0
        for metric in self.meter_metrics.itervalues():
            messages = metric.report(timestamp)
            metrics.extend(messages)
            events += 1

        return (metrics, events)

    def flush_plugin_metrics(self, interval, timestamp):
        metrics = []
        events = 0

        for metric in self.plugin_metrics.itervalues():
            messages = metric.flush(interval, timestamp)
            metrics.extend(messages)
            events += 1

        return (metrics, events)

    def flush_metrics_summary(self, messages, num_stats,
                              per_metric, timestamp):
        messages.append((self.internal_metrics_prefix + "numStats",
                         num_stats, timestamp))
        for name, (value, duration) in per_metric.iteritems():
            messages.extend([
                (self.internal_metrics_prefix +
                 "flush.%s.count" % name,
                 value, timestamp),
                (self.internal_metrics_prefix +
                 "flush.%s.duration" % name,
                 duration * 1000, timestamp)])
            log.msg("Flushed %d %s metrics in %.6f" %
                    (value, name, duration))
        for metric_type, duration in self.process_timings.iteritems():
            messages.extend([
                (self.internal_metrics_prefix +
                 "receive.%s.count" %
                 metric_type, self.by_type[metric_type], timestamp),
                (self.internal_metrics_prefix +
                 "receive.%s.duration" %
                 metric_type, duration * 1000, timestamp)
                ])
            log.msg("Processing %d %s metrics took %.6f" %
                    (self.by_type[metric_type], metric_type, duration))
        self.process_timings.clear()
        self.by_type.clear()

    def update_metrics(self):
        for metric in self.meter_metrics.itervalues():
            metric.tick()
