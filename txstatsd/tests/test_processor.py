import time

from twisted.plugin import getPlugins
from twisted.trial.unittest import TestCase

from txstatsd.server.processor import MessageProcessor
from txstatsd.itxstatsd import IMetricFactory


class Timer(object):

    def __init__(self, times=None):
        if times is None:
            times = []
        self.times = times

    def set(self, times):
        self.times = times

    def __call__(self):
        return self.times.pop(0)


class TestMessageProcessor(MessageProcessor):

    def __init__(self):
        super(TestMessageProcessor, self).__init__(
            plugins=getPlugins(IMetricFactory))
        self.failures = []

    def fail(self, message):
        self.failures.append(message)


class ProcessMessagesTest(TestCase):

    def setUp(self):
        self.processor = TestMessageProcessor()

    def test_rebuild_message(self):
        self.assertEquals(
            self.processor.rebuild_message("c", "gorets", ["1", "c"]),
            "gorets:1|c")

    def test_metric_names(self):
        """We return the names of all seen metrics."""
        kinds = set(["ms", "c", "g", "pd"])
        for kind in kinds:
            self.processor.process("%s:1|%s" % (kind, kind))
        self.assertEquals(kinds, set(self.processor.get_metric_names()))

    def test_receive_counter(self):
        """
        A counter message takes the format 'gorets:1|c', where 'gorets' is the
        identifier and '1' means the counter will be incremented by one
        unit. 'c' is simply used to signal that this is a counter message.
        """
        self.processor.process("gorets:1|c")
        self.assertEqual(1, len(self.processor.counter_metrics))
        self.assertEqual(1.0, self.processor.counter_metrics["gorets"])

    def test_receive_counter_rate(self):
        """
        A counter message can also take the format 'gorets:1|c|@01', where
        'gorets' is the identifier,'1' means the counter will be incremented by
        one unit and '@0.1' means the sample rate is '0.1'. Effectively, the
        counter will be multiplied by the sample rate to estimate the actual
        counter value.
        """
        self.processor.process("gorets:1|c|@0.1")
        self.assertEqual(1, len(self.processor.counter_metrics))
        self.assertEqual(10.0, self.processor.counter_metrics["gorets"])

    def test_receive_timer(self):
        """
        A timer message takes the format 'glork:320|ms', where 'glork' is the
        identifier and '320' is the time in milliseconds.
        """
        self.processor.process("glork:320|ms")
        self.assertEqual(1, len(self.processor.timer_metrics))
        self.assertEqual([320], self.processor.timer_metrics["glork"])

    def test_receive_gauge_metric(self):
        """
        A gauge metric message takes the form:
        '<name>:<count>|g'.
        'g' indicates this is a gauge metric message.
        """
        self.processor.process("gorets:9.6|g")
        self.assertEqual(1, len(self.processor.gauge_metrics))
        self.assertEqual(
            [9.6, 'gorets'],
            self.processor.gauge_metrics.pop())

    def test_receive_distinct_metric(self):
        """
        A distinct metric message takes the form:
        '<name>:<item>|pd'.
        'pd' indicates this is a probabilistic distinct metric message.
        """
        self.processor.process("gorets:one|pd")
        self.assertEqual(1, len(self.processor.plugin_metrics))
        self.assertTrue(self.processor.plugin_metrics["gorets"].count() > 0)

    def test_receive_message_no_fields(self):
        """
        If a timer message has no fields, it is logged and discarded.
        """
        self.processor.process("glork")
        self.assertEqual(0, len(self.processor.timer_metrics))
        self.assertEqual(0, len(self.processor.counter_metrics))
        self.assertEqual(["glork"], self.processor.failures)

    def test_receive_counter_no_value(self):
        """
        If a counter message has no value, it is logged and discarded.
        """
        self.processor.process("gorets:|c")
        self.assertEqual(0, len(self.processor.counter_metrics))
        self.assertEqual(["gorets:|c"], self.processor.failures)

    def test_receive_timer_no_value(self):
        """
        If a timer message has no value, it is logged and discarded.
        """
        self.processor.process("glork:|ms")
        self.assertEqual(0, len(self.processor.timer_metrics))
        self.assertEqual(["glork:|ms"], self.processor.failures)

    def test_receive_not_enough_fields(self):
        """
        If a timer message has not enough fields, it is logged and discarded.
        """
        self.processor.process("glork:1")
        self.assertEqual(0, len(self.processor.timer_metrics))
        self.assertEqual(0, len(self.processor.counter_metrics))
        self.assertEqual(["glork:1"], self.processor.failures)

    def test_receive_too_many_fields(self):
        """
        If a timer message has too many fields, it is logged and discarded.
        """
        self.processor.process("gorets:1|c|@0.1|yay")
        self.assertEqual(0, len(self.processor.timer_metrics))
        self.assertEqual(0, len(self.processor.counter_metrics))
        self.assertEqual(["gorets:1|c|@0.1|yay"], self.processor.failures)


class ProcessorStatsTest(TestCase):

    def setUp(self):
        self.timer = Timer()
        self.processor = MessageProcessor(time_function=self.timer)

    def test_process_keeps_processing_time(self):
        """
        When a message is processed, we keep the time it took to process it for
        later reporting.
        """
        self.timer.set([0, 5])
        self.processor.process("gorets:1|c")
        self.assertEqual(5, self.processor.process_timings["c"])
        self.assertEquals(1, self.processor.by_type["c"])

    def test_flush_tracks_flushing_time(self):
        """
        When flushing metrics, we track the time each metric type took to be
        flushed.
        """
        self.timer.set([0,
                        0, 1, # counter
                        1, 3, # timer
                        3, 6, # gauge
                        6, 10, # meter
                        10, 15, # plugin
                        ])
        def flush_metrics_summary(messages, num_stats, per_metric, timestamp):
            self.assertEqual((0, 1), per_metric["counter"])
            self.assertEqual((0, 2), per_metric["timer"])
            self.assertEqual((0, 3), per_metric["gauge"])
            self.assertEqual((0, 4), per_metric["meter"])
            self.assertEqual((0, 5), per_metric["plugin"])
        self.addCleanup(setattr, self.processor, "flush_metrics_summary",
                        self.processor.flush_metrics_summary)
        self.processor.flush_metrics_summary = flush_metrics_summary
        self.processor.flush()

    def test_flush_metrics_summary(self):
        """
        When flushing the metrics summary, we report duration and count of
        flushing each different type of metric as well as processing time.
        """
        per_metric = {"counter": (10, 1)}
        self.processor.process_timings = {"c": 1}
        self.processor.by_type = {"c": 42}
        messages = []
        self.processor.flush_metrics_summary(messages, 1, per_metric, 42)
        self.assertEqual(5, len(messages))
        self.assertEqual([('statsd.numStats', 1, 42),
                          ('statsd.flush.counter.count', 10, 42),
                          ('statsd.flush.counter.duration', 1000, 42),
                          ('statsd.receive.c.count', 42, 42),
                          ('statsd.receive.c.duration', 1000, 42)],
                          messages)
        self.assertEquals({}, self.processor.process_timings)
        self.assertEquals({}, self.processor.by_type)


class FlushMessagesTest(TestCase):

    def setUp(self):
        self.processor = MessageProcessor(time_function=lambda: 42,
                                          plugins=getPlugins(IMetricFactory))

    def test_flush_no_stats(self):
        """
        Flushing the message processor when there are no stats available should
        still produce one message where C{statsd.numStats} is set to zero.
        """
        self.assertEqual(("statsd.numStats", 0, 42), self.processor.flush()[0])

    def test_flush_counter(self):
        """
        If a counter is present, flushing it will generate a counter message
        normalized to the default interval.
        """
        self.processor.counter_metrics["gorets"] = 42
        messages = self.processor.flush()
        self.assertEqual(("stats.gorets", 4, 42), messages[0])
        self.assertEqual(("stats_counts.gorets", 42, 42), messages[1])
        self.assertEqual(("statsd.numStats", 1, 42), messages[2])
        self.assertEqual(0, self.processor.counter_metrics["gorets"])

    def test_flush_counter_one_second_interval(self):
        """
        It is possible to flush counters with a one-second interval, in which
        case the counter value will be unchanged.
        """
        self.processor.counter_metrics["gorets"] = 42
        messages = self.processor.flush(interval=1000)
        self.assertEqual(("stats.gorets", 42, 42), messages[0])
        self.assertEqual(("stats_counts.gorets", 42, 42), messages[1])
        self.assertEqual(("statsd.numStats", 1, 42), messages[2])
        self.assertEqual(0, self.processor.counter_metrics["gorets"])

    def test_flush_single_timer_single_time(self):
        """
        If a single timer with a single data point is present, all of upper,
        threshold_upper, lower, mean will be set to the same value. Timer is
        reset after flush is called.
        """
        self.processor.timer_metrics["glork"] = [24]
        messages = self.processor.flush()
        self.assertEqual(("stats.timers.glork.count", 1, 42), messages[0])
        self.assertEqual(("stats.timers.glork.lower", 24, 42), messages[1])
        self.assertEqual(("stats.timers.glork.mean", 24, 42), messages[2])
        self.assertEqual(("stats.timers.glork.upper", 24, 42), messages[3])
        self.assertEqual(("stats.timers.glork.upper_90", 24, 42), messages[4])
        self.assertEqual(("statsd.numStats", 1, 42), messages[5])
        self.assertEqual([], self.processor.timer_metrics["glork"])

    def test_flush_single_timer_multiple_times(self):
        """
        If a single timer with multiple data points is present:
        - lower will be set to the smallest value
        - upper will be set to the largest value
        - upper_90 will be set to the 90th percentile
        - count will be the count of data points
        - mean will be the mean value within the 90th percentile
        """
        self.processor.timer_metrics["glork"] = [4, 8, 15, 16, 23, 42]
        messages = self.processor.flush()
        self.assertEqual(("stats.timers.glork.count", 6, 42), messages[0])
        self.assertEqual(("stats.timers.glork.lower", 4, 42), messages[1])
        self.assertEqual(("stats.timers.glork.mean", 13, 42), messages[2])
        self.assertEqual(("stats.timers.glork.upper", 42, 42), messages[3])
        self.assertEqual(("stats.timers.glork.upper_90", 23, 42), messages[4])
        self.assertEqual(("statsd.numStats", 1, 42), messages[5])
        self.assertEqual([], self.processor.timer_metrics["glork"])

    def test_flush_single_timer_50th_percentile(self):
        """
        It is possible to flush the timers with a different percentile, in this
        example, 50%.

        If a single timer with multiple data points is present:
        - lower will be set to the smallest value
        - upper will be set to the largest value
        - upper_50 will be set to the 50th percentile
        - count will be the count of data points
        - mean will be the mean value within the 50th percentile
        """
        self.processor.timer_metrics["glork"] = [4, 8, 15, 16, 23, 42]
        messages = self.processor.flush(percent=50)
        self.assertEqual(("stats.timers.glork.count", 6, 42), messages[0])
        self.assertEqual(("stats.timers.glork.lower", 4, 42), messages[1])
        self.assertEqual(("stats.timers.glork.mean", 9, 42), messages[2])
        self.assertEqual(("stats.timers.glork.upper", 42, 42), messages[3])
        self.assertEqual(("stats.timers.glork.upper_50", 15, 42), messages[4])
        self.assertEqual(("statsd.numStats", 1, 42), messages[5])
        self.assertEqual([], self.processor.timer_metrics["glork"])

    def test_flush_gauge_metric(self):
        """
        Test the correct rendering of the Graphite report for
        a gauge metric.
        """

        self.processor.process("gorets:9.6|g")

        messages = self.processor.flush()
        self.assertEqual(
            ("stats.gauge.gorets.value", 9.6, 42), messages[0])
        self.assertEqual(
            ("statsd.numStats", 1, 42), messages[1])
        self.assertEqual(0, len(self.processor.gauge_metrics))

    def test_flush_distinct_metric(self):
        """
        Test the correct rendering of the Graphite report for
        a distinct metric.
        """

        self.processor.process("gorets:item|pd")

        messages = self.processor.flush()
        self.assertEqual(("stats.pdistinct.gorets.count", 1, 42), messages[0])
        self.assertEqual(("stats.pdistinct.gorets.count_1day",
                        5552568545, 42), messages[1])
        self.assertEqual(("stats.pdistinct.gorets.count_1hour",
                        5552568545, 42), messages[2])
        self.assertEqual(("stats.pdistinct.gorets.count_1min",
                        5552568545, 42), messages[3])

    def test_flush_plugin_arguments(self):
        """Test the passing of arguments for flush."""

        class FakeMetric(object):
            def flush(self, interval, timestamp):
                self.data = interval, timestamp
                return []

        self.processor.plugin_metrics["somemetric"] = FakeMetric()
        self.processor.flush(41000)
        self.assertEquals((41, 42),
            self.processor.plugin_metrics["somemetric"].data)


class FlushMeterMetricMessagesTest(TestCase):

    def setUp(self):
        self.processor = MessageProcessor(time_function=self.wall_clock_time)
        self.time_now = int(time.time())

    def wall_clock_time(self):
        return self.time_now

    def mark_minutes(self, minutes):
        for i in range(1, minutes * 60, 5):
            self.processor.update_metrics()

    def test_flush_meter_metric(self):
        """
        Test the correct rendering of the Graphite report for
        a meter metric.
        """
        self.processor.process("gorets:3.0|m")

        self.time_now += 1
        messages = self.processor.flush()
        self.assertEqual(
            ("stats.meter.gorets.15min_rate", 0.0, self.time_now),
            messages[0])
        self.assertEqual(
            ("stats.meter.gorets.1min_rate", 0.0, self.time_now),
            messages[1])
        self.assertEqual(
            ("stats.meter.gorets.5min_rate", 0.0, self.time_now),
            messages[2])
        self.assertEqual(
            ("stats.meter.gorets.count", 3.0, self.time_now),
            messages[3])
        self.assertEqual(
            ("stats.meter.gorets.mean_rate", 3.0, self.time_now),
            messages[4])
        self.assertEqual(
            ("statsd.numStats", 1, self.time_now),
            messages[5])

        # As we are employing the expected results from test_ewma.py
        # we perform the initial tick(), before advancing the clock 60sec.
        self.processor.update_metrics()

        self.mark_minutes(1)
        self.time_now += 60
        messages = self.processor.flush()
        self.assertEqual(
            ("stats.meter.gorets.15min_rate", 0.561304, self.time_now),
            messages[0])
        self.assertEqual(
            ("stats.meter.gorets.1min_rate", 0.220728, self.time_now),
            messages[1])
        self.assertEqual(
            ("stats.meter.gorets.5min_rate", 0.491238, self.time_now),
            messages[2])
        self.assertEqual(
            ("stats.meter.gorets.count", 3.0, self.time_now),
            messages[3])
        self.assertEqual(
            ("stats.meter.gorets.mean_rate", 0.049180, self.time_now),
            messages[4])
        self.assertEqual(
            ("statsd.numStats", 1, self.time_now), messages[5])
