import time

from unittest import TestCase

from txstatsd.server.configurableprocessor import ConfigurableMessageProcessor


class FlushMessagesTest(TestCase):

    def test_flush_counter_with_empty_prefix(self):
        """
        Ensure no prefix features if none is supplied.
        B{Note}: The C{ConfigurableMessageProcessor} reports
        the counter value, and not the normalized version as
        seen in the StatsD-compliant C{Processor}.
        """
        configurable_processor = ConfigurableMessageProcessor(
            time_function=lambda: 42)
        configurable_processor.process("gorets:17|c")
        messages = configurable_processor.flush()
        self.assertEqual(2, len(messages))
        counters = messages[0].splitlines()
        self.assertEqual("gorets.count 17 42", counters[0])
        self.assertEqual("statsd.numStats 1 42", messages[1].splitlines()[0])

    def test_flush_counter_with_prefix(self):
        """
        Ensure the prefix features if one is supplied.
        """
        configurable_processor = ConfigurableMessageProcessor(
            time_function=lambda: 42, message_prefix="test.metric")
        configurable_processor.process("gorets:17|c")
        messages = configurable_processor.flush()
        self.assertEqual(2, len(messages))
        counters = messages[0].splitlines()
        self.assertEqual("test.metric.gorets.count 17 42", counters[0])
        self.assertEqual("test.metric.statsd.numStats 1 42",
                         messages[1].splitlines()[0])

    def test_flush_single_timer_single_time(self):
        """
        If a single timer with a single data point is present, all
        percentiles will be set to the same value.
        """
        configurable_processor = ConfigurableMessageProcessor(
            time_function=lambda: 42)

        configurable_processor.process("glork:24|ms")
        messages = configurable_processor.flush()

        self.assertEqual(2, len(messages))
        timers = messages[0].splitlines()
        self.assertEqual("glork.min 24.0 42", timers[0])
        self.assertEqual("glork.max 24.0 42", timers[1])
        self.assertEqual("glork.mean 24.0 42", timers[2])
        self.assertEqual("glork.stddev 0.0 42", timers[3])
        self.assertEqual("glork.median 24.0 42", timers[4])
        self.assertEqual("glork.75percentile 24.0 42", timers[5])
        self.assertEqual("glork.95percentile 24.0 42", timers[6])
        self.assertEqual("glork.98percentile 24.0 42", timers[7])
        self.assertEqual("glork.99percentile 24.0 42", timers[8])
        self.assertEqual("glork.999percentile 24.0 42", timers[9])
        self.assertEqual("statsd.numStats 1 42", messages[1].splitlines()[0])

    def test_flush_single_timer_multiple_times(self):
        """
        Test reporting of multiple timer metric samples.
        """
        configurable_processor = ConfigurableMessageProcessor(
            time_function=lambda: 42)

        configurable_processor.process("glork:4|ms")
        configurable_processor.update_metrics()
        configurable_processor.process("glork:8|ms")
        configurable_processor.update_metrics()
        configurable_processor.process("glork:15|ms")
        configurable_processor.update_metrics()
        configurable_processor.process("glork:16|ms")
        configurable_processor.update_metrics()
        configurable_processor.process("glork:23|ms")
        configurable_processor.update_metrics()
        configurable_processor.process("glork:42|ms")
        configurable_processor.update_metrics()

        messages = configurable_processor.flush()
        self.assertEqual(2, len(messages))
        timers = messages[0].splitlines()
        self.assertEqual("glork.min 4.0 42", timers[0])
        self.assertEqual("glork.max 42.0 42", timers[1])
        self.assertEqual("glork.mean 18.0 42", timers[2])
        self.assertTrue(timers[3].startswith("glork.stddev 13.4907"))
        self.assertEqual("glork.median 15.5 42", timers[4])
        self.assertEqual("glork.75percentile 27.75 42", timers[5])
        self.assertEqual("glork.95percentile 42.0 42", timers[6])
        self.assertEqual("glork.98percentile 42.0 42", timers[7])
        self.assertEqual("glork.99percentile 42.0 42", timers[8])
        self.assertEqual("glork.999percentile 42.0 42", timers[9])
        self.assertEqual("statsd.numStats 1 42", messages[1].splitlines()[0])


class FlushMeterMetricMessagesTest(TestCase):

    def setUp(self):
        self.configurable_processor = ConfigurableMessageProcessor(
            time_function=self.wall_clock_time, message_prefix="test.metric")
        self.time_now = int(time.time())

    def wall_clock_time(self):
        return self.time_now

    def mark_minutes(self, minutes):
        for i in range(1, minutes * 60, 5):
            self.processor.update_metrics()

    def test_flush_meter_metric_with_prefix(self):
        """
        Test the correct rendering of the Graphite report for
        a meter metric when a prefix is supplied.
        """
        self.configurable_processor.process("gorets:3.0|m")

        self.time_now += 1
        messages = self.configurable_processor.flush()
        self.assertEqual(2, len(messages))
        meter_metric = messages[0].splitlines()
        self.assertEqual(
            "test.metric.gorets.count 3.0 %s" % self.time_now,
            meter_metric[0])
        self.assertEqual(
            "test.metric.gorets.mean_rate 3.0 %s" % self.time_now,
            meter_metric[1])
        self.assertEqual(
            "test.metric.gorets.1min_rate 0.0 %s" % self.time_now,
            meter_metric[2])
        self.assertEqual(
            "test.metric.gorets.5min_rate 0.0 %s" % self.time_now,
            meter_metric[3])
        self.assertEqual(
            "test.metric.gorets.15min_rate 0.0 %s" % self.time_now,
            meter_metric[4])
        self.assertEqual(
            "test.metric.statsd.numStats 1 %s" % self.time_now,
            messages[1].splitlines()[0])
