
from unittest import TestCase

from twisted.plugins.distinct_plugin import distinct_metric_factory

from txstatsd.server.loggingprocessor import LoggingMessageProcessor


class FakeMeterMetric(object):
    def report(self, *args):
        return [('Sample report', 1, 2)]

class TestLogger(object):
    def __init__(self):
        self.log = ''

    def info(self, measurement):
        self.log += measurement + "\n"


class TestLoggingMessageProcessor(TestCase):

    def test_logger_with_no_info(self):
        def invoker():
            logger = 'logger'
            LoggingMessageProcessor(logger)

        self.assertRaises(TypeError, invoker)

    def test_logger_with_non_callable_info(self):
        def invoker():
            class Logger(object):
                def __init__(self):
                    self.info = 'logger'

            logger = Logger()
            LoggingMessageProcessor(logger)

        self.assertRaises(TypeError, invoker)

    def test_logger(self):
        logger = TestLogger()
        processor = LoggingMessageProcessor(logger)
        metric = FakeMeterMetric()
        processor.meter_metrics['test'] = metric
        processor.flush()
        expected = ["Out: %s %s %s" % message
                for message in metric.report()]
        self.assertFalse(set(expected).difference(logger.log.splitlines()))

    def test_logger_plugin(self):
        logger = TestLogger()
        processor = LoggingMessageProcessor(
            logger, plugins=[distinct_metric_factory],
            time_function=lambda: 42)
        msg_in = "gorets:17|pd"
        processor.process(msg_in)
        processor.flush()
        messages = processor.plugin_metrics['gorets'].flush(
            10, processor.time_function())
        expected = ["In: %s" % msg_in]\
            + ["Out: %s %s %s" % message
                for message in messages]
        self.assertFalse(set(expected).difference(logger.log.splitlines()))
