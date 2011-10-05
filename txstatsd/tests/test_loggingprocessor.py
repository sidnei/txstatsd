
from unittest import TestCase

from txstatsd.server.loggingprocessor import LoggingMessageProcessor


class FakeMeterMetric(object):
    def report(self, *args):
        return 'Sample report'


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
        class Logger(object):
            def __init__(self):
                self.log = ''

            def info(self, measurement):
                self.log += measurement

        logger = Logger()
        processor = LoggingMessageProcessor(logger)
        metric = FakeMeterMetric()
        processor.meter_metrics['test'] = metric
        processor.flush()
        self.assertEqual(metric.report(), logger.log)
