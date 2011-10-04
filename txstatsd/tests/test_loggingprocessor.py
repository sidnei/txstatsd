
from unittest import TestCase

from txstatsd.server.loggingprocessor import LoggingMessageProcessor


class FakeMeterMetric(object):
    def report(self, *args):
        return 'Sample report'


class TestLoggingMessageProcessor(TestCase):

    def test_logger_with_no_info(self):
        with self.assertRaises(TypeError):
            logger = 'logger'
            LoggingMessageProcessor(logger)

    def test_logger_with_non_callable_info(self):
        with self.assertRaises(TypeError):
            class L(object):
                def __init__(self):
                    self.info = 'logger'

            logger = L()
            LoggingMessageProcessor(logger)

    def test_logger(self):
        class L(object):
            def __init__(self):
                self.log = ''

            def info(self, measurement):
                self.log += measurement

        logger = L()
        processor = LoggingMessageProcessor(logger)
        metric = FakeMeterMetric()
        processor.meter_metrics['test'] = metric
        processor.flush()
        self.assertEqual(metric.report(), logger.log)
