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
        expected = ["In: %s" % msg_in] + ["Out: %s %s %s" % message
                                          for message in messages]
        self.assertFalse(set(expected).difference(logger.log.splitlines()))
