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

from txstatsd.server.configurableprocessor import ConfigurableMessageProcessor


class LoggingMessageProcessor(ConfigurableMessageProcessor):
    """
    This specialised C{MessageProcessor} logs the received metrics
    using the supplied logger (which should have a callable C{info}
    attribute.)
    """

    def __init__(self, logger, time_function=time.time,
                 message_prefix="", plugins=None, **kwz):
        super(LoggingMessageProcessor, self).__init__(
            time_function=time_function, message_prefix=message_prefix,
            plugins=plugins, **kwz)

        logger_info = getattr(logger, "info", None)
        if logger_info is None or not callable(logger_info):
            raise TypeError()
        self.logger = logger

    def process_message(self, message, metric_type, key, fields):
        self.logger.info("In: %s" % message)
        return super(LoggingMessageProcessor, self).process_message(
            message, metric_type, key, fields)

    def flush(self, interval=10000, percent=90):
        """Log all received metric samples to the supplied logger."""
        messages = list(super(LoggingMessageProcessor, self).flush(
            interval=interval, percent=percent))
        for msg in messages:
            self.logger.info("Out: %s %s %s" % msg)
        return messages
