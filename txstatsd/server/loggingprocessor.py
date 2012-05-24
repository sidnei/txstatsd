
import time

from txstatsd.server.configurableprocessor import ConfigurableMessageProcessor


class LoggingMessageProcessor(ConfigurableMessageProcessor):
    """
    This specialised C{MessageProcessor} logs the received metrics
    using the supplied logger (which should have a callable C{info}
    attribute.)
    """

    def __init__( self, logger, time_function=time.time,
            message_prefix="", plugins=None, **kwz ):
        super(LoggingMessageProcessor, self).__init__(
            time_function=time_function, message_prefix=message_prefix,
            plugins=plugins, **kwz )

        logger_info = getattr(logger, "info", None)
        if logger_info is None or not callable(logger_info):
            raise TypeError()
        self.logger = logger

    def process_message(self, message, metric_type, key, fields):
        self.logger.info("In: %s" % message)
        return super(LoggingMessageProcessor, self)\
            .process_message(message, metric_type, key, fields)

    def flush(self, interval=10000, percent=90):
        """Log all received metric samples to the supplied logger."""
        messages = list( super(LoggingMessageProcessor, self)\
            .flush(interval=interval, percent=percent) )
        for msg in messages:
            self.logger.info("Out: %s %s %s" % msg)
        return messages
