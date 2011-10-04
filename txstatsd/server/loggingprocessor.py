
import time

from txstatsd.server.configurableprocessor import ConfigurableMessageProcessor


class LoggingMessageProcessor(ConfigurableMessageProcessor):
    """
    This specialised C{MessageProcessor} logs the received metrics
    using the supplied logger (which should have a callable C{info}
    attribute.)
    """

    def __init__(self, logger, time_function=time.time, message_prefix=""):
        super(LoggingMessageProcessor, self).__init__(
            time_function=time_function, message_prefix=message_prefix)

        logger_info = getattr(logger, 'info', None)
        if logger_info is None or not callable(logger_info):
            raise TypeError()
        self.logger = logger

    def flush(self):
        """Log all received metric samples to the supplied log file."""
        timestamp = int(self.time_function())

        def log_metrics(metrics):
            for metric in metrics.itervalues():
                report = metric.report(timestamp)
                for measurement in report.splitlines():
                    self.logger.info(measurement)

        log_metrics(self.counter_metrics)
        log_metrics(self.gauge_metrics)
        log_metrics(self.meter_metrics)
        log_metrics(self.timer_metrics)

