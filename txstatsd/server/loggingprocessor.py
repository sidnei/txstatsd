
import time

from logbook import Logger, RotatingFileHandler

from txstatsd.server.configurableprocessor import ConfigurableMessageProcessor


class LoggingMessageProcessor(ConfigurableMessageProcessor):
    """
    This specialised C{MessageProcessor} logs the received metrics
    to the supplied log file.
    """

    def __init__(self, log_path, time_function=time.time, message_prefix=""):
        super(LoggingMessageProcessor, self).__init__(
            time_function=time_function, message_prefix=message_prefix)

        self.log = Logger('metrics')
        self.logging_handler = RotatingFileHandler(log_path)

    def flush(self):
        """Log all received metric samples to the supplied log file."""
        timestamp = int(self.time_function())

        def log_metrics(metrics):
            for metric in metrics.itervalues():
                report = metric.report(timestamp)
                for measurement in report.splitlines():
                    self.log.info(measurement)

        with self.logging_handler:
            log_metrics(self.counter_metrics)
            log_metrics(self.gauge_metrics)
            log_metrics(self.meter_metrics)
            log_metrics(self.timer_metrics)

