
import time

from logbook import Handler, Logger

from txstatsd.server.configurableprocessor import ConfigurableMessageProcessor


class LoggingMessageProcessor(ConfigurableMessageProcessor):
    """
    This specialised C{MessageProcessor} logs the received metrics
    using the supplied logging handler.
    """

    def __init__(self, logging_handler, time_function=time.time,
                 message_prefix=""):
        super(LoggingMessageProcessor, self).__init__(
            time_function=time_function, message_prefix=message_prefix)

        if not isinstance(logging_handler, Handler):
            raise TypeError('Expecting a logbook Handler')
        self.logging_handler = logging_handler
        self.log = Logger('metrics')

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

