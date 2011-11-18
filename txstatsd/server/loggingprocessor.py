
import time

from txstatsd.server.configurableprocessor import ConfigurableMessageProcessor


class LoggingMessageProcessor(ConfigurableMessageProcessor):
    """
    This specialised C{MessageProcessor} logs the received metrics
    using the supplied logger (which should have a callable C{info}
    attribute.)
    """

    def __init__(self, logger, time_function=time.time, message_prefix="", plugins=None):
        super(LoggingMessageProcessor, self).__init__(
            time_function=time_function, message_prefix=message_prefix,
            plugins=plugins)

        logger_info = getattr(logger, 'info', None)
        if logger_info is None or not callable(logger_info):
            raise TypeError()
        self.logger = logger

    def flush(self, interval=10000, percent=90):
        """Log all received metric samples to the supplied log file."""
        timestamp = int(self.time_function())
        interval = interval / 1000

        def log_metrics(metrics):
            for metric in metrics.itervalues():
                report = metric.report(timestamp)
                for measurement in report.splitlines():
                    self.logger.info(measurement)

        log_metrics(self.counter_metrics)
        log_metrics(self.gauge_metrics)
        log_metrics(self.meter_metrics)
        log_metrics(self.timer_metrics)
        
        for metric in self.plugin_metrics.itervalues():
            report = metric.flush(interval, timestamp)
            for measurement in report.splitlines():
                self.logger.info(measurement)

