from twisted.internet.defer import maybeDeferred
from twisted.internet.task import LoopingCall
from twisted.python import log

from functools import wraps

from twisted.application.service import Service


class ReportingService(Service):

    def __init__(self):
        self.tasks = []

    def schedule(self, function, interval, report_function):
        """
        Schedule C{function} to be called every C{interval} seconds and then
        report gathered metrics to C{Graphite} using C{report_function}.
        """
        task = LoopingCall(self.wrapped(function, report_function))
        self.tasks.append((task, interval))

    def wrapped(self, function, report_function):
        def report_metrics(metrics):
            """For each metric returned, call C{report_function} with it."""
            for name, value in metrics.items():
                report_function(name, value)
            return metrics

        @wraps(function)
        def wrapper():
            """Wrap C{function} to report metrics or log a failure."""
            deferred = maybeDeferred(function)
            deferred.addCallback(report_metrics)
            deferred.addErrback(lambda failure: log.err(
                failure, "Error while processing %s" % function.func_name))
            return deferred
        return wrapper

    def startService(self):
        for task, interval in self.tasks:
            task.start(interval, now=False)

    def stopService(self):
        for task, interval in self.tasks:
            task.stop()
