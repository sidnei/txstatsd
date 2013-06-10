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

import os
import sys
import time
import logging
import threading
import traceback
import Queue

from twisted.internet.defer import maybeDeferred
from twisted.internet.task import LoopingCall
from twisted.python import log

from functools import wraps

from twisted.application.service import Service


class ReportingService(Service):

    def __init__(self, instance_name="", clock=None):
        self.tasks = []
        self.clock = clock
        self.instance_name = instance_name

    def schedule(self, function, interval, report_function):
        """
        Schedule C{function} to be called every C{interval} seconds and then
        report gathered metrics to C{Graphite} using C{report_function}.

        If C{report_function} is C{None}, it just calls the function without
        reporting the metrics.
        """
        if report_function is not None:
            call = self.wrapped(function, report_function)
        else:
            call = function
        task = LoopingCall(call)
        if self.clock is not None:
            task.clock = self.clock
        self.tasks.append((task, interval))
        if self.running:
            task.start(interval, now=True)

    def wrapped(self, function, report_function):
        def report_metrics(metrics):
            """For each metric returned, call C{report_function} with it."""
            for name, value in metrics.items():
                if self.instance_name:
                    name = self.instance_name + "." + name
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
        Service.startService(self)
        for task, interval in self.tasks:
            task.start(interval, now=False)

    def stopService(self):
        for task, interval in self.tasks:
            task.stop()
        Service.stopService(self)


class ReactorInspector(threading.Thread):
    """Log message with a time delta from the last call."""

    def __init__(self, reactor_call, metrics, loop_time=3, log=log.msg):
        self.running = False
        self.stopped = False
        self.queue = Queue.Queue()
        self.reactor_call = reactor_call
        self.loop_time = loop_time
        self.last_responsive_ts = 0
        self.reactor_thread = None
        self.metrics = metrics
        super(ReactorInspector, self).__init__()
        self.daemon = True
        self.log = log

    def start(self):
        """Start the thread. Should be called from the reactor main thread."""
        self.reactor_thread = threading.currentThread().ident
        if not self.running:
            self.running = True
            super(ReactorInspector, self).start()

    def stop(self):
        """Stop the thread."""
        self.stopped = True
        self.log("ReactorInspector: stopped")

    def dump_frames(self):
        """Dump frames info to log file."""
        current = threading.currentThread().ident
        frames = sys._current_frames()
        for frame_id, frame in frames.iteritems():
            if frame_id == current:
                continue

            stack = ''.join(traceback.format_stack(frame))

            if frame_id == self.reactor_thread:
                title = "Dumping Python frame for reactor main thread"
            else:
                title = "Dumping Python frame"
            self.log("%s %s (pid: %d):\n%s" %
                     (title, frame_id, os.getpid(), stack),
                     logLevel=logging.DEBUG)

    def run(self):
        """Start running the thread."""
        self.log("ReactorInspector: started")
        msg_id = 0
        oldest_pending_request_ts = time.time()
        while not self.stopped:
            def task(msg_id=msg_id, tini=time.time()):
                """Put result in queue with initial and completed times."""
                self.queue.put((msg_id, tini, time.time()))
            self.reactor_call(task)
            time.sleep(self.loop_time)
            try:
                id_sent, tini, tsent = self.queue.get_nowait()
            except Queue.Empty:
                # Oldest pending request is still out there
                delay = time.time() - oldest_pending_request_ts
                self.metrics.gauge("delay", delay)
                self.log("ReactorInspector: detected unresponsive!"
                         " (current: %d, pid: %d) delay: %.3f" % (
                             msg_id, os.getpid(), delay),
                         logLevel=logging.CRITICAL)
                self.dump_frames()
            else:
                delay = tsent - tini
                self.metrics.gauge("delay", delay)
                if msg_id > id_sent:
                    self.log("ReactorInspector: late (current: %d, "
                             "got: %d, pid: %d, cleaning queue) "
                             "delay: %.3f" % (msg_id, id_sent,
                                              os.getpid(), delay),
                             logLevel=logging.WARNING)
                    while not self.queue.empty():
                        self.queue.get_nowait()
                    # About to start a new request with nothing pending
                    oldest_pending_request_ts = time.time()
                else:
                    assert msg_id == id_sent
                    # About to start a new request with nothing pending
                    self.last_responsive_ts = time.time()
                    oldest_pending_request_ts = self.last_responsive_ts
                    self.log("ReactorInspector: ok (msg: %d, "
                             "pid: %d) delay: %.3f" % (
                                 msg_id, os.getpid(), delay),
                             logLevel=logging.DEBUG)
            finally:
                msg_id += 1


class ReactorInspectorService(Service):
    """Start/stop the reactor inspector service."""

    def __init__(self, reactor, metrics, loop_time=3):
        self.inspector = ReactorInspector(
            reactor.callFromThread, metrics, loop_time)

    def startService(self):
        Service.startService(self)
        self.inspector.start()

    def stopService(self):
        self.inspector.stop()
        Service.stopService(self)
