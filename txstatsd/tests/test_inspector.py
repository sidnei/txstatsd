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
"""Tests for the ReactorInspector."""

import re
import time
import logging
import threading

from twisted.trial.unittest import TestCase as TwistedTestCase
from twisted.internet import reactor, defer

from txstatsd.report import ReactorInspector


def parse_delay(msg):
    return float(re.search("delay: (\d+.\d{1,3})", msg).group(1))


class ReactorInspectorTestCase(TwistedTestCase):
    """Test the ReactorInspector class."""

    def setUp(self):
        """Set up."""

        class Helper(object):
            """Fake object with a controllable call."""
            def __init__(self):
                self.call_count = 1
                self.calls = []
                self.ri = None

            def call(self, func):
                """Call function when counter is 0, then stop running."""
                self.call_count -= 1
                self.calls.append(func)
                if self.call_count == 0:
                    for f in self.calls:
                        f()
                if self.call_count <= 0:
                    self.ri.stop()

        class FakeMetrics(object):
            """Fake Metrics object that records calls."""
            def __init__(self):
                """Initialize calls."""
                self.calls = []

            def meter(self, name, count):
                """Record call to meter()."""
                self.calls.append(("meter", name, count))

            def gauge(self, name, val):
                """Record call to gauge()."""
                self.calls.append(("gauge", name, round(val, 3)))

        def log(msg, logLevel=None):
            self.logged.append((msg, logLevel))

        self.helper = Helper()
        self.fake_metrics = FakeMetrics()
        self.ri = ReactorInspector(self.helper.call, self.fake_metrics,
                                   loop_time=.1, log=log)
        self.helper.ri = self.ri
        self.logged = []

    def check_log(self, *expected, **kw):
        logLevel = kw.get("logLevel", None)
        for (msg, level) in self.logged:
            if level == logLevel and all(m in msg for m in expected):
                return msg
        return False

    def run_ri(self, call_count=None, join=True):
        """Set the call count and then run the ReactorInspector."""
        if call_count is not None:
            self.helper.call_count = call_count
        # pylint: disable=W0201
        self.start_ts = time.time()
        self.ri.start()
        # Reactor will stop after call_count calls, thanks to helper
        if join:
            self.ri.join()

    def test_stop(self):
        """It stops."""
        self.run_ri(1000, join=False)
        assert self.ri.is_alive()
        self.ri.stop()
        self.ri.join()
        self.assertFalse(self.ri.is_alive())

    @defer.inlineCallbacks
    def test_dump_frames(self):
        """Test how frames are dumped.

        Rules:
        - own frame must not be logged
        - must log all other threads
        - main reactor thread must have special title
        """
        # other thread, whose frame must be logged
        waitingd = defer.Deferred()

        def waiting_function():
            """Function with funny name to be checked later."""
            reactor.callFromThread(waitingd.callback, True)
            # wait have a default value; pylint: disable=E1120
            event.wait()

        event = threading.Event()
        threading.Thread(target=waiting_function).start()
        # Make sure the thread has entered the waiting_function
        yield waitingd

        # Set reactor_thread since we're not starting the ReactorInspector
        # thread here.
        self.ri.reactor_thread = threading.currentThread().ident

        # dump frames in other thread, also
        def dumping_function():
            """Function with funny name to be checked later."""
            time.sleep(.1)
            self.ri.dump_frames()
            reactor.callFromThread(d.callback, True)

        d = defer.Deferred()
        threading.Thread(target=dumping_function).start()
        yield d
        event.set()

        # check
        self.assertFalse(self.check_log("dumping_function",
                                        logLevel=logging.DEBUG))
        self.assertTrue(self.check_log("Dumping Python frame",
                                       "waiting_function",
                                       logLevel=logging.DEBUG))
        self.assertTrue(self.check_log("Dumping Python frame",
                                       "reactor main thread",
                                       logLevel=logging.DEBUG))

    def test_reactor_ok(self):
        """Reactor working fast."""
        self.run_ri()
        ok_line = self.assertTrue(self.check_log("ReactorInspector: ok",
                                                 logLevel=logging.DEBUG))
        # Check the metrics
        delay = parse_delay(ok_line)
        expected_metric = ("gauge", "delay", delay)
        self.assertEqual([expected_metric], self.fake_metrics.calls)
        self.assertTrue(self.ri.last_responsive_ts >= self.start_ts)

    @defer.inlineCallbacks
    def test_reactor_blocked(self):
        """Reactor not working fast."""
        dump_frames_called = defer.Deferred()
        self.ri.dump_frames = lambda: dump_frames_called.callback(True)
        self.run_ri(0)
        yield dump_frames_called
        log_line = self.check_log("ReactorInspector",
                                  "detected unresponsive",
                                  logLevel=logging.CRITICAL)
        self.assertTrue(log_line)
        delay = parse_delay(log_line)
        self.assertTrue(delay >= .1)  # waited for entire loop time
        # Check the metrics
        expected_metric = ("gauge", "delay", delay)
        self.assertEqual([expected_metric], self.fake_metrics.calls)

        self.assertTrue(self.ri.last_responsive_ts < self.start_ts)

    def test_reactor_back_alive(self):
        """Reactor resurrects after some loops."""
        self.run_ri(3)
        late_line = self.check_log("ReactorInspector: late",
                                   "got: 0", logLevel=logging.WARNING)
        self.assertTrue(late_line)
        delay = parse_delay(late_line)
        self.assertTrue(delay >= .2)  # At least 2 cycles of delay
        # Check the metrics
        expected_metric = ("gauge", "delay", delay)
        self.assertEqual(expected_metric, self.fake_metrics.calls[-1])

        self.assertTrue(self.ri.queue.empty())
        # A late reactor is not considered responsive (until a successful loop)
        self.assertTrue(self.ri.last_responsive_ts < self.start_ts)
