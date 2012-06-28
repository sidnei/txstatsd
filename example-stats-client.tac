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

import socket
import random

from twisted.internet import reactor
from twisted.internet import task

from twisted.application.service import Application

from txstatsd.client import (
    TwistedStatsDClient, StatsDClientProtocol)
from txstatsd.metrics.metrics import Metrics
from txstatsd.process import PROCESS_STATS
from txstatsd.report import ReportingService


STATSD_HOST = "127.0.0.1"
STATSD_PORT = 8125

application = Application("example-stats-client")
statsd_client = TwistedStatsDClient(STATSD_HOST, STATSD_PORT)
metrics = Metrics(connection=statsd_client,
                  namespace=socket.gethostname() + ".example-client")

reporting = ReportingService()
reporting.setServiceParent(application)

for report in PROCESS_STATS:
    reporting.schedule(report, 10, metrics.increment)

def random_walker(name):
    """Meters a random walk."""
    if random.random() > 0.5:
        metrics.increment(name)
    else:
        metrics.decrement(name)

def random_normal(name):
    """Meters samples from a normal distribution."""
    metrics.timing(name, random.normalvariate(10, 3))


for n in range(5):
    t = task.LoopingCall(random_walker, name="walker%i" % n)
    t.start(0.5, now=False)

for n in range(5):
    t = task.LoopingCall(random_normal, name="normal%i" % n)
    t.start(0.5, now=False)


protocol = StatsDClientProtocol(statsd_client)
reactor.listenUDP(0, protocol)
