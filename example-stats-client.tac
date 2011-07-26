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
    reporting.schedule(report, 10, meter.increment)

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


protocol = StatsDClientProtocol(STATSD_HOST, STATSD_PORT,
                                statsd_client, 6000)
reactor.listenUDP(0, protocol)
