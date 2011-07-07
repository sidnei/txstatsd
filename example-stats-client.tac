import socket
import random

from twisted.internet import reactor
from twisted.internet import task

from twisted.application.service import Application

from txstatsd.protocol import StatsDClientProtocol
from txstatsd.metrics import TransportMeter
from txstatsd.process import PROCESS_STATS


application = Application("example-stats-client")
meter = TransportMeter(prefix=socket.gethostname() + ".client")


def random_walker(name):
    """Meters a random walk."""
    if random.random() > 0.5:
        meter.increment(name)
    else:
        meter.decrement(name)

def random_normal(name):
    """Meters samples from a normal distribution."""
    meter.timing(name, random.normalvariate(10, 3))


for n in range(5):
    t = task.LoopingCall(random_walker, name="walker%i" % n)
    t.start(0.5, now=False)

for n in range(5):
    t = task.LoopingCall(random_normal, name="normal%i" % n)
    t.start(0.5, now=False)


protocol = StatsDClientProtocol("127.0.0.1", 8125, meter,
                                PROCESS_STATS, 6000)
reactor.listenUDP(0, protocol)
