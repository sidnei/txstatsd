from zope.interface import implements

from twisted.internet import task, interfaces
from twisted.internet.protocol import (
    DatagramProtocol, ReconnectingClientFactory, Protocol)


class StatsDServerProtocol(DatagramProtocol):
    """A Twisted-based implementation of the StatsD server.

    Data is received via UDP for local aggregation and then sent to a Graphite
    server via TCP.
    """

    def __init__(self, processor,
                 monitor_message=None, monitor_response=None):
        self.processor = processor
        self.monitor_message = monitor_message
        self.monitor_response = monitor_response

    def datagramReceived(self, data, (host, port)):
        """Process received data and store it locally."""
        if data == self.monitor_message:
            # Send the expected response to the
            # monitoring agent.
            self.transport.write(self.monitor_response, (host, port))
        else:
            self.processor.process(data)


class GraphiteProtocol(Protocol):
    """A client protocol for talking to Graphite.

    Messages to Graphite are line-based and C{\n}-separated.
    """

    implements(interfaces.IPushProducer)

    def __init__(self, processor, interval, clock=None):
        self.paused = False
        self.processor = processor
        self.interval = interval
        self.flush_task = task.LoopingCall(self.flushProcessor)
        if clock is not None:
            self.flush_task.clock = clock
        self.flush_task.start(self.interval / 1000, False)

    def connectionMade(self):
        """
        A connection has been made, register ourselves as a producer for the
        bound transport.
        """
        self.transport.registerProducer(self, True)

    def flushProcessor(self):
        """Flush messages queued in the processor to Graphite."""
        for message in self.processor.flush(interval=self.interval):
            if self.connected and not self.paused:
                self.transport.writeSequence((message, "\n"))

    def pauseProducing(self):
        """Pause producing messages, since the buffer is full."""
        self.paused = True

    stopProducing = pauseProducing

    def resumeProducing(self):
        """We can write to the transport again. Yay!."""
        self.paused = False


class GraphiteClientFactory(ReconnectingClientFactory):
    """A reconnecting Graphite client."""

    def __init__(self, processor, interval):
        self.processor = processor
        self.interval = interval

    def buildProtocol(self, addr):
        """
        Build a new instance of the L{Graphite} protocol, bound to the
        L{MessageProcessor}.
        """
        self.resetDelay()
        protocol = GraphiteProtocol(self.processor, self.interval)
        protocol.factory = self
        return protocol
