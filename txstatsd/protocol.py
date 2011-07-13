from twisted.internet import task, defer
from twisted.protocols.basic import LineOnlyReceiver
from twisted.internet.protocol import (
    DatagramProtocol, ReconnectingClientFactory)


class StatsDServerProtocol(DatagramProtocol):
    """A Twisted-based implementation of the StatsD server.

    Data is received via UDP for local aggregation and then sent to a Graphite
    server via TCP.
    """

    def __init__(self, processor):
        self.processor = processor

    def datagramReceived(self, data, (host, port)):
        """Process received data and store it locally."""
        self.processor.process(data)


class StatsDClientProtocol(DatagramProtocol):
    """A Twisted-based implementation of the StatsD client protocol.

    Data is sent via UDP to a StatsD server for aggregation.
    """

    def __init__(self, host, port, meter, interval=None):
        self.host = host
        self.port = port
        self.meter = meter
        self.interval = interval

    def startProtocol(self):
        """Connect to destination host."""
        self.meter.connect(self.transport, self.host, self.port)

    def stopProtocol(self):
        """Connection was lost."""
        self.meter.disconnect()


class GraphiteProtocol(LineOnlyReceiver):
    """A client protocol for talking to Graphite.

    Messages to Graphite are line-based and C{\n}-separated.
    """

    delimiter = "\n"

    def __init__(self, processor, interval):
        self.processor = processor
        self.interval = interval
        self.flush_task = task.LoopingCall(self.flushProcessor)
        self.flush_task.start(self.interval / 1000, False)

    @defer.inlineCallbacks
    def flushProcessor(self):
        """Flush messages queued in the processor to Graphite."""
        for message in self.processor.flush(interval=self.interval):
            for line in message.splitlines():
                if self.connected:
                    self.sendLine(line)
                yield


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
