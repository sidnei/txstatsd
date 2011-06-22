import logging

from twisted.python import log
from twisted.internet import task
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
        log.msg("Received data from %s:%d" % (host, port),
                logLevel=logging.DEBUG)
        self.processor.process(data)


class StatsDClientProtocol(DatagramProtocol):
    """A Twisted-based implementation of the StatsD client protocol.

    Data is sent via ConnectedUDP to a StatsD server for aggregation.
    """

    def __init__(self, host, port, meter):
        self.host = host
        self.port = port
        self.meter = meter

    def startProtocol(self):
        """Connect to destination host."""
        self.meter.connected(self.transport, self.host, self.port)

    def stopProtocol(self):
        """Connection was lost."""
        self.meter.disconnected()


class GraphiteProtocol(LineOnlyReceiver):
    """A client protocol for talking to Graphite.

    Messages to Graphite are line-based and C{\n}-separated.
    """

    delimiter = "\n"

    def __init__(self, processor, interval):
        self.processor = processor
        self.interval = interval
        self.flush_task = None

    def connectionMade(self):
        """
        Once a connection has been made, schedule a L{MessageProcessor.flush}
        call to happen some time in the future.
        """
        log.msg("Connected. Scheduling flush to now + %ds." %
                (self.interval / 1000), logLevel=logging.DEBUG)
        self.flush_task = task.LoopingCall(self.flushProcessor)
        self.flush_task.start(self.interval / 1000)

    def connectionLost(self, reason):
        """
        If the connection has been lost, cancel scheduled calls to
        L{MessageProcessor.flush} until the connection is restored.
        """
        log.msg("Connection lost.", logLevel=logging.DEBUG)
        if self.flush_task is not None:
            log.msg("Canceling scheduled flush.", logLevel=logging.DEBUG)
            self.flush_task.stop()

    def flushProcessor(self):
        """Flush messages queued in the processor to Graphite."""
        log.msg("Flushing messages.", logLevel=logging.DEBUG)
        for message in self.processor.flush(interval=self.interval):
            for line in message.splitlines():
                self.sendLine(line)


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
