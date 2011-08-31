from twisted.internet import task, defer
from twisted.internet.protocol import (
    DatagramProtocol, ReconnectingClientFactory)
from twisted.protocols.basic import LineOnlyReceiver


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
