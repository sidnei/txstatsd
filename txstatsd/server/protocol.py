from twisted.internet.protocol import (
    DatagramProtocol, Protocol, Factory)
from twisted.protocols.basic import LineReceiver


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


class StatsDTCPServerProtocol(LineReceiver):
    """A Twisted-based implementation of the StatsD server over TCP.

    Data is received via TCP for local aggregation and then sent to a Graphite
    server via TCP.
    """

    def __init__(self, processor,
                 monitor_message=None, monitor_response=None):
        self.processor = processor
        self.monitor_message = monitor_message
        self.monitor_response = monitor_response

    def lineReceived(self, data):
        """Process received data and store it locally."""
        if data == self.monitor_message:
            # Send the expected response to the
            # monitoring agent.
            self.transport.write(self.monitor_response)
        else:
            self.processor.process(data)


class StatsDTCPServerFactory(Factory):

    def __init__(self, processor,
                 monitor_message=None, monitor_response=None):
        self.processor = processor
        self.monitor_message = monitor_message
        self.monitor_response = monitor_response

    def buildProtocol(self, addr):
        return StatsDTCPServerProtocol(self.processor,
            self.monitor_message, self.monitor_response)

