
import socket

from twisted.internet.protocol import DatagramProtocol


class StatsDClientProtocol(DatagramProtocol):
    """A Twisted-based implementation of the StatsD client protocol.

    Data is sent via UDP to a StatsD server for aggregation.
    """

    def __init__(self, host, port, client, interval=None):
        self.host = host
        self.port = port
        self.client = client
        self.interval = interval

    def startProtocol(self):
        """Connect to destination host."""
        self.client.connect(self.transport)

    def stopProtocol(self):
        """Connection was lost."""
        self.client.disconnect()


class TwistedStatsDClient(object):

    def __init__(self, host, port,
                 connect_callback=None, disconnect_callback=None):
        """Build a connection that reports to the endpoint (on
        C{host} and C{port}) using UDP.

        @param host: The StatsD server host.
        @param port: The StatsD server port.
        @param connect_callback: The callback to invoke on connection.
        @param disconnect_callback: The callback to invoke on disconnection.
        """

        self.host = host
        self.port = port
        self.connect_callback = connect_callback
        self.disconnect_callback = disconnect_callback

        self.transport = None

    def connect(self, transport=None):
        """Connect to the StatsD server."""
        self.transport = transport
        if self.transport is not None:
            if self.connect_callback is not None:
                self.connect_callback()

    def disconnect(self):
        """Disconnect from the StatsD server."""
        if self.disconnect_callback is not None:
            self.disconnect_callback()
        self.transport = self.host = self.port = None

    def write(self, data):
        """Send the metric to the StatsD server."""
        if self.transport is not None:
            self.transport.write(data, (self.host, self.port))


class UdpStatsDClient(object):

    def __init__(self, host=None, port=None):
        """Build a connection that reports to C{host} and C{port})
        using UDP.

        @param host: The StatsD host.
        @param port: The StatsD port.
        """
        self.host = host
        self.port = port

    def connect(self):
        """Connect to the StatsD server."""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.addr = (self.host, self.port)

    def disconnect(self):
        """Disconnect from the StatsD server."""
        if self.socket is not None:
            self.socket.close()
        self.socket = None

    def write(self, data):
        """Send the metric to the StatsD server."""
        if self.addr is None or self.socket is None:
            return
        self.socket.sendto(data, self.addr)


class InternalClient(object):
    """A connection that can be used inside the C{StatsD} daemon itself."""

    def __init__(self, processor):
        """
        A connection that writes directly to the C{MessageProcessor}.
        """
        self._processor = processor

    def connect(self):
        pass

    def disconnect(self):
        pass

    def write(self, data):
        """Write directly to the C{MessageProcessor}."""
        self._processor.process(data)
