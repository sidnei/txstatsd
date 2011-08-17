import socket

from twisted.internet.protocol import DatagramProtocol


class StatsDClientProtocol(DatagramProtocol):
    """A Twisted-based implementation of the StatsD client protocol.

    Data is sent via UDP to a StatsD server for aggregation.
    """

    def __init__(self, client):
        self.client = client

    def startProtocol(self):
        """Connect to destination host."""
        self.client.connect(self.transport)

    def stopProtocol(self):
        """Connection was lost."""
        self.client.disconnect()


class TwistedStatsDClient(object):

    def __init__(self, host, port, connect_callback=None,
                 disconnect_callback=None):
        """
        Build a connection that reports to the endpoint (on C{host} and
        C{port}) using UDP.

        @param host: The StatsD server host.
        @param port: The StatsD server port.
        @param connect_callback: The callback to invoke on connection.
        @param disconnect_callback: The callback to invoke on disconnection.
        """

        # Twisted currently does not offer an asynchronous
        # getaddrinfo-like functionality
        # (http://twistedmatrix.com/trac/ticket/4362).
        # See UdpStatsDClient.
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
        self.transport = None

    def write(self, data):
        """Send the metric to the StatsD server.
                
        @param data: The data to be sent.
        @raise twisted.internet.error.MessageLength: If the size of data
            is too great.
        """
        if self.transport is not None:
            try:
                return self.transport.write(data, (self.host, self.port))
            except (OverflowError, TypeError, socket.error, socket.gaierror):
                return None


class UdpStatsDClient(object):

    def __init__(self, host=None, port=None):
        """Build a connection that reports to C{host} and C{port})
        using UDP.

        @param host: The StatsD host.
        @param port: The StatsD port.
        @raise ValueError: If the C{host} and C{port} cannot be
            resolved (for the case where they are not C{None}).
        """
        if host is not None and port is not None:
            try:
                socket.getaddrinfo(host, port,
                                   socket.AF_INET, socket.SOCK_DGRAM)
            except (TypeError, socket.error, socket.gaierror):
                raise ValueError("The address cannot be resolved.")

        self.host = host
        self.port = port
        self.socket = None

    def connect(self):
        """Connect to the StatsD server."""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def disconnect(self):
        """Disconnect from the StatsD server."""
        if self.socket is not None:
            self.socket.close()
        self.socket = None

    def write(self, data):
        """Send the metric to the StatsD server."""
        if self.host is None or self.port is None or self.socket is None:
            return
        try:
            return self.socket.sendto(data, (self.host, self.port))
        except (socket.error, socket.herror, socket.gaierror):
            return None


class InternalClient(object):
    """A connection that can be used inside the C{StatsD} daemon itself."""

    def __init__(self, processor):
        """
        A connection that writes directly to the C{MessageProcessor}.
        """
        self._processor = processor

    def write(self, data):
        """Write directly to the C{MessageProcessor}."""
        self._processor.process(data)
