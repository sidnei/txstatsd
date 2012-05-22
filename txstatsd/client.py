import socket

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.protocol import DatagramProtocol
from twisted.python import log

from txstatsd.hashing import ConsistentHashRing


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

    def __init__(self, host, port,
                 connect_callback=None,
                 disconnect_callback=None,
                 resolver_errback=None):
        """
        Build a connection that reports to the endpoint (on C{host} and
        C{port}) using UDP.

        @param host: The StatsD server host.
        @param port: The StatsD server port.
        @param resolver_errback: The errback to invoke should
            issues occur resolving the supplied C{host}.
        @param connect_callback: The callback to invoke on connection.
        @param disconnect_callback: The callback to invoke on disconnection.
        """
        from twisted.internet import reactor

        self.reactor = reactor

        @inlineCallbacks
        def resolve(host):
            self.host = yield reactor.resolve(host)
            returnValue(self.host)

        self.original_host = host
        self.host = None
        self.resolver = resolve(host)
        if resolver_errback is None:
            self.resolver.addErrback(log.err)
        else:
            self.resolver.addErrback(resolver_errback)

        self.port = port
        self.connect_callback = connect_callback
        self.disconnect_callback = disconnect_callback

        self.transport = None

    def __str__(self):
        return "%s:%d" % (self.original_host, self.port)

    @inlineCallbacks
    def connect(self, transport=None):
        """Connect to the StatsD server."""
        host = yield self.resolver
        if host is not None:
            self.transport = transport
            if self.transport is not None:
                if self.connect_callback is not None:
                    self.connect_callback()

    def disconnect(self):
        """Disconnect from the StatsD server."""
        if self.disconnect_callback is not None:
            self.disconnect_callback()
        self.transport = None

    def write(self, data, callback=None):
        """Send the metric to the StatsD server.

        @param data: The data to be sent.
        @param callback: The callback to which the result should be sent.
            B{Note}: The C{callback} will be called in the C{reactor}
            thread, and not in the thread of the original caller.
        """
        self.reactor.callFromThread(self._write, data, callback)

    def _write(self, data, callback):
        """Send the metric to the StatsD server.

        @param data: The data to be sent.
        @param callback: The callback to which the result should be sent.
        @raise twisted.internet.error.MessageLengthError: If the size of data
            is too large.
        """
        if self.host is not None and self.transport is not None:
            try:
                bytes_sent = self.transport.write(data, (self.host, self.port))
                if callback is not None:
                    callback(bytes_sent)
            except (OverflowError, TypeError, socket.error, socket.gaierror):
                if callback is not None:
                    callback(None)


class UdpStatsDClient(object):

    def __init__(self, host=None, port=None):
        """Build a connection that reports to C{host} and C{port})
        using UDP.

        @param host: The StatsD host.
        @param port: The StatsD port.
        @raise ValueError: If the C{host} and C{port} cannot be
            resolved (for the case where they are not C{None}).
        """
        self.original_host = self.host = host
        self.port = port

        if host is not None and port is not None:
            try:
                self.host, self.port = socket.getaddrinfo(
                    host, port, socket.AF_INET,
                    socket.SOCK_DGRAM, socket.SOL_UDP)[0][4]
            except (TypeError, IndexError, socket.error, socket.gaierror):
                raise ValueError("The address cannot be resolved.")

        self.socket = None

    def __str__(self):
        return "%s:%d" % (self.original_host, self.port)

    def connect(self):
        """Connect to the StatsD server."""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setblocking(0)

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
        except (socket.error, socket.gaierror):
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


class ConsistentHashingClient(object):

    def __init__(self, clients):
        self.ring = ConsistentHashRing(clients)

    def write(self, data):
        """Hash based on the metric name, then send to the right client."""
        metric_name, rest = data.split(":", 1)
        client = self.ring.get_node(metric_name)
        client.write(data)
