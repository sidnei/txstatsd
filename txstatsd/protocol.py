# Copyright (C) 2011-2012 Canonical Services Ltd
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import socket

from twisted.internet.defer import inlineCallbacks, returnValue, Deferred
from twisted.internet.protocol import DatagramProtocol
from twisted.python import log


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


class DataQueue(object):
    """Manages the queue of sent data, so that it can be really sent later when
    the host is resolved."""

    LIMIT = 1000

    def __init__(self):
        self._queue = []

    def write(self, data, callback):
        """Queue the given data, so that it's sent later."""
        if len(self._queue) < self.LIMIT:
            self._queue.append((data, callback))

    def flush(self):
        """Flush the queue, returning its items."""
        items = self._queue
        self._queue = []
        return items


class TransportGateway(object):
    """Responsible for sending datagrams to the actual transport."""

    def __init__(self, transport, reactor):
        self.transport = transport
        self.reactor = reactor

    def write(self, data, callback):
        """Writes the data to the transport."""
        self.reactor.callFromThread(self._write, data, callback)

    def _write(self, data, callback):
        """Send the metric to the StatsD server.

        @param data: The data to be sent.
        @param callback: The callback to which the result should be sent.
        @raise twisted.internet.error.MessageLengthError: If the size of data
            is too large.
        """
        try:
            bytes_sent = self.transport.write(data)
            if callback is not None:
                callback(bytes_sent)
        except (OverflowError, TypeError, socket.error, socket.gaierror):
            if callback is not None:
                callback(None)


class TwistedStatsDClient(object):

    def __init__(self, host, port, connect_callback=None,
                 disconnect_callback=None):
        """Avoid using this initializer directly; Instead, use the create()
        static method, otherwise the messages won't be really delivered.

        If you still need to use this directly and want to resolve the host
        yourself, remember to call host_resolved() as soon as it's resolved.

        @param host: The StatsD server host.
        @param port: The StatsD server port.
        @param connect_callback: The callback to invoke on connection.
        @param disconnect_callback: The callback to invoke on disconnection.
        """
        from twisted.internet import reactor

        self.reactor = reactor

        self.host = host
        self.port = port
        self.connect_callback = connect_callback
        self.disconnect_callback = disconnect_callback
        self.data_queue = DataQueue()

        self.transport = None
        self.transport_gateway = None

    def __str__(self):
        return "%s:%d" % (self.host, self.port)

    @staticmethod
    def create(host, port, connect_callback=None, disconnect_callback=None,
               resolver_errback=None):
        """Create an instance that resolves the host to an IP asynchronously.

        Will queue all messages while the host is not yet resolved.

        Build a connection that reports to the endpoint (on C{host} and
        C{port}) using UDP.

        @param host: The StatsD server host.
        @param port: The StatsD server port.
        @param resolver_errback: The errback to invoke should
            issues occur resolving the supplied C{host}.
        @param connect_callback: The callback to invoke on connection.
        @param disconnect_callback: The callback to invoke on disconnection."""
        from twisted.internet import reactor

        instance = TwistedStatsDClient(
            host=host, port=port, connect_callback=connect_callback,
            disconnect_callback=disconnect_callback)

        if resolver_errback is None:
            resolver_errback = log.err

        instance.resolve_later = reactor.resolve(host)
        instance.resolve_later.addCallbacks(instance.host_resolved,
                                            resolver_errback)

        return instance

    def connect(self, transport=None):
        """Connect to the StatsD server."""
        if transport is not None:
            self.transport = transport

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
        if self.transport_gateway is not None:
            return self.transport_gateway.write(data, callback)
        return self.data_queue.write(data, callback)

    def host_resolved(self, ip):
        """Callback used when the host is resolved to an IP address."""
        self.host = ip
        self.transport.connect(self.host, self.port)
        self.transport_gateway = TransportGateway(self.transport, self.reactor)

        if self.connect_callback is not None:
            self.connect_callback()

        self._flush_items()

    def _flush_items(self):
        """Flush all items (data, callback) from the DataQueue to the
        TransportGateway."""
        for item in self.data_queue.flush():
            data, callback = item
            self.transport_gateway.write(data, callback)
