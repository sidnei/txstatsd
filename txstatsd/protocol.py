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

from twisted.internet.defer import inlineCallbacks, returnValue
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
