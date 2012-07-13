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

try:
    import twisted
except ImportError:
    # If twisted is missing, still provide the non-twisted client
    pass
else:
    from txstatsd.protocol import (
        StatsDClientProtocol,
        TwistedStatsDClient,
    )
        
from txstatsd.hashing import ConsistentHashRing


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
