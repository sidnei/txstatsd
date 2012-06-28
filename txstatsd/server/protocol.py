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

from twisted.internet.protocol import (
    DatagramProtocol, Factory)
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

