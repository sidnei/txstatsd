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
"""Tests for the various client classes."""

import sys
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, Deferred
from twisted.python import log
from twisted.trial.unittest import TestCase

import txstatsd.client
import txstatsd.metrics.metric
import txstatsd.metrics.metrics
from txstatsd.metrics.metric import Metric
from txstatsd.client import (
    StatsDClientProtocol, TwistedStatsDClient, UdpStatsDClient,
    ConsistentHashingClient)


class FakeClient(object):

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.data = []

    def __str__(self):
        return "%s:%d" % (self.host, self.port)

    def write(self, data):
        self.data.append(data)


class TestClient(TestCase):

    def setUp(self):
        super(TestClient, self).setUp()
        self.client = None
        self.exception = None

    def tearDown(self):
        if self.client:
            self.client.transport.stopListening()
        super(TestClient, self).tearDown()

    def test_twistedstatsd_write_with_wellformed_address(self):
        self.client = TwistedStatsDClient('127.0.0.1', 8000)
        protocol = StatsDClientProtocol(self.client)
        reactor.listenUDP(0, protocol)

        def ensure_bytes_sent(bytes_sent):
            self.assertEqual(bytes_sent, len('message'))

        def exercise(callback):
            self.client.write('message', callback=callback)

        d = Deferred()
        d.addCallback(ensure_bytes_sent)
        reactor.callWhenRunning(exercise, d.callback)
        return d

    @inlineCallbacks
    def test_twistedstatsd_with_malformed_address_and_errback(self):
        def ensure_exception_raised(ignore):
            self.assertTrue(self.exception.startswith("DNS lookup failed"))

        def capture_exception_raised(failure):
            self.exception = failure.getErrorMessage()

        yield TwistedStatsDClient(
            '256.0.0.0', 1,
            resolver_errback=capture_exception_raised)

        d = Deferred()
        d.addCallback(ensure_exception_raised)
        reactor.callLater(.5, d.callback, None)
        yield d

    @inlineCallbacks
    def test_twistedstatsd_with_malformed_address_and_no_errback(self):
        def ensure_exception_raised(ignore):
            self.assertTrue(self.exception.startswith("DNS lookup failed"))

        def capture_exception_raised(failure):
            self.exception = failure.getErrorMessage()

        self.patch(log, "err", capture_exception_raised)

        yield TwistedStatsDClient('256.0.0.0', 1)

        d = Deferred()
        d.addCallback(ensure_exception_raised)
        reactor.callLater(.5, d.callback, None)
        yield d

    def test_udpstatsd_wellformed_address(self):
        client = UdpStatsDClient('localhost', 8000)
        self.assertEqual(client.host, '127.0.0.1')
        client = UdpStatsDClient(None, None)
        self.assertEqual(client.host, None)

    def test_udpstatsd_malformed_address(self):
        self.assertRaises(ValueError,
                          UdpStatsDClient, 'localhost', -1)
        self.assertRaises(ValueError,
                          UdpStatsDClient, 'localhost', 'malformed')
        self.assertRaises(ValueError,
                          UdpStatsDClient, 0, 8000)

    def test_udpstatsd_socket_nonblocking(self):
        client = UdpStatsDClient('localhost', 8000)
        client.connect()
        # According to the python docs (and the source, I've checked)
        # setblocking(0) is the same as settimeout(0.0).
        self.assertEqual(client.socket.gettimeout(), 0.0)

    def test_udp_client_can_be_imported_without_twisted(self):
        """Ensure that the twisted-less client can be used without twisted."""
        unloaded = [(name, mod) for (name, mod) in sys.modules.items()
                    if 'twisted' in name]
        def restore_modules():
            for name, mod in unloaded:
                sys.modules[name] = mod
        self.addCleanup(restore_modules)

        # Mark everything twistedish as unavailable
        for name, mod in unloaded:
            sys.modules[name] = None

        reload(txstatsd.client)
        reload(txstatsd.metrics.metrics)
        reload(txstatsd.metrics.metric)
        for mod in sys.modules:
            if 'twisted' in mod:
                self.assertIsNone(sys.modules[mod])


class TestConsistentHashingClient(TestCase):

    def test_hash_with_single_client(self):
        clients = [
            FakeClient("127.0.0.1", 10001),
            ]
        client = ConsistentHashingClient(clients)
        bar = Metric(client, "bar")
        foo = Metric(client, "foo")
        dba = Metric(client, "dba")
        bar.send("1")
        foo.send("1")
        dba.send("1")
        self.assertEqual(clients[0].data, ["bar:1",
                                           "foo:1",
                                           "dba:1"])

    def test_hash_with_two_clients(self):
        clients = [
            FakeClient("127.0.0.1", 10001),
            FakeClient("127.0.0.1", 10002),
            ]
        client = ConsistentHashingClient(clients)
        bar = Metric(client, "bar")
        foo = Metric(client, "foo")
        dba = Metric(client, "dba")
        bar.send("1")
        foo.send("1")
        dba.send("1")
        self.assertEqual(clients[0].data, ["bar:1",
                                           "dba:1"])
        self.assertEqual(clients[1].data, ["foo:1"])

    def test_hash_with_three_clients(self):
        clients = [
            FakeClient("127.0.0.1", 10001),
            FakeClient("127.0.0.1", 10002),
            FakeClient("127.0.0.1", 10003),
            ]
        client = ConsistentHashingClient(clients)
        bar = Metric(client, "bar")
        foo = Metric(client, "foo")
        dba = Metric(client, "dba")
        bar.send("1")
        foo.send("1")
        dba.send("1")
        self.assertEqual(clients[0].data, ["bar:1"])
        self.assertEqual(clients[1].data, ["foo:1"])
        self.assertEqual(clients[2].data, ["dba:1"])
