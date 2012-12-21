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

from mocker import Mocker, expect
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
    ConsistentHashingClient
)
from txstatsd.protocol import DataQueue, TransportGateway


class FakeClient(object):

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.data = []
        self.connect_called = False
        self.disconnect_called = False

    def __str__(self):
        return "%s:%d" % (self.host, self.port)

    def write(self, data):
        self.data.append(data)

    def connect(self):
        self.connect_called = True

    def disconnect(self):
        self.disconnect_called = True


class TestClient(TestCase):

    def setUp(self):
        super(TestClient, self).setUp()
        self.client = None
        self.exception = None
        self.mocker = Mocker()

    def tearDown(self):
        if self.client:
            self.client.transport.stopListening()
        super(TestClient, self).tearDown()

    def build_protocol(self):
        protocol = StatsDClientProtocol(self.client)
        reactor.listenUDP(0, protocol)

    def test_twistedstatsd_write(self):
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
    def test_twistedstatsd_write_with_host_resolved(self):
        self.client = yield TwistedStatsDClient.create(
            'localhost', 8000)
        protocol = StatsDClientProtocol(self.client)
        reactor.listenUDP(0, protocol)

        def ensure_bytes_sent(bytes_sent):
            self.assertEqual(bytes_sent, len('message'))
            self.assertEqual(self.client.host, '127.0.0.1')

        def exercise(callback):
            self.client.write('message', callback=callback)

        d = Deferred()
        d.addCallback(ensure_bytes_sent)
        reactor.callWhenRunning(exercise, d.callback)
        yield d

    @inlineCallbacks
    def test_twistedstatsd_with_malformed_address_and_errback(self):
        def ensure_exception_raised(exception):
            self.assertTrue(exception.startswith("DNS lookup failed"))

        def capture_exception_raised(failure):
            exception = failure.getErrorMessage()
            self.deferred_instance.callback(exception)

        self.deferred_instance = TwistedStatsDClient.create(
            '256.0.0.0', 1,
            resolver_errback=capture_exception_raised)

        self.deferred_instance.addCallback(ensure_exception_raised)
        yield self.deferred_instance

    @inlineCallbacks
    def test_twistedstatsd_with_malformed_address_and_no_errback(self):
        def ensure_exception_raised(exception):
            self.assertTrue(exception.startswith("DNS lookup failed"))

        def capture_exception_raised(failure):
            exception = failure.getErrorMessage()
            self.deferred_instance.callback(exception)

        self.patch(log, "err", capture_exception_raised)

        self.deferred_instance = TwistedStatsDClient.create(
            '256.0.0.0', 1)

        self.deferred_instance.addCallback(ensure_exception_raised)
        yield self.deferred_instance

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
            reload(txstatsd.client)
            reload(txstatsd.metrics.metrics)
            reload(txstatsd.metrics.metric)
        self.addCleanup(restore_modules)

        # Mark everything twistedish as unavailable
        for name, mod in unloaded:
            sys.modules[name] = None

        reload(txstatsd.client)
        reload(txstatsd.metrics.metrics)
        reload(txstatsd.metrics.metric)
        for mod in sys.modules:
            if 'twisted' in mod:
                self.assertTrue(sys.modules[mod] is None)

    def test_starts_with_data_queue(self):
        """The client starts with a DataQueue."""
        self.client = TwistedStatsDClient('127.0.0.1', 8000)
        self.build_protocol()

        self.assertIsInstance(self.client.data_queue, DataQueue)

    def test_starts_with_transport_gateway(self):
        """The client starts with a TransportGateway."""
        self.client = TwistedStatsDClient('127.0.0.1', 8000)

        self.assertTrue(self.client.transport_gateway is None)

        self.build_protocol()

        self.assertIsInstance(self.client.transport_gateway, TransportGateway)

    def test_passes_transport_to_gateway(self):
        """The client passes the transport to the gateway as soon as the client
        is connected."""
        self.client = TwistedStatsDClient('127.0.0.1', 8000)
        self.build_protocol()

        self.assertEqual(self.client.transport_gateway.transport,
                         self.client.transport)

    def test_passes_reactor_to_gateway(self):
        """The client passes the reactor to the gateway as soon as the client
        is connected."""
        self.client = TwistedStatsDClient('127.0.0.1', 8000)
        self.build_protocol()

        self.assertEqual(self.client.transport_gateway.reactor,
                         self.client.reactor)


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

    def test_connect_with_two_clients(self):
        clients = [
            FakeClient("127.0.0.1", 10001),
            FakeClient("127.0.0.1", 10002),
            ]
        client = ConsistentHashingClient(clients)
        client.connect()
        self.assertTrue(clients[0].connect_called)
        self.assertTrue(clients[1].connect_called)

    def test_disconnect_with_two_clients(self):
        clients = [
            FakeClient("127.0.0.1", 10001),
            FakeClient("127.0.0.1", 10002),
            ]
        client = ConsistentHashingClient(clients)
        client.disconnect()
        self.assertTrue(clients[0].disconnect_called)
        self.assertTrue(clients[1].disconnect_called)
