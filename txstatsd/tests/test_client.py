"""Tests for the various client classes."""

from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.trial.unittest import TestCase

from txstatsd.client import (
    StatsDClientProtocol, TwistedStatsDClient, UdpStatsDClient)


class TestClient(TestCase):

    def setUp(self):
        super(TestClient, self).setUp()
        self.client = None

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

    def test_twistedstatsd_write_with_malformed_address(self):
        self.client = TwistedStatsDClient('256.0.0.0', 1)
        protocol = StatsDClientProtocol(self.client)
        reactor.listenUDP(0, protocol)

        def ensure_bytes_sent(bytes_sent):
            self.assertEqual(bytes_sent, None)

        def exercise(callback):
            self.client.write('message', callback=callback)

        d = Deferred()
        d.addCallback(ensure_bytes_sent)
        reactor.callWhenRunning(exercise, d.callback)
        return d

    def test_udpstatsd_wellformed_address(self):
        client = UdpStatsDClient('localhost', 8000)
        self.assertEqual(client.host, 'localhost')
        client = UdpStatsDClient(None, None)
        self.assertEqual(client.host, None)

    def test_udpstatsd_malformed_address(self):
        self.assertRaises(ValueError,
                          UdpStatsDClient, 'localhost', -1)
        self.assertRaises(ValueError,
                          UdpStatsDClient, 'localhost', 'malformed')
        self.assertRaises(ValueError,
                          UdpStatsDClient, 0, 8000)
