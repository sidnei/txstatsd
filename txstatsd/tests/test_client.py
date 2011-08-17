"""Tests for the various client classes."""

from twisted.internet.test.reactormixins import ReactorBuilder

from txstatsd.client import (
    StatsDClientProtocol, TwistedStatsDClient, UdpStatsDClient)


class ClientTestsBuilder(ReactorBuilder):

    def test_twistedstatsd_write_with_wellformed_address(self):
        client = TwistedStatsDClient('localhost', 8000)
        protocol = StatsDClientProtocol(client)
        reactor = self.buildReactor()
        reactor.listenUDP(0, protocol)

        self.assertEqual(client.write('message'), len('message'))

    def test_twistedstatsd_write_with_malformed_address(self):
        client = TwistedStatsDClient('localhost', -1)
        protocol = StatsDClientProtocol(client)
        reactor = self.buildReactor()
        reactor.listenUDP(0, protocol)

        self.assertEqual(client.write('message'), None)

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

globals().update(ClientTestsBuilder.makeTestCaseClasses())
