"""Tests for the Graphite Protocol classes."""

from twisted.trial.unittest import TestCase

from txstatsd.server.protocol import GraphiteProtocol, GraphiteClientFactory
from twisted.internet import task
from twisted.test import proto_helpers


class FakeProcessor(object):

    def __init__(self):
        self.sequence = 0

    def flush(self, interval):
        """Always produce a sequence number followed by 9 lines of output"""
        self.sequence += 1
        return [str(self.sequence)]


class FakeTransport(object):

    def __init__(self):
        self.messages = []

    def writeSequence(self, sequence):
        data, separator = sequence
        self.messages.append(data)


class TestGraphiteProtocol(TestCase):

    def setUp(self):
        super(TestGraphiteProtocol, self).setUp()
        self.processor = FakeProcessor()
        self.transport = FakeTransport()
        self.clock = task.Clock()
        self.protocol = GraphiteProtocol(self.processor, 1000, clock=self.clock)
        self.protocol.transport = self.transport
        self.protocol.connected = True

    def test_write_unless_paused(self):
        """
        If the producer isn't paused, then write to the transport. Once the
        producer is paused, nothing is written to the transport anymore.
        """
        self.assertEqual(0, len(self.transport.messages))
        self.clock.advance(1)
        self.assertEqual(1, len(self.transport.messages))
        self.clock.advance(1)
        self.assertEqual(2, len(self.transport.messages))
        self.protocol.pauseProducing()
        self.clock.advance(1)
        self.assertEqual(2, len(self.transport.messages))
        
    def test_paused_producer_discards_everything_until_resumed(self):
        """
        If the producer is paused, everything is discarded until the producer
        is resumed.
        """
        self.protocol.pauseProducing()
        self.assertEqual(0, len(self.transport.messages))
        self.clock.advance(1)
        self.assertEqual(0, len(self.transport.messages))
        self.clock.advance(1)
        self.assertEqual(0, len(self.transport.messages))
        self.protocol.resumeProducing()
        self.clock.advance(1)
        self.assertEqual(1, len(self.transport.messages))
        self.assertEqual("3", self.transport.messages[-1])

    def test_stopped_producer_discards_everything(self):
        """
        If the producer is stopped, everything is discarded.
        """
        self.assertEqual(0, len(self.transport.messages))
        self.clock.advance(1)
        self.assertEqual(1, len(self.transport.messages))
        self.protocol.stopProducing()
        self.clock.advance(1)
        self.assertEqual(1, len(self.transport.messages))
        self.clock.advance(1)
        self.assertEqual(1, len(self.transport.messages))


class TestProducerRegistration(TestCase):

    def test_register_producer(self):
        """
        The Graphite protocol client registers itself as a producer of the
        transport it's connected to.
        """
        processor = FakeProcessor()

        # We don't care about the address argument here.
        client = GraphiteClientFactory(processor, 1).buildProtocol(None)
        clientTransport = proto_helpers.StringTransport()
        client.makeConnection(clientTransport)
        
        # check that the producer is registered
        self.assertTrue(clientTransport.producer is client)

        # check the streaming attribute
        self.assertTrue(clientTransport.streaming)
        client.flush_task.stop()
