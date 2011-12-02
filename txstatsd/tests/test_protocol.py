"""Tests for the Graphite Protocol classes."""

from twisted.internet import reactor, task
from twisted.test import proto_helpers
from twisted.trial.unittest import TestCase

from txstatsd.server.protocol import GraphiteProtocol, GraphiteClientFactory


class FakeProcessor(object):

    def __init__(self):
        self.sequence = 0

    def flush(self, interval):
        """Always produce a sequence number"""
        self.sequence += 1
        return [("foo.bar", str(self.sequence), 42)]


class FakeTransport(object):

    def __init__(self):
        self.messages = []

    def write(self, data):
        self.messages.append(data)


class TestGraphiteProtocol(TestCase):

    def setUp(self):
        super(TestGraphiteProtocol, self).setUp()
        self.processor = FakeProcessor()
        self.transport = FakeTransport()
        self.clock = task.Clock()
        self.protocol = GraphiteProtocol(self.processor, 1000,
                                         clock=self.clock)
        self.protocol.transport = self.transport
        self.protocol.connected = True

    def test_write_unless_paused(self):
        """
        If the producer isn't paused, then write to the transport. Once the
        producer is paused, nothing is written to the transport anymore.
        """
        self.assertEqual(0, len(self.transport.messages))
        self.clock.advance(1)
        self.assertEqual(2, len(self.transport.messages))
        self.clock.advance(1)
        self.assertEqual(4, len(self.transport.messages))
        self.protocol.pauseProducing()
        self.clock.advance(1)
        self.assertEqual(4, len(self.transport.messages))
        
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
        self.assertEqual(2, len(self.transport.messages))
        # Last message is the message graphite metric.
        self.assertEqual("foo.bar 3 42\n", self.transport.messages[-2])

    def test_stopped_producer_discards_everything(self):
        """
        If the producer is stopped, everything is discarded.
        """
        self.assertEqual(0, len(self.transport.messages))
        self.clock.advance(1)
        self.assertEqual(2, len(self.transport.messages))
        self.protocol.stopProducing()
        self.clock.advance(1)
        self.assertEqual(2, len(self.transport.messages))
        self.clock.advance(1)
        self.assertEqual(2, len(self.transport.messages))


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


class Logger(object):
    def __init__(self):
        self.log = None

    def info(self, message):
        self.log = message


class TestPausedMessagingLogging(TestCase):

    def setUp(self):
        super(TestPausedMessagingLogging, self).setUp()
        self.processor = FakeProcessor()
        self.transport = FakeTransport()
        self.clock = task.Clock()
        self.logger = Logger()
        self.protocol = GraphiteProtocol(self.processor, 1000,
                                         clock=self.clock, logger=self.logger)
        self.protocol.transport = self.transport
        self.protocol.connected = True

    def test_paused_producer_logging(self):
        """Ensure we log the pausing of messaging to Graphite."""
        def logged(message):
            self.assertTrue(self.logger.log.startswith(message))

        self.protocol.stopProducing()
        return task.deferLater(reactor, 0.2, logged,
                               'Paused messaging Graphite')

    def test_resumed_producer_logging(self):
        """Ensure we log the resumption of messaging to Graphite."""
        def logged(message):
            self.assertTrue(self.logger.log.startswith(message))

        self.protocol.resumeProducing()
        return task.deferLater(reactor, 0.2, logged,
                               'Resumed messaging Graphite')
