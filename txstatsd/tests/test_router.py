from unittest import TestCase

from twisted.internet.protocol import DatagramProtocol, Factory
from twisted.protocols.basic import LineReceiver
from twisted.application.service import MultiService
from twisted.internet import reactor, defer
from twisted.trial.unittest import TestCase as TxTestCase

from txstatsd.server.processor import MessageProcessor
from txstatsd.server.router import Router

from twisted.internet.base import DelayedCall
DelayedCall.debug = True


class TestMessageProcessor(object):

    def __init__(self):
        self.messages = []

    def process_message(self, *args):
        self.messages.append(args)


class RouteMessagesTest(TestCase):

    def setUp(self):
        self.processor = TestMessageProcessor()
        self.router = Router(self.processor, "")

    def update_rules(self, rules_config):
        self.router.rules = self.router.build_rules(rules_config)

    def test_message_processor_integration(self):
        """
        A message gets routed to the processor.
        """
        processor = MessageProcessor()
        router = Router(processor, "")
        router.process("gorets:1|c")
        self.assertEqual(len(processor.counter_metrics), 1)

    def test_receive_counter(self):

        self.router.process("gorets:1|c")
        self.assertEqual(len(self.processor.messages), 1)

    def test_any_and_drop(self):
        """
        Any message gets dropped with the drop rule.
        """
        self.update_rules("any => drop")
        self.router.process("gorets:1|c")
        self.assertEqual(len(self.processor.messages), 0)

    def test_metric_path_like(self):
        """
        Any message gets dropped with the drop rule.
        """
        self.update_rules("path_like goret* => drop")
        self.router.process("gorets:1|c")
        self.router.process("gorets:1|d")
        self.router.process("goret:1|d")
        self.router.process("nomatch:1|d")
        self.assertEqual(len(self.processor.messages), 1)
        self.assertEqual(self.processor.messages[0][2], "nomatch")

    def test_not(self):
        """
        Any message gets dropped with the drop rule.
        """
        self.update_rules("not path_like goret* => drop")
        self.router.process("gorets:1|c")
        self.router.process("nomatch:1|d")
        self.assertEqual(len(self.processor.messages), 1)
        self.assertEqual(self.processor.messages[0][2], "gorets")

    def test_rewrite(self):
        """
        Any message gets dropped with the drop rule.
        """
        self.update_rules(r"any => rewrite (gorets) glork.\1")
        self.router.process("gorets:1|c")
        self.router.process("nomatch:1|d")
        self.assertEqual(len(self.processor.messages), 2)
        self.assertEqual(self.processor.messages[0][2], "glork.gorets")
        self.assertEqual(self.processor.messages[1][2], "nomatch")


class TestUDPRedirect(TxTestCase):

    def setUp(self):
        self.service = MultiService()
        self.received = []

        class Collect(DatagramProtocol):

            def datagramReceived(cself, data, (host, port)):
                self.got_data(data)

        self.port = reactor.listenUDP(0, Collect())

        self.processor = TestMessageProcessor()
        self.router = Router(self.processor,
            r"any => redirect_udp 127.0.0.1 %s" %
            (self.port.getHost().port,),
            service=self.service)
        self.service.startService()
        return self.router.ready

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.service.stopService()
        self.port.stopListening()

    def test_redirect(self):
        """
        Any message gets dropped with the drop rule.
        """
        message = "gorets:1|c"
        d = defer.Deferred()

        def got_data(data):
            self.assertEqual(data, message)
            d.callback(True)
        self.got_data = got_data
        self.router.process(message)
        return d


class TestTCPRedirect(TestUDPRedirect):

    def setUp(self):
        self.service = MultiService()
        self.received = []

        class Collect(LineReceiver):

            def lineReceived(cself, data):
                self.got_data(data)

        class CollectFactory(Factory):

            def buildProtocol(self, addr):
                return Collect()

        self.port = reactor.listenTCP(0, CollectFactory())

        self.processor = TestMessageProcessor()
        self.router = Router(self.processor,
            r"any => redirect_tcp 127.0.0.1 %s" %
            (self.port.getHost().port,),
            service=self.service)
        self.service.startService()
        return self.router.ready
