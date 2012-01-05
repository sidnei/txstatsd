from unittest import TestCase

from txstatsd.server.processor import MessageProcessor
from txstatsd.server.router import Router


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
        self.assertEqual(1, len(processor.counter_metrics))

    def test_receive_counter(self):

        self.router.process("gorets:1|c")
        self.assertEqual(1, len(self.processor.messages))

    def test_any_and_drop(self):
        """
        Any message gets dropped with the drop rule.
        """
        self.update_rules("any => drop")
        self.router.process("gorets:1|c")
        self.assertEqual(0, len(self.processor.messages))
