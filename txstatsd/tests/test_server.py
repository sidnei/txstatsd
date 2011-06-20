from unittest import TestCase

from txstatsd.server import MessageProcessor


class TestMessageProcessor(MessageProcessor):

    def __init__(self):
        super(TestMessageProcessor, self).__init__()
        self.failures = []

    def fail(self, message):
        self.failures.append(message)


class MessageProcessorTest(TestCase):

    def setUp(self):
        self.processor = TestMessageProcessor()

    def test_receive_counter(self):
        """
        A counter message takes the format 'gorets:1|c', where 'gorets' is the
        identifier and '1' means the counter will be incremented by one
        unit. 'c' is simply used to signal that this is a counter message.
        """
        self.processor.process("gorets:1|c")
        self.assertEqual(1, len(self.processor.counters))
        self.assertEqual(1.0, self.processor.counters["gorets"])

    def test_receive_counter_rate(self):
        """
        A counter message can also take the format 'gorets:1|c|@01', where
        'gorets' is the identifier,'1' means the counter will be incremented by
        one unit and '@0.1' means the sample rate is '0.1'. Effectively, the
        counter will be multiplied by the sample rate to estimate the actual
        counter value.
        """
        self.processor.process("gorets:1|c|@0.1")
        self.assertEqual(1, len(self.processor.counters))
        self.assertEqual(10.0, self.processor.counters["gorets"])

    def test_receive_timer(self):
        """
        A timer message takes the format 'glork:320|ms', where 'glork' is the
        identifier and '320' is the time in milliseconds.
        """
        self.processor.process("glork:320|ms")
        self.assertEqual(1, len(self.processor.timers))
        self.assertEqual([320], self.processor.timers["glork"])

    def test_receive_message_no_fields(self):
        """
        If a timer message has no fields, it is logged and discarded.
        """
        self.processor.process("glork")
        self.assertEqual(0, len(self.processor.timers))
        self.assertEqual(0, len(self.processor.counters))
        self.assertEqual(["glork"], self.processor.failures)

    def test_receive_counter_no_value(self):
        """
        If a counter message has no value, it is logged and discarded.
        """
        self.processor.process("gorets:|c")
        self.assertEqual(0, len(self.processor.counters))
        self.assertEqual(["gorets:|c"], self.processor.failures)

    def test_receive_timer_no_value(self):
        """
        If a timer message has no value, it is logged and discarded.
        """
        self.processor.process("glork:|ms")
        self.assertEqual(0, len(self.processor.timers))
        self.assertEqual(["glork:|ms"], self.processor.failures)

    def test_receive_not_enough_fields(self):
        """
        If a timer message has not enough fields, it is logged and discarded.
        """
        self.processor.process("glork:1")
        self.assertEqual(0, len(self.processor.timers))
        self.assertEqual(0, len(self.processor.counters))
        self.assertEqual(["glork:1"], self.processor.failures)

    def test_receive_too_many_fields(self):
        """
        If a timer message has too many fields, it is logged and discarded.
        """
        self.processor.process("glork:1|c|@0.1|yay")
        self.assertEqual(0, len(self.processor.timers))
        self.assertEqual(0, len(self.processor.counters))
        self.assertEqual(["glork:1|c|@0.1|yay"], self.processor.failures)
