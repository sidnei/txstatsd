"""Tests for the Metrics convenience class."""

from unittest import TestCase

from txstatsd.metrics.metrics import Metrics


class FakeStatsDClient(object):

    def connect(self):
        """Connect to the StatsD server."""
        pass

    def disconnect(self):
        """Disconnect from the StatsD server."""
        pass

    def write(self, data):
        """Send the metric to the StatsD server."""
        self.data = data


class TestMetrics(TestCase):

    def setUp(self):
        self.connection = FakeStatsDClient()
        self.metrics = Metrics(self.connection, 'txstatsd.tests')

    def test_gauge(self):
        """Test reporting of a gauge metric sample."""
        self.metrics.gauge('gauge', 102)
        self.assertEqual(self.connection.data,
                         'txstatsd.tests.gauge:102|g')

    def test_meter(self):
        """Test reporting of a meter metric sample."""
        self.metrics.meter('meter', 3)
        self.assertEqual(self.connection.data,
                         'txstatsd.tests.meter:3|m')

    def test_counter(self):
        """Test the increment and decrement operations."""
        self.metrics.increment('counter', 18)
        self.assertEqual(self.connection.data,
                         'txstatsd.tests.counter:18|c')
        self.metrics.decrement('counter', 9)
        self.assertEqual(self.connection.data,
                         'txstatsd.tests.counter:-9|c')

    def test_timing(self):
        """Test the timing operation."""
        self.metrics.timing('timing', 101123)
        self.assertEqual(self.connection.data,
                         'txstatsd.tests.timing:101123|ms')

    def test_generic(self):
        """Test the GenericMetric class."""
        self.metrics.report('users', "pepe", "pd")
        self.assertEqual(self.connection.data,
                         'txstatsd.tests.users:pepe|pd')

    def test_empty_namespace(self):
        """Test reporting of an empty namespace."""
        self.metrics.namespace = None
        self.metrics.gauge('gauge', 213)
        self.assertEqual(self.connection.data,
                         'gauge:213|g')

        self.metrics.namespace = ''
        self.metrics.gauge('gauge', 413)
        self.assertEqual(self.connection.data,
                         'gauge:413|g')
