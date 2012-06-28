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
"""Tests for the Metrics convenience class."""

import re
import time
from unittest import TestCase
from txstatsd.metrics.extendedmetrics import ExtendedMetrics
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
        self.metrics.timing('timing', 101.1234)
        self.assertEqual(self.connection.data,
                         'txstatsd.tests.timing:101123.4|ms')

    def test_timing_automatic(self):
        """Test the automatic timing operation with explicit reset"""
        start_time = time.time()

        self.metrics.reset_timing()
        time.sleep(.1)
        self.metrics.timing('timing')

        elapsed = time.time() - start_time

        label, val, units = re.split(":|\|", self.connection.data)
        self.assertEqual(label, 'txstatsd.tests.timing')
        self.assertEqual(units, 'ms')
        self.assertTrue(100 <= float(val) <= elapsed * 1000)

    def test_timing_automatic_implicit_reset(self):
        """Test the automatic timing operation with implicit reset"""
        start_time = time.time()

        self.metrics.timing('something_else')
        time.sleep(.1)
        self.metrics.timing('timing')

        elapsed = time.time() - start_time

        label, val, units = re.split(":|\|", self.connection.data)
        self.assertEqual(label, 'txstatsd.tests.timing')
        self.assertEqual(units, 'ms')
        self.assertTrue(100 <= float(val) <= elapsed * 1000)

    def test_generic(self):
        """Test the GenericMetric class."""
        self.metrics.report('users', "pepe", "pd")
        self.assertEqual(self.connection.data,
                         'txstatsd.tests.users:pepe|pd')

    def test_generic_extra(self):
        """Test the GenericMetric class."""
        self.metrics.report('users', "pepe", "pd", 100)
        self.assertEqual(self.connection.data,
                         'txstatsd.tests.users:pepe|pd|100')

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


class TestExtendedMetrics(TestMetrics):
    def setUp(self):
        super(TestExtendedMetrics, self).setUp()
        self.metrics = ExtendedMetrics(self.connection, 'txstatsd.tests')

    def test_counter(self):
        """Test the increment and decrement operations."""
        self.metrics.increment('counter', 18)
        self.assertEqual(self.connection.data,
                         'txstatsd.tests.counter:18|c')
        self.metrics.decrement('counter', 9)
        self.assertEqual(self.connection.data,
                         'txstatsd.tests.counter:9|c')

    def test_sli(self):
        """Test SLI call."""
        self.metrics.sli('users', 100)
        self.assertEqual(self.connection.data,
                         'txstatsd.tests.users:100|sli')

        self.metrics.sli('users', 200, 2)
        self.assertEqual(self.connection.data,
                         'txstatsd.tests.users:200|sli|2')

        self.metrics.sli_error('users')
        self.assertEqual(self.connection.data,
                         'txstatsd.tests.users:error|sli')
