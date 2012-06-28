# -*- coding: utf-8 *-*
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

import ConfigParser
from cStringIO import StringIO
from twisted.trial.unittest import TestCase

from twisted.plugins.sli_plugin import SLIMetricFactory
from txstatsd.metrics.slimetric import (
    SLIMetricReporter, BetweenCondition, AboveCondition, BelowCondition)
from txstatsd import service
from txstatsd.tests.test_processor import TestMessageProcessor


class TestConditions(TestCase):

    def test_below(self):
        c = BelowCondition(5)
        self.assertEquals(c(2), True)
        self.assertEquals(c(6), False)

    def test_above(self):
        c = AboveCondition(5)
        self.assertEquals(c(2), False)
        self.assertEquals(c(6), True)

    def test_between(self):
        c = BetweenCondition(2.5, 5)
        self.assertEquals(c(2), False)
        self.assertEquals(c(6), False)
        self.assertEquals(c(2.6), True)

    def test_below_linear(self):
        c = BelowCondition(5, 1)
        self.assertEquals(c(5.5, 1), True)
        self.assertEquals(c(6.5, 2), True)
        self.assertEquals(c(8.5, 3), False)

    def test_above_linear(self):
        c = AboveCondition(4, 1)
        self.assertEquals(c(5.5, 1), True)
        self.assertEquals(c(6.5, 2), True)
        self.assertEquals(c(7, 3), False)


class TestParsing(TestCase):
    def setUp(self):
        self.processor = TestMessageProcessor()

    def test_parse(self):
        self.processor.process('txstatsd.tests.users:100|sli')
        self.processor.process('txstatsd.tests.users:100|sli|2')
        self.processor.process('txstatsd.tests.users:error|sli')


class TestMetric(TestCase):
    def setUp(self):
        self.sli = SLIMetricReporter('test', {
                        "red": BelowCondition(5),
                        "yellow": BelowCondition(3)})

    def test_count_all(self):
        self.sli.update(1)
        self.sli.update(1)
        self.assertEquals(self.sli.count, 2)

    def test_count_error(self):
        self.sli.update(1)
        self.sli.update("error")
        self.assertEquals(self.sli.count, 2)
        self.assertEquals(self.sli.error, 1)
        self.assertEquals(self.sli.counts["red"], 1)

    def test_count_threshold(self):
        self.assertEquals(self.sli.count, 0)
        self.assertEquals(self.sli.counts["red"], 0)
        self.assertEquals(self.sli.counts["yellow"], 0)
        for i in range(1, 7):
            self.sli.update(i)
        self.assertEquals(self.sli.count, 6)
        self.assertEquals(self.sli.counts["red"], 4)
        self.assertEquals(self.sli.counts["yellow"], 2)

    def test_reports(self):
        self.test_count_threshold()
        rows = sorted(self.sli.flush(0, 0))
        self.assertEquals(
            [("test.count", 6, 0),
            ("test.count_red", 4, 0),
            ("test.count_yellow", 2, 0),
            ("test.error", 0, 0)],
            rows)

    def test_clear(self):
        self.sli.update(1)
        self.sli.update(1)
        self.assertEquals(self.sli.count, 2)
        self.sli.flush(0, 0)
        self.sli.update(1)
        self.assertEquals(self.sli.count, 1)


class TestMetricLinear(TestCase):
    def setUp(self):
        self.sli = SLIMetricReporter('test', {
                        "red": BelowCondition(5, 1),
                        "yellow": BelowCondition(3, 1)})

    def test_count_threshold(self):
        self.assertEquals(self.sli.count, 0)
        self.assertEquals(self.sli.counts["red"], 0)
        self.assertEquals(self.sli.counts["yellow"], 0)
        for i in range(1, 7):
            self.sli.update(7, i)
        self.assertEquals(self.sli.count, 6)
        self.assertEquals(self.sli.counts["red"], 4)
        self.assertEquals(self.sli.counts["yellow"], 2)


class TestFactory(TestCase):
    def test_configure(self):
        class TestOptions(service.OptionsGlue):
            optParameters = [["test", "t", "default", "help"]]
            config_section = "statsd"

        o = TestOptions()
        config_file = ConfigParser.RawConfigParser()
        config_file.readfp(StringIO("[statsd]\n\n[plugin_sli]\n"
            "rules = \n"
            "   test_o-k => red IF below 5\n"
            "   test_o-k => green IF between 0.1 3\n"
            "   other* => red IF above 4\n"))
        o.configure(config_file)
        smf = SLIMetricFactory()
        smf.configure(o)
        smr = smf.build_metric("", "test_o-k")
        rc = smr.conditions["red"]
        self.assertTrue(isinstance(rc, BelowCondition))
        self.assertEquals(rc.value, 5)
        gc = smr.conditions["green"]
        self.assertTrue(isinstance(gc, BetweenCondition))
        self.assertEquals(gc.hi, 3)
        self.assertEquals(gc.low, 0.1)
        smr = smf.build_metric("", "otherXX")
        rc = smr.conditions["red"]
        self.assertTrue(isinstance(rc, AboveCondition))
        self.assertEquals(rc.value, 4)

    def test_configure_linear(self):
        class TestOptions(service.OptionsGlue):
            optParameters = [["test", "t", "default", "help"]]
            config_section = "statsd"

        o = TestOptions()
        config_file = ConfigParser.RawConfigParser()
        config_file.readfp(StringIO("[statsd]\n\n[plugin_sli]\n"
            "rules = \n"
            "   test => red IF below 5 1\n"
            "   test => green IF above 3 1\n"))
        o.configure(config_file)
        smf = SLIMetricFactory()
        smf.configure(o)
        smr = smf.build_metric("", "test")
        rc = smr.conditions["red"]
        self.assertTrue(isinstance(rc, BelowCondition))
        self.assertEquals(rc.value, 5)
        self.assertEquals(rc.slope, 1)
        rc = smr.conditions["green"]
        self.assertTrue(isinstance(rc, AboveCondition))
        self.assertEquals(rc.value, 3)
        self.assertEquals(rc.slope, 1)
