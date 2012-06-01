# -*- coding: utf-8 *-*
import ConfigParser
from cStringIO import StringIO
from twisted.trial.unittest import TestCase

from twisted.plugins.sli_plugin import SLIMetricFactory
from txstatsd.metrics.slimetric import (
    SLIMetricReporter, BetweenCondition, AboveCondition, BelowCondition)
from txstatsd import service


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


class TestMetric(TestCase):
    def setUp(self):
        self.sli = SLIMetricReporter('test', {
                        "red": BelowCondition(5),
                        "yellow": BelowCondition(3)})

    def test_count_all(self):
        self.sli.update(1)
        self.sli.update(1)
        self.assertEquals(self.sli.count, 2)

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
            ("test.count_yellow", 2, 0)],
            rows)


class TestFactory(TestCase):
    def test_configure(self):
        class TestOptions(service.OptionsGlue):
            optParameters = [["test", "t", "default", "help"]]
            config_section = "statsd"

        o = TestOptions()
        config_file = ConfigParser.RawConfigParser()
        config_file.readfp(StringIO("[statsd]\n\n[plugin_sli]\n"
            "rules = \n"
            "   test => red IF below 5\n"
            "   test => green IF between 0.1 3\n"
            "   other => red IF above 4\n"))
        o.configure(config_file)
        smf = SLIMetricFactory()
        smf.configure(o)
        smr = smf.build_metric("", "test")
        rc = smr.conditions["red"]
        self.assertTrue(isinstance(rc, BelowCondition))
        self.assertEquals(rc.value, 5)
        gc = smr.conditions["green"]
        self.assertTrue(isinstance(gc, BetweenCondition))
        self.assertEquals(gc.hi, 3)
        self.assertEquals(gc.low, 0.1)
        smr = smf.build_metric("", "other")
        rc = smr.conditions["red"]
        self.assertTrue(isinstance(rc, AboveCondition))
        self.assertEquals(rc.value, 4)
