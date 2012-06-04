# -*- coding: utf-8 *-*
import fnmatch
import re

from zope.interface import implements

from twisted.plugin import IPlugin
from txstatsd.itxstatsd import IMetricFactory
from txstatsd.metrics.slimetric import (
    SLIMetricReporter, BetweenCondition, AboveCondition, BelowCondition)


class SLIMetricFactory(object):
    implements(IMetricFactory, IPlugin)

    name = "SLI"
    metric_type = "sli"

    def __init__(self):
        self.config = {}

    def build_metric(self, prefix, name, wall_time_func=None):
        path = prefix + name
        result = {}
        for pattern, conditions in self.config.items():
            if fnmatch.fnmatch(path, pattern):
                result.update(conditions)
        return SLIMetricReporter(path, result)

    def configure(self, options):
        self.section = dict(options.get("plugin_sli", {}))

        rules = self.section.get("rules", None)

        if rules is None:
            return

        rules = rules.strip()
        regexp = "([\w\.\*\?]+) => (\w+) IF (\w+)(.*)"
        mo = re.compile(regexp)
        for line_no, rule in enumerate(rules.split("\n")):

            result = mo.match(rule)
            if result is None:
                raise TypeError("Did not match rule spec: %s (rule %d: %s)"
                    % (regexp, line_no, rule))

            head, label, cname, cparams = result.groups()
            cparams = cparams[1:]

            self.config.setdefault(head, {})

            method = getattr(self, "build_" + cname, None)
            if method is None:
                raise TypeError("cannot build condition: %s %s" % (
                    cname, cparams))

            cobj = method(*cparams.split(" "))
            self.config[head][label] = cobj

    def build_above(self, value):
        return AboveCondition(float(value))

    def build_below(self, value):
        return BelowCondition(float(value))

    def build_between(self, low, hi):
        return BetweenCondition(float(low), float(hi))

sli_metric_factory = SLIMetricFactory()
