# -*- coding: utf-8 *-*
import fnmatch

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
        for rule in rules.split("\n"):
            head = rule.split("=>")[0].strip()
            self.config.setdefault(head, {})

            body = rule.split("=>")[1].strip()
            label = body.split(" IF ")[0].strip()
            condition = body.split(" IF ")[1].strip()
            condition_name = condition.split(" ")[0].strip()
            condition_params = condition.split(" ")[1:]
            method = getattr(self, "build_" + condition_name, None)
            if method is None:
                raise TypeError("cannot build condition: %s" % condition)

            cobj = method(*condition_params)
            self.config[head][label] = cobj

    def build_above(self, value):
        return AboveCondition(float(value))

    def build_below(self, value):
        return BelowCondition(float(value))

    def build_between(self, low, hi):
        return BetweenCondition(float(low), float(hi))

sli_metric_factory = SLIMetricFactory()
