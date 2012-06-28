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
        if prefix:
            if not prefix[-1] == ".":
                prefix = prefix + "."
            path = prefix + name
        else:
            path = name
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
        regexp = "([\w\.\*\?\_\-]+) => (\w+) IF (\w+)(.*)"
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

    def build_above(self, value, slope=0):
        return AboveCondition(float(value), float(slope))

    def build_below(self, value, slope=0):
        return BelowCondition(float(value), float(slope))

    def build_between(self, low, hi):
        return BetweenCondition(float(low), float(hi))

sli_metric_factory = SLIMetricFactory()
