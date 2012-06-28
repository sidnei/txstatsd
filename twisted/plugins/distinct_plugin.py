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

from zope.interface import implements

from twisted.plugin import IPlugin
from txstatsd.itxstatsd import IMetricFactory
from txstatsd.metrics.distinctmetric import DistinctMetricReporter


class DistinctMetricFactory(object):
    implements(IMetricFactory, IPlugin)

    name = "pdistinct"
    metric_type = "pd"

    def build_metric(self, prefix, name, wall_time_func=None):
        return DistinctMetricReporter(name, prefix=prefix,
                                      wall_time_func=wall_time_func)

    def configure(self, options):
        pass

distinct_metric_factory = DistinctMetricFactory()
