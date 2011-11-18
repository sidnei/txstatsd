from zope.interface import implements

from twisted.plugin import IPlugin
from txstatsd.itxstatsd import IMetricFactory
from txstatsd.metrics.distinctmetric import DistinctMetricReporter

class DistinctMetricFactory(object):
    implements(IMetricFactory, IPlugin)
    
    name = "pdistinct"
    metric_kind_key = "pd"
    
    def build_metric(self, prefix, name, wall_time_func=None):
        return DistinctMetricReporter(name, prefix=prefix,
                                      wall_time_func=wall_time_func)
        
    def configure(self, options):
        pass
        
distinct_metric_factory = DistinctMetricFactory()


        