from zope.interface import implements

from twisted.plugin import IPlugin
from twisted.application.service import IServiceMaker

from txstatsd.server import service


class StatsDServiceMaker(object):
    implements(IServiceMaker, IPlugin)
    tapname = "statsd"
    description = "Collect and aggregate stats for graphite."
    options = service.StatsDOptions

    def makeService(self, options):
        """
        Construct a StatsD service.
        """
        return service.createService(options)

# Now construct an object which *provides* the relevant interfaces
serviceMaker = StatsDServiceMaker()
