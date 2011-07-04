from zope.interface import implements

from twisted.plugin import IPlugin
from twisted.application.service import IServiceMaker
from twisted.application import internet

from txstatsd import service


class TxStatsdServiceMaker(object):
    implements(IServiceMaker, IPlugin)
    tapname = "txstatsd"
    description = "Collect and aggregate stats for graphite."
    options = service.StatsdOptions

    def makeService(self, options):
        """
        Construct a txStatsD service.
        """
        return service.createService(options)

# Now construct an object which *provides* the relevant interfaces
serviceMaker = TxStatsdServiceMaker()