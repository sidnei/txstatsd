from twisted.application.service import Application

from txstatsd import service

application = Application("statsd")

statsd_service = service.createService(service.StatsDOptions())
statsd_service.setServiceParent(application)
