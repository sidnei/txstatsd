# -*- coding: utf-8 *-*
import json

from twisted.application import service, internet
from twisted.web import server, resource, http


class Status(resource.Resource):
    isLeaf = True
    time_high_water = 0.7

    def __init__(self, processor, statsd_service):
        resource.Resource.__init__(self)
        self.processor = processor
        self.statsd_service = statsd_service

    def render_GET(self, request):
        data = dict(flush_time=self.processor.last_flush_duration,
                    process_time=self.processor.last_process_duration,
                    flush_interval=self.statsd_service.flush_interval)
        if data["flush_interval"] * self.time_high_water < (
                data["process_time"] + data["flush_time"]):
            data["status"] = "ERROR"
            request.setResponseCode(http.INTERNAL_SERVER_ERROR)
            return json.dumps(data)
        data["status"] = "OK"
        return json.dumps(data)


class Metrics(resource.Resource):

    def __init__(self, processor):
        resource.Resource.__init__(self)
        self.processor = processor

    def getChild(self, name, request):
        metric = self.processor.timer_metrics.get(name, None) or \
                self.processor.plugin_metrics.get(name, None)
        if metric is None:
            return resource.NoResource()

        meth = getattr(metric, "getResource", None)

        if meth is None:
            return resource.NoResource()

        return meth()


def makeService(options, processor, statsd_service):

    if options["http-port"] is None:
        return service.MultiService()

    root = resource.Resource()
    root.putChild("status", Status(processor, statsd_service))
    root.putChild("metrics", Metrics(processor))
    site = server.Site(root)
    s = internet.TCPServer(int(options["http-port"]), site)
    return s
