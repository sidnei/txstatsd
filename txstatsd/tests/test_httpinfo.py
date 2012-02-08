# -*- coding: utf-8 *-*
import json

from twisted.trial.unittest import TestCase

from twisted.internet import reactor, defer, protocol
from twisted.application.service import IService
from twisted.web.client import Agent

from txstatsd.metrics.timermetric import TimerMetricReporter
from txstatsd.server import httpinfo
from txstatsd import service


class Dummy:
    flush_interval = 10
    last_flush_duration = 3
    last_process_duration = 2


class ResponseCollector(protocol.Protocol):

    def __init__(self, finished):
        self.finished = finished
        self.data = []

    def dataReceived(self, bytes):
        self.data.append(bytes)

    def connectionLost(self, reason):
        self.finished.callback("".join(self.data))


def collect_response(response):
    d = defer.Deferred()
    c = ResponseCollector(d)
    response.deliverBody(c)
    return d


class HttpException(Exception):

    def __init__(self, response):
        super(HttpException, self).__init__(response.phrase)
        self.response = response


class ServiceTestsBuilder(TestCase):

    def setUp(self):
        self.service = None

    @defer.inlineCallbacks
    def get_results(self, path, **kwargs):
        webport = 12323
        o = service.StatsDOptions()
        o["http-port"] = webport
        d = Dummy()
        d.__dict__.update(kwargs)
        self.service = s = httpinfo.makeService(o, d, d)
        s.startService()
        agent = Agent(reactor)

        result = yield agent.request('GET',
            'http://localhost:%s/%s' % (webport, path))
        if result.code != 200:
            raise HttpException(result)
        data = yield collect_response(result)
        defer.returnValue(data)

    def tearDown(self):
        if self.service is not None:
            self.service.stopService()

    @defer.inlineCallbacks
    def test_httpinfo_ok(self):
        data = yield self.get_results("status")
        self.assertEquals(json.loads(data)["status"], "OK")

    @defer.inlineCallbacks
    def test_httpinfo_error(self):
        try:
            data = yield self.get_results("status", last_flush_duration=30)
        except HttpException, e:
            self.assertEquals(e.response.code, 500)
        else:
            self.fail("Not 500")

    @defer.inlineCallbacks
    def test_httpinfo_timer(self):
        try:
            data = yield self.get_results("metrics/gorets",
                timer_metrics={'gorets': 100})
        except HttpException, e:
            self.assertEquals(e.response.code, 404)
        else:
            self.fail("Not 404")

    @defer.inlineCallbacks
    def test_httpinfo_timer2(self):
        """Returns the canonical empty histogram without data."""
        tmr = TimerMetricReporter('gorets')
        data = yield self.get_results("metrics/gorets",
            timer_metrics={'gorets': tmr})
        self.assertEquals(json.loads(data)["histogram"], [0.] * 10)

    @defer.inlineCallbacks
    def test_httpinfo_timer3(self):
        """Returns a valid histogram with data."""

        tmr = TimerMetricReporter('gorets')
        for i in range(1, 1001):
            tmr.histogram.update(i)
        data = yield self.get_results("metrics/gorets",
            timer_metrics={'gorets': tmr})
        hist = json.loads(data)
        self.assertTrue(isinstance(hist, dict))
        self.assertEquals(sum(hist["histogram"]), 1000)

    @defer.inlineCallbacks
    def test_httpinfo_fake_plugin(self):
        """Also works for plugins."""

        tmr = TimerMetricReporter('gorets')
        data = yield self.get_results("metrics/gorets",
            timer_metrics={}, plugin_metrics={'gorets': tmr})
        hist = json.loads(data)
        self.assertTrue(isinstance(hist, dict))
