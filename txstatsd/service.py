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

import getopt
import sys
import time
import ConfigParser
import platform
import functools

from twisted.application.internet import UDPServer, TCPServer
from twisted.application.service import MultiService
from twisted.python import usage, log
from twisted.plugin import getPlugins

from txstatsd.client import InternalClient
from txstatsd.metrics.metrics import Metrics
from txstatsd.metrics.extendedmetrics import ExtendedMetrics
from txstatsd.server.processor import MessageProcessor
from txstatsd.server.configurableprocessor import ConfigurableMessageProcessor
from txstatsd.server.loggingprocessor import LoggingMessageProcessor
from txstatsd.server.protocol import (
    StatsDServerProtocol, StatsDTCPServerFactory)
from txstatsd.server.router import Router
from txstatsd.server import httpinfo
from txstatsd.report import ReportingService
from txstatsd.itxstatsd import IMetricFactory
from twisted.application.service import Service
from twisted.internet import task


def accumulateClassList(classObj, attr, listObj,
                        baseClass=None, excludeClass=None):
    """Accumulate all attributes of a given name in a class hierarchy
    into a single list.

    Assuming all class attributes of this name are lists.
    """
    for base in classObj.__bases__:
        accumulateClassList(base, attr, listObj, excludeClass=excludeClass)
    if excludeClass != classObj:
        if baseClass is None or baseClass in classObj.__bases__:
            listObj.extend(classObj.__dict__.get(attr, []))


class OptionsGlue(usage.Options):
    """Extends usage.Options to also read parameters from a config file."""

    optParameters = [
        ["config", "c", None, "Config file to use."]]

    def __init__(self):
        parameters = []
        accumulateClassList(self.__class__, 'optParameters',
                            parameters, excludeClass=OptionsGlue)
        for parameter in parameters:
            if parameter[0] == "config" or parameter[1] == "c":
                raise ValueError("the --config/-c parameter is reserved.")

        self.overridden_options = []

        super(OptionsGlue, self).__init__()

    def opt_config(self, config_path):
        self['config'] = config_path

    opt_c = opt_config

    def parseOptions(self, options=None):
        """Obtain overridden options."""

        if options is None:
            options = sys.argv[1:]
        try:
            opts, args = getopt.getopt(options,
                                       self.shortOpt, self.longOpt)
        except getopt.error, e:
            raise usage.UsageError(str(e))

        for opt, arg in opts:
            if opt[1] == '-':
                opt = opt[2:]
            else:
                opt = opt[1:]
            self.overridden_options.append(opt)

        super(OptionsGlue, self).parseOptions(options=options)

    def postOptions(self):
        """Read the configuration file if one is provided."""
        if self['config'] is not None:
            config_file = ConfigParser.RawConfigParser()
            config_file.read(self['config'])

            self.configure(config_file)

    def overridden_option(self, opt):
        """Return whether this option was overridden."""
        return opt in self.overridden_options

    def configure(self, config_file):
        """Read the configuration items, coercing types as required."""
        for name, value in config_file.items(self.config_section):
            self._coerce_option(name, value)

        for section in sorted(config_file.sections()):
            if section.startswith("plugin_"):
                self[section] = config_file.items(section)
            if section.startswith("carbon-cache"):
                for name, value in config_file.items(section):
                    self._coerce_option(name, value)

    def _coerce_option(self, name, value):
        """Coerce a single option, checking for overriden options."""
        # Overridden options have precedence
        if not self.overridden_option(name):
            # Options appends '=' when gathering the parameters
            if (name + '=') in self.longOpt:
                # Coerce the type if required
                if name in self._dispatch:
                    if isinstance(self._dispatch[name], usage.CoerceParameter):
                        value = self._dispatch[name].coerce(value)
                    else:
                        self._dispatch[name](name, value)
                        return
                self[name] = value


class StatsDOptions(OptionsGlue):
    """
    The set of configuration settings for txStatsD.
    """

    optParameters = [
        ["carbon-cache-host", "h", None,
         "The host where carbon cache is listening.", str],
        ["carbon-cache-port", "p", None,
         "The port where carbon cache is listening.", int],
        ["carbon-cache-name", "n", None,
         "An identifier for the carbon-cache instance."],
        ["listen-port", "l", 8125,
         "The UDP port where we will listen.", int],
        ["flush-interval", "i", 60000,
         "The number of milliseconds between each flush.", int],
        ["prefix", "x", None,
         "Prefix to use when reporting stats.", str],
        ["instance-name", "N", None,
         "Instance name for our own stats reporting.", str],
        ["report", "r", None,
         "Which additional stats to report {process|net|io|system}.", str],
        ["monitor-message", "m", "txstatsd ping",
         "Message we expect from monitoring agent.", str],
        ["monitor-response", "o", "txstatsd pong",
         "Response we should send monitoring agent.", str],
        ["statsd-compliance", "s", 1,
         "Produce StatsD-compliant messages.", int],
        ["dump-mode", "d", 0,
         "Dump received and aggregated metrics"
         " before passing them to carbon.", int],
        ["routing", "g", "",
         "Routing rules", str],
        ["listen-tcp-port", "t", None,
         "The TCP port where we will listen.", int],
        ["max-queue-size", "Q", 20000,
         "Maximum send queue size per destination.", int],
        ["max-datapoints-per-message", "M", 1000,
         "Maximum datapoints per message to carbon-cache.", int],
        ["http-port", "P", None,
         "The httpinfo port.", int],
        ]

    def __init__(self):
        self.config_section = 'statsd'
        super(StatsDOptions, self).__init__()
        self["carbon-cache-host"] = []
        self["carbon-cache-port"] = []
        self["carbon-cache-name"] = []

    def opt_carbon_cache_host(self, host):
        self["carbon-cache-host"].append(host)

    def opt_carbon_cache_port(self, port):
        self["carbon-cache-port"].append(usage.portCoerce(port))

    def opt_carbon_cache_name(self, name):
        self["carbon-cache-name"].append(name)


class StatsDService(Service):

    def __init__(self, carbon_client, processor, flush_interval, clock=None):
        self.carbon_client = carbon_client
        self.processor = processor
        self.flush_interval = flush_interval
        self.flush_task = task.LoopingCall(self.flushProcessor)
        if clock is not None:
            self.flush_task.clock = clock

    def flushProcessor(self):
        """Flush messages queued in the processor to Graphite."""
        flushed = 0
        start = time.time()
        for metric, value, timestamp in self.processor.flush(
                interval=self.flush_interval):
            self.carbon_client.sendDatapoint(metric, (timestamp, value))
            flushed += 1
        log.msg("Flushed total %d metrics in %.6f" %
                (flushed, time.time() - start))

    def startService(self):
        self.flush_task.start(self.flush_interval / 1000, False)

    def stopService(self):
        if self.flush_task.running:
            self.flush_task.stop()


def report_client_manager_stats():
    from carbon.instrumentation import stats

    current_stats = stats.copy()
    for name, value in list(current_stats.items()):
        del current_stats[name]
        if name.startswith("destinations"):
            current_stats[name.replace(":", "_")] = value
        stats[name] = 0
    return current_stats


def createService(options):
    """Create a txStatsD service."""
    from carbon.routers import ConsistentHashingRouter
    from carbon.client import CarbonClientManager
    from carbon.conf import settings

    settings.MAX_QUEUE_SIZE = options["max-queue-size"]
    settings.MAX_DATAPOINTS_PER_MESSAGE = options["max-datapoints-per-message"]

    root_service = MultiService()
    root_service.setName("statsd")

    prefix = options["prefix"]
    if prefix is None:
        prefix = "statsd"

    instance_name = options["instance-name"]
    if not instance_name:
        instance_name = platform.node()

    # initialize plugins
    plugin_metrics = []
    for plugin in getPlugins(IMetricFactory):
        plugin.configure(options)
        plugin_metrics.append(plugin)

    processor = None
    if options["dump-mode"]:
        # LoggingMessageProcessor supersedes
        #  any other processor class in "dump-mode"
        assert not hasattr(log, 'info')
        log.info = log.msg # for compatibility with LMP logger interface
        processor = functools.partial(LoggingMessageProcessor, logger=log)

    if options["statsd-compliance"]:
        processor = (processor or MessageProcessor)(plugins=plugin_metrics)
        input_router = Router(processor, options['routing'], root_service)
        connection = InternalClient(input_router)
        metrics = Metrics(connection)
    else:
        processor = (processor or ConfigurableMessageProcessor)(
            message_prefix=prefix,
            internal_metrics_prefix=prefix + "." + instance_name + ".",
            plugins=plugin_metrics)
        input_router = Router(processor, options['routing'], root_service)
        connection = InternalClient(input_router)
        metrics = ExtendedMetrics(connection)

    if not options["carbon-cache-host"]:
        options["carbon-cache-host"].append("127.0.0.1")
    if not options["carbon-cache-port"]:
        options["carbon-cache-port"].append(2004)
    if not options["carbon-cache-name"]:
        options["carbon-cache-name"].append(None)

    reporting = ReportingService(instance_name)
    reporting.setServiceParent(root_service)

    reporting.schedule(report_client_manager_stats,
                       options["flush-interval"] / 1000,
                       metrics.gauge)

    if options["report"] is not None:
        from txstatsd import process
        from twisted.internet import reactor

        reporting.schedule(
            process.report_reactor_stats(reactor), 60, metrics.gauge)
        reports = [name.strip() for name in options["report"].split(",")]
        for report_name in reports:
            for reporter in getattr(process, "%s_STATS" %
                                    report_name.upper(), ()):
                reporting.schedule(reporter, 60, metrics.gauge)

    # XXX Make this configurable.
    router = ConsistentHashingRouter()
    carbon_client = CarbonClientManager(router)
    carbon_client.setServiceParent(root_service)

    for host, port, name in zip(options["carbon-cache-host"],
                                options["carbon-cache-port"],
                                options["carbon-cache-name"]):
        carbon_client.startClient((host, port, name))

    statsd_service = StatsDService(carbon_client, input_router,
                                   options["flush-interval"])
    statsd_service.setServiceParent(root_service)

    statsd_server_protocol = StatsDServerProtocol(
        input_router,
        monitor_message=options["monitor-message"],
        monitor_response=options["monitor-response"])

    listener = UDPServer(options["listen-port"], statsd_server_protocol)
    listener.setServiceParent(root_service)

    if options["listen-tcp-port"] is not None:
        statsd_tcp_server_factory = StatsDTCPServerFactory(
                input_router,
                monitor_message=options["monitor-message"],
                monitor_response=options["monitor-response"])

        listener = TCPServer(options["listen-tcp-port"],
            statsd_tcp_server_factory)
        listener.setServiceParent(root_service)

    httpinfo_service = httpinfo.makeService(options, processor, statsd_service)
    httpinfo_service.setServiceParent(root_service)

    return root_service
