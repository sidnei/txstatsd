
import getopt
import socket
import sys
import ConfigParser

from twisted.application.internet import UDPServer
from twisted.application.service import MultiService
from twisted.python import usage
from twisted.plugin import getPlugins

from txstatsd.client import InternalClient
from txstatsd.metrics.metrics import Metrics
from txstatsd.server.processor import MessageProcessor
from txstatsd.server.configurableprocessor import ConfigurableMessageProcessor
from txstatsd.server.protocol import StatsDServerProtocol
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

        for section in config_file.sections():
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
        ["report", "r", None,
         "Which additional stats to report {process|net|io|system}.", str],
        ["monitor-message", "m", "txstatsd ping",
         "Message we expect from monitoring agent.", str],
        ["monitor-response", "o", "txstatsd pong",
         "Response we should send monitoring agent.", str],
        ["statsd-compliance", "s", 1,
         "Produce StatsD-compliant messages.", int],
        ["max-queue-size", "Q", 1000,
         "Maximum send queue size per destination.", int],
        ["max-datapoints-per-message", "M", 500,
         "Maximum datapoints per message to carbon-cache.", int],
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
        for message in self.processor.flush(interval=self.flush_interval):
            # XXX This is nasty. We should instead change 'flush' to not build up
            # strings so that we can use the pickle-based protocol with less
            # overhead.
            for line in filter(None, message.splitlines()):
                metric, value, timestamp = line.split()
                self.carbon_client.sendDatapoint(metric, (timestamp, value))

    def startService(self):
        self.flush_task.start(self.flush_interval / 1000, False)

    def stopService(self):
        if self.flush_task.running:
            self.flush_task.stop()


def report_client_manager_stats():
    from carbon.instrumentation import stats

    current_stats = stats.copy()
    stats.clear()
    for name in list(current_stats.keys()):
        if not name.startswith("destinations"):
            del current_stats[name]
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
        prefix = socket.gethostname() + ".statsd"

    # initialize plugins
    plugin_metrics = []
    for plugin in getPlugins(IMetricFactory):
        plugin.configure(options)
        plugin_metrics.append(plugin)

    if options["statsd-compliance"]:
        processor = MessageProcessor(plugins=plugin_metrics)
        connection = InternalClient(processor)
        metrics = Metrics(connection, namespace=prefix)
    else:
        processor = ConfigurableMessageProcessor(message_prefix=prefix,
                                                 plugins=plugin_metrics)
        connection = InternalClient(processor)
        metrics = Metrics(connection)

    if not options["carbon-cache-host"]:
        options["carbon-cache-host"].append("127.0.0.1")
    if not options["carbon-cache-port"]:
        options["carbon-cache-port"].append(2004)
    if not options["carbon-cache-name"]:
        options["carbon-cache-name"].append(None)

    reporting = ReportingService()
    reporting.setServiceParent(root_service)

    # Schedule updates for those metrics expecting to be
    # periodically updated, for example the meter metric.
    reporting.schedule(processor.update_metrics, 5, None)
    reporting.schedule(report_client_manager_stats, 10, metrics.gauge)

    if options["report"] is not None:
        from txstatsd import process
        from twisted.internet import reactor

        reporting.schedule(
            process.report_reactor_stats(reactor), 10, metrics.gauge)
        reports = [name.strip() for name in options["report"].split(",")]
        for report_name in reports:
            for reporter in getattr(process, "%s_STATS" %
                                    report_name.upper(), ()):
                reporting.schedule(reporter, 10, metrics.gauge)

    # XXX Make this configurable.
    router = ConsistentHashingRouter()
    carbon_client = CarbonClientManager(router)
    carbon_client.setServiceParent(root_service)

    for host, port, name in zip(options["carbon-cache-host"],
                                options["carbon-cache-port"],
                                options["carbon-cache-name"]):
        carbon_client.startClient((host, port, name))

    statsd_service = StatsDService(carbon_client, processor,
                                   options["flush-interval"])
    statsd_service.setServiceParent(root_service)

    statsd_server_protocol = StatsDServerProtocol(
        processor,
        monitor_message=options["monitor-message"],
        monitor_response=options["monitor-response"])
    listener = UDPServer(options["listen-port"], statsd_server_protocol)
    listener.setServiceParent(root_service)

    return root_service
