
import getopt
import socket
import sys
import ConfigParser

from twisted.application.internet import TCPClient, UDPServer
from twisted.application.service import MultiService
from twisted.python import usage

from txstatsd.client import InternalClient
from txstatsd.metrics.metrics import Metrics
from txstatsd.server.processor import MessageProcessor
from txstatsd.server.configurableprocessor import ConfigurableMessageProcessor
from txstatsd.server.protocol import (
    GraphiteClientFactory, StatsDServerProtocol)
from txstatsd.report import ReportingService


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
        ["config", "c", None, "Config file to use."]
    ]

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
            # Overridden options have precedence
            if not self.overridden_option(name):
                # Options appends '=' when gathering the parameters
                if (name + '=') in self.longOpt:
                    # Coerce the type if required
                    if name in self._dispatch:
                        value = self._dispatch[name].coerce(value)
                    self[name] = value


class StatsDOptions(OptionsGlue):
    """
    The set of configuration settings for txStatsD.
    """
    optParameters = [
        ["carbon-cache-host", "h", "localhost",
         "The host where carbon cache is listening."],
        ["carbon-cache-port", "p", 2003,
         "The port where carbon cache is listening.", int],
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
         "Produce StatsD-compliant messages.", int]
    ]

    def __init__(self):
        self.config_section = 'statsd'
        super(StatsDOptions, self).__init__()


def createService(options):
    """Create a txStatsD service."""

    service = MultiService()
    service.setName("statsd")

    prefix = options["prefix"]
    if prefix is None:
        prefix = socket.gethostname() + ".statsd"

    if options["statsd-compliance"]:
        processor = MessageProcessor()
        connection = InternalClient(processor)
        metrics = Metrics(connection, namespace=prefix)
    else:
        processor = ConfigurableMessageProcessor(message_prefix=prefix)
        connection = InternalClient(processor)
        metrics = Metrics(connection)

    if options["report"] is not None:
        from txstatsd import process
        from twisted.internet import reactor

        reporting = ReportingService()
        reporting.setServiceParent(service)
        reporting.schedule(
            process.report_reactor_stats(reactor), 10, metrics.gauge)
        reports = [name.strip() for name in options["report"].split(",")]
        for report_name in reports:
            for reporter in getattr(process, "%s_STATS" %
                                    report_name.upper(), ()):
                reporting.schedule(reporter, 10, metrics.gauge)

    # Schedule updates for those metrics expecting to be
    # periodically updated, for example the meter metric.
    metrics_updater = ReportingService()
    metrics_updater.setServiceParent(service)
    metrics_updater.schedule(processor.update_metrics, 5, None)

    factory = GraphiteClientFactory(processor, options["flush-interval"],
                                    prefix=prefix)
    client = TCPClient(options["carbon-cache-host"],
                       options["carbon-cache-port"],
                       factory)
    client.setServiceParent(service)

    statsd_server_protocol = StatsDServerProtocol(
        processor,
        monitor_message=options["monitor-message"],
        monitor_response=options["monitor-response"])
    listener = UDPServer(options["listen-port"], statsd_server_protocol)
    listener.setServiceParent(service)

    return service
