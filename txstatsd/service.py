import socket
import ConfigParser

from twisted.application.internet import TCPClient, UDPServer
from twisted.application.service import MultiService
from twisted.internet import reactor
from twisted.python import usage, util

from txstatsd import process
from txstatsd.client import InternalClient
from txstatsd.metrics.metrics import Metrics
from txstatsd.server.processor import MessageProcessor
from txstatsd.server.protocol import GraphiteClientFactory, StatsDServerProtocol
from txstatsd.report import ReportingService

_unset = object()


class OptionsGlue(usage.Options):
    """
    Extends usage.Options to also read parameters from a config file.
    """

    optParameters = []

    def __init__(self):
        self.glue_defaults = {}
        self._config_file = None
        config = ["config", "c", None, "Config file to use."]

        new_params = []

        def process_parameter(parameter):
            long, short, default, doc, paramType = util.padTo(5, parameter)
            self.glue_defaults[long] = default
            new_params.append(
                [long, short, _unset, doc, paramType])

        for parameter in self.glue_parameters:
            if parameter[0] == "config" or parameter[1] == "c":
                raise ValueError("the --config/-c parameter is reserved.")
            process_parameter(parameter)
        process_parameter(config)

        # we need to change self.__class__.optParameters as usage.Options
        # will collect the config from there, not self.optParameters:
        # reflect.accumulateClassList(self.__class__, 'optParameters',
        #                            parameters)
        self.__class__.optParameters = new_params

        super(OptionsGlue, self).__init__()

    def __getitem__(self, item):
        result = super(OptionsGlue, self).__getitem__(item)
        if result is not _unset:
            return result

        fname = super(OptionsGlue, self).__getitem__("config")
        if fname is not _unset:
            self._config_file = ConfigParser.RawConfigParser()
            self._config_file.read(fname)

        if self._config_file is not None:
            try:
                result = self._config_file.get("statsd", item)
            except ConfigParser.NoOptionError:
                pass
            else:
                if item in self._dispatch:
                    result = self._dispatch[item].coerce(result)
                return result

        return self.glue_defaults[item]


class StatsDOptions(OptionsGlue):
    """
    The set of configuration settings for txStatsD.
    """
    glue_parameters = [
        ["carbon-cache-host", "h", "localhost",
         "The host where carbon cache is listening."],
        ["carbon-cache-port", "p", 2003,
         "The port where carbon cache is listening.", int],
        ["listen-port", "l", 8125,
         "The UDP port where we will listen.", int],
        ["flush-interval", "i", 60000,
         "The number of milliseconds between each flush.", int],
        ["prefix", "p", None,
         "Prefix to use when reporting stats.", str],
        ["report", "r", None,
         "Which additional stats to report {process|net|io|system}.", str],
        ["monitor-message", "m", "txstatsd ping",
         "Message we expect from monitoring agent.", str],
        ["monitor-response", "o", "txstatsd pong",
         "Response we should send monitoring agent.", str]
    ]


def createService(options):
    """Create a txStatsD service."""

    service = MultiService()
    service.setName("statsd")
    processor = MessageProcessor()
    prefix = options["prefix"]
    if prefix is None:
        prefix = socket.gethostname() + ".statsd"

    connection = InternalClient(processor)
    metrics = Metrics(connection, namespace=prefix)

    if options["report"] is not None:
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

    factory = GraphiteClientFactory(processor, options["flush-interval"])
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
