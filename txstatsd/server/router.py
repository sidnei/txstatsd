"""
Routes messages to different processors.

Rules are of the form:
    condition => target
Where each on of condition and target are of the form:
    name [arguments]*
And the arguments are dependant on the name.

Each rule is applied to the message on the order they are specified.

Conditions supported:
    any: will match all messages
    metric_type [type]+: will match a metric of any of the types specified
    path_like fnmatch_exp: will match the path against the expression with
        fnmatch.
    not [rule..]: will return the negation of the result of rule


Targets supported:
    drop: will drop the message, stopping any further processing.
    redirect_udp host port: will send to (host, port) by udp
    redirect_tcp host port: will send to (host, port) by tcp
    rewrite pattern repl: will rewrite the path like re.sub
    set_metric_type metric_type: will make the metric of type metric_type

"""
import fnmatch
import re
import time

from zope.interface import implements

from twisted.application.internet import UDPServer
from twisted.application.service import Service
from twisted.internet import interfaces
from twisted.internet.protocol import (
    DatagramProtocol, ReconnectingClientFactory, Protocol)
from twisted.internet import defer
from twisted.python import log

from txstatsd.server.processor import BaseMessageProcessor


class StopProcessingException(Exception):

    pass


class TCPRedirectService(Service):

    def __init__(self, host, port, factory):
        self.host = host
        self.port = port
        self.factory = factory

    def startService(self):
        from twisted.internet import reactor

        reactor.connectTCP(self.host, self.port, self.factory)
        return Service.startService(self)

    def stopService(self):
        self.factory.stopTrying()
        if self.factory.protocol:
            self.factory.protocol.transport.loseConnection()
        return Service.stopService(self)


class TCPRedirectClientFactory(ReconnectingClientFactory):

    def __init__(self, callback=None):
        self.callback = callback
        self.protocol = None

    def buildProtocol(self, addr):
        from twisted.internet import reactor

        self.resetDelay()
        self.protocol = TCPRedirectProtocol()
        if self.callback:
            reactor.callLater(0, self.callback)
            self.callback = None

        return self.protocol

    def write(self, data):
        if self.protocol:
            self.protocol.write(data)


class TCPRedirectProtocol(Protocol):
    """A client protocol for redicting messages over tcp.
    """

    implements(interfaces.IPushProducer)

    def __init__(self):
        self.paused = False
        self.last_paused = None
        self.dropped = 0

    def connectionMade(self):
        """
        A connection has been made, register ourselves as a producer for the
        bound transport.
        """
        self.transport.registerProducer(self, True)

    def pauseProducing(self):
        """Pause producing messages, since the buffer is full."""
        self.last_paused = int(time.time())
        self.paused = True

    stopProducing = pauseProducing

    def resumeProducing(self):
        """We can write to the transport again. Yay!."""
        time_now = int(time.time())
        log("Resumed TCP redirect. "
            "Dropped %s messages during %s seconds ",
                self.dropped, time_now - self.last_paused)
        self.paused = False
        self.dropped = 0
        self.last_paused = None

    def write(self, line):
        if self.paused:
            self.dropped += 1
            return

        if line[-2:] != "\r\n":
            if line[-1] == "\r":
                line += "\n"
            else:
                line += "\r\n"
        self.transport.write(line)


class UDPRedirectProtocol(DatagramProtocol):

    def __init__(self, host, port, callback):
        self.host = host
        self.port = port
        self.callback = callback

    def startProtocol(self):
        self.transport.connect(self.host, self.port)
        self.callback()

    def write(self, data):
        if self.transport is not None:
            self.transport.write(data)


class Router(BaseMessageProcessor):

    def __init__(self, message_processor, rules_config, service=None):
        """Configure a router with rules_config.

        rules_config is a new_line separeted list of rules.
        """
        self.rules_config = rules_config
        self.message_processor = message_processor
        self.flush = message_processor.flush
        self.ready = defer.succeed(None)
        self.service = service
        self.rules = self.build_rules(rules_config)

    def build_condition(self, condition):
        condition_parts = [
            p.strip() for p in condition.split(" ") if p]
        condition_factory = getattr(self,
            "build_condition_" + condition_parts[0], None)
        if condition_factory is None:
            raise ValueError("unknown condition %s" %
                            (condition_parts[0],))
        condition_function = condition_factory(*condition_parts[1:])
        return condition_function

    def build_rules(self, rules_config):
        rules = []
        for line in rules_config.split("\n"):
            if not line:
                continue

            condition, target = line.split("=>")
            condition_function = self.build_condition(condition)

            target_parts = [
                p.strip() for p in target.split(" ") if p]

            target_factory = getattr(self,
                "build_target_" + target_parts[0], None)

            if target_factory is None:
                raise ValueError("unknown target %s" %
                                (target_parts[0],))

            rules.append((
                condition_function,
                target_factory(*target_parts[1:])))
        return rules

    def build_condition_any(self):
        """Returns a condition that always matches."""
        return lambda *args: True

    def build_condition_not(self, *args):
        """
        Returns a condition that negates the condition from its arguments.
        """
        other_condition = self.build_condition(" ".join(args))

        def not_condition(metric_type, key, fields):
            return not other_condition(metric_type, key, fields)
        return not_condition

    def build_condition_metric_type(self, *metric_types):
        """Returns a condition that matched on metric kind."""
        def metric_type_condition(metric_type, key, fields):
            return (metric_type in metric_types)
        return metric_type_condition

    def build_condition_path_like(self, pattern):
        def path_like_condition(metric_type, key, fields):
            return fnmatch.fnmatch(key, pattern)
        return path_like_condition

    def build_target_drop(self):
        """Returns a target that stops the processing of a message."""
        def drop(*args):
            return
        return drop

    def build_target_rewrite(self, pattern, repl, dup=False):
        rexp = re.compile(pattern)

        def rewrite_target(metric_type, key, fields):
            if dup and rexp.match(key) is not None:
                yield metric_type, key, fields
            key = rexp.sub(repl, key)
            yield metric_type, key, fields

        return rewrite_target

    def build_target_set_metric_type(self, metric_type, dup=False):
        def set_metric_type(_, key, fields):
            if dup:
                yield _, key, fields
            yield metric_type, key, fields
        return set_metric_type

    def build_target_redirect_udp(self, host, port):
        if self.service is None:
            return lambda *args: True

        port = int(port)
        d = defer.Deferred()
        self.ready.addCallback(lambda _: d)
        protocol = UDPRedirectProtocol(host, port, lambda: d.callback(None))

        client = UDPServer(0, protocol)
        client.setServiceParent(self.service)

        def redirect_udp_target(metric_type, key, fields):
            message = self.rebuild_message(metric_type, key, fields)
            protocol.write(message)
            yield metric_type, key, fields
        return redirect_udp_target

    def build_target_redirect_tcp(self, host, port):
        if self.service is None:
            return lambda *args: True

        port = int(port)
        d = defer.Deferred()
        self.ready.addCallback(lambda _: d)
        factory = TCPRedirectClientFactory(lambda: d.callback(None))

        redirect_service = TCPRedirectService(host, port, factory)
        redirect_service.setServiceParent(self.service)

        def redirect_tcp_target(metric_type, key, fields):
            message = self.rebuild_message(metric_type, key, fields)
            factory.write(message)
            yield metric_type, key, fields
        return redirect_tcp_target

    def process_message(self, message, metric_type, key, fields):
        metrics = [(metric_type, key, fields)]
        if self.rules:
            pending, metrics = metrics, []
            for condition, target in self.rules:
                if not pending:
                    return
                for metric_type, key, fields in pending:
                    if not condition(metric_type, key, fields):
                        metrics.append((metric_type, key, fields))
                        continue
                    result = target(metric_type, key, fields)
                    if result is not None:
                        metrics.extend(result)
                pending = metrics

        for (metric_type, key, fields) in metrics:
            message = self.rebuild_message(metric_type, key, fields)
            self.message_processor.process_message(message, metric_type,
                                                   key, fields)
