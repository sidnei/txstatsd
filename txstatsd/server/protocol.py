import time

from zope.interface import implements

from twisted.internet import interfaces, reactor, task
from twisted.internet.protocol import (
    DatagramProtocol, ReconnectingClientFactory, Protocol)

from txstatsd.metrics.countermetric import CounterMetricReporter


class StatsDServerProtocol(DatagramProtocol):
    """A Twisted-based implementation of the StatsD server.

    Data is received via UDP for local aggregation and then sent to a Graphite
    server via TCP.
    """

    def __init__(self, processor,
                 monitor_message=None, monitor_response=None):
        self.processor = processor
        self.monitor_message = monitor_message
        self.monitor_response = monitor_response

    def datagramReceived(self, data, (host, port)):
        """Process received data and store it locally."""
        if data == self.monitor_message:
            # Send the expected response to the
            # monitoring agent.
            self.transport.write(self.monitor_response, (host, port))
        else:
            self.processor.process(data)


class GraphiteProtocol(Protocol):
    """A client protocol for talking to Graphite.

    Messages to Graphite are line-based and C{\n}-separated.
    """

    implements(interfaces.IPushProducer)

    def __init__(self, processor, interval, clock=None,
                logger=None, prefix=''):
        self.paused = False
        self.processor = processor
        self.interval = interval

        if logger is not None:
            logger_info = getattr(logger, 'info', None)
            if logger_info is None or not callable(logger_info):
                raise TypeError("logger missing callable info attribute")
        self.logger = logger

        self.flush_task = task.LoopingCall(self.flushProcessor)
        if clock is not None:
            self.flush_task.clock = clock
        self.flush_task.start(self.interval / 1000, False)

        # Initial state represents being able to message Graphite.
        self.message_graphite_metric = CounterMetricReporter(
            'message.graphite', prefix)
        self.message_graphite_metric.mark(1)
        self.pause_began = None

    def connectionMade(self):
        """
        A connection has been made, register ourselves as a producer for the
        bound transport.
        """
        self.transport.registerProducer(self, True)

    def flushProcessor(self):
        """Flush messages queued in the processor to Graphite."""
        for message in self.processor.flush(interval=self.interval):
            if self.connected and not self.paused:
                self.transport.write(message)

        self.flush_message_graphite_metric()

    def flush_message_graphite_metric(self):
        """Record whether we are paused or not."""
        if self.connected and not self.paused:
            if self.pause_began is None:
                timestamp = int(time.time())
            else:
                timestamp = int(self.pause_began)
            self.transport.write(
                self.message_graphite_metric.report(timestamp))
            self.message_graphite_metric.mark(1)
            self.pause_began = None
        else:
            if self.pause_began is None:
                self.pause_began = time.time()
            self.message_graphite_metric.mark(0)

    def pauseProducing(self):
        """Pause producing messages, since the buffer is full."""
        time_now = int(time.time())
        self.log('Paused messaging Graphite ' + str(time_now))
        self.paused = True

    stopProducing = pauseProducing

    def resumeProducing(self):
        """We can write to the transport again. Yay!."""
        time_now = int(time.time())
        self.log('Resumed messaging Graphite ' + str(time_now))
        self.paused = False

    def log(self, message):
        if self.logger is not None:
            # Ensure the logging is performed on some other thread.
            reactor.callInThread(self.logger.info, message)


class GraphiteClientFactory(ReconnectingClientFactory):
    """A reconnecting Graphite client."""

    def __init__(self, processor, interval, logger=None, prefix=''):
        self.processor = processor
        self.interval = interval
        self.prefix = prefix
        self.logger = logger

    def buildProtocol(self, addr):
        """
        Build a new instance of the L{Graphite} protocol, bound to the
        L{MessageProcessor}.
        """
        self.resetDelay()
        protocol = GraphiteProtocol(self.processor, self.interval,
                                    logger=self.logger, prefix=self.prefix)
        protocol.factory = self
        return protocol
