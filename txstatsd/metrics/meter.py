import time
import socket
import random


class BaseMeter(object):

    def __init__(self, prefix="", sample_rate=1):
        """Base implementation for a C{StatsD} client utility.

        All networking logic is left for the subclasses.

        prefix: a string to prepend to all metrics. useful to insert stuff like
            server name.
        sample_rate: only write C{sample_rate} percent of messages to the wire.
        """
        self.prefix = prefix
        self.sample_rate = sample_rate

    def increment(self, name, count=1):
        """Report and increase in name by count."""
        self.send("%s:%s|c" % (name, count))

    def decrement(self, name, count=1):
        """Report and decrease in name by count."""
        self.increment(name, count * -1)

    def done(self, name):
        """Report that name completed successfully."""
        self.increment(name + ".done")

    def error(self, name):
        """Report that name failed."""
        self.increment(name + ".error")

    def timing(self, name, duration):
        """Report that name took duration ms."""
        self.send("%s:%s|ms" % (name, duration))

    def send(self, data):
        """Send out C{data} to C{StatsD} if over C{sample_rate}."""

        if self.sample_rate < 1:
            if random.random() > self.sample_rate:
                return
            data += "|@%s" % (self.sample_rate,)

        if self.prefix:
            data = self.prefix + "." + data

        self.write(data)

    def write(self, data):
        """Write out C{data} to the network."""
        raise NotImplementedError()


class InProcessMeter(BaseMeter):
    """A meter that can be used inside the C{StatsD} daemon itself."""

    def __init__(self, processor, prefix="", sample_rate=1):
        self.processor = processor
        BaseMeter.__init__(self, prefix=prefix, sample_rate=sample_rate)

    def write(self, data):
        """Pass the data along directly to the C{Processor}."""
        self.processor.process(data)


class Meter(BaseMeter):
    """A trivial, non-Twisted-dependent meter."""

    def __init__(self, host=None, port=None, prefix="", sample_rate=1):
        """Build a meter that reports to host:port over udp.

        host: statsd host
        port: statsd port
        prefix: a string to prepend to all metrics. useful to insert stuff like
            server name.
        sample_rate: only write C{sample_rate} percent of messages to the wire.
        """
        BaseMeter.__init__(self, prefix=prefix, sample_rate=sample_rate)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.addr = (host, port)

    def write(self, data):
        """Write out C{data} to the network."""
        if self.addr is None or self.socket is None:
            return
        self.socket.sendto(data, self.addr)

    def close(self):
        """Close the socket."""
        self.socket.close()
        self.socket = None


class Measure(object):
    """Context Manager for generic measuring."""

    def __init__(self, prefix, operation_name, meter=None):
        if meter is None:
            meter = Meter(prefix)
        self.meter = meter
        self.operation_name = operation_name
        self.before = None

    def __enter__(self):
        """Increase operation meter."""
        self.before = time.time()
        self.meter.increment(self.operation_name)

    def __exit__(self, exception_type, value, traceback):
        """
        Record time since __enter__, increase either the .done or the
        .error meter, and decrease the operation meter.

        """
        if exception_type is None:
            self.meter.timing(self.operation_name, time.time() - self.before)
            self.meter.increment(self.operation_name + '.done')
        else:
            self.meter.increment(self.operation_name + '.error')
        self.meter.decrement(self.operation_name)


class TransportMeter(BaseMeter):

    transport = None
    host = None
    port = None

    def __init__(self, prefix="", sample_rate=1,
                 connect_callback=None, disconnect_callback=None):
        self.connect_callback = connect_callback
        self.disconnect_callback = disconnect_callback
        BaseMeter.__init__(self, prefix=prefix, sample_rate=sample_rate)

    def connect(self, transport, host, port):
        self.transport = transport
        self.host = host
        self.port = port
        if self.connect_callback is not None:
            self.connect_callback()

    def disconnect(self):
        if self.disconnect_callback is not None:
            self.disconnect_callback()
        self.transport = self.host = self.port = None

    def write(self, data):
        """Send metrics to the StatsD server using the transport."""
        if self.transport is not None:
            self.transport.write(data, (self.host, self.port))
