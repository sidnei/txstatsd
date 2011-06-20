import re
import logging

from twisted.python import log
from twisted.internet.protocol import DatagramProtocol


SPACES = re.compile("\s+")
SLASHES = re.compile("\/+")
NON_ALNUM = re.compile("[^a-zA-Z_\-0-9\.]")
RATE = re.compile("^@([\d\.]+)")


def normalize_key(key):
    """
    Normalize a key that might contain spaces, forward-slashes and other
    special characters into something that is acceptable by graphite.
    """
    key = SPACES.sub("_", key)
    key = SLASHES.sub("-", key)
    key = NON_ALNUM.sub("", key)
    return key


class MessageProcessor(object):

    def __init__(self):
        self.timers = {}
        self.counters = {}

    def fail(self, message):
        """Log and discard malformed message."""
        log.msg("Bad line: %r" % message, logLevel=logging.DEBUG)

    def process(self, message):
        """
        Process a single entry, adding it to either C{counters} or C{timers}
        depending on which kind of message it is.
        """
        if not ":" in message:
            return self.fail(message)

        key, data = message.strip().split(":", 1)
        if not "|" in data:
            return self.fail(message)

        fields = data.split("|")
        if len(fields) < 2 or len(fields) > 3 :
            return self.fail(message)

        try:
            value = int(fields[0])
        except (TypeError, ValueError):
            return self.fail(message)

        key = normalize_key(key)

        if fields[1] == "c":
            rate = 1
            if len(fields) == 3:
                match = RATE.match(fields[2])
                if match is None:
                    return self.fail(message)
                rate = match.group(1)
            if key not in self.counters:
                self.counters[key] = 0
            self.counters[key] += value * (1 / float(rate))
        elif fields[1] == "ms":
            if key not in self.timers:
                self.timers[key] = []
            self.timers[key].append(value)
        else:
            return self.fail(message)


class StatsD(DatagramProtocol):
    """A Twisted-based implementation of the StatsD server.

    Data is received via UDP for local aggregation and then sent to a graphite
    server via TCP.
    """

    def datagramReceived(self, data, (host, port)):
        """Process received data and store it locally."""
