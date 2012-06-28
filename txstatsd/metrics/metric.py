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

import random


class Metric(object):
    """
    The foundation metric from which the specialized
    metrics are derived.
    """

    def __init__(self, connection, name, sample_rate=1):
        """Construct a metric that reports samples to the supplied
        C{connection}.

        @param connection: The connection endpoint representing
            the StatsD server.
        @param name: Specific description for this metric.
        @param sample_rate: Restrict the number of samples sent
            to the StatsD server based on the supplied C{sample_rate}.
        """
        self.connection = connection
        self.name = name
        self.sample_rate = sample_rate

    def clear(self):
        """Responsibility of the specialized metrics."""
        pass

    def send(self, data):
        """
        Message the C{data} to the C{StatsD} server according to the
        C{sample_rate}.
        """

        if self.sample_rate < 1:
            if random.random() > self.sample_rate:
                return
            data += "|@%s" % (self.sample_rate,)

        data = self.name + ":" + data

        self.write(data)

    def write(self, data):
        """Message the C{data} to the C{StatsD} server."""
        if self.connection is not None:
            self.connection.write(data)
