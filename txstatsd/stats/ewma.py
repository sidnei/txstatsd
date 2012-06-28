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
"""
An exponentially-weighted moving average.

See:
- U{UNIX Load Average Part 1: How It Works
    <http://www.teamquest.com/pdfs/whitepaper/ldavg1.pdf>}
- U{UNIX Load Average Part 2: Not Your Average Average
    <http://www.teamquest.com/pdfs/whitepaper/ldavg2.pdf>}
"""

import math


class Ewma(object):
    M1_ALPHA  = 1 - math.exp(-5 / 60.0)
    M5_ALPHA  = 1 - math.exp(-5 / 60.0 / 5)
    M15_ALPHA = 1 - math.exp(-5 / 60.0 / 15)

    @classmethod
    def one_minute_ewma(cls):
        """
        Creates a new C{Ewma} which is equivalent to the UNIX one minute
        load average and which expects to be ticked every 5 seconds.
        """
        return Ewma(Ewma.M1_ALPHA, 5)

    @classmethod
    def five_minute_ewma(cls):
        """
        Creates a new C{Ewma} which is equivalent to the UNIX five minute
        load average and which expects to be ticked every 5 seconds.
        """
        return Ewma(Ewma.M5_ALPHA, 5)

    @classmethod
    def fifteen_minute_ewma(cls):
        """
        Creates a new C{Ewma} which is equivalent to the UNIX fifteen
        minute load average and which expects to be ticked every 5 seconds.
        """
        return Ewma(Ewma.M15_ALPHA, 5)

    def __init__(self, alpha, interval):
        """Create a new C{Ewma} with a specific smoothing constant.
        
        @param alpha: The smoothing constant.
        @param interval: The expected tick interval in seconds.
        """
        self.interval = interval
        self.alpha = float(alpha)

        self.initialized = False
        self.rate = 0.0
        self.uncounted = 0

    def update(self, n):
        """Update the moving average with a new value."""
        self.uncounted += n

    def tick(self):
        """Mark the passage of time and decay the current rate accordingly."""
        count = self.uncounted
        self.uncounted = 0
        instant_rate = float(count) / self.interval
        if self.initialized:
            self.rate += (self.alpha * (instant_rate  - self.rate))
        else:
            self.rate = instant_rate
            self.initialized = True
