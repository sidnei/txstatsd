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

from time import time
from random import random
from math import exp
from bisect import insort


class ExponentiallyDecayingSample(object):
    """
    An exponentially-decaying random sample of values. Uses Cormode et
    al's forward-decaying priority reservoir sampling method to produce a
    statistically representative sample, exponentially biased towards newer
    entries.

    See:
    - U{Cormode et al. Forward Decay: A Practical Time Decay Model for
      Streaming Systems. ICDE '09: Proceedings of the 2009 IEEE International
      Conference on Data Engineering (2009)
      <http://www.research.att.com/people/Cormode_Graham/
      library/publications/CormodeShkapenyukSrivastavaXu09.pdf>}
    """

    # 10 minutes (in seconds)
    RESCALE_THRESHOLD = 60 * 10

    def __init__(self, reservoir_size, alpha, wall_time=None):
        """Creates a new C{ExponentiallyDecayingSample}.

        @param reservoir_size: The number of samples to keep in the sampling
            reservoir.
        @parama alpha: The exponential decay factor; the higher this is,
            the more biased the sample will be towards newer values.
        """
        self.alpha = alpha
        self.reservoir_size = reservoir_size

        if wall_time is None:
            wall_time = time
        self.tick = wall_time
        self.clear()

    def clear(self):
        self._values = []
        self.count = 0
        self.start_time = self.tick()
        self.next_scale_time = (
            self.tick() + self.RESCALE_THRESHOLD)

    def size(self):
        return min(self.reservoir_size, self.count)

    def update(self, value, timestamp=None):
        """Adds an old value with a fixed timestamp to the sample.

        @param value: The value to be added.
        @param timestamp: The epoch timestamp of *value* in seconds.
        """

        if timestamp is None:
            timestamp = self.tick()
        now = timestamp
            
        if now >= self.next_scale_time:
            self.rescale(now, self.next_scale_time)

        priority = exp(self.alpha * (timestamp - self.start_time)) / random()
        values = self._values

        if self.count < self.reservoir_size:
            self.count += 1
            insort(values, (priority, value))
        else:
            if values[0][0] < priority:
                insort(values, (priority, value))
                values.pop(0)
            
    def get_values(self):
        return [v for (k, v) in self._values]

    def rescale(self, now, next):
        """
        A common feature of the above techniques - indeed, the key technique
        that allows us to track the decayed weights efficiently - is that they
        maintain counts and other quantities based on g(ti - L), and only
        scale by g(t - L) at query time. But while g(ti - L)/g(t-L) is
        guaranteed to lie between zero and one, the intermediate values of
        g(ti - L) could become very large. For polynomial functions, these
        values should not grow too large, and should be effectively
        represented in practice by floating point values without loss of
        precision. For exponential functions, these values could grow quite
        large as new values of (ti - L) become large, and potentially exceed
        the capacity of common floating point types. However, since the values
        stored by the algorithms are linear combinations of g values (scaled
        sums), they can be rescaled relative to a new landmark. That is, by
        the analysis of exponential decay in Section III-A, the choice of L
        does not affect the final result. We can therefore multiply each value
        based on L by a factor of exp(-alpha(L' - L)), and obtain the correct
        value as if we had instead computed relative to a new landmark L' (and
        then use this new L' at query time). This can be done with a linear
        pass over whatever data structure is being used.
        """

        self.next_scale_time = (now + self.RESCALE_THRESHOLD)
        old_start_time = self.start_time
        self.start_time = now

        new_values = []
        for k, v in self._values:
            nk = k * exp(-self.alpha * (self.start_time - old_start_time))
            insort(new_values, (nk, v))
        self._values = new_values
