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
import sys


class UniformSample(object):
    """
    A random sample of a stream of values. Uses Vitter's Algorithm R to
    produce a statistically representative sample.
    
    See:
    - U{Random Sampling with a Reservoir
        <http://www.cs.umd.edu/~samir/498/vitter.pdf>}
    """

    def __init__(self, reservoir_size):
        """Creates a new C{UniformSample}.

        @param reservoir_size: The number of samples to keep in the sampling
            reservoir.
        """
        self._values = [0 for i in range(reservoir_size)]
        self._count = 0
        self.clear()
    
    def clear(self):
        self._values = [0 for i in range(len(self._values))]
        self._count = 0

    def size(self):
        c = self._count
        return len(self._values) if c > len(self._values) else c

    def update(self, value):
        self._count += 1
        if self._count <= len(self._values):
            self._values[self._count - 1] = value
        else:
            r = random.randint(1, sys.maxint) % self._count
            if r < len(self._values):
                self._values[r] = value

    def get_values(self):
        s = self.size()
        return [self._values[i] for i in range(0, s)]
