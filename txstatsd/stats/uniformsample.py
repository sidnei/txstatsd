
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
