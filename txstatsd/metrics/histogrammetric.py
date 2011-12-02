import math

from txstatsd.stats.exponentiallydecayingsample \
    import ExponentiallyDecayingSample
from txstatsd.stats.uniformsample import UniformSample


class HistogramMetricReporter(object):
    """
    A metric which calculates the distribution of a value.

    See:
    - U{Accurately computing running variance
          <http://www.johndcook.com/standard_deviation.html>}
    """

    @classmethod
    def using_uniform_sample(cls, prefix=""):
        """
        Uses a uniform sample of 1028 elements, which offers a 99.9%
        confidence level with a 5% margin of error assuming a normal
        distribution.
         """
        sample = UniformSample(1028)
        return HistogramMetricReporter(sample, prefix=prefix)

    @classmethod
    def using_exponentially_decaying_sample(cls, prefix=""):
        """
        Uses an exponentially decaying sample of 1028 elements, which offers
        a 99.9% confidence level with a 5% margin of error assuming a normal
        distribution, and an alpha factor of 0.015, which heavily biases
        the sample to the past 5 minutes of measurements.
        """
        sample = ExponentiallyDecayingSample(1028, 0.015)
        return HistogramMetricReporter(sample, prefix=prefix)

    def __init__(self, sample, prefix=""):
        """Creates a new HistogramMetric with the given sample.

        @param sample: The sample to create a histogram from.
        """
        self.sample = sample

        if prefix:
            prefix += "."
        self.prefix = prefix

        self._min = 0
        self._max = 0
        self._sum = 0

        # These are for the Welford algorithm for calculating running
        # variance without floating-point doom.
        self.variance = [-1.0, 0.0]    # M, S
        self.count = 0

        self.clear()

    def clear(self):
        """Clears all recorded values."""
        self.sample.clear()
        self.count = 0
        self._max = None
        self._min = None
        self._sum = 0
        self.variance = [-1.0, 0.0]

    def update(self, value, name=""):
        """Adds a recorded value.

        @param value: The length of the value.
        """
        self.count += 1
        self.sample.update(value)
        self.set_max(value)
        self.set_min(value)
        self._sum += value
        self.update_variance(value)

    def report(self, timestamp):
        # median, 75, 95, 98, 99, 99.9 percentile
        percentiles = self.percentiles(0.5, 0.75, 0.95, 0.98, 0.99, 0.999)
        metrics = []
        items = {
            ".min": self.min(),
            ".max": self.max(),
            ".mean": self.mean(),
            ".stddev": self.std_dev(),
            ".median": percentiles[0],
            ".75percentile": percentiles[1],
            ".95percentile": percentiles[2],
            ".98percentile": percentiles[3],
            ".99percentile": percentiles[4],
            ".999percentile": percentiles[5]}

        for item, value in items.itervalues():
            metrics.append((self.prefix + self.name + item, value, timestamp))
        return metrics

    def min(self):
        """Returns the smallest recorded value."""
        return self._min if self.count > 0 else 0.0

    def max(self):
        """Returns the largest recorded value."""
        return self._max if self.count > 0 else 0.0

    def mean(self):
        """Returns the arithmetic mean of all recorded values."""
        return float(self._sum) / self.count if self.count > 0 else 0.0

    def std_dev(self):
        """Returns the standard deviation of all recorded values."""
        return math.sqrt(self.get_variance()) if self.count > 0 else 0.0

    def percentiles(self, *percentiles):
        """Returns a list of values at the given percentiles.

        @param percentiles one or more percentiles
        """

        scores = [0.0 for i in range(len(percentiles))]
        if self.count > 0:
            values = self.sample.get_values()
            values.sort()

            for i in range(len(percentiles)):
                p = percentiles[i]
                pos = p * (len(values) + 1)
                if pos < 1:
                    scores[i] = values[0]
                elif pos >= len(values):
                    scores[i] = values[-1]
                else:
                    lower = values[int(pos) - 1]
                    upper = values[int(pos)]
                    scores[i] = lower + (pos - math.floor(pos)) * (
                        upper - lower)

        return scores

    def get_values(self):
        """Returns a list of all values in the histogram's sample."""
        return self.sample.get_values()

    def get_variance(self):
        if self.count <= 1:
            return 0.0
        return self.variance[1] / (self.count - 1)

    def set_max(self, potential_max):
        if self._max is None:
            self._max = potential_max
        else:
            self._max = max(self.max(), potential_max)

    def set_min(self, potential_min):
        if self._min is None:
            self._min = potential_min
        else:
            self._min = min(self.min(), potential_min)

    def update_variance(self, value):
        old_values = self.variance
        new_values = [0.0, 0.0]
        if old_values[0] == -1:
            new_values[0] = value
            new_values[1] = 0.0
        else:
            old_m = old_values[0]
            old_s = old_values[1]

            new_m = old_m + (float(value - old_m) / self.count)
            new_s = old_s + (float(value - old_m) * (value - new_m))

            new_values[0] = new_m
            new_values[1] = new_s

        self.variance = new_values
