import random

from unittest import TestCase

from txstatsd.stats.exponentiallydecayingsample import (
    ExponentiallyDecayingSample)


class TestExponentiallyDecayingSample(TestCase):

    def test_100_out_of_1000_elements(self):
        population = [i for i in range(0, 100)]
        sample = ExponentiallyDecayingSample(1000, 0.99)
        for i in population:
            sample.update(i)

        self.assertEqual(sample.size(), 100, 'Should have 100 elements')
        self.assertEqual(len(sample.get_values()), 100,
                         'Should have 100 elements')
        self.assertEqual(
            len(set(sample.get_values()).difference(set(population))), 0,
            'Should only have elements from the population')

    def test_100_out_of_10_elements(self):
        population = [i for i in range(0, 10)]
        sample = ExponentiallyDecayingSample(100, 0.99)
        for i in population:
            sample.update(i)

        self.assertEqual(sample.size(), 10)
        self.assertEqual(len(sample.get_values()), 10,
                         'Should have 10 elements')
        self.assertEqual(
            len(set(sample.get_values()).difference(set(population))), 0,
            'Should only have elements from the population')

    def test_heavily_biased_100_out_of_1000_elements(self):
        population = [i for i in range(0, 100)]
        sample = ExponentiallyDecayingSample(1000, 0.01)
        for i in population:
            sample.update(i)

        self.assertEqual(sample.size(), 100, 'Should have 100 elements')
        self.assertEqual(len(sample.get_values()), 100,
                         'Should have 100 elements')

        self.assertEqual(
            len(set(sample.get_values()).difference(set(population))), 0,
            'Should only have elements from the population')

    def test_ewma_sample_load(self):

        _time = [10000]

        def wtime():
            return _time[0]

        sample = ExponentiallyDecayingSample(100, 0.99, wall_time=wtime)
        sample.RESCALE_THRESHOLD = 100
        sample.clear()
        for i in xrange(10000000):
            sample.update(random.normalvariate(0, 10))
            _time[0] += 1

        self.assertEqual(sample.size(), 100)
        self.assertEqual(len(sample.get_values()), 100,
                         'Should have 100 elements')
    test_ewma_sample_load.skip = "takes too long to run"

