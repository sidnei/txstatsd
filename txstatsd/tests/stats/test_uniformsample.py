
from unittest import TestCase

from txstatsd.stats.uniformsample import UniformSample


class TestUniformSample(TestCase):
    def test_100_out_of_1000_elements(self):
        population = [i for i in range(0, 1000)]
        sample = UniformSample(100)
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
        sample = UniformSample(100)
        for i in population:
            sample.update(i)

        self.assertEqual(sample.size(), 10, 'Should have 10 elements')
        self.assertEqual(len(sample.get_values()), 10,
                         'Should have 10 elements')
        self.assertEqual(
            len(set(sample.get_values()).difference(set(population))), 0,
            'Should only have elements from the population')
