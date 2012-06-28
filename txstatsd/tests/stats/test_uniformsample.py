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
