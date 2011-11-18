# Copyright (C) 2011 Canonical
# All Rights Reserved
import random

from scipy.stats import chi2

from twisted.trial.unittest import TestCase
from twisted.plugin import getPlugins
from twisted.plugins import distinct_plugin
import txstatsd.metrics.distinctmetric as distinct
from txstatsd.itxstatsd import IMetricFactory


class TestHash(TestCase):
    def test_hash_chars(self):
        "For one table, all chars map to different chars"
        results = set()
        for c in range(256):
            random.seed(1)
            h = distinct.hash(chr(c))
            results.add(h)
        self.assertEquals(len(results), 256)

    def test_chi_square(self):
        N = 10000

        for (bits, buckets) in [(-1, 1024), (24, 256),
                                (16, 256), (8, 256), (0, 256)]:
            bins = [0] * buckets
            for i in range(N):
                v = distinct.hash(str(i))
                if bits < 0:
                    bin = v / (0xFFFFFFFF / buckets)
                else:
                    bin = (v >> bits) & 0xFF
                bins[bin] += 1
            value = sum(((x - N / buckets) ** 2) / (N / buckets) for x in bins)
            pval = chi2.cdf(value, N)
            if pval > 0.5:
                print bins, pval
            self.assertTrue(pval < 0.5, "bits %s, pval == %s" % (bits, pval))
    test_chi_square.skip = "Takes too long to run every time."


class TestZeros(TestCase):
    def test_zeros(self):
        self.assertEquals(distinct.zeros(1), 0)
        self.assertEquals(distinct.zeros(2), 1)
        self.assertEquals(distinct.zeros(4), 2)
        self.assertEquals(distinct.zeros(5), 0)
        self.assertEquals(distinct.zeros(8), 3)
        self.assertEquals(distinct.zeros(9), 0)


class TestDistinct(TestCase):
    def test_all(self):
        random.seed(1)

        for r in [1000, 10000]:
            cd = distinct.SlidingDistinctCounter(32, 32)
            for i in range(r):
                cd.add(1, str(i))
            error = abs(cd.distinct() - r)
            self.assertTrue(error < 0.15 * r)


class TestDistinctMetricReporter(TestCase):
    def test_reports(self):
        random.seed(1)
        _wall_time = [0]
        def _time():
            return _wall_time[0]

        dmr = distinct.DistinctMetricReporter("test", wall_time_func=_time)
        for i in range(3000):
            _wall_time[0] = i * 50
            dmr.update(str(i))
        now = _time()
        self.assertTrue(abs(dmr.count() - 3000) < 600)
        self.assertTrue(abs(dmr.count_1min(now) - 1) < 2)
        self.assertTrue(abs(dmr.count_1hour(now) - 72) < 15)
        self.assertTrue(abs(dmr.count_1day(now) - 1728) < 500)
        self.assertTrue("count_1hour" in dmr.flush(1, now))


class TestPlugin(TestCase):
    
    def test_factory(self):
        self.assertTrue(distinct_plugin.distinct_metric_factory in \
                        list(getPlugins(IMetricFactory)))

