from twisted.internet import defer
from twisted.trial.unittest import TestCase

from txstatsd.process import (
    load_file, parse_meminfo, parse_loadavg,
    parse_self_stat, parse_stat)


meminfo = """\
MemTotal:        8190436 kB
MemFree:          995724 kB
Buffers:            8052 kB
Cached:           344824 kB
SwapCached:       170828 kB
Active:          4342436 kB
Inactive:         907076 kB
Active(anon):    4210168 kB
Inactive(anon):   756096 kB
Active(file):     132268 kB
Inactive(file):   150980 kB
Unevictable:      641692 kB
Mlocked:          641676 kB
SwapTotal:      23993336 kB
SwapFree:       22750588 kB
Dirty:               740 kB
Writeback:             0 kB
AnonPages:       5453396 kB
Mapped:           259524 kB
Shmem:             69952 kB
Slab:             142444 kB
SReclaimable:      83188 kB
SUnreclaim:        59256 kB
KernelStack:       16144 kB
PageTables:        88384 kB
NFS_Unstable:          0 kB
Bounce:                0 kB
WritebackTmp:          0 kB
CommitLimit:    28088552 kB
Committed_AS:   11178564 kB
VmallocTotal:   34359738367 kB
VmallocUsed:      156208 kB
VmallocChunk:   34359579400 kB
HardwareCorrupted:     0 kB
HugePages_Total:       0
HugePages_Free:        0
HugePages_Rsvd:        0
HugePages_Surp:        0
Hugepagesize:       2048 kB
DirectMap4k:     7968640 kB
DirectMap2M:      415744 kB"""


statinfo = """\
cpu  44642345 6565172 27609170 848974013 5375635 400632 242300 0 0
cpu0 7502727 773752 4008318 103388372 671741 0 6480 0 0
cpu1 7140105 561312 3709753 105843865 221109 0 11039 0 0
cpu2 6485488 578759 3307639 107013675 386463 0 10814 0 0
cpu3 5469398 803450 3307418 106708952 433981 175 11856 0 0
cpu4 4601909 495132 2001703 108701221 891609 350011 124470 0 0
cpu5 4325073 1420692 5568774 101166757 2332634 19709 66875 0 0
cpu6 5707616 942378 3362802 106940331 226355 119 5241 0 0
cpu7 3410029 989697 2342763 109210840 211743 30618 5525 0 0
intr 2186910471 443661231 2 0 0 9 0 0 0 1 0 0 0 0 0 0 0 5452634 71 2 """\
"""1448 0 0 1683898 334567 166419398 158798078 227908335 143039080 """\
"""123052218 0 0 0 0 16259372 149832869 284925055 0 0 0 0 0 0 0 0 0 0 0 0 """\
"""0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 """\
"""0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 """\
"""0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 """\
"""0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 """\
"""0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 """\
"""0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 """\
"""0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 """\
"""0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 """\
"""0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 """\
"""0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 """\
"""0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 """\
"""0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
ctxt 10909103455
btime 1275408387
processes 1194434
procs_running 2
procs_blocked 0
softirq 3272951654 0 2325187294 190325 152108502 15769070 0 5522892 """\
"""330581157 983191 442609223"""


selfstat = "26920 (cat) R 27702 26920 27702 34821 26920 4202496 259 0 " \
"0 0 0 0 0 0 20 0 1 0 113990478 5627904 181 18446744073709551615 4194304 " \
"4247172 140735730372032 140735730370408 139849348338784 0 0 0 0 0 0 0 17 " \
"6 0 0 0 0 0"


selfstat_short = "26920 (cat) R 27702 26920 27702 34821 26920 4202496 259 0 " \
"0 0 0 0 0 0 20 0 1 0 113990478 5627904 181 1"


class TestSystemPerformance(TestCase):
    """Test system performance monitoring."""

    def assertSuccess(self, deferred, result=None):
        """
        Assert that the given C{deferred} results in the given C{result}.
        """
        self.assertTrue(isinstance(deferred, defer.Deferred))
        return deferred.addCallback(self.assertEqual, result)

    def test_read(self):
        """We can read files non blocking."""
        d = load_file(__file__)
        return self.assertSuccess(d, open(__file__).read())

    def test_loadinfo(self):
        """We understand loadinfo."""
        loadinfo = "1.02 1.08 1.14 2/2015 19420"
        self.assertEqual(parse_loadavg(loadinfo), {
            "loadavg.oneminute": 1.02,
            "loadavg.fiveminutes": 1.08,
            "loadavg.fifthteenminutes": 1.14})

    def test_meminfo(self):
        """We understand meminfo."""
        r = parse_meminfo(meminfo)
        self.assertEqual(r['meminfo.Buffers'], 8052 * 1024)
        self.assert_('meminfo.HugePages_Rsvd' not in r)

    def test_statinfo(self):
        """We understand statinfo."""
        r = parse_stat(statinfo)
        self.assertEqual(r['stat.cpu1.nice'], 561312)

    def test_self_stat(self):
        """We understand self/stat."""
        r = parse_self_stat(selfstat)
        self.assertEqual(r['self.stat.rss'], 181)

    def test_self_stat_short(self):
        """We understand the short version of self/stat."""
        r = parse_self_stat(selfstat_short)
        self.assertEqual(r['self.stat.rss'], 181)
