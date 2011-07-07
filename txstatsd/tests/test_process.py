import os
import psutil

from mocker import MockerTestCase
from twisted.internet import defer
from twisted.trial.unittest import TestCase

from txstatsd.process import (
    load_file, parse_meminfo, parse_loadavg,
    report_self_stat, report_system_stat)


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


class TestSystemPerformance(TestCase, MockerTestCase):
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
        """System stat info is collected through psutil."""
        cpu_times = psutil.cpu_times()
        mock = self.mocker.replace("psutil.cpu_times")
        self.expect(mock()).result(cpu_times)
        self.mocker.replay()

        result = report_system_stat()
        self.assertEqual(cpu_times.idle, result["stat.cpu.idle"])
        self.assertEqual(cpu_times.iowait, result["stat.cpu.iowait"])
        self.assertEqual(cpu_times.irq, result["stat.cpu.irq"])
        self.assertEqual(cpu_times.nice, result["stat.cpu.nice"])
        self.assertEqual(cpu_times.system, result["stat.cpu.system"])
        self.assertEqual(cpu_times.user, result["stat.cpu.user"])

    def test_self_statinfo(self):
        """Process stat info is collected through psutil."""
        process = psutil.Process(os.getpid())
        vsize, rss = process.get_memory_info()
        utime, stime = process.get_cpu_times()
        cpu_percent = process.get_cpu_percent()
        memory_percent = process.get_memory_percent(),

        mock = self.mocker.mock()
        self.expect(mock.get_memory_info()).result((vsize, rss))
        self.expect(mock.get_cpu_times()).result((utime, stime))
        self.expect(mock.get_cpu_percent()).result(cpu_percent)
        self.expect(mock.get_memory_percent()).result(memory_percent)
        self.mocker.replay()

        result = report_self_stat(process=mock)
        self.assertEqual(utime, result["self.stat.cpu.user"])
        self.assertEqual(stime, result["self.stat.cpu.system"])
        self.assertEqual(cpu_percent, result["self.stat.cpu.percent"])
        self.assertEqual(vsize, result["self.stat.memory.vsize"])
        self.assertEqual(rss, result["self.stat.memory.rss"])
        self.assertEqual(memory_percent, result["self.stat.memory.percent"])
