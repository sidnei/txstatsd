import os
import psutil

from mocker import MockerTestCase
from twisted.internet import defer
from twisted.trial.unittest import TestCase

from txstatsd.process import (
    load_file, parse_meminfo, parse_loadavg,
    report_process_memory_and_cpu, report_process_io_counters,
    report_process_net_stats, report_system_stats)


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

        result = report_system_stats()
        self.assertEqual(cpu_times.idle, result["sys.cpu.idle"])
        self.assertEqual(cpu_times.iowait, result["sys.cpu.iowait"])
        self.assertEqual(cpu_times.irq, result["sys.cpu.irq"])
        self.assertEqual(cpu_times.nice, result["sys.cpu.nice"])
        self.assertEqual(cpu_times.system, result["sys.cpu.system"])
        self.assertEqual(cpu_times.user, result["sys.cpu.user"])

    def test_self_statinfo(self):
        """Process stat info is collected through psutil."""
        process = psutil.Process(os.getpid())
        vsize, rss = process.get_memory_info()
        utime, stime = process.get_cpu_times()
        cpu_percent = process.get_cpu_percent()
        memory_percent = process.get_memory_percent()

        mock = self.mocker.mock()
        self.expect(mock.get_memory_info()).result((vsize, rss))
        self.expect(mock.get_cpu_times()).result((utime, stime))
        self.expect(mock.get_cpu_percent()).result(cpu_percent)
        self.expect(mock.get_memory_percent()).result(memory_percent)
        self.expect(mock.get_num_threads()).result(1)
        self.mocker.replay()

        result = report_process_memory_and_cpu(process=mock)
        self.assertEqual(utime, result["proc.cpu.user"])
        self.assertEqual(stime, result["proc.cpu.system"])
        self.assertEqual(cpu_percent, result["proc.cpu.percent"])
        self.assertEqual(vsize, result["proc.memory.vsize"])
        self.assertEqual(rss, result["proc.memory.rss"])
        self.assertEqual(memory_percent, result["proc.memory.percent"])
        self.assertEqual(1, result["proc.threads"])

    def test_ioinfo(self):
        """Process IO info is collected through psutil."""
        mock = self.mocker.mock()
        self.expect(mock.get_io_counters).result(None)
        self.mocker.replay()

        # If the version of psutil doesn't have the C{get_io_counters},
        # then io stats are not included in the output.
        result = report_process_io_counters(process=mock)
        self.failIf("proc.io.read.count" in result)
        self.failIf("proc.io.write.count" in result)
        self.failIf("proc.io.read.bytes" in result)
        self.failIf("proc.io.write.bytes" in result)

    def test_ioinfo_with_get_io_counters(self):
        """
        Process IO info is collected through psutil.

        If C{get_io_counters} is implemented by the L{Process} object,
        then io information will be returned with the process information.
        """
        io_counters = (10, 42, 125, 16)

        mock = self.mocker.mock()
        self.expect(mock.get_io_counters).result(mock)
        self.expect(mock.get_io_counters()).result(io_counters)
        self.mocker.replay()

        result = report_process_io_counters(process=mock)
        self.assertEqual(10, result["proc.io.read.count"])
        self.assertEqual(42, result["proc.io.write.count"])
        self.assertEqual(125, result["proc.io.read.bytes"])
        self.assertEqual(16, result["proc.io.write.bytes"])

    def test_netinfo_no_get_connections(self):
        """
        Process connection info is collected through psutil.

        If the version of psutil doesn't implement C{get_connections} for
        L{Process}, then no information is returned.
        """
        mock = self.mocker.mock()
        self.expect(mock.get_connections).result(None)
        self.mocker.replay()

        # If the version of psutil doesn't have the C{get_io_counters},
        # then io stats are not included in the output.
        result = report_process_net_stats(process=mock)
        self.failIf("proc.net.status.established" in result)

    def test_netinfo_with_get_connections(self):
        """
        Process connection info is collected through psutil.

        If the version of psutil implements C{get_connections} for L{Process},
        then a count of connections in each state is returned.
        """
        connections = [
            (115, 2, 1, ("10.0.0.1", 48776),
             ("93.186.135.91", 80), "ESTABLISHED"),
            (117, 2, 1, ("10.0.0.1", 43761),
             ("72.14.234.100", 80), "CLOSING"),
            (119, 2, 1, ("10.0.0.1", 60759),
             ("72.14.234.104", 80), "ESTABLISHED"),
            (123, 2, 1, ("10.0.0.1", 51314),
             ("72.14.234.83", 443), "SYN_SENT")
            ]

        mock = self.mocker.mock()
        self.expect(mock.get_connections).result(mock)
        self.expect(mock.get_connections()).result(connections)
        self.mocker.replay()

        result = report_process_net_stats(process=mock)
        self.assertEqual(2, result["proc.net.status.established"])
        self.assertEqual(1, result["proc.net.status.closing"])
        self.assertEqual(1, result["proc.net.status.syn_sent"])
