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

import os
import psutil
import sys

import mock
from twisted.trial.unittest import TestCase

from txstatsd.process import (
    ProcessReport, parse_meminfo, parse_loadavg, parse_netdev,
    report_system_stats, report_reactor_stats, report_threadpool_stats, report_counters)


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


netdev = """Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
lo: 635698677 2126380    0    0    0     0          0         0 635698677 2126380    0    0    0     0       0          0
eth0: 206594440  189319    0    0    0     0          0         0 23357088  165086    0    0    0     0       0          0
tun0: 5138313   24837    0    0    0     0          0         0  5226635   26986    0    0    0     0       0          0"""


class TestSystemPerformance(TestCase):
    """Test system performance monitoring."""

    def test_loadinfo(self):
        """We understand loadinfo."""
        loadinfo = "1.02 1.08 1.14 2/2015 19420"
        self.assertEqual(parse_loadavg(loadinfo), {
            "sys.loadavg.oneminute": 1.02,
            "sys.loadavg.fiveminutes": 1.08,
            "sys.loadavg.fifthteenminutes": 1.14})

    def test_meminfo(self):
        """We understand meminfo."""
        r = parse_meminfo(meminfo)
        self.assertEqual(r['sys.mem.Buffers'], 8052 * 1024)
        self.assert_('sys.mem.HugePages_Rsvd' not in r)

    def test_cpu_counters(self):
        """System cpu counters are collected through psutil."""
        cpu_times = psutil.cpu_times()
        with mock.patch("psutil.cpu_times"):
            psutil.cpu_times.return_value = cpu_times
            result = report_system_stats()
            psutil.cpu_times.assert_called_once_with(percpu=False)
        # cpu_times is platform-dependent
        if sys.platform.lower().startswith("linux"):
            self.assertEqual(cpu_times.user, result["sys.cpu.user"])
            self.assertEqual(cpu_times.system, result["sys.cpu.system"])
            self.assertEqual(cpu_times.idle, result["sys.cpu.idle"])
            self.assertEqual(cpu_times.iowait, result["sys.cpu.iowait"])
            self.assertEqual(cpu_times.irq, result["sys.cpu.irq"])
        elif sys.platform.lower().startswith("win32"):
            self.assertEqual(cpu_times.user, result["sys.cpu.user"])
            self.assertEqual(cpu_times.system, result["sys.cpu.system"])
            self.assertEqual(cpu_times.idle, result["sys.cpu.idle"])
        elif sys.platform.lower().startswith("darwin"):
            self.assertEqual(cpu_times.user, result["sys.cpu.user"])
            self.assertEqual(cpu_times.system, result["sys.cpu.system"])
            self.assertEqual(cpu_times.idle, result["sys.cpu.idle"])
            self.assertEqual(cpu_times.nice, result["sys.cpu.nice"])
        elif sys.platform.lower().startswith("freebsd"):
            self.assertEqual(cpu_times.user, result["sys.cpu.user"])
            self.assertEqual(cpu_times.system, result["sys.cpu.system"])
            self.assertEqual(cpu_times.idle, result["sys.cpu.idle"])
            self.assertEqual(cpu_times.irq, result["sys.cpu.irq"])

    def test_per_cpu_counters(self):
        """System percpu counters are collected through psutil."""
        cpu_times = psutil.cpu_times()
        with mock.patch("psutil.cpu_times"):
            psutil.cpu_times.return_value = [cpu_times, cpu_times]
            result = report_system_stats(percpu=True)
            psutil.cpu_times.assert_called_once_with(percpu=True)
        # cpu_times is platform-dependent
        if sys.platform.lower().startswith("linux"):
            self.assertEqual(cpu_times.user, result["sys.cpu.000.user"])
            self.assertEqual(cpu_times.system, result["sys.cpu.000.system"])
            self.assertEqual(cpu_times.idle, result["sys.cpu.000.idle"])
            self.assertEqual(cpu_times.iowait, result["sys.cpu.000.iowait"])
            self.assertEqual(cpu_times.irq, result["sys.cpu.001.irq"])
            self.assertEqual(cpu_times.user, result["sys.cpu.001.user"])
            self.assertEqual(cpu_times.system, result["sys.cpu.001.system"])
            self.assertEqual(cpu_times.idle, result["sys.cpu.001.idle"])
            self.assertEqual(cpu_times.iowait, result["sys.cpu.001.iowait"])
            self.assertEqual(cpu_times.irq, result["sys.cpu.001.irq"])
        elif sys.platform.lower().startswith("win32"):
            self.assertEqual(cpu_times.user, result["sys.cpu.000.user"])
            self.assertEqual(cpu_times.system, result["sys.cpu.000.system"])
            self.assertEqual(cpu_times.idle, result["sys.cpu.000.idle"])
            self.assertEqual(cpu_times.user, result["sys.cpu.001.user"])
            self.assertEqual(cpu_times.system, result["sys.cpu.001.system"])
            self.assertEqual(cpu_times.idle, result["sys.cpu.001.idle"])
        elif sys.platform.lower().startswith("darwin"):
            self.assertEqual(cpu_times.user, result["sys.cpu.000.user"])
            self.assertEqual(cpu_times.system, result["sys.cpu.000.system"])
            self.assertEqual(cpu_times.idle, result["sys.cpu.000.idle"])
            self.assertEqual(cpu_times.nice, result["sys.cpu.000.nice"])
            self.assertEqual(cpu_times.user, result["sys.cpu.001.user"])
            self.assertEqual(cpu_times.system, result["sys.cpu.001.system"])
            self.assertEqual(cpu_times.idle, result["sys.cpu.001.idle"])
            self.assertEqual(cpu_times.nice, result["sys.cpu.001.nice"])
        elif sys.platform.lower().startswith("freebsd"):
            self.assertEqual(cpu_times.user, result["sys.cpu.000.user"])
            self.assertEqual(cpu_times.system, result["sys.cpu.000.system"])
            self.assertEqual(cpu_times.idle, result["sys.cpu.000.idle"])
            self.assertEqual(cpu_times.irq, result["sys.cpu.000.irq"])
            self.assertEqual(cpu_times.user, result["sys.cpu.001.user"])
            self.assertEqual(cpu_times.system, result["sys.cpu.001.system"])
            self.assertEqual(cpu_times.idle, result["sys.cpu.001.idle"])
            self.assertEqual(cpu_times.irq, result["sys.cpu.001.irq"])

    def test_self_cpu_and_memory_stats(self):
        """
        Process cpu and memory stats are collected through psutil.

        If the L{Process} implementation does not have C{get_num_threads} then
        the number of threads will not be included in the output.
        """
        process = psutil.Process(os.getpid())
        vsize, rss = process.memory_info()
        cpu_percent = process.cpu_percent()
        memory_percent = process.memory_percent()

        proc = mock.Mock()
        proc.memory_info.return_value = (vsize, rss)
        proc.cpu_percent.return_value = cpu_percent
        proc.memory_percent.return_value = memory_percent
        proc.num_threads = None
        result = ProcessReport(process=proc).get_memory_and_cpu()
        self.assertEqual(cpu_percent, result["proc.cpu.percent"])
        self.assertEqual(vsize, result["proc.memory.vsize"])
        self.assertEqual(rss, result["proc.memory.rss"])
        self.assertEqual(memory_percent, result["proc.memory.percent"])
        self.failIf("proc.threads" in result)

    def test_self_cpu_counters(self):
        """
        Process cpu counters are collected through psutil.
        """
        process = psutil.Process(os.getpid())
        utime, stime = process.cpu_times()

        proc = mock.Mock()
        proc.cpu_times.return_value = (utime, stime)
        result = ProcessReport(process=proc).get_cpu_counters()
        proc.cpu_times.assert_called_once_with()
        self.assertEqual(utime, result["proc.cpu.user"])
        self.assertEqual(stime, result["proc.cpu.system"])

    def test_self_cpu_and_memory_stats_with_num_threads(self):
        """
        Process cpu and memory stats are collected through psutil.

        If the L{Process} implementation contains C{get_num_threads} then the
        number of threads will be included in the output.

        """
        process = psutil.Process(os.getpid())
        vsize, rss = process.memory_info()
        cpu_percent = process.cpu_percent()
        memory_percent = process.memory_percent()

        proc = mock.Mock()
        proc.memory_info.return_value = (vsize, rss)
        proc.cpu_percent.return_value = cpu_percent
        proc.memory_percent.return_value = memory_percent
        proc.num_threads.return_value = 1
        result = ProcessReport(process=proc).get_memory_and_cpu()
        proc.memory_info.assert_called_once_with()
        proc.cpu_percent.assert_called_once_with()
        proc.memory_percent.assert_called_once_with()
        proc.num_threads.assert_called_once_with()


        self.assertEqual(cpu_percent, result["proc.cpu.percent"])
        self.assertEqual(vsize, result["proc.memory.vsize"])
        self.assertEqual(rss, result["proc.memory.rss"])
        self.assertEqual(memory_percent, result["proc.memory.percent"])
        self.assertEqual(1, result["proc.threads"])

    def test_ioinfo(self):
        """Process IO info is collected through psutil."""
        # If the version of psutil doesn't have the C{get_io_counters},
        # then io stats are not included in the output.
        proc = mock.Mock()
        proc.io_counters = None
        result = ProcessReport(process=proc).get_io_counters()
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

        proc = mock.Mock()
        proc.io_counters.return_value = io_counters
        result = ProcessReport(process=proc).get_io_counters()
        proc.io_counters.assert_called_once_with()
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
        # If the version of psutil doesn't have the C{get_io_counters},
        # then io stats are not included in the output.
        proc = mock.Mock()
        proc.connections = None
        result = ProcessReport(process=proc).get_net_stats()
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

        proc = mock.Mock()
        proc.connections.return_value = connections
        result = ProcessReport(process=proc).get_net_stats()
        proc.connections.assert_called_once_with()
        self.assertEqual(2, result["proc.net.status.established"])
        self.assertEqual(1, result["proc.net.status.closing"])
        self.assertEqual(1, result["proc.net.status.syn_sent"])

    def test_reactor_stats(self):
        """Given a twisted reactor, pull out some stats from it."""
        mock_reactor = mock.Mock()
        mock_reactor.getReaders.return_value = [None, None, None]
        mock_reactor.getWriters.return_value = [None, None]
        result = report_reactor_stats(mock_reactor)()
        self.assertEqual(3, result["reactor.readers"])
        self.assertEqual(2, result["reactor.writers"])

    def test_threadpool_stats(self):
        """Given a twisted threadpool, pull out some stats from it."""
        mock_reactor = mock.Mock()
        mock_reactor.q.qsize.return_value = 42
        mock_reactor.threads = 6 * [None]
        mock_reactor.waiters = 2 * [None]
        mock_reactor.working = 4 * [None]

        result = report_threadpool_stats(mock_reactor)()
        self.assertEqual(42, result["threadpool.queue"])
        self.assertEqual(6, result["threadpool.threads"])
        self.assertEqual(2, result["threadpool.waiters"])
        self.assertEqual(4, result["threadpool.working"])

    def test_netdev(self):
        """
        C{parse_netdev} returns a stat for sent/received bytes and packets for
        each network interfaces.
        """
        self.assertEqual(parse_netdev(netdev), {
            "sys.net.lo.bytes.received": 635698677,
            "sys.net.lo.bytes.sent": 635698677,
            "sys.net.lo.packets.received": 2126380,
            "sys.net.lo.packets.sent": 2126380,
            "sys.net.eth0.bytes.received": 206594440,
            "sys.net.eth0.bytes.sent": 23357088,
            "sys.net.eth0.packets.received": 189319,
            "sys.net.eth0.packets.sent": 165086,
            "sys.net.tun0.bytes.received": 5138313,
            "sys.net.tun0.bytes.sent": 5226635,
            "sys.net.tun0.packets.received": 24837,
            "sys.net.tun0.packets.sent": 26986})

    def test_report_counters(self):
        """
        C{report_counters} keeps the last value of a called function and on the
        next call returns the difference between current return value and
        previous return value.
        """
        def generate():
            yield {"foo": 1}
            yield {"foo": 5}
            yield {"foo": 10}
            yield {"foo": 17}
        generate = generate()
        def reporter():
            return generate.next()
        wrapped = report_counters(reporter)
        self.assertEqual({}, wrapped())
        self.assertEqual({"foo": 4}, wrapped())
        self.assertEqual({"foo": 5}, wrapped())
        self.assertEqual({"foo": 7}, wrapped())


