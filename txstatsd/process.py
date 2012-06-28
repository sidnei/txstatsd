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
import socket
import psutil

from functools import update_wrapper


MEMINFO_KEYS = ("MemTotal:", "MemFree:", "Buffers:",
                "Cached:", "SwapCached:", "SwapTotal:",
                "SwapFree:")

MULTIPLIERS = {"kB": 1024, "mB": 1024 * 1024}


def load_file(filename):
    """Load a file into memory."""
    with open(filename, "r") as f:
        return f.read()


def parse_meminfo(data, prefix="sys.mem"):
    """Parse data from /proc/meminfo."""
    result = {}

    for line in data.split("\n"):
        if not line:
            continue
        parts = [x for x in line.split(" ") if x]
        if not parts[0] in MEMINFO_KEYS:
            continue

        multiple = 1

        if len(parts) == 3:
            multiple = MULTIPLIERS[parts[2]]

        # remove ':'
        label = parts[0][:-1]
        amount = int(parts[1]) * multiple
        result[prefix + "." + label] = amount

    return result


def parse_loadavg(data, prefix="sys.loadavg"):
    """Parse data from /proc/loadavg."""
    return dict(zip(
        (prefix + ".oneminute",
         prefix + ".fiveminutes",
         prefix + ".fifthteenminutes"),
        [float(x) for x in data.split()[:3]]))


def parse_netdev(data, prefix="sys.net"):
    """Parse data from /proc/net/dev."""
    lines = data.splitlines()
    # Parse out the column headers as keys.
    _, receive_columns, transmit_columns = lines[1].split("|")
    columns = ["recv_%s" % column for column in receive_columns.split()]
    columns.extend(["send_%s" % column for column in transmit_columns.split()])

    # Parse out the network devices.
    result = {}
    for line in lines[2:]:
        if not ":" in line:
            continue
        device, data = line.split(":")
        device = device.strip()
        data = dict(zip(columns, map(int, data.split())))
        result.update({
            "%s.%s.bytes.received" % (prefix, device): data["recv_bytes"],
            "%s.%s.bytes.sent" % (prefix, device): data["send_bytes"],
            "%s.%s.packets.received" % (prefix, device): data["recv_packets"],
            "%s.%s.packets.sent" % (prefix, device): data["send_packets"]})
    return result


class ProcessReport(object):

    def __init__(self, process=None):
        self._process = process

    @property
    def process(self):
        """Override property with current process on first access."""
        if self._process is None:
            self._process = psutil.Process(os.getpid())
        return self._process

    def get_memory_and_cpu(self, prefix="proc"):
        """Report memory and CPU stats for C{process}."""
        vsize, rss = self.process.get_memory_info()
        result = {prefix + ".cpu.percent": self.process.get_cpu_percent(),
                  prefix + ".memory.percent":
                    self.process.get_memory_percent(),
                  prefix + ".memory.vsize": vsize,
                  prefix + ".memory.rss": rss}
        if getattr(self.process, "get_num_threads", None) is not None:
            result[prefix + ".threads"] = self.process.get_num_threads()
        return result

    def get_cpu_counters(self, prefix="proc"):
        """Report memory and CPU counters for C{process}."""
        utime, stime = self.process.get_cpu_times()
        result = {prefix + ".cpu.user": utime,
                  prefix + ".cpu.system": stime}
        return result

    def get_io_counters(self, prefix="proc.io"):
        """Report IO statistics for C{process}."""
        result = {}
        if getattr(self.process, "get_io_counters", None) is not None:
            (read_count, write_count,
             read_bytes, write_bytes) = self.process.get_io_counters()
            result.update({
                prefix + ".read.count": read_count,
                prefix + ".write.count": write_count,
                prefix + ".read.bytes": read_bytes,
                prefix + ".write.bytes": write_bytes})
        return result

    def get_net_stats(self, prefix="proc.net"):
        """Report active connection statistics for C{process}."""
        result = {}
        if getattr(self.process, "get_connections", None) is not None:
            for connection in self.process.get_connections():
                fd, family, _type, laddr, raddr, status = connection
                if _type == socket.SOCK_STREAM:
                    key = prefix + ".status.%s" % status.lower()
                    if not key in result:
                        result[key] = 1
                    else:
                        result[key] += 1
        return result


def report_counters(report_function, *args, **kwargs):
    """
    Report difference between last value and current value for wrapped
    function.
    """
    def generate():
        last_value = None
        while True:
            result = {}
            new_value = report_function(*args, **kwargs)
            if last_value is None:
                last_value = new_value
            else:
                for key, value in new_value.iteritems():
                    result[key] = value - last_value[key]
                last_value = new_value
            yield result
    generator = generate()
    def report():
        return generator.next()
    update_wrapper(report, report_function)
    return report


process_report = ProcessReport()
report_process_memory_and_cpu = process_report.get_memory_and_cpu
report_process_cpu_counters = report_counters(process_report.get_cpu_counters)
report_process_io_counters = report_counters(process_report.get_io_counters)
report_process_net_stats = process_report.get_net_stats


def report_system_stats(prefix="sys", percpu=False):
    cpu_times = psutil.cpu_times(percpu=percpu)
    system_stats = {}
    if not percpu:
        for mode, time in cpu_times._asdict().iteritems():
            system_stats[prefix + ".cpu." + mode] = time
    else:
        for idx, cpu_time in enumerate(cpu_times):
            for mode, time in cpu_time._asdict().iteritems():
                system_stats[prefix + ".cpu.%03d." % idx + mode] = time
    return system_stats


def report_threadpool_stats(threadpool, prefix="threadpool"):
    """Report stats about a given threadpool."""
    def report():
        return {prefix + ".working": len(threadpool.working),
                prefix + ".queue": threadpool.q.qsize(),
                prefix + ".waiters": len(threadpool.waiters),
                prefix + ".threads": len(threadpool.threads)}
    update_wrapper(report, report_threadpool_stats)
    return report


def report_reactor_stats(reactor, prefix="reactor"):
    """Report statistics about a twisted reactor."""
    def report():
        return {prefix + ".readers": len(reactor.getReaders()),
                prefix + ".writers": len(reactor.getWriters())}

    update_wrapper(report, report_reactor_stats)
    return report


def report_file_stats(filename, parser):
    """Read statistics from a file and report them."""
    def report():
        return parser(load_file(filename))
    update_wrapper(report, report_file_stats)
    return report


PROCESS_STATS = (report_process_memory_and_cpu,)

COUNTER_STATS = (report_process_cpu_counters,)

IO_STATS = (report_process_io_counters,)

NET_STATS = (report_process_net_stats,)

SYSTEM_STATS = (report_file_stats("/proc/meminfo", parse_meminfo),
                report_file_stats("/proc/loadavg", parse_loadavg),
                report_counters(report_file_stats("/proc/net/dev", parse_netdev)),
                report_counters(report_system_stats),
                )
