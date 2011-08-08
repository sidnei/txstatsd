import os
import socket
import psutil

from functools import update_wrapper

from twisted.internet import defer, fdesc, error


MEMINFO_KEYS = ("MemTotal:", "MemFree:", "Buffers:",
                "Cached:", "SwapCached:", "SwapTotal:",
                "SwapFree:")

MULTIPLIERS = {"kB": 1024, "mB": 1024 * 1024}


def load_file(filename):
    """Load a file into memory with non blocking reads."""
    fd = os.open(filename, os.O_RDONLY)
    fdesc.setNonBlocking(fd)

    chunks = []
    d = defer.Deferred()

    def read_loop(data=None):
        """Inner loop."""
        if data is not None:
            chunks.append(data)
        r = fdesc.readFromFD(fd, read_loop)
        if isinstance(r, error.ConnectionDone):
            os.close(fd)
            d.callback("".join(chunks))
        elif r is not None:
            os.close(fd)
            d.errback(r)

    read_loop("")
    return d


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
        utime, stime = self.process.get_cpu_times()
        result = {prefix + ".cpu.percent": self.process.get_cpu_percent(),
                  prefix + ".cpu.user": utime,
                  prefix + ".cpu.system": stime,
                  prefix + ".memory.percent": self.process.get_memory_percent(),
                  prefix + ".memory.vsize": vsize,
                  prefix + ".memory.rss": rss}
        if getattr(self.process, "get_num_threads", None) is not None:
            result[prefix + ".threads"] = self.process.get_num_threads()
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


process_report = ProcessReport()
report_process_memory_and_cpu = process_report.get_memory_and_cpu
report_process_io_counters = process_report.get_io_counters
report_process_net_stats = process_report.get_net_stats


def report_system_stats(prefix="sys"):
    cpu_times = psutil.cpu_times()._asdict()
    system_stats = {}
    for mode, time in cpu_times.iteritems():
        system_stats[prefix + ".cpu." + mode] = time
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
        deferred = load_file(filename)
        deferred.addCallback(parser)
        return deferred
    update_wrapper(report, report_file_stats)
    return report


PROCESS_STATS = (report_process_memory_and_cpu,)

IO_STATS = (report_process_io_counters,)

NET_STATS = (report_process_net_stats,)

SYSTEM_STATS = (report_file_stats("/proc/meminfo", parse_meminfo),
                report_file_stats("/proc/loadavg", parse_loadavg),
                report_file_stats("/proc/net/dev", parse_netdev),
                report_system_stats)
