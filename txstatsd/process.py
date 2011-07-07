import os
import inspect
import socket
import psutil

from twisted.internet import defer, fdesc, error
from twisted.python import log


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


def parse_meminfo(data, prefix="meminfo."):
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
        result[prefix + label] = amount

    return result


def parse_loadavg(data, prefix="loadavg."):
    """Parse data from /proc/loadavg."""
    return dict(zip(
        (prefix + "oneminute",
         prefix + "fiveminutes",
         prefix + "fifthteenminutes"),
        [float(x) for x in data.split()[:3]]))


def report_process_memory_and_cpu(process=psutil.Process(os.getpid()),
                          prefix="proc."):
    """Report memory and CPU stats for C{process}."""
    vsize, rss = process.get_memory_info()
    utime, stime = process.get_cpu_times()
    result = {prefix + "cpu.percent": process.get_cpu_percent(),
              prefix + "cpu.user": utime,
              prefix + "cpu.system": stime,
              prefix + "memory.percent": process.get_memory_percent(),
              prefix + "memory.vsize": vsize,
              prefix + "memory.rss": rss,
              prefix + "threads": process.get_num_threads()}
    return result


def report_process_io_counters(process=psutil.Process(os.getpid()),
                       prefix="proc.io."):
    """Report IO statistics for C{process}."""
    result = {}
    if getattr(process, "get_io_counters", None) is not None:
        (read_count, write_count,
         read_bytes, write_bytes) = process.get_io_counters()
        result.update({
            prefix + "read.count": read_count,
            prefix + "write.count": write_count,
            prefix + "read.bytes": read_bytes,
            prefix + "write.bytes": write_bytes})
    return result


def report_process_net_stats(process=psutil.Process(os.getpid()),
                             prefix="proc.net."):
    """Report active connection statistics for C{process}."""
    result = {}
    if getattr(process, "get_connections", None) is not None:
        for connection in process.get_connections():
            fd, family, _type, laddr, raddr, status = connection
            if _type == socket.SOCK_STREAM:
                key = prefix + "status.%s" % status.lower()
                if not key in result:
                    result[key] = 1
                else:
                    result[key] += 1
    return result


def report_system_stats(prefix="sys."):
    cpu_times = psutil.cpu_times()
    return {prefix + "cpu.idle": cpu_times.idle,
            prefix + "cpu.iowait": cpu_times.iowait,
            prefix + "cpu.irq": cpu_times.irq,
            prefix + "cpu.nice": cpu_times.nice,
            prefix + "cpu.system": cpu_times.system,
            prefix + "cpu.user": cpu_times.user}


class report_threadpool_stats(object):
    """Report stats about a given threadpool."""

    def __init__(self, threadpool, prefix="threadpool."):
        self.threadpool = threadpool
        self.prefix = prefix

    def __call__(self):
        return {self.prefix + "workers": len(self.threadpool.working),
                self.prefix + "queue": self.threadpool.q.qsize(),
                self.prefix + "waiters": len(self.threadpool.waiters),
                self.prefix + "threads": len(self.threadpool.threads)}


class report_reactor_stats(object):
    """Report statistics about a twisted reactor."""

    def __init__(self, reactor, prefix="reactor."):
        self.reactor = reactor
        self.prefix = prefix

    def __call__(self):
        return {self.prefix + "readers": len(self.reactor.getReaders()),
                self.prefix + "writers": len(self.reactor.getWriters())}


PROCESS_STATS = ((None, report_process_memory_and_cpu),)

IO_STATS = ((None, report_process_io_counters),)

NET_STATS = ((None, report_process_net_stats),)

SYSTEM_STATS = (("/proc/meminfo", parse_meminfo),
                ("/proc/loadavg", parse_loadavg),
                (None, report_system_stats),)


def send_metrics(metrics, meter):
    """Put a dict of values in stats."""
    for name, value in metrics.items():
        meter.increment(name, value)


def report_stats(stats, meter):
    """
    Read C{filename} then call C{function} to parse the contents, then report
    to C{StatsD}.
    """

    deferreds = []
    for filename, func in stats:
        if filename is not None:
            name = filename
            deferred = load_file(filename)
            deferred.addCallback(func)
        else:
            if inspect.isfunction(func):
                name = func.func_name
            elif inspect.ismethod(func):
                name = func.im_class.__name__ + "." + func.func_name
            deferred = defer.maybeDeferred(func)

        deferred.addCallback(send_metrics, meter)
        deferred.addErrback(lambda failure: log.err(
            failure, "Error while processing %s" % name))
        deferreds.append(deferred)

    return defer.DeferredList(deferreds)
