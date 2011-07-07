import os

from twisted.internet import defer, fdesc, error
from twisted.python import log


MEMINFO_KEYS = ("MemTotal:", "MemFree:", "Buffers:",
                "Cached:", "SwapCached:", "SwapTotal:",
                "SwapFree:")

SELF_STAT_POSITIONS = {"utime": 13,
                       "stime": 14,
                       "vsize": 22,
                       "rss": 23,
                       "blkio_ticks": 41,
                       "gtime": 42}

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


def parse_self_stat(data, prefix="self.stat."):
    """Parse data from /proc/self/stat."""
    parts = data.split()
    result = {}

    for key, value in SELF_STAT_POSITIONS.items():
        if len(parts) <= value:
            continue

        result[prefix + key] = int(parts[value])

    return result


def parse_stat(data, prefix="stat."):
    """Parse data from /proc/stat."""
    result = {}

    for line in data.split("\n"):
        if not line:
            continue
        parts = [x for x in line.split(" ") if x]
        label = parts[0]
        if len(label) < 3 or label[:3] != 'cpu':
            continue

        for key, value in zip("user nice system idle iowait irq "
                              "softirq steal guest".split(),
                              (int(x) for x in parts[1:])):
            result[prefix + label + "." + key] = value

    return result


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


PROCESS_STATS = (("/proc/self/stat", parse_self_stat),)

SYSTEM_STATS = (("/proc/meminfo", parse_meminfo),
                ("/proc/loadavg", parse_loadavg),
                ("/proc/stat", parse_stat),) + PROCESS_STATS


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
        deferred = load_file(filename)
        deferred.addCallback(func)
        deferred.addCallback(send_metrics, meter)
        deferred.addErrback(lambda failure: log.err(
            failure, "Error while processing %s" % filename))
        deferreds.append(deferred)

    return defer.DeferredList(deferreds)
