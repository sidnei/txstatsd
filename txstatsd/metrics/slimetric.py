# -*- coding: utf-8 *-*


class BelowCondition(object):

    def __init__(self, value, slope=0):
        self.value = value
        self.slope = slope

    def __call__(self, value, size=1):
        return value < self.value + self.slope * size


class AboveCondition(object):

    def __init__(self, value, slope=0):
        self.value = value
        self.slope = slope

    def __call__(self, value, size=1):
        return value > self.value + self.slope * size


class BetweenCondition(object):

    def __init__(self, low, hi):
        self.low = low
        self.hi = hi

    def __call__(self, value, size=1):
        return self.low < value < self.hi


class SLIMetricReporter(object):
    def __init__(self, name, conditions):
        self.name = name
        self.conditions = conditions
        self.conditions = conditions
        self.clear()

    def clear(self):
        self.counts = dict((k, 0) for k in self.conditions)
        self.count = 0
        self.error = 0

    def process(self, fields):
        size = 1
        if len(fields) == 3:
            size = float(fields[2])

        value = "error"
        if value != fields[0]:
            value = float(fields[0])
        self.update(value, size)

    def update(self, value, size=1):
        self.count += 1
        if value == "error":
            self.error += 1
        else:
            for k, condition in self.conditions.items():
                if condition(value, size):
                    self.counts[k] += 1

    def flush(self, interval, timestamp):
        metrics = []
        for item, value in self.counts.items():
            metrics.append((self.name + ".count_" + item,
                            value, timestamp))
        metrics.append((self.name + ".count",
                            self.count, timestamp))
        metrics.append((self.name + ".error",
                            self.error, timestamp))

        self.clear()
        return metrics
