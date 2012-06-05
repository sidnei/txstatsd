# -*- coding: utf-8 *-*


class BelowCondition(object):

    def __init__(self, value):
        self.value = value

    def __call__(self, value):
        return value < self.value


class AboveCondition(object):

    def __init__(self, value):
        self.value = value

    def __call__(self, value):
        return value > self.value

class BetweenCondition(object):

    def __init__(self, low, hi):
        self.low = low
        self.hi = hi

    def __call__(self, value):
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

    def process(self, fields):
        self.update(float(fields[0]))

    def update(self, value):
        self.count += 1
        for k, condition in self.conditions.items():

            if condition(value):
                self.counts[k] += 1

    def flush(self, interval, timestamp):
        metrics = []
        for item, value in self.counts.items():
            metrics.append((self.name + ".count_" + item,
                            value, timestamp))
        metrics.append((self.name + ".count",
                            self.count, timestamp))

        self.clear()
        return metrics
