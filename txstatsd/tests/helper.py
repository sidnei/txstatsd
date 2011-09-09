from txstatsd.metrics.metrics import Metrics

class FakeStatsDClient(object):
    """A fake C{StatsDClient} that simply appends to metrics.data on write."""

    def __init__(self, metrics):
        self.metrics = metrics

    def write(self, data):
        self.metrics.data.append(data)


class FakeMetrics(Metrics):
    """A fake C{IMeter} that simply stores metrics locally."""

    def __init__(self, namespace=""):
        Metrics.__init__(self, FakeStatsDClient(self), namespace=namespace)
        self.data = []
