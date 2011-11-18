from zope.interface import Interface, Attribute


class IMetricFactory(Interface):
    name = Attribute("""
        @type name C{str}
        @ivar name: The name of this kind of metric
        """)

    metric_type = Attribute("""
        @type metric_type: C{str}
        @ivar metric_type: The string that will be used by clients to send
        metrics of this kind to the server.
        """)

    def build_metric(prefix, name, wall_time_func=None):
        """
        Returns an object that implements the C{IMetric} interface for name.

        @type prefix: C{str}
        @param prefix: The prefix used for reporting this metric.
        @type name: C{str}
        @param name: The name used for reporting this metric.
        """

    def configure(options):
        """
        Configures the factory. Will be called at startup by the service
        factory.

        @type options: C{twisted.python.usage.Options}
        @param options: The configuration options.
        """


class IMetric(Interface):
    def process(fields):
        """
        Process new data for this metric.

        @type fields: C{list}
        @param fields: The list of message parts. Usually in the form of
        (value, metric_type, [sample_rate])
        """

    def flush(interval, timestamp):
        """
        Returns a string with new line separated list of metrics to report.

        @type interval: C{float}
        @param interval: The time since last flush.
        @type timestamp: C{float}
        @param timestamp: The timestamp for now.
        """
