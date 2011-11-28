from zope.interface import Interface

class IMetrics(Interface):
    """
    Provides a global utility for sending out metric information.
    """

    def gauge(name, value, sample_rate=1):
        """Record an absolute reading for C{name} with C{value}."""

    def increment(name, value=1, sample_rate=1):
        """Increment counter C{name} by C{count}."""

    def decrement(name, value=1, sample_rate=1):
        """Decrement counter C{name} by C{count}."""

    def timing(name, duration=None, sample_rate=1):
        """Report that C{name} took C{duration} seconds."""
