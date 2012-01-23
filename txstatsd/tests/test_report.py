from twisted.trial.unittest import TestCase
from twisted.internet.task import Clock

from txstatsd.report import ReportingService


class TestReportingService(TestCase):

    def test_start_stop_with_no_tasks(self):
        """Service can be started/stopped even if there are no tasks."""
        service = ReportingService()
        self.assertEqual(0, len(service.tasks))
        service.startService()
        self.assertTrue(service.running)
        service.stopService()
        self.assertFalse(service.running)

    def test_schedule_with_report_function(self):
        """Scheduling with a report function wraps original function."""
        clock = Clock()
        service = ReportingService(clock=clock)

        def foo():
            return {"foo": 1}

        called = []
        def report(name, value):
            called.append((name, value))

        service.schedule(foo, 1, report)
        self.assertEqual(1, len(service.tasks))
        service.startService()
        clock.advance(1)
        self.assertEquals([("foo", 1)], called)

    def test_schedule_without_report_function(self):
        """Scheduling without a report function calls original function."""
        clock = Clock()
        service = ReportingService(clock=clock)

        called = []
        def foo():
            called.append(("foo", 1))

        service.schedule(foo, 1, None)
        self.assertEqual(1, len(service.tasks))
        service.startService()
        clock.advance(1)
        self.assertEquals([("foo", 1)], called)

    def test_schedule_when_running(self):
        """Schedule after service is running runs the task immediately."""
        clock = Clock()
        service = ReportingService(clock=clock)
        service.startService()

        called = []
        def foo():
            called.append(("foo", 1))

        service.schedule(foo, 1, None)
        self.assertEqual(1, len(service.tasks))
        self.assertEquals([("foo", 1)], called)

    def test_report_with_instance_name(self):
        """
        If an instance_name was provided when creating the ReportingService, it
        gets prepended to the metric name when reporting.
        """
        clock = Clock()
        service = ReportingService(instance_name="instance-1", clock=clock)

        def foo():
            return {"foo": 1}

        called = []
        def report(name, value):
            called.append((name, value))

        service.schedule(foo, 1, report)
        self.assertEqual(1, len(service.tasks))
        service.startService()
        clock.advance(1)
        self.assertEquals([("instance-1.foo", 1)], called)
