"""Tests for metrics context manager."""

from mocker import ANY, MockerTestCase
from txstatsd.metrics.meter import Measure


class MeasureTest(MockerTestCase):
    """Test case for the L{Measure} context manager."""

    def test_measure(self):
        """Basic test."""
        operation_name = 'fake_function'
        meter = self.mocker.mock()
        meter.increment(operation_name)
        meter.timing(operation_name, ANY)
        meter.increment(operation_name + '.done')
        meter.decrement(operation_name)
        self.mocker.replay()
        result = []

        def fake_function():
            """Useles method."""
            result.append(0)

        with Measure('test_prefix', 'fake_function', meter):
            fake_function()
        self.assertEqual([0], result)

    def test_measure_timing(self):
        """Test the timing works."""
        operation_name = 'fake_function'
        MockTime = self.mocker.replace('time.time')  # pylint: disable=C0103
        self.expect(MockTime()).result(10)
        self.expect(MockTime()).result(15)
        meter = self.mocker.mock()
        meter.increment(operation_name)
        meter.timing(operation_name, 5)
        meter.increment(operation_name + '.done')
        meter.decrement(operation_name)
        self.mocker.replay()

        def fake_function():
            """Useless method."""

        with Measure('test_prefix', 'fake_function', meter):
            fake_function()

    def test_measure_handle_exceptions(self):
        """Test exceptions."""

        class TestException(Exception):
            """Exception used to test the wrapper."""
            pass

        operation_name = 'fake_function'
        meter = self.mocker.mock()
        meter.increment(operation_name)
        meter.increment(operation_name + '.error')
        meter.decrement(operation_name)
        self.mocker.replay()

        def fake_function():
            """Fake Method that raises an exception."""
            raise TestException()

        try:
            with Measure('test_prefix', 'fake_function', meter):
                fake_function()
        except TestException:
            self.assertTrue(True)
        else:
            self.fail('measure context manager did not reraise exception.')
