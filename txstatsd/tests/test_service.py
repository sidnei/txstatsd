import ConfigParser
import tempfile
from unittest import TestCase

from txstatsd import service


class GlueOptionsTestCase(TestCase):

    def test_defaults(self):
        """
        Defaults get passed over to the instance.
        """
        class TestOptions(service.OptionsGlue):
            glue_parameters = [["test", "t", "default", "help"]]

        o = TestOptions()
        o.parseOptions([])
        self.assertEquals("default", o["test"])

    def test_set_parameter(self):
        """
        A parameter can be set from the command line
        """
        class TestOptions(service.OptionsGlue):
            glue_parameters = [["test", "t", "default", "help"]]

        o = TestOptions()
        o.parseOptions(["--test", "notdefault"])
        self.assertEquals("notdefault", o["test"])

    def test_no_config_option(self):
        """
        A parameter can be set from the command line
        """
        class TestOptions(service.OptionsGlue):
            glue_parameters = [["config", "c", "default", "help"]]

        self.assertRaises(ValueError, lambda: TestOptions())

    def get_file_parser(self, glue_parameters_config=None, **kwargs):
        """
        Create a simple option parser that reads from disk.
        """
        if glue_parameters_config is None:
            glue_parameters_config = [["test", "t", "default", "help"]]
        f = tempfile.NamedTemporaryFile()

        config = ConfigParser.RawConfigParser()
        config.add_section('statsd')
        if not kwargs:
            config.set('statsd', 'test', 'configvalue')
        else:
            for k, v in kwargs.items():
                config.set('statsd', k, v)
        config.write(f)
        f.flush()

        class TestOptions(service.OptionsGlue):
            glue_parameters = glue_parameters_config
        return f, TestOptions()

    def test_reads_from_config(self):
        """
        A parameter can be set from the config file.
        """
        f, o = self.get_file_parser()
        o.parseOptions(["--config", f.name])
        self.assertEquals("configvalue", o["test"])

    def test_cmdline_overrides_config(self):
        """
        A parameter from the cmd line overrides the config.
        """
        f, o = self.get_file_parser()
        o.parseOptions(["--config", f.name, "--test", "cmdline"])
        self.assertEquals("cmdline", o["test"])

    def test_ensure_config_values_coerced(self):
        """
        Parameters come out of config files casted properly.
        """
        f, o = self.get_file_parser([["number", "n", 5, "help", int]],
            number=10)
        o.parseOptions(["--config", f.name])
        self.assertEquals(10, o["number"])

    def test_support_default_not_in_config(self):
        """
        Parameters not in config files still produce a lookup in defaults.
        """
        f, o = self.get_file_parser([["number", "n", 5, "help", int]])
        o.parseOptions(["--config", f.name])
        self.assertEquals(5, o["number"])


class ServiceTests(TestCase):

    def test_service(self):
        """
        The StatsD service can be created.
        """
        o = service.StatsDOptions()
        s = service.createService(o)
        self.assertTrue(isinstance(s, service.MultiService))
