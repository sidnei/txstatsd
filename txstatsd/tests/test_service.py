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
        self.assertEquals(o["test"], "default")
    
    def test_set_parameter(self):
        """
        A parameter can be set from the command line
        """
        class TestOptions(service.OptionsGlue):
            glue_parameters = [["test", "t", "default", "help"]]
            
        o = TestOptions()
        o.parseOptions(["--test", "notdefault"])
        self.assertEquals(o["test"], "notdefault")
        
    def test_no_config_option(self):
        """
        A parameter can be set from the command line
        """
        class TestOptions(service.OptionsGlue):
            glue_parameters = [["config", "c", "default", "help"]]
            
        
        self.assertRaises(ValueError, lambda: TestOptions())
        
        
    def get_file_parser(self, gparam=None, **kwargs):
        """
        Create a simple option parser that reads from disk.
        """
        if gparam is None:
            gparam = [["test", "t", "default", "help"] ]
        f = tempfile.NamedTemporaryFile()
        
        config = ConfigParser.RawConfigParser()
        config.add_section('main')
        if not kwargs:
            config.set('main', 'test', 'configvalue')
        else:
            for k, v in kwargs.items():
                config.set('main', k, v)
        config.write(f)
        f.flush()
        
        class TestOptions(service.OptionsGlue):
            glue_parameters = gparam
        return f, TestOptions()
        
    def test_reads_from_config(self):
        """
        A parameter can be set from the config file.
        """
        f, o = self.get_file_parser()
        o.parseOptions(["--config", f.name])
        self.assertEquals(o["test"], "configvalue")
    
    def test_cmdline_overrides_config(self):
        """
        A parameter can be set from the config file.
        """
        f, o = self.get_file_parser()
        o.parseOptions(["--config", f.name, "--test", "cmdline"])
        self.assertEquals(o["test"], "cmdline")
        
    def test_ensure_config_values_coerced(self):
        f, o = self.get_file_parser([["number", "n", 5, "help", int]],
            number=10)
        o.parseOptions(["--config", f.name])
        self.assertEquals(o["number"], 10)
        
    def test_support_default_not_in_config(self):
        f, o = self.get_file_parser([["number", "n", 5, "help", int]])
        o.parseOptions(["--config", f.name])
        self.assertEquals(o["number"], 5)
        
        
class ServiceTests(TestCase):
    def test_service(self):
        o = service.StatsdOptions()
        s = service.createService(o)
        self.assertTrue(isinstance(s, service.MultiService))