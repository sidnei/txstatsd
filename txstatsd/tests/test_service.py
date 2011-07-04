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
        
        
    def get_file_parser(self):
        """
        Create a simple option parser that reads from disk.
        """
        f = tempfile.NamedTemporaryFile()
        
        config = ConfigParser.RawConfigParser()
        config.add_section('main')
        config.set('main', 'test', 'configvalue')
        config.write(f)
        f.flush()
        
        class TestOptions(service.OptionsGlue):
            glue_parameters = [["test", "t", "default", "help"]]
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
    
class ServiceTests(TestCase):
    def test_service(self):
        o = service.StatsdOptions()
        s = service.createService(o)
        self.assertTrue(isinstance(s, service.MultiService))