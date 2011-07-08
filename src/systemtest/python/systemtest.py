#!/usr/bin/python
#
# dedupv1 - iSCSI based Deduplication System for Linux
# 
# (C) 2008 Dirk Meister
# (C) 2009 - 2011, Dirk Meister, Paderborn Center for Parallel Computing
# 

import unittest
import glob

from time import sleep, time
import optparse
import tempfile
from run import Run
import os
import sys
import simplejson
import re
import stat
import shutil
import signal
import hashlib
import random
import socket

output_dir = None
configuration = {}

def touchFile(path):
    f = open(path, "w")
    f = None

def from_fsc_root(path):
    mod = sys.modules[__name__]
    return os.path.abspath(os.path.join(os.path.dirname(mod.__file__), "../../..", path))

def from_fsc_systemtest_root(path):
    return os.path.join(from_fsc_root("src/systemtest"), path)

class FSCSystemTest(unittest.TestCase):
    
    def run_fs_c(self, arguments):
        return self.run("%s/fs-c %s" % (self.get_bin_directory(), arguments))
    
    def get_working_directory(self):
        work_dir = configuration["work dir"]
        return os.path.abspath(from_fsc_root(work_dir))
    
    def get_bin_directory(self):
        dir = configuration["bin dir"]
        return os.path.abspath(from_fsc_root(dir))
    
    def setUp(self):
        print
        self.run = Run()
        
        def setUpWorkingDirectory():
            work_dir = self.get_working_directory()
            shutil.rmtree(work_dir, True)
            os.mkdir(work_dir)
        
        setUpWorkingDirectory()
    
    def test_ignore_symlinks(self):
        """ test_ignore_symlinks
        
            Tests if symlinks are ignored in the default configuration
        """
        self._ignore_symlink_test(True)
     
    def test_follow_symlinks(self):
        """ test_follow_symlinks
        
            Tests if symlinks are followed when --follow-symlinks is given
        """
        self._ignore_symlink_test(False)
        
    def test_find_simple_redundancy(self):
        """ test_find_simple_redundancy
        """
        work_dir = self.get_working_directory()
        def prepareWorkingDirectory():            
            os.mkdir(os.path.join(work_dir, "c"))
            
            urandom = open("/dev/urandom")
            data = urandom.read(1024 * 1024)
            
            f1 = open(os.path.join(work_dir, "c", "1"), "w")
            f2 = open(os.path.join(work_dir, "c", "2"), "w")
            
            f1.write(data)
            f2.write(data)            
            
        prepareWorkingDirectory()         
        output = self.run_fs_c("trace -f %s" % work_dir)
            
        self.assertTrue(any(re.search(r"Redundancy: .* \(50,00%\)", line) for line in output.split("\n")))
  
    def test_find_simple_shifted_redundancy(self):
        """ test_find_simple_shifted_redundancy
        """
        work_dir = self.get_working_directory()
        def prepareWorkingDirectory():            
            os.mkdir(os.path.join(work_dir, "c"))

            f1 = open(os.path.join(work_dir, "c", "1"), "w")
            f2 = open(os.path.join(work_dir, "c", "2"), "w")
            
            urandom = open("/dev/urandom")
            
            # Shifting
            data = urandom.read(4 * 1024)
            f2.write(data)   
                        
            data = urandom.read(1024 * 1024)
            f1.write(data)
            f2.write(data)            
            
        prepareWorkingDirectory()         
        output = self.run_fs_c("trace -f %s" % work_dir)
        
        percent_intervals = [(0, 0), (95, 100), (45, 50)]
        
        for line in output.split("\n"):
            match = re.search("Redundancy: .* \((\d{1,2}),\d{1,2}%\)", line)
            if match:
                percent_value = int(match.groups()[0])
                self.assertTrue(len(percent_intervals) > 0)
                
                percent_interval = percent_intervals[0]
                del percent_intervals[0]
                
                self.assertTrue(percent_value >= percent_interval[0])
                self.assertTrue(percent_value <= percent_interval[1])
                
        #self.assertTrue(any(re.search(r"Redundancy: .* \(50,00%\)", line) for line in output.split("\n")))
        
    def _ignore_symlink_test(self, ignore):
        work_dir = self.get_working_directory()
            
        def prepareWorkingDirectory():            
            os.mkdir(os.path.join(work_dir, "c"))
            os.mkdir(os.path.join(work_dir, "c/a"))
            touchFile(os.path.join(work_dir, "c/a", "b"))   
            os.symlink("a", os.path.join(work_dir, "c/c"))
        prepareWorkingDirectory()
        
        if ignore:
            output = self.run_fs_c("trace -f %s" % os.path.join(work_dir, "c"))
            expected_count = 1
        else:
            output = self.run_fs_c("trace --follow-symlinks -f %s" % os.path.join(work_dir, "c"))
            expected_count = 2
        self.assertEqual(expected_count, output.count("a/b"))
        
    def tearDown(self):
        pass
            
def perform_systemtest(system_test_classes, option_args = None):
    
    def decorate_parser(parser = None):
        if not parser:
            parser = optparse.OptionParser()        
        parser.add_option("--xml",
            action="store_true",
            dest="xml",
            default=False)
        parser.add_option("--output",
            dest="output",
            default=".")
        parser.add_option("--repeat_until_error",
                          action="store_true",
                          dest="repeat_until_error",
                          default=False)
        parser.add_option("--config", dest="config", default=None)
        return parser
    
    def get_test_suite(argv):
        suite = unittest.TestSuite()

        test_name_pattern_list = [re.compile("^%s$" % p) for p in argv]
        if len(test_name_pattern_list) == 0:
            # default pattern
             test_name_pattern_list.append(re.compile("^.*$"))
             
        for system_test_class in system_test_classes:
            for test_case_name in unittest.defaultTestLoader.getTestCaseNames(system_test_class):
                for test_name_pattern in test_name_pattern_list:
                    if test_name_pattern.match(test_case_name):
                        suite.addTest(system_test_class(test_case_name))
        return suite
    
    def get_test_runner():
        if options.xml:
            import xmlrunner
            return xmlrunner.XMLTestRunner(output=options.output, verbose=True)
        else:
            return unittest.TextTestRunner(verbosity=2)    
        
    def get_configuration():
        if not options.config:
            config_dir = from_fsc_systemtest_root("resources/conf")
        
            hostname = socket.gethostname()
            hostname_config_filename = os.path.join(config_dir, hostname + ".conf")
            if os.path.exists(hostname_config_filename):
                options.config = hostname_config_filename
            else:
                options.config = os.path.join(config_dir, "default.conf")
            
        print "Using configuration file", options.config
        return simplejson.loads(open(options.config, "r").read())
        
    global output_dir, configuration
    try:    
        if not option_args:
            option_args = decorate_parser().parse_args()
        (options, argv) = option_args

        output_dir = options.output
        configuration = get_configuration()

        repeat_count = 1
        while True:
            if options.repeat_until_error:
                print "Iteration", repeat_count
            suite = get_test_suite(argv)
            runner = get_test_runner()
            result = runner.run(suite)
            if not options.repeat_until_error or not result.wasSuccessful():
                break
            repeat_count += 1
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    perform_systemtest(
            [
                FSCSystemTest
            ])
