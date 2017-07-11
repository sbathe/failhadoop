#!/usr/bin/env python
import sys
import json, yaml, os, sys

def load_config(args):
    """
    Read the given file to build configuration, the configuration needs to be
    json
    """
    config = dict()
    #loc = [conf, os.curdir+"config.json", ]
    locations = [ os.curdir, os.path.expanduser("~"), "/etc/failhadoop",
                 os.environ.get("FAILHADOOP_ROOT") ]
    if args.conf:
        locations.append(args.conf)

    for loc in locations:
      try:
        with open(os.path.join(loc,"config.json")) as source:
            conf = json.load(source)
            config.update(conf)
      except IOError:
        pass
      except:
          print("Cannot load config from any of the locations {0}".format(locations))
   # Override config elements from command line
    for a in vars(args):
       config[a] = getattr(args,a)

    return config

def check_component_exists(config, component):
    ''' Check if we have any test cases for the component'''
    if os.path.isdir(os.path.join(config['testcaseroot'],component.upper())):
        return True
    return False

def check_testcase_exists(config,component,testnumber):
    '''Check if we have the stated failure case in our repository'''
    if os.path.isdir(os.path.join(config['testcaseroot'],component.upper(),testnumber)):
        return True
    return False

def load_testconfig(config,component,testnumber):
    '''Load the test config. Here is what we need:
    mode: ansible or ambari APIs
    If its ansible, we also need hostgroup to run against
    If its ambari APIs, for now we will need, the script to do all the work we
    need done. At some later point we can build more functions to pass in
    config parameters we can change, services we need stopped and all
    '''
    testconfig_file = os.path.join(config['testcaseroot'],component.upper(),testnumber,
                 'testconfig.json')
    try:
      with open(testconfig_file) as conf:
        testconfig = json.load(conf)
      return testconfig
    except:
        return None

def get_test_script(config, component, testnumber):
   ''' Check if there is a action script and return the name of the script,
   action.sh or action.py'''
   files = ['action.py', 'action.sh']
   targetdir = os.path.join(config['testcaseroot'],component.upper(),testnumber)
   for f in files:
       if os.path.exists(os.path.join(targetdir, f)):
           return (True, f)
   return(False)
