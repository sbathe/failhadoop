#!/usr/bin/env python
import sys
import json, yaml, os, sys, re
from collections import defaultdict
import random

def load_config(args):
    """
    Read the given file to build configuration, the configuration needs to be
    json
    """
    config = dict()
    #loc = [conf, os.curdir+"config.json", ]
    locations = [ os.curdir, os.path.expanduser("~"), "/etc/failhadoop",
                 os.environ.get("FAILHADOOP_ROOT") ]
#    if args.conf:
#        locations.append(args.conf)

    for loc in locations:
      try:
        with open(os.path.join(loc,"config.json")) as source:
            conf = json.load(source)
            config.update(conf)
      except IOError:
        pass
      except:
          print("Cannot load config from any of the locations {0}".format(locations))
    try:
        with open(args.conf) as source:
            conf = json.load(source)
            config.update(conf)
    except IOError:
          print("Cannot load config from any of the locations {0}".format(locations))

   # Override config elements from command line
    for a in vars(args):
       config[a] = getattr(args,a)

    return config

def defaultdict_to_regular(d):
      if isinstance(d, defaultdict):
        d = {k: defaultdict_to_regular(v) for k, v in d.items()}
      return d

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
   files = ['action.py', 'action.sh', 'action.yml']
   targetdir = os.path.join(config['testcaseroot'],component.upper(),testnumber)
   for f in files:
       if os.path.exists(os.path.join(targetdir, f)):
           return (True, f)
   return(False)

def return_random_item(dictionary):
    c = random.choice(list(dictionary.keys()))
    n = random.choice(dictionary[c])
    return (c,n)

def return_testcase_dict(dirname, ignore_list = []):
    m = defaultdict(list)
    include = re.compile('action*')
    ignore = re.compile('action.retry')
    for root, dirs, files in os.walk(dirname):
        for f in files:
            if include.match(f) and not ignore.match(f):
                c, n = root.split('/')[-2:]
                m[c].append(n)
    d = defaultdict_to_regular(m)
    if ignore_list:
      [d.pop(i) for i in l if i in list(d.keys())]
    return d

def return_random_testcase(dirname):
    n = return_testcase_dict(dirname)
    return return_random_item(n)

def match_files(dirname, includes, excludes, clear_dirs):
    files = list()
    include = re.compile(includes)
    ignore  = re.compile(excludes)
    for root, dirs, files in os.walk(dirname):
        for f in files:
            if include.match(f) and not ignore.match(f):
                files.append(os.path.join(root,f))
        if clear_dirs:
            dirs = list()
    return files

def check_for_playbook(dirname):
    playbook = match_files(dirname,'action.yml','action.retry')
    if script:
        return (True, script[0])
    else:
        return(False)

def check_for_script(dirname):
    script = match_files(dirname,'action*','action.retry')
    if script:
        return(True,script[0])
    else:
        return(False)
