#!/usr/local/bin/python3
import sys
#print(sys.path)
import json, yaml, os, sys, argparse
import failhadoop
#from ansible.executor.task_queue_manager import TaskQueueManager
#from ansible.plugins.callback import CallbackBase
#from ansible.executor.stats import AggregateStats
#print(dir(failhadoop))
#from failhadoop import ansible_helpers

"""
TODO:
    - Add error handling all over, its a major flaw right now
"""
parser = argparse.ArgumentParser()
parser.add_argument("-v", action="store_true", default=False, dest='verbose',
                    help="increase output verbosity")
parser.add_argument("-d","--dry-run", action="store_true", default=False, dest='dry',
                    help="increase output verbosity")
parser.add_argument("-c", "--config", action="store", default='config.json', dest='conf',
                    help="config file to load, should be json")
parser.add_argument("-i", "--inventory-dir", action="store", default='/tmp/inventory', dest='inventory_dir',
                    help="localtion of the ansible inventory")
parser.add_argument("--testcase-root", action="store", default=os.path.join(os.path.expanduser("~"),'repos/Hadoop-Failures'), dest='testcaseroot', help="location of the hadoop failure repository")
parser.add_argument("--ssh-key", action="store", default=os.path.join(os.path.expanduser("~"),'.ssh/id_rsa'), dest='ssh_key', help="location of the ssh private key used by ansible to ssh")
parser.add_argument("--service", action="store", dest='service',
                    help="service to run test case against")
parser.add_argument("--testnumber", action="store", dest='testno',
                    help="Component test case number to run")
parser.add_argument("--random", action="store_true", dest='random',
                    help="Generate a random test case component and number")
args = parser.parse_args()
verbose = args.verbose
dry = args.dry

def sanitize_args(args):
    '''Need to write our own, argparse is not flexible enough'''
    if (args.service and args.testno) or args.random:
        pass
    else:
        print('Please specify the service and testcase number or ask me to\
              generate that at random (--service and --testnumber, or --random)')
        sys.exit(2)

def load_config(args, conf='config.json'):
    """
    Read the given file to build configuration, the configuration needs to be
    json
    """
    config = dict()
    #loc = [conf, os.curdir+"config.json", ]
    for loc in args.conf, os.curdir, os.path.expanduser("~"), "/etc/failhadoop", os.environ.get("FAILHADOOP_ROOT"):
      try:
        with open(os.path.join(loc,"config.json")) as source:
            conf = json.load(source)
            config.update(conf)
      except IOError:
        pass
      except:
          pass
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
   files = ['action.py', 'action.sh', 'action.yml']
   targetdir = os.path.join(config['testcaseroot'],component.upper(),testnumber)
   for f in files:
       if os.path.exists(os.path.join(targetdir, f)):
           return (True, f)
   return(False)

# Here starts the main execution
sanitize_args(args)
config = load_config(args)
if args.service and args.testno:
  component = args.service
  testno = args.testno
else:
    component, testno = failhadoop.utils.return_random_testcase(config['testcaseroot'])
    print("{0}, {1}".format(component, testno))

if dry:
    print('Dry run, will not run the test')

if not check_component_exists(config, component):
    print("Cannot find {0} in {1}".format(component.upper(),
                                          config['testcaseroot']))
    sys.exit(1)

if not check_testcase_exists(config,component,testno):
    print("Cannot find directory named {0} at {1}".format(testno,os.path.join(config['testcaseroot'],component.upper())))
    sys.exit(1)

script = get_test_script(config, component, testno)
if script and verbose:
  print(script)
  print(script[1][-3:])

if not script:
    if verbose:
        print("There does not seem to any action.py or action.sh in {0}".format(os.path.join(config['testcaseroot'],component.upper(),testno)))
        sys.exit(1)

testconfig = load_testconfig(config,component,testno)
scriptdir = os.path.join(config['testcaseroot'],component.upper(), testno)
config['service'] = args.service
config['config_file'] = args.conf if args.conf else 'config.json'
config.update(testconfig)
if testconfig:
    if (testconfig['mode'] == 'ansible' and script[1][-3:] == 'yml'):
        if dry:
            print("Will running playbook: {0} on the remote hosts".format(script[1]))
            print("Will call run_playbook with args {0}, {1}, {2}".format(config['inventory_dir'],[os.path.join(scriptdir,
                                                             script[1])], config))
            sys.exit(0)

        if verbose:
            print("Running playbook: {0} on the remote hosts".format(script[1]))
            print("calling run_playbook with args {0}, {1}, {2}".format(config['inventory_dir'],[os.path.join(scriptdir,
                                                             script[1])], config))
        results = failhadoop.ansible_helpers.run_playbook(config['inventory_dir'],[os.path.join(scriptdir,script[1])],config)
    else:
        if dry:
            print("Will Run script: {0} on the remote hosts".format(script[1]))
            print("will call run_play")
            sys.exit(0)
        if verbose:
            print("Running script: {0} on the remote hosts".format(script[1]))
            print("calling run_play")
        results = failhadoop.ansible_helpers.run_play(config['inventory_dir'],testconfig['hostpattern'],os.path.join(scriptdir, script[1]),connection='ssh')

    tqm = results[1]
    stats = tqm._stats
    # Test if success for record_logs
    run_success = True
    hosts = sorted(stats.processed.keys())
    for h in hosts:
        t = stats.summarize(h)
        if t['unreachable'] > 0 or t['failures'] > 0:
            run_success = False

    tqm.send_callback('default', success=run_success)
else:
    print("""There does not seem to any testconfig.json in {0}""".format(os.path.join(config['testcaseroot'],component.upper(),testno)))
    sys.exit(1)
