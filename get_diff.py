#!/usr/bin/env python2
import argparse, json, os, time, sys
import requests, difflib

"""

"""

def load_config(config_file="config.json"):
    """
    Read the given file to build configuration, the configuration needs to be
    json
    """
    return json.load(open(config_file))

def setup_ambari_session(config):
    ambari_url = config['ambari']['protocol'] + '://' + config['ambari']['host'] + ':' + config['ambari']['port']
    s = requests.Session()
    s.auth = (config['ambari']['user'],config['ambari']['pass'])
    s.headers.update({'X-Requested-By':'failhadoop'})
    r = s.get(ambari_url + '/api/v1/clusters')
    assert r.status_code == 200, "We did not get a valid respose code back from \
                      Ambari, you might want to check either server or auth \
                      parameters"
    return(s)

def get_current_config_tag(config, cluster, ambari_session, config_element):
    ambari_url = config['ambari']['protocol'] + '://' + config['ambari']['host'] + ':' + config['ambari']['port']
    r = ambari_session.get(ambari_url + '/api/v1/clusters/' + cluster + '?fields=Clusters/desired_configs/' + config_element)
    return r.json()['Clusters']['desired_configs'][config_element]['tag']

def get_tagged_config(config, cluster, ambari_session, config_element, tag):
    ambari_url = config['ambari']['protocol'] + '://' + config['ambari']['host'] + ':' + config['ambari']['port']
    r = ambari_session.get(ambari_url + '/api/v1/clusters/' + cluster + '/configurations?type=' + config_element + '&tag=' + tag)
    return r.json()['items'][0]

def get_config_version_tags(config, cluster, ambari_session, config_element):
    """
    Returns a list of tuples (tag, version) that Ambari knows about the config_element
    """
    ambari_url = config['ambari']['protocol'] + '://' +\
        config['ambari']['host'] + ':' + config['ambari']['port']
    r = ambari_session.get(ambari_url + '/api/v1/clusters/' + cluster +
                           '/configurations?type=' + config_element)
    tp = list()
    for e in r.json()['items']:
        tp.append((e['tag'],e['version']))
    return tp

def get_config_diff(old_config, new_config):
    """Input is a pair of json / dict for old config and new_config"""
    out = str()
    a = json.dumps(old_config, indent=2, sort_keys=True).splitlines(1)
    b = json.dumps(new_config, indent=2, sort_keys=True).splitlines(1)
    for l in difflib.unified_diff(a, b):
        out += l
    return out

def sanitize_input(args):
    if not args.cluster:
        print("Name of the Cluster on Ambari is required")
        parser.print_help()
        sys.exit(2)

    if args.tag1 and not args.tag2:
        print("I need 2 tags to compare")
        parser.print_help()
        sys.exit(2)

parser = argparse.ArgumentParser()
parser.add_argument("-v", action="store_true", default=False, dest='verbose',
                    help="increase output verbosity")
parser.add_argument("-c", "--config", action="store", default='config.json', dest='conf',
                    help="config file to load, should be json")
parser.add_argument("--get-tags", action="store_true", default=True,
                    dest="get_tags", help='get tags and configs of the element')
parser.add_argument("--cluster", action="store", default=None, dest='cluster')
parser.add_argument("--element", action="store", dest='element', default=None)
parser.add_argument("--tag1", action="store", dest='tag1', default=None)
parser.add_argument("--tag2", action="store", dest='tag2', default=None)
args = parser.parse_args()

with open(args.conf) as fp:
  config = json.load(fp)

sanitize_input(args)

s = setup_ambari_session(config)

if not args.tag1:
    if args.get_tags:
        tags_and_versions = get_config_version_tags(config,args.cluster, s,
                                                args.element) 
        print("Ambari has following tags and versions for {0}:\n\t{1}".format(args.element, tags_and_versions))

if args.tag1:
    config1 = get_tagged_config(config, args.cluster, s, args.element,
                                args.tag1)
    config2 = get_tagged_config(config, args.cluster, s, args.element,
                                args.tag2)
    diff = get_config_diff(config1, config2)
    print("Here goes the diff between {0} and {1}: \n{2}".format(args.tag1,
          args.tag2, diff))

