#!/usr/bin/env python
from ambariclient.client import Ambari
import argparse, json, collections, os, yaml
import socket, requests, difflib


# We use python ambari client from
# https://github.com/jimbobhickville/python-ambariclient for most of the
# read-only operations

# Most of the configuration related stuff is shamelessly borrowed from jupyter
# notebooks at http://nbviewer.jupyter.org/github/seanorama/ambari-bootstrap/tree/master/api-examples/

# TODO: 
# - Handle exceptions at all stages
# - Make a more featureful inventory with host args and child groups for
# clusters

def load_config(config_file="config.json"):
    """
    Read the given file to build configuration, the configuration needs to be
    json
    """
    return json.load(open(config_file))

def amclient(config):
    ''' Take a config json as input, return a ambariclient.Client.Ambari
    instance'''
    client = Ambari(config['ambari']['host'], port=config['ambari']['port'],
        username=config['ambari']['user'], password=config['ambari']['pass'])
    return client

def get_inventory(client):
    """
    Get details of hosts and the components they run from Ambari and create
    Inventory files for Anasible
    """
    inventory = dict()
    for c in client.clusters().to_dict():
      cluster = c['cluster_name']
      host_components = client.clusters(cluster).host_components().to_dict()
      inventory[cluster] = dict()
      for hc in host_components:
        if hc['component_name'] not in inventory[cluster].keys():
          inventory[cluster][hc['component_name']] = dict()
          inventory[cluster][hc['component_name']]['hosts'] = dict()
          inventory[cluster][hc['component_name']]['hosts'][hc['host_name']] = dict()
        elif hc['host_name'] not in inventory[cluster][hc['component_name']]['hosts']:
          inventory[cluster][hc['component_name']]['hosts'][hc['host_name']] = dict()
      #clist = collections.defaultdict(list)
      #for hc in host_components:
      #  clist[hc['component_name']].append(hc['host_name'])
    return inventory

def write_inventory(inventory, outdir='/tmp/inventory'):
    if not os.path.isdir(outdir):
        try:
            os.mkdir(outdir)
        except:
            print("Cannot create outdir: {0}".format(outdir))

    for k in inventory.keys():
      with open(os.path.join(outdir,k+'.yaml'),'w') as outfile:
        yaml.dump(inventory[k], outfile, default_flow_style=False)

def fetch_inventory(config_file='config.json'):
    '''
    Checks if we already have a inventory written, if not fetch, write and
    return the inventory
    '''
    config = load_config(config_file)
    if 'inventory_dir' in config.keys():
        outdir = config['inventory_dir']
    else:
        outdir = '/tmp/inventory'

    client = amclient(config)
    inventory = get_inventory(client)
    write_inventory(inventory,outdir)

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

def update_component_config(config, cluster, ambari_session, config_element, new_element_config ):
    '''
    new_element_config should be an element similar to the one returned by
    get_current_config_tag. Only replace the content / elements you want to
    replace. Rest of the things will be taken carfe by this function
    '''
    ambari_url = config['ambari']['protocol'] + '://' + config['ambari']['host'] + ':' + config['ambari']['port']
    # Manipulate the new_element_config
    new_element_config['tag'] = 'version' + str(int(round(time.time() * 1000000000)))
    del new_element_config['Config']
    del new_element_config['href']
    del new_element_config['version']
    new_element_config = {"Clusters": {"desired_config": new_element_config}}
    body = new_element_config
    r = ambari_session.put(ambari_url + '/api/v1/clusters/' + cluster, data=json.dumps(body))
    return r

def get_config_diff(old_config, new_config):
    out = str()
    a = json.dumps(old_config, indent=2, sort_keys=True).splitlines(1)
    b = json.dumps(new_config, indent=2, sort_keys=True).splitlines(1)
    for l in difflib.unified_diff(a, b):
        out += l
    return out
