#!/usr/bin/env python
from ambariclient.client import Ambari
import argparse, json, collections, os, yaml
import socket

# TODO: 
# - Handle exceptions at all stages
# - Make a more featureful inventory with host args and child groups for
# clusters
"""
#  we make this as a lib instead of executable
parser = argparse.ArgumentParser()
parser.add_argument("--list", action="store_true", default=True, dest='list', help="list host inventory from Ambari")
parser.add_argument("--refresh", action="store_true", default=False, dest='refresh', help="Force refresh host inventory from Ambari")
parser.add_argument("-c", "--config", action="store", dest='config_file', default="config.json", help="Configuration file to load, default config.json from CWD")
args = parser.parse_args()
"""
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

'''
Rest of it is moot, cause now we just write the inventory for ansible, rest is
upto ansible to read. Essentially, no dynamic inventory for now
if os.path.isfile('inventory.json'):
        with open('inventory.json') as f:
          a = json.load(f)
        print(json.dumps(a, indent=2, sort_keys=True))
    else:
        inventory = get_inventory(client)
        write_inventory(inventory)
        print(json.dumps(inventory,indent=2,sort_keys=True)) 
'''
