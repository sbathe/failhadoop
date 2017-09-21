#!/usr/bin/env python
from ambariclient.client import Ambari
import argparse, json, collections, os, yaml, time
import socket, requests, difflib, urllib
from failhadoop import utils


# We use python ambari client from
# https://github.com/jimbobhickville/python-ambariclient for most of the
# read-only operations

# Most of the configuration related stuff is shamelessly borrowed from jupyter
# notebooks at http://nbviewer.jupyter.org/github/seanorama/ambari-bootstrap/tree/master/api-examples/

# TODO:
# - Handle exceptions at all stages:
#   - possibly write a generic try-catch function and get all API requests
#     pass through it
# - Make a more featureful inventory with host args and child groups for
# clusters
STOP_ALL='{"RequestInfo":{"context":"_PARSE_.STOP.ALL_SERVICES","operation_level":{"level":"CLUSTER","cluster_name":"sbathe-hdp"}},"Body":{"ServiceInfo":{"state":"INSTALLED"}}}'
START_ALL='{"RequestInfo":{"context":"_PARSE_.START.ALL_SERVICES","operation_level":{"level":"CLUSTER","cluster_name":"sbathe-hdp"}},"Body":{"ServiceInfo":{"state":"STARTED"}}}'

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
    host = urllib.parse.urlparse(client.__dict__['base_url']).netloc.split(':')[0]
    factory = lambda: collections.defaultdict(factory)
    inventory = factory()
    for c in client.clusters().to_dict():
      cluster = c['cluster_name']
      host_components = client.clusters(cluster).host_components().to_dict()
      for hc in host_components:
        inventory[host][cluster][hc['component_name']]['hosts'][hc['host_name']] = dict()
    inventory[host][cluster]['AMBARI_SERVER']['hosts'][host] = dict()
    return utils.defaultdict_to_regular(inventory)

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

# TODO: Add Support for multiple clusters in an Ambari instance. This works now
# cause we assume Ambari manages only one cluster per instance
# Maybe all we need to do is to pass a list and let downstream handle how it
# wants to iterate
def get_clusters(config, ambari_session):
    '''
    Returns the cluster managed by Ambari. Will need to refactor once Ambari
    starts managing more than one cluster
    '''
    ambari_url = config['ambari']['protocol'] + '://' +\
                 config['ambari']['host'] + ':' + config['ambari']['port']
    r = ambari_session.get(ambari_url + '/api/v1/clusters/')
    if r.status_code == 200:
        cluster = r.json()['items'][0]['Clusters']['cluster_name']
    else:
        cluster = None
    return cluster

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
    new_element_config['tag'] = 'version' + str(round(time.time()))
    del new_element_config['Config']
    del new_element_config['href']
    del new_element_config['version']
    new_element_config = {"Clusters": {"desired_config": new_element_config}}
    body = new_element_config
    r = ambari_session.put(ambari_url + '/api/v1/clusters/' + cluster, data=json.dumps(body))
    return r

def get_config_version_tags(config, cluster, ambari_session, config_element):
    """
    Returns a list of tuples (tag, version) we know about the config_element
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
    out = str()
    a = json.dumps(old_config, indent=2, sort_keys=True).splitlines(1)
    b = json.dumps(new_config, indent=2, sort_keys=True).splitlines(1)
    for l in difflib.unified_diff(a, b):
        out += l
    return out

def stop_service_component_on_host(config, cluster, session,host,component):
    ambari_url = config['ambari']['protocol'] + '://' + config['ambari']['host'] + ':' + config['ambari']['port']
    post_uri = ambari_url + '/api/v1/clusters/{0}/hosts/{1}/host_components/{2}'.format(cluster,host,component)
    post_data = {
                 "HostRoles":
                   {"state": "INSTALLED"}
                }
    r = session.put(post_uri, data=json.dumps(post_data))
    return r

def start_service_component_on_host(config, cluster, session,host,component):
    ambari_url = config['ambari']['protocol'] + '://' + config['ambari']['host'] + ':' + config['ambari']['port']
    post_uri = ambari_url + '/api/v1/clusters/{0}/hosts/{1}/host_components/{2}'.format(cluster,host,component)
    post_data = {
                 "HostRoles":
                   {"state": "STARTED"}
                }
    r = session.put(post_uri, data=json.dumps(post_data))
    return r

#https://community.hortonworks.com/questions/91083/rest-api-to-stopstart-a-host-component-running-on.html
# https://docs.google.com/document/d/1L4ER6kqyBtIh0XhChREB2yI8FKseDTGX3pRnEWtJiPc/edit
def restart_services_in_bulk(config, cluster, session, service, component ):
    ambari_url = config['ambari']['protocol'] + '://' +\
       config['ambari']['host'] + ':' + config['ambari']['port']
    post_uri = ambari_url + '/api/v1/clusters/{0}/requests'.format(cluster)
    post_data = {
        "RequestInfo": {
            "command": "RESTART",
            "context": "Restart services on the selected hosts",
            "operation_level": {
              "level": "HOST",
              "cluster_name": "{0}".format(cluster)
            }
         },
         "Requests/resource_filters": [
            {
              "service_name": "{0}".format(service),
              "component_name": "{0}".format(component),
              "hosts_predicate":
                "HostRoles/component_name={0}".format(component)
            }
          ]
        }
    r = session.post(post_uri,data=json.dumps(post_data))
    return r

# https://issues.apache.org/jira/browse/AMBARI-14394
def restart_stale_configs(config, cluster, session):
    ambari_url = config['ambari']['protocol'] + '://' +\
       config['ambari']['host'] + ':' + config['ambari']['port']
    post_uri = ambari_url + '/api/v1/clusters/{0}/requests'.format(cluster)
    post_data = {
      "RequestInfo": {
        "context": "Restart stale",
        "operational_level": "host_component",
        "command": "RESTART"
      },
      "Requests/resource_filters": [
        {
          "hosts_predicate": "HostRoles/stale_configs=true"
        }
      ]
    }
    r = session.post(post_uri,data=json.dumps(post_data))
    return r

def put_to_ambari(session, url, put_data):
    #print('Calling {0} with {1} as data'.format(url,put_data))
    r = session.put(url, data=put_data)
    return r

def get_from_ambari(session, url):
    r = session.get(url)
    return r

def stop_all(config, cluster, session):
    ambari_url = config['ambari']['protocol'] + '://' +\
       config['ambari']['host'] + ':' + config['ambari']['port']
    put_url = ambari_url + '/api/v1/clusters/{0}/services'.format(cluster)
    #print('Calling {0} with {1} as data'.format(put_url,STOP_ALL))
    r = put_to_ambari(session,put_url,STOP_ALL)
    return r

def start_all(config, cluster, session):
    ambari_url = config['ambari']['protocol'] + '://' +\
       config['ambari']['host'] + ':' + config['ambari']['port']
    put_url = ambari_url + '/api/v1/clusters/{0}/services'.format(cluster)
    r = put_to_ambari(session,put_url,START_ALL)
    return r

# Can be optimized by using subqueries or by requesting specifc fields from
# Amabri
def monitor_ambari_request(session, url):
    completed = ['COMPLETED','FAILED','TIMEDOUT','ABORTED']
    status_codes = [200,201, 202]
    task_complete = 0
    while True:
      r = get_from_ambari(session, url)
      status_code = r.status_code
      state = r.json()['Requests']['request_status']
    #  print(status_code, state)
      if (status_code in status_codes) and (state not in completed):
        time.sleep(10)
        print('sleeping')
        continue
      else:
        if r.json()['Requests']['request_status'] != 'COMPLETED':
          return(False, r.json())
        return(True,r.json())
