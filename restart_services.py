#!/usr/bin/env python
import argparse, failhadoop
parser = argparse.ArgumentParser()
parser.add_argument("-v", action="store_true", default=False, dest='verbose',
                    help="increase output verbosity")
parser.add_argument("-c", "--config", action="store", default='config.json', dest='conf',
                    help="config file to load, should be json")
parser.add_argument("--service", action="store", dest='service',
                    help="service to which the component to restart belongs", required=True)
parser.add_argument("--component", action="store", dest='component',
                    help="component to restart", required=True)
args = parser.parse_args()

config = failhadoop.utils.load_config(args)
service = args.service
component = args.component
config['service'] = service
config['component'] = component

s = failhadoop.ambari_helpers.setup_ambari_session(config)
cluster = failhadoop.ambari_helpers.get_clusters(config,s)
r = failhadoop.ambari_helpers.restart_services_in_bulk(config, cluster, s, service, component)
if r.status_code >= 200 and r.status_code <= 299:
    print("Services restarted successfully, track the progress at:\n\t {0}".format(r.json()['href']))
else:
    print("failed. Check error {0}".format(r.json()))
