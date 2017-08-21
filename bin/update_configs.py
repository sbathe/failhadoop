#!/usr/bin/env python
import failhadoop
import argparse, yaml, json
parser = argparse.ArgumentParser()
parser.add_argument("-v", action="store_true", default=False, dest='verbose',
                    help="increase output verbosity")
parser.add_argument("-c", "--config", action="store", dest='conf',
                    help="config file to load, should be json")
parser.add_argument("--testconfig", action="store", dest='testconfig',
                    help="Path to the json that has details on configuration changes")
args = parser.parse_args()

config = failhadoop.utils.load_config(args)
if args.verbose:
    print(json.dumps(config,indent=2))
testconfig = json.load(open(args.testconfig))
s = failhadoop.ambari_helpers.setup_ambari_session(config)
cluster = config['cluster'] if 'cluster' in config.keys() else failhadoop.ambari_helpers.get_clusters(config,s)
if args.verbose:
    print(cluster)

for i in testconfig['configs']:
    element = i['element']
    if args.verbose:
        print(element)
    cur_tag = failhadoop.ambari_helpers.get_current_config_tag(config, cluster, s, element)
    cur_config = failhadoop.ambari_helpers.get_tagged_config(config, cluster, s,
                                                             element, cur_tag)
    new_config = failhadoop.ambari_helpers.get_tagged_config(config, cluster, s,
                                                             element, cur_tag)
    new_config['properties'].update(i['properties'])
    r = failhadoop.ambari_helpers.update_component_config(config, cluster, s, element,
                            new_config )
    if not (r.status_code >= 200 and r.status_code <= 299):
        print("Updating configurations failed. The error returned is:\n{0}".format(json.dumps(r.json(),indent=2)))
        sys.exit(2)

r = failhadoop.ambari_helpers.restart_stale_configs(config, cluster, s)
if not (r.status_code >= 200 and r.status_code <= 299):
    print("Updating configurations failed. The error returned is:\n{0}".format(json.dumps(r.json(),indent=2)))
    sys.exit(2)
else:
    print("Configs updated and service restart request sent. You can track status for restart at:{0}".format(json.dumps(r.json(),indent=2)))
