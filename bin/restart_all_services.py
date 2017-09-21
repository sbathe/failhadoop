#!/usr/bin/env python
# TODO: Add Support for multiple clusters in an Ambari instance. This works now
# cause we assume Ambari manages only one cluster per instance
# The change needs to go in ambari_helpers, we can just consume it here

import argparse, failhadoop, sys, time
parser = argparse.ArgumentParser()
parser.add_argument("-v", action="store_true", default=False, dest='verbose',
                    help="increase output verbosity")
parser.add_argument("-c", "--config", action="store", default='config.json', dest='conf',
                    help="config file to load, should be json")
args = parser.parse_args()

config = failhadoop.utils.load_config(args)

s = failhadoop.ambari_helpers.setup_ambari_session(config)
cluster = failhadoop.ambari_helpers.get_clusters(config,s)

# stop all services on the given cluster
r = failhadoop.ambari_helpers.stop_all(config,cluster,s)
request_url = r.json()['href']
#Monitor the stop, we will stay in the loop till the services are stopped. This
#may take a long time
print('Restarting services, this may take a long time')
a = failhadoop.ambari_helpers.monitor_ambari_request(s,req)
if a[0]:
    print('Services Stopped, will wait for a minute before starting them again')
    time.sleep(60)
else:
    print('Stop all failed. Please check Ambari.\n{0}'.format(a[1]))

r = failhadoop.ambari_helpers.start_all(config,cluster,s)
if a[0]:
    print('Services Started, all yours')
else:
    print('Start all failed. Please check Ambari.\n{0}'.format(a[1]))

