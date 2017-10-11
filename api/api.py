#!/usr/local/bin/python3

import os, sys, json, flask, requests
import failhadoop
import subprocess

# The config file for the API. It will contain a default cluster definition and
# specification as well. Would be similar to the failhadoop config file
# Minimum required:
#    1 - Failhadoop config root
#    2 - Testcase Root
cfg_file = '/usr/local/etc/failhadoop/api.json'
with open(cfg_file) as f:
  flask_conf = json.load(f)

base_uri = '/failhadoop'
app = flask.Flask(__name__)

@app.route(base_uri, methods = ['GET'])
def return_help():
    help_text = '''
    You have reached root of failures.</br>
    /failhadoop/configs will tell you about the configs I hold</br>
    /failhadoop/random will run a random failure on the default cluster</br>
    /failhadoop/{config}/{cluster}/random will run a random failure on the
    specific cluster</br>
    /failhadoop/{config}/{cluster}/{service}/{testnumber} will run the
    specified test on the specified config and cluster</br>
    '''
    return help_text, 200

@app.route(base_uri + '/random', methods = ['GET'])
def run_random_failure():
    cmd = ['fail.py',
           '-c','/usr/local/etc/failhadoop/supportOsp.json','-i','/usr/local/etc/failhadoop/inventory/','--testcase-root',
           '~/repos/Hadoop-Failures','-v','--random', '--dry-run']
    p = subprocess.Popen(cmd,stdout =
                         subprocess.PIPE,stderr=subprocess.PIPE,stdin=subprocess.PIPE)
    out, err = p.communicate()
    return out

@app.route(base_uri + '/<config>/<cluster>/random', methods = ['GET'])
def run_random_failure_on_cluster(config, cluster):
    # ADD processing for url inputs
    # Check if the config exists in the config root
    d = flask_conf['failhadoop_config_root']
    full_path = os.path.join(d, config + '.json')
    if not os.path.exists(full_path):
        return "Required config file {0} in the URL is not on this server".format(full_path), 500

    cmd = ['fail.py',
           '-c','{0}/{1}.json'.format(d,config),'-i','{0}/inventory/'.format(d),'--testcase-root',
           '{0}'.format(flask_conf['testcase_root']),'-v','--random',
           '--dry-run']
    p = subprocess.Popen(cmd,stdout =
                         subprocess.PIPE,stderr=subprocess.PIPE,stdin=subprocess.PIPE)
    out, err = p.communicate()
    return out

@app.route(base_uri + '/<config>/<cluster>/<service>/<testnumber>', methods = ['GET'])
def run_failure_on_cluster(config, cluster, service, testnumber):
    # ADD processing for url inputs
    # Check if the config exists in the config root
    d = flask_conf['failhadoop_config_root']
    full_path = os.path.join(d, config + '.json')
    if not os.path.exists(full_path):
        return "Required config file {0} in the URL is not on this server".format(full_path), 500

    cmd = ['fail.py',
           '-c','{0}/{1}.json'.format(d,config),'-i','{0}/inventory/'.format(d),'--testcase-root',
           '{0}'.format(flask_conf['testcase_root']),'-v','--service',service,
           '--testnumber', testnumber,
           '--dry-run']
    p = subprocess.Popen(cmd,stdout =
                         subprocess.PIPE,stderr=subprocess.PIPE,stdin=subprocess.PIPE)
    out, err = p.communicate()
    return out

if __name__ == '__main__':
  debug = True
  app.run(debug=debug)
