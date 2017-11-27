#!/usr/bin/env python
import sys
import json, yaml, os, sys, re
from collections import defaultdict
import random, time

def read_lock(lockfile):
    with open(lockfile) as lock:
        data = lock.readlines()
    return data

def check_lock(data, cluster):
    for e in data:
        if cluster in e.split(';'):
            return (True)
    return (False)

def get_lock_data(data, cluster):
    for e in data:
        if cluster in e.split(';'):
            return e

def write_new_lock(lockfile, cluster, component, testnumber):
    line = "{0};{1};{2};{3}\n".format(cluster,component,testnumber,time.strftime('%c',time.localtime()))
    #try:
    with open(lockfile,'a') as lock:
        lock.write(line)
    return True

def release_cluster_lock(lockfile, cluster):
        data = read_lock(lockfile)
        for e in data:
            if cluster in e.split(';'):
                data.remove(e)
        try:
            with open(lockfile,'w') as lock:
                lock.writelines(data)
            return True
        except:
            return False

def file_ansible_logs(dictionary):
    '''Takes the dictionary resulting from fail.py Popen.communicate call'''
    ansible_log = 'ansible_log_' + time.strftime("%Y%m%d%H%M%s", time.localtime()) + '.log'
    results = dict()
    with open(ansible_log,'w') as asl:
        try:
            json.dump(dictionary, asl, indent=2)
            results['log_success'] = True
            results['msg'] = 'log written to: ' + ansible_log
        except:
            results['success'] = False
            results['msg'] = 'Cannot write to: ' + ansible_log
    return results

def file_ansible_logsv2(results):
    '''Takes in the results tuple from run_play and run_playbook and logs the results to a file'''
    stats = results[1]._stats
    summary = failhadoop.ansible_helpers.summarize_stats(stats)
    results[2][0]['summary'] = failhadoop.ansible_helpers.summarize_stats(stats)
    ansible_log = 'ansible_log_' + time.strftime("%Y%m%d%H%M%s", time.localtime()) + '.log'
    results = dict()

    with open(ansible_log,'w') as asl:
        try:
            json.dump(results[2], asl, indent=2)
            results['success'] = True
            results['msg'] = 'log written to: ' + ansible_log
        except:
            results['success'] = False
            results['msg'] = 'Cannot write to: ' + ansible_log
    return results
