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
