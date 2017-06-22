# failhadoop
A framework for running various failure scenarios against a Hadoop cluster
Features:
- Gets host, services, components inventory from Ambari
- Supports running adhoc scripts on selected hostgroups. Uses Ansible as a framework for running tasks

Roadmap:
 - Change configuration parameters via Ambari
 - Change service states via Ambari
 - Be able to run Ansible Playbooks as well
