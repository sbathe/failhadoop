#!/usr/bin/env python
# Taken imostly verbatim from  http://docs.ansible.com/ansible/dev_guide/developing_api.html
# Also good reference at: https://serversforhackers.com/running-ansible-2-programmatically

import json
from collections import namedtuple
from ansible.parsing.dataloader import DataLoader
from ansible.vars import VariableManager
from ansible.inventory import Inventory
from ansible.playbook.play import Play
from ansible.executor.task_queue_manager import TaskQueueManager
from ansible.executor.playbook_executor import PlaybookExecutor
from ansible.playbook.block import Block
from ansible.playbook.play_context import PlayContext
from ansible.plugins.callback import CallbackBase
from ansible.executor.stats import AggregateStats

def run_playbook(inventory_root, playbook, extra_vars=None, connection='ssh', module_path=None, forks=5, become=None,
                 become_method=None, become_user=None, check=False):
   # We need a way to pass options to Ansible. We do so by passing a tuple object
   # This is not a exhaustive list, we only do what we need
   Options = namedtuple('Options', ['connection', 'module_path', 'forks', 'become', 'become_method', 'become_user', 'check'])
   # initialize needed objects
   stats = AggregateStats()
   variable_manager = VariableManager()
   variable_manager.extra_vars = extra_vars
   loader = DataLoader()
   options = Options(connection=connection, module_path=module_path, forks=forks, become=become, become_method=become_method, become_user=become_user, check=check)
   passwords = dict(vault_pass='secret') # this is required, for now would
   # mostly be junk for us

   # create inventory and pass to var manager
   inventory = Inventory(loader=loader, variable_manager=variable_manager, host_list=inventory_root)
   variable_manager.set_inventory(inventory)
   pbex = PlaybookExecutor(playbook, inventory=inventory, variable_manager=variable_manager, loader=loader,
                                passwords=passwords)
   result = pbex.run()
   tqm = self.pbex._tqm
   return (result, tqm)


def run_play(inventory_root,host_pattern,script, connection='ssh',module_path=None, forks=5, become=None,
                  become_method=None, become_user=None, check=False):
      # We need a way to pass options to Ansible. We do so by passing a tuple object
      # This is not a exhaustive list, we only do what we need
      Options = namedtuple('Options', ['connection', 'module_path', 'forks', 'become', 'become_method', 'become_user', 'check'])
      # initialize needed objects
      stats = AggregateStats()
      variable_manager = VariableManager()
      loader = DataLoader()
      options = Options(connection=connection, module_path=module_path, forks=forks, become=become, become_method=become_method, become_user=become_user, check=check)
      passwords = dict(vault_pass='secret') # this is required, for now would
      # mostly be junk for us

      # create inventory and pass to var manager
      inventory = Inventory(loader=loader, variable_manager=variable_manager, host_list=inventory_root)
      variable_manager.set_inventory(inventory)
      # create play with tasks
      play_source =  dict(
            name = "Play adhoc test",
            hosts = host_pattern,
            gather_facts = 'no',
            tasks = [
                dict(action=dict(module='script', args=script), register='shell_out'),
                dict(action=dict(module='debug',
                                 args=dict(msg='{{shell_out.stdout}}')))
             ]
        )
      play = Play().load(play_source, variable_manager=variable_manager, loader=loader)
    #  return play

      # actually run it
      tqm = None
      try:
        tqm = TaskQueueManager(
                  inventory=inventory,
                  variable_manager=variable_manager,
                  loader=loader,
                  options=options,
                  passwords=passwords,
                  stdout_callback='default'
              )
        result = tqm.run(play)
      finally:
        if tqm is not None:
            tqm.cleanup()
      return (result, tqm)

