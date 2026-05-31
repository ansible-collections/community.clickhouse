#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright: (c) 2026, Rafal Kozlowski (@rkozlo) <rafalkozlowski07@gmail.com>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function

__metaclass__ = type

DOCUMENTATION = r'''
---
module: clickhouse_settings_profile

short_description: Creates, removes or modify a ClickHouse settings profile using the clickhouse-driver Client interface

description:
  - Creates, remove or modify a ClickHouse settings profile using the
    L(clickhouse-driver,https://clickhouse-driver.readthedocs.io/en/latest) Client interface.

attributes:
  check_mode:
    description: Supports check_mode.
    support: full
  idempotent:
    description:
      - Module can't determine what data type is certain setting.
        So when passing setting that will be resolved by server to another value module will be not idempotent.
        For example passing 1G as value server will store this as 100000000.
    support: partial

author:
  - Rafal Kozlowski (@rkozlo)

extends_documentation_fragment:
  - community.clickhouse.client_inst_opts
  - community.clickhouse.cluster_inst_opts
  - community.clickhouse.entity_inst_opts

version_added: '2.3.0'

options:
  state:
    description:
      - Whether the settings profile should be present or not.
      - If C(present), the module will create or update the settings profile to match the provided parameters.
      - If C(absent), the module will remove the settings profile if it exists.
    type: str
    choices: ["present", "absent"]
    default: "present"
  name:
    description:
      - The name of the row policy to manage.
    type: str
    required: true
'''

EXAMPLES = r'''
---
- name: Create plain settings profile
  community.clickhouse.clickhouse_settings_profile:
    name: test_profile

- name: Create settings profile with setting
  community.clickhouse.clickhouse_settings_profile:
    name: test_profile
    settings:
      max_threads:
        value: 1

- name: Create settings profile with settings
  community.clickhouse.clickhouse_settings_profile:
    name: test_profile
    settings:
      max_threads:
        value: 1
      max_memory_usage:
        value: 10000000000

- name: Create settings profile with profiles
  community.clickhouse.clickhouse_settings_profile:
    name: test_profile
    profiles:
      - web

- name: Create settings profile with settings and profiles
  community.clickhouse.clickhouse_settings_profile:
    name: test_profile
    settings:
      max_threads:
        value: 1
      max_memory_usage:
        value: 10000000000
    profiles:
      - web
- name: Drop settings profile
  community.clickhouse.clickhouse_settings_profile:
    name: test_profile
    state: absent
'''

RETURN = r'''
executed_statements:
  description:
  - Data-modifying executed statements.
  returned: on success
  type: list
  sample: [
    "CREATE SETTINGS PROFILE `test_profile` SETTINGS INHERIT `inh_1`, max_threads='1'",
    "ALTER SETTINGS PROFILE `test_profile` SETTINGS INHERIT `inh_1`, max_threads='1'",
    "DROP SETTINGS PROFILE `test_profile`",
  ]
'''

from ansible.module_utils.basic import AnsibleModule

from ansible_collections.community.clickhouse.plugins.module_utils.clickhouse import (
    check_clickhouse_driver,
    client_common_argument_spec,
    connect_to_db_via_client,
    execute_query,
    get_main_conn_kwargs,
    get_on_cluster_clause,
    validate_identifier,
    cluster_argument_spec,
)
from ansible_collections.community.clickhouse.plugins.module_utils.entity_settings import (
    EntitySettings,
    get_settings_argument_spec,
)

executed_statements = []


class ClickHouseSettingsProfile:
    def __init__(self, module, client, name):
        self.module = module
        self.client = client
        validate_identifier(module, name, "collection name")
        self.name = name
        self.settings = EntitySettings(self.module, self.client, self.name, 'profile')
        self._exists = None

    @property
    def exists(self):
        if self._exists is None:
            query = f"SELECT 1 FROM system.settings_profiles WHERE name = '{self.name}'"
            result = execute_query(self.module, self.client, query)
            if result:
                self._exists = True
            else:
                self._exists = False
        return self._exists

    def create(self, settings, profiles, cluster=None):

        query = f"CREATE SETTINGS PROFILE `{self.name}`"
        query += get_on_cluster_clause(self.module, cluster)
        settings_entity = self.settings.compare_and_build_clause(settings, profiles)
        query += settings_entity[1]

        executed_statements.append(query)

        if not self.module.check_mode:
            execute_query(self.module, self.client, query)
        return True

    def alter(self, settings, profiles, cluster=None):
        if not self.exists:
            return False
        query = f"ALTER SETTINGS PROFILE `{self.name}`"
        query += get_on_cluster_clause(self.module, cluster)

        settings_entity = self.settings.compare_and_build_clause(settings, profiles)

        if settings_entity[0] is False:
            return False
        query += settings_entity[1]
        executed_statements.append(query)

        if not self.module.check_mode:
            execute_query(self.module, self.client, query)
        return True

    def drop(self, cluster=None):
        query = f"DROP SETTINGS PROFILE `{self.name}`"
        query += get_on_cluster_clause(self.module, cluster)
        executed_statements.append(query)

        if not self.module.check_mode:
            execute_query(self.module, self.client, query)
        return True


def main():
    # Set up arguments.
    # If there are common arguments shared across several modules,
    # create the common_argument_spec() function under plugins/module_utils/*
    # and invoke here to return a dict with those arguments
    argument_spec = client_common_argument_spec()
    argument_spec.update(
        state=dict(type="str", choices=["present", "absent"], default="present"),
        name=dict(type="str", required=True),
    )
    argument_spec.update(cluster_argument_spec())
    argument_spec.update(get_settings_argument_spec())

    module = AnsibleModule(
        argument_spec=argument_spec,
        supports_check_mode=True,
    )

    # Assign passed options to variables
    client_kwargs = module.params["client_kwargs"]
    # The reason why these arguments are separate from client_kwargs
    # is that we need to protect some sensitive data like passwords passed
    # to the module from logging (see the arguments above with no_log=True);
    # Such data must be passed as module arguments (not nested deep in values).
    main_conn_kwargs = get_main_conn_kwargs(module)
    state = module.params["state"]
    name = module.params["name"]
    cluster = module.params["cluster"]
    profiles = module.params["profiles"]
    settings = module.params["settings"]

    # Will fail if no driver informing the user
    check_clickhouse_driver(module)

    # Connect to DB
    client = connect_to_db_via_client(module, main_conn_kwargs, client_kwargs)

    # Do the job
    changed = False
    settings_profile = ClickHouseSettingsProfile(module, client, name)
    if state == "present":
        if not settings_profile.exists:
            changed = settings_profile.create(settings, profiles, cluster)
        else:
            changed = settings_profile.alter(settings, profiles, cluster)
    else:
        if settings_profile.exists:
            changed = settings_profile.drop(cluster)

    # Close connection
    client.disconnect_connection()

    # Users will get this in JSON output after execution
    module.exit_json(changed=changed, executed_statements=executed_statements)


if __name__ == "__main__":
    main()
