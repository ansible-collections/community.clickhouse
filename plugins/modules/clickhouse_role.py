#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright: (c) 2024, Don Naro (@oranod) <dnaro@redhat.com>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function

__metaclass__ = type

DOCUMENTATION = r"""
---
module: clickhouse_role

short_description: Creates or removes a ClickHouse role.

description:
  - Creates or removes a ClickHouse role.

attributes:
  check_mode:
    description: Supports check_mode.
    support: full

version_added: '0.3.0'

author:
  - Don Naro (@oranod)

extends_documentation_fragment:
  - community.clickhouse.client_inst_opts

options:
  state:
    description:
      - Role state.
      - C(present) creates the role if it does not exist.
      - C(absent) deletes the role if it exists.
    type: str
    choices: ['present', 'absent']
    default: 'present'
  name:
    description:
      - Role name to add or remove.
    type: str
    required: true
"""

EXAMPLES = r"""
- name: Create role
  community.clickhouse.clickhouse_role:
    name: test_role
    state: present

- name: Remove role
  community.clickhouse.clickhouse_role:
    name: test_role
    state: absent
"""

RETURN = r"""
executed_statements:
  description:
  - Data-modifying executed statements.
  returned: on success
  type: list
  sample: ['CREATE ROLE test_role']
"""

from ansible.module_utils.basic import AnsibleModule

from ansible_collections.community.clickhouse.plugins.module_utils.clickhouse import (
    check_clickhouse_driver,
    client_common_argument_spec,
    connect_to_db_via_client,
    execute_query,
    get_main_conn_kwargs,
)

executed_statements = []


class ClickHouseRole:
    def __init__(self, module, client, name):
        self.module = module
        self.client = client
        self.name = name
        self.exists = self.check_exists()

    def check_exists(self):
        query = "SELECT 1 FROM system.roles WHERE name = '%s' LIMIT 1" % self.name
        result = execute_query(self.module, self.client, query)
        return bool(result)

    def create(self):
        if not self.exists:
            query = "CREATE ROLE %s" % self.name
            executed_statements.append(query)

            if not self.module.check_mode:
                execute_query(self.module, self.client, query)

            self.exists = True
            return True
        else:
            return False

    def drop(self):
        if self.exists:
            query = "DROP ROLE %s" % self.name
            executed_statements.append(query)

            if not self.module.check_mode:
                execute_query(self.module, self.client, query)

            self.exists = False
            return True
        else:
            return False


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

    # Instantiate an object of module class
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

    # Will fail if no driver informing the user
    check_clickhouse_driver(module)

    # Connect to DB
    client = connect_to_db_via_client(module, main_conn_kwargs, client_kwargs)

    # Do the job
    changed = False
    role = ClickHouseRole(module, client, name)

    if state == "present":
        if not role.exists:
            changed = role.create()
        else:
            pass
    else:
        # If state is absent
        if role.exists:
            changed = role.drop()

    # Close connection
    client.disconnect_connection()

    # Users will get this in JSON output after execution
    module.exit_json(changed=changed, executed_statements=executed_statements)


if __name__ == "__main__":
    main()
