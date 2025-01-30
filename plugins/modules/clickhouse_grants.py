#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright: (c) 2025, Andrew Klychkov (@Andersson007) <andrew.a.klychkov@gmail.com>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

DOCUMENTATION = r'''
---
module: clickhouse_grants

short_description: TBD

description:
  - Grants, updates, or removes privileges using the
    L(clickhouse-driver,https://clickhouse-driver.readthedocs.io/en/latest) Client interface.

attributes:
  check_mode:
    description: Supports check_mode.
    support: full

author:
  - Andrew Klychkov (@Andersson007)

extends_documentation_fragment:
  - community.clickhouse.client_inst_opts

version_added: '0.8.0'

options:
  state:
    description:
      - User state.
      - If C(present), will grant or update privileges.
      - If C(absent), will revoke privileges if granted.
    type: str
    choices: ['present', 'absent']
    default: 'present'
  grantee:
    description: TBD
    type: str
    required: true
'''

EXAMPLES = r'''
- name: TBD Grant privileges
  community.clickhouse.clickhouse_user:
    login_host: localhost
    login_user: alice
    login_db: foo
    login_password: my_password
    grantee: bob
    # TBD
'''

RETURN = r'''
# TBD
executed_statements:
  description:
  - Data-modifying executed statements.
  returned: on success
  type: list
  sample: ["TBD"]
'''

from ansible.module_utils.basic import AnsibleModule

from ansible_collections.community.clickhouse.plugins.module_utils.clickhouse import (
    check_clickhouse_driver,
    client_common_argument_spec,
    connect_to_db_via_client,
    execute_query,
    get_main_conn_kwargs,
)

PRIV_ERR_CODE = 497
executed_statements = []


class ClickHouseGrants():
    def __init__(self, module, client, grantee):
        # TODO Maybe move the function determining if the
        # user/group exists or not from here to another class?
        self.changed = False
        self.module = module
        self.client = client
        self.grantee = grantee
        # Set default values, then update
        self.grantee_exists = False
        # Fetch actual values from DB and
        # update the attributes with them
        self.__populate_info()

    def __populate_info(self):
        # WIP
        # TODO Should we check the existence for a group too?
        # TODO Should we move it from here to a separate class instead?
        # Collecting user information
        query = ("SELECT 1 FROM system.users "
                 "WHERE name = '%s'" % self.grantee)

        result = execute_query(self.module, self.client, query)

        if result == PRIV_ERR_CODE:
            login_user = self.module.params['login_user']
            msg = "Not enough privileges for user: %s" % login_user
            self.module.fail_json(msg=msg)

        if result != []:
            self.grantee_exists = True

    def get(self):
        # WIP
        return {}

    def update(self):
        # WIP
        return True

    def revoke(self):
        # WIP
        return True


def main():
    argument_spec = client_common_argument_spec()
    argument_spec.update(
        state=dict(type='str', choices=['present', 'absent'], default='present'),
        grantee=dict(type='str', required=True),
    )

    # Instantiate an object of module class
    module = AnsibleModule(
        argument_spec=argument_spec,
        supports_check_mode=True,
    )

    # Assign passed options to variables
    client_kwargs = module.params['client_kwargs']
    # The reason why these arguments are separate from client_kwargs
    # is that we need to protect some sensitive data like passwords passed
    # to the module from logging (see the arguments above with no_log=True);
    # Such data must be passed as module arguments (not nested deep in values).
    main_conn_kwargs = get_main_conn_kwargs(module)
    state = module.params['state']
    grantee = module.params['grantee']

    # Will fail if no driver informing the user
    check_clickhouse_driver(module)

    # Connect to DB
    client = connect_to_db_via_client(module, main_conn_kwargs, client_kwargs)

    # Do the job
    # TODO Check if the grantee not exits, fail here
    changed = False
    grants = ClickHouseGrants(module, client, grantee)
    # Get current grants
    # TODO Should be returned via diff
    start_grants = grants.get()

    if state == 'present':
        changed = grants.update()
    elif state == 'absent':
        changed = grants.revoke()

    end_grants = grants.get()
    # Close connection
    client.disconnect_connection()

    # Users will get this in JSON output after execution
    module.exit_json(
        changed=changed,
        executed_statements=executed_statements,
        # TODO Change the below to use diff
        start_grants=start_grants,
        end_grants=end_grants,
    )


if __name__ == '__main__':
    main()
