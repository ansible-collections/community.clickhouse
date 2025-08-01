#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright: (c) 2025, Andrew Klychkov (@Andersson007) <andrew.a.klychkov@gmail.com>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

DOCUMENTATION = r'''
---
module: clickhouse_grants

short_description: Manage grants for ClickHouse users and roles

description:
  - Grants, updates, or revokes privileges for ClickHouse users and roles.
  - This module uses the L(clickhouse-driver,https://clickhouse-driver.readthedocs.io/en/latest) client interface.

attributes:
  check_mode:
    description: Supports check_mode.
    support: full

author:
  - Andrew Klychkov (@Andersson007)

extends_documentation_fragment:
  - community.clickhouse.client_inst_opts

version_added: '0.9.0'

options:
  state:
    description:
      - If C(present), the module will grant or update privileges.
      - If C(absent), the module will revoke all privileges from the O(grantee).
    type: str
    choices: ['present', 'absent']
    default: 'present'
  grantee:
    description:
      - A user or a role to grant, update, or revoke privileges for.
    type: str
    required: true
  append:
    description:
      - If set to C(true) (the default), the module will append
        the privileges specified in O(grants) to the privileges the O(grantee)
        already has.
      - If set to C(false), the module will revoke all
        current privileges from the O(grantee) before granting the new ones.
    type: bool
    default: true
  grants:
    description:
      - Privileges to grant. This option is required when C(state) is C(present).
    type: dict
    suboptions:
      global:
        description:
          - A dictionary of global privileges to grant. These apply to all databases.
        type: dict
        suboptions:
          grants:
            description:
              - A dictionary of privileges.
              - Keys are the names of privileges, for example C(CREATE USER).
              - Values are booleans (or C(0)/C(1)) indicating whether to grant the privilege
                with the C(WITH GRANT OPTION).
            type: dict
            required: true
      databases:
        description:
          - A dictionary of privileges on specific databases, tables, or columns.
          - Keys are database names.
        type: dict
        suboptions:
          '<database_name>':
            description:
              - Dictionary of privileges for a database. Use the actual database name as the key.
            type: dict
            suboptions:
              grants:
                description:
                  - Privileges on all tables in the database.
                  - See the description of C(global.grants) for more details.
                type: dict
              '<table_name>':
                description:
                  - Dictionary of privileges for a table. Use the actual table name as the key.
                type: dict
                suboptions:
                  grants:
                    description:
                      - Privileges on all columns in the table.
                      - See the description of C(global.grants) for more details.
                    type: dict
                  '<column_name>':
                    description:
                      - Dictionary of privileges for a column. Use the actual column name as the key.
                    type: dict
                    suboptions:
                      grants:
                        description:
                          - Privileges on the column.
                          - See the description of C(global.grants) for more details.
                        type: dict
                        required: true
'''

EXAMPLES = r'''
- name: Grant global privileges to a user
  community.clickhouse.clickhouse_grants:
    grantee: alice
    state: present
    grants:
      global:
        "ALTER USER": 1       # With grant option
        "CREATE DATABASE": 0  # Without grant option
        "CREATE USER": 0      # Without grant option

- name: Grant privileges on a specific database
  community.clickhouse.clickhouse_grants:
    grantee: bob
    state: present
    grants:
      databases:
        infra:
          grants:
            "SELECT": 1  # With grant option
            "INSERT": 0  # Without grant option

- name: Replace all existing privileges for a user
  community.clickhouse.clickhouse_grants:
    grantee: david
    state: present
    append: false
    grants:
      databases:
        bar:
          grants:
            "SELECT": 0  # Without grant option

- name: Revoke all privileges from a user
  community.clickhouse.clickhouse_grants:
    grantee: eve
    state: absent
'''

RETURN = r'''
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


def get_grants(module, client, name):
    query = ("SELECT access_type, database, "
             "table, column, is_partial_revoke, grant_option "
             "FROM system.grants WHERE user_name = '%s' "
             "OR role_name = '%s'" % (name, name))

    result = execute_query(module, client, query)

    if result == PRIV_ERR_CODE:
        return {str(PRIV_ERR_CODE): "Not enough privileges"}

    grants = []
    for row in result:
        grants.append({
            "access_type": row[0],
            "database": row[1],
            "table": row[2],
            "column": row[3],
            "is_partial_revoke": row[4],
            "grant_option": row[5],
        })

    return grants


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
        else:
            self.module.fail_json(msg="Grantee %s does not exist" % self.grantee)

    def get(self):
        # WIP
        grants_list = get_grants(self.module, self.client, self.grantee)

        grants = {
            "global": {
                "grants": {},
                "part_revokes": set(),
            },
            "databases": {},
        }

        for e in grants_list:
            # If database is not specified, it's about global grants
            if e["database"] is None:
                if e["is_partial_revoke"]:
                    grants["global"]["part_revokes"] = e["access_type"]
                else:
                    grants["global"]["grants"][e["access_type"]] = e["grant_option"]
            # If database is specified
            else:
                grants["databases"][e["database"]] = {}
                # If table is not specified, it's about database-level grants
                if e["table"] is None:
                    if e["is_partial_revoke"]:
                        grants["databases"][e["database"]]["part_revokes"] = e["access_type"]
                    else:
                        grants["databases"][e["database"]]["grants"] = {}
                        grants["databases"][e["database"]]["grants"][e["access_type"]] = e["grant_option"]

                else:
                    grants["databases"][e["database"]][e["table"]] = {}
                    # If table is specified and columnt is not,
                    # grant it at the table level
                    if e["column"] is None:
                        if e["is_partial_revoke"]:
                            grants["databases"][e["database"]][e["table"]]["part_revokes"] = e["access_type"]
                        else:
                            grants["databases"][e["database"]][e["table"]]["grants"] = {}
                            grants["databases"][e["database"]][e["table"]]["grants"][e["access_type"]] = e["grant_option"]

                    # If column is specified, grant it for the column
                    else:
                        grants["databases"][e["database"]][e["table"]][e["column"]] = {}

                        if e["is_partial_revoke"]:
                            grants["databases"][e["database"]][e["table"]][e["column"]]["part_revokes"] = e["access_type"]
                        else:
                            grants["databases"][e["database"]][e["table"]][e["column"]]["grants"] = {}
                            grants["databases"][e["database"]][e["table"]][e["column"]]["grants"][e["access_type"]] = e["grant_option"]

        return grants

    def update(self):
        # TBD
        return True

    def revoke(self):
        # TBD
        return True


def main():
    argument_spec = client_common_argument_spec()
    argument_spec.update(
        state=dict(type='str', choices=['present', 'absent'], default='present'),
        grantee=dict(type='str', required=True),
        append=dict(type='bool', default=True),
        grants=dict(type='dict'),
    )

    # Instantiate an object of module class
    module = AnsibleModule(
        argument_spec=argument_spec,
        supports_check_mode=True,
        required_if=[
            ('state', 'present', ['grants']),
        ],
    )

    # Assign passed options to variables
    client_kwargs = module.params['client_kwargs']
    # The reason why these arguments are separate from client_kwargs
    # is that we need to protect some sensitive data like passwords passed
    # to the module from logging (see the arguments above with no_log=True);
    # Such data must be passed as module arguments (not nested deep in values).
    main_conn_kwargs = get_main_conn_kwargs(module)
    state = module.params['state']
    append = module.params['append']
    grantee = module.params['grantee']
    grants = module.params['grants']

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
