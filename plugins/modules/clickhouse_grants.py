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
  exclusive:
    description:
      - If set to C(false) (the default), the module will append
        the privileges specified in O(privileges) to the privileges the O(grantee)
        already has.
      - If set to C(true), the module will revoke all
        current privileges from the O(grantee) before granting the new ones.
    type: bool
    default: false
  privileges:
    description:
      - Privileges to grant. This option is required when C(state) is C(present).
      - It's a list of dictionaries, where each dictionary specifies a set of privileges on a database object.
    type: list
    elements: dict
    suboptions:
      object:
        description:
          - The database object to grant privileges on.
          - Use C(*.*) for global privileges, C(database.*) for all tables in a database,
            and C(database.table) for a specific table.
        type: str
        required: true
      privs:
        description:
          - A dictionary of privileges.
          - Keys are privilege names, like C(CREATE USER) or C(SELECT(column1, column2)).
          - Values are booleans indicating whether to grant the privilege
            with the C(WITH GRANT OPTION).
          - Alternatively, you can use the O(grant_option) parameter to apply the same setting to all privileges in this set.
        type: dict
        required: true
      grant_option:
        description:
          - A boolean that applies to all privileges in this set.
          - If specified, it overrides any individual grant option settings within O(privs).
        type: bool
'''

EXAMPLES = r'''
- name: Grant global privileges to a user
  community.clickhouse.clickhouse_grants:
    grantee: alice
    privileges:
      - object: '*.*'
        privs:
          "ALTER USER": true       # With grant option
          "CREATE DATABASE": false # Without grant option
          "CREATE USER": false     # Without grant option

- name: Grant privileges on a specific database
  community.clickhouse.clickhouse_grants:
    grantee: bob
    privileges:
      - object: 'infra.*'
        privs:
          "SELECT": true  # With grant option
          "INSERT": false # Without grant option

- name: Grant SELECT on specific columns of a table
  community.clickhouse.clickhouse_grants:
    grantee: carol
    privileges:
      - object: 'sales.customers'
        privs:
          "SELECT(name, email)": false # Without grant option

- name: Replace all existing privileges for a user
  community.clickhouse.clickhouse_grants:
    grantee: david
    exclusive: true
    privileges:
      - object: 'bar.*'
        privs:
          "SELECT": false  # Without grant option

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


class ClickHouseGrants():
    def __init__(self, module, client, grantee):
        self.changed = False
        self.module = module
        self.client = client
        self.grantee = grantee
        self.grantee_exists = False
        self.__populate_info()

    def __populate_info(self):
        query = ("SELECT 1 FROM system.users "
                 "WHERE name = '%s'" % self.grantee)

        result = execute_query(self.module, self.client, query)

        if result == PRIV_ERR_CODE:
            login_user = self.module.params['login_user']
            msg = "Not enough privileges for user: %s" % login_user
            self.module.fail_json(msg=msg)

        if result:
            self.grantee_exists = True
        else:
            self.module.fail_json(msg="Grantee %s does not exist" % self.grantee)

    def get(self):
        query = "SHOW GRANTS FOR %s" % self.grantee
        result = execute_query(self.module, self.client, query)

        if result == PRIV_ERR_CODE:
            login_user = self.module.params['login_user']
            msg = "Not enough privileges for user: %s to SHOW GRANTS" % login_user
            self.module.fail_json(msg=msg)

        grants = {}
        import re
        for row in result:
            grant_statement = row[0]
            match = re.match(r'GRANT (.+?) ON (.+?) TO .+?( WITH GRANT OPTION)?$', grant_statement)
            if not match:
                continue

            privs_str, obj, grant_option_str = match.groups()
            grant_option = (grant_option_str is not None)

            if obj not in grants:
                grants[obj] = {}

            privs = [p.strip().upper() for p in privs_str.split(',')]
            for priv in privs:
                grants[obj][priv] = grant_option

        return grants

    def _get_desired_grants(self):
        privileges = self.module.params['privileges']
        if not privileges:
            return {}

        desired_grants = {}
        for p in privileges:
            obj = p['object']
            if obj not in desired_grants:
                desired_grants[obj] = {}

            grant_option_override = p.get('grant_option')
            for priv, grant_option in p['privs'].items():
                final_grant_option = grant_option_override if grant_option_override is not None else grant_option
                desired_grants[obj][priv.upper()] = bool(final_grant_option)

        return desired_grants

    def update(self):
        desired = self._get_desired_grants()
        current = self.get()
        exclusive = self.module.params['exclusive']

        all_current_privs = set()
        for obj, privs in current.items():
            for priv, go in privs.items():
                all_current_privs.add((priv, obj, go))

        all_desired_privs = set()
        for obj, privs in desired.items():
            for priv, go in privs.items():
                all_desired_privs.add((priv, obj, go))

        to_revoke = set()
        if exclusive:
            to_revoke = all_current_privs - all_desired_privs

        to_grant = all_desired_privs - all_current_privs

        if not to_revoke and not to_grant:
            return self.changed

        self.changed = True
        if self.module.check_mode:
            return self.changed

        from collections import defaultdict
        revokes_by_obj = defaultdict(list)
        for priv, obj, go in to_revoke:
            revokes_by_obj[obj].append(priv)

        for obj, privs in revokes_by_obj.items():
            privs_str = ', '.join(privs)
            query = f"REVOKE {privs_str} ON {obj} FROM {self.grantee}"
            execute_query(self.module, self.client, query)
            executed_statements.append(query)

        grants_go_by_obj = defaultdict(list)
        grants_no_go_by_obj = defaultdict(list)

        for priv, obj, go in to_grant:
            if go:
                grants_go_by_obj[obj].append(priv)
            else:
                grants_no_go_by_obj[obj].append(priv)

        for obj, privs in grants_go_by_obj.items():
            privs_str = ', '.join(privs)
            query = f"GRANT {privs_str} ON {obj} TO {self.grantee} WITH GRANT OPTION"
            execute_query(self.module, self.client, query)
            executed_statements.append(query)

        for obj, privs in grants_no_go_by_obj.items():
            privs_str = ', '.join(privs)
            query = f"GRANT {privs_str} ON {obj} TO {self.grantee}"
            execute_query(self.module, self.client, query)
            executed_statements.append(query)

        return self.changed

    def revoke(self):
        # TBD
        return True


def main():
    argument_spec = client_common_argument_spec()
    argument_spec.update(
        state=dict(type='str', choices=['present', 'absent'], default='present'),
        grantee=dict(type='str', required=True),
        exclusive=dict(type='bool', default=False),
        privileges=dict(type='list', elements='dict'),
    )

    # Instantiate an object of module class
    module = AnsibleModule(
        argument_spec=argument_spec,
        supports_check_mode=True,
        required_if=[
            ('state', 'present', ['privileges']),
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
    grantee = module.params['grantee']

    # Will fail if no driver informing the user
    check_clickhouse_driver(module)

    # Connect to DB
    client = connect_to_db_via_client(module, main_conn_kwargs, client_kwargs)

    # Do the job
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
