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
  idempotent:
    description: At second run will not change anything.
    support: full

author:
  - Andrew Klychkov (@Andersson007)
  - Fabian Kohn (@fako1024)
  - Rafal Kozlowski (@rkozlo)

extends_documentation_fragment:
  - community.clickhouse.client_inst_opts
  - community.clickhouse.cluster_inst_opts

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
          - Keys are privilege names, like C(CREATE USER) or V(SELECT(column1, column2\)).
          - Values are booleans indicating whether to grant the privilege
            with the C(WITH GRANT OPTION).
          - Alternatively, you can use the C(grant_option) parameter to apply the same setting to all privileges in this set.
        type: dict
        required: true
      grant_option:
        description:
          - A boolean that applies to all privileges in this set.
          - If specified, it overrides any individual grant option settings within C(privs).
        type: bool
  revokes:
    description:
      - A list of privileges to partial revoke from the O(grantee).
      - At this moment this works only with state C(present). State C(absent) will not grant any privileges.
      - At this moment this option only revokes privileges that C(grantee) already have.
        So when privileges passed in C(privileges) are not granted yet, they will not be revoked.
        This may require running twice task at this moment. This will be fixed in future releases.
    type: list
    elements: dict
    version_added: '2.4.0'
    suboptions:
      object:
        description:
          - The database object to revoke privileges from.
          - Use C(*.*) for global privileges, C(database.*) for all tables in a database, and C(database.table) for a specific table.
          - When passed single element like C(*), C(POSTGRES) access_object will be used ex. privs READ .
        type: str
        required: true
      privs:
        description:
          - A list of privileges to revoke.
          - Each privilege can be specified with or without column restrictions.
          - Elements can be passed as separate entries in the list or as a single comma-separated string.
        type: list
        elements: str
        required: true
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

- name: Grant privileges on a cluster
  community.clickhouse.clickhouse_grants:
    grantee: bob
    cluster: test_cluster
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

- name: Partialy revoke single table
  community.clickhouse.clickhouse_grants:
    grantee: david
    exclusive: true
    revokes:
      - object: 'bar.foo'
        privs:
          - SELECT

- name: Partialy revoke single columns
  community.clickhouse.clickhouse_grants:
    grantee: david
    exclusive: true
    revokes:
      - object: 'bar.foo'
        privs:
          - SELECT(a,b)

- name: Partialy revoke more than one statement
  community.clickhouse.clickhouse_grants:
    grantee: david
    exclusive: true
    revokes:
      - object: 'bar.foo'
        privs:
          - SELECT, INSERT

- name: Partialy revoke more than one statement in two objects
  community.clickhouse.clickhouse_grants:
    grantee: david
    exclusive: true
    revokes:
      - object: 'bar.foo'
        privs:
          - SELECT
          - INSERT
'''

RETURN = r'''
executed_statements:
  description:
  - Data-modifying executed statements.
  returned: on success
  type: list
  sample: ['GRANT SELECT ON foo.* TO alice', 'REVOKE INSERT ON foo.* FROM alice']
diff:
  description:
  - Differences between the previous and current state.
  - Only returned when diff mode is enabled (with C(--diff) or in C(check_mode)).
  returned: when diff mode is enabled or check_mode is true
  type: dict
  contains:
    before:
      description: Grants before the change.
      returned: always
      type: dict
      sample:
        "*.*":
          "CREATE USER": false
        "foo.*":
          "INSERT": false
          "SELECT": true
    after:
      description: Grants after the change.
      returned: always
      type: dict
      sample:
        "*.*":
          "CREATE USER": false
        "foo.*":
          "DELETE": true
          "INSERT": false
          "SELECT": true
'''

import re
from collections import defaultdict

from ansible.module_utils.basic import AnsibleModule

from ansible_collections.community.clickhouse.plugins.module_utils.clickhouse import (
    check_clickhouse_driver,
    client_common_argument_spec,
    connect_to_db_via_client,
    execute_query,
    get_main_conn_kwargs,
    get_on_cluster_clause,
    cluster_argument_spec,
    get_server_version,
)

executed_statements = []

# Compile regex pattern once for performance
GRANT_REGEX = re.compile(r'GRANT (.+?) ON (.+?) TO .+?( WITH GRANT OPTION)?(?: ON CLUSTER .+)?$')
# Columns for system.grants and keys for dictionaries in _grants
grants_columns = ['access_type', 'access_object', 'database', 'table', 'column', 'is_partial_revoke', 'grant_option']


class ClickHouseGrants():
    def __init__(self, module, client, grantee, cluster=None):
        self.changed = False
        self.module = module
        self.client = client
        self.grantee = grantee
        self.cluster = cluster
        self.__check_grantee_exists()
        self._grants = None

    def __check_grantee_exists(self):
        # Check if grantee exists as either a user or a role
        exec_kwargs = {'params': {'name': self.grantee}}
        query = ("SELECT 1 FROM system.users WHERE name = %(name)s "
                 "UNION ALL "
                 "SELECT 1 FROM system.roles WHERE name = %(name)s "
                 "LIMIT 1")

        result = execute_query(self.module, self.client, query, exec_kwargs)

        if not result:
            self.module.fail_json(msg="Grantee %s does not exist" % self.grantee)

    @property
    def grants(self):
        """Fetch grantee privileges."""
        """This method works only on ClickHouse 25.8 and later, as it relies on the system.grants table."""
        """At this moment it is only used for checking the presence of partial revokes, so it is not called on older versions."""
        if self._grants is None:
            query_parameters = {'params': {'name': self.grantee}}
            query = f"""SELECT
                        {', '.join(grants_columns)}
                    FROM system.grants WHERE user_name = %(name)s
                    OR role_name = %(name)s"""
            result = execute_query(self.module, self.client, query, query_parameters)
            self._grants = [dict(zip(grants_columns, row)) for row in result]
        return self._grants

    def get(self):
        query = "SHOW GRANTS FOR `%s`" % self.grantee
        result = execute_query(self.module, self.client, query)

        grants = {}
        for row in result:
            grant_statement = row[0]
            match = GRANT_REGEX.match(grant_statement)
            if not match:
                continue

            privs_str, obj, grant_option_str = match.groups()
            grant_option = (grant_option_str is not None)

            # Normalize object name: ClickHouse 25.x returns '*' instead of '*.*' for global grants
            if obj == '*':
                obj = '*.*'

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

    def _parse_priv_entries(self, priv):
        '''Unpack values passed in module. Returns list of dicts where key is statement and value list of columns.'''
        PRIV_REGEX = re.compile(r'(?P<statement>[\w\s]+)(\((?P<col>[^)]*)\))?')
        result = defaultdict(list)

        for match in PRIV_REGEX.finditer(priv):
            stmt = match.group('statement').strip()
            col_str = match.group('col')
            if col_str is not None:
                cols = [c.strip() for c in col_str.split(',') if c.strip()]
                result[stmt] = cols
            else:
                result[stmt] = []
        return result

    def _privilege_fully_present(self, obj, privs, revoke=0):
        """
        Check whether privileges described by `privs` on `obj` are already
        represented in current grants.

        Returns True if no change is needed, False if change is required.
        """
        is_table_object = '.' in obj

        object_grants = [
            grant for grant in self.grants
            if self._grant_matches_object(grant, obj)
        ]

        requested_partial_flag = int(bool(revoke))

        for stmt, cols in privs.items():
            # For a partial revoke, there must be an underlying privilege
            # to revoke. If there isn't one, there is nothing to do.
            if revoke and not self._has_source_privilege(object_grants, stmt):
                continue

            if not self._privilege_covered(
                object_grants,
                stmt,
                cols,
                requested_partial_flag,
                is_table_object,
            ):
                return False

        return True

    def _grant_matches_object(self, grant, obj):
        """Return True if grant applies to the requested object."""
        if '.' in obj:
            db, tbl = obj.split('.', 1)
            database = None if db == '*' else db
            table = None if tbl == '*' else tbl

            return (
                # Exact object
                (
                    grant['database'] == database
                    and grant['table'] == table
                )
                # *.*
                or (
                    grant['database'] is None
                    and grant['table'] is None
                )
                # db.*
                or (
                    grant['database'] == database
                    and grant['table'] is None
                )
            )

        # ClickHouse represents wildcard access_object as an empty string.
        access_object = '' if obj == '*' else obj

        return (
            grant['access_object'] == access_object
            or grant['access_object'] == ''
        )

    def _has_source_privilege(self, grants, stmt):
        """
        Return True if an underlying privilege exists for a partial revoke.

        ALL is considered a source for any specific privilege.
        """
        stmt = stmt.upper()

        return any(
            grant['access_type'].upper() in (stmt, 'ALL')
            and not int(grant.get('is_partial_revoke') or 0)
            for grant in grants
        )

    def _privilege_covered(
        self,
        grants,
        stmt,
        cols,
        partial_revoke,
        is_table_object,
    ):
        """Return True if grants fully cover the requested privilege."""
        stmt = stmt.upper()
        required_cols = set(cols)
        covered_cols = set()

        for grant in grants:
            grant_type = grant['access_type'].upper()

            # For normal grants, ALL covers any specific privilege.
            # For partial revokes, only the exact privilege counts.
            if grant_type != stmt and not (
                not partial_revoke and grant_type == 'ALL'
            ):
                continue

            grant_partial_flag = int(grant.get('is_partial_revoke') or 0)

            if grant_partial_flag != partial_revoke:
                continue

            if not is_table_object:
                return True

            # Unrestricted grant covers all columns.
            if not grant['column']:
                return True

            covered_cols.add(grant['column'])

        # No columns requested means the whole privilege is requested.
        # It therefore requires an unrestricted grant.
        if not required_cols:
            return False

        return required_cols.issubset(covered_cols)

    def update(self):
        desired = self._get_desired_grants()
        current = self.get()
        partial_revokes = self.module.params.get('revokes', [])
        exclusive = self.module.params['exclusive']

        # Use set comprehensions for better performance
        all_current_privs = {(priv, obj, go)
                             for obj, privs in current.items()
                             for priv, go in privs.items()}

        all_desired_privs = {(priv, obj, go)
                             for obj, privs in desired.items()
                             for priv, go in privs.items()}

        to_revoke = all_current_privs - all_desired_privs if exclusive else set()
        to_grant = all_desired_privs - all_current_privs

        if not to_revoke and not to_grant and not partial_revokes:
            return self.changed

        queries = []
        revokes_by_obj = defaultdict(list)
        for priv, obj, go in to_revoke:
            revokes_by_obj[obj].append(priv)

        for obj, privs in revokes_by_obj.items():
            privs_str = ', '.join(sorted(privs))
            query = "REVOKE {0} ON {1} FROM '{2}'".format(privs_str, obj, self.grantee)
            query += get_on_cluster_clause(self.module, self.cluster)
            queries.append(query)

        grants_go_by_obj = defaultdict(list)
        grants_no_go_by_obj = defaultdict(list)

        for priv, obj, go in to_grant:
            if go:
                grants_go_by_obj[obj].append(priv)
            else:
                grants_no_go_by_obj[obj].append(priv)

        for obj, privs in grants_go_by_obj.items():
            privs_str = ', '.join(sorted(privs))
            query = "GRANT {0} ON {1} TO '{2}' WITH GRANT OPTION".format(privs_str, obj, self.grantee)
            query += get_on_cluster_clause(self.module, self.cluster)
            queries.append(query)

        for obj, privs in grants_no_go_by_obj.items():
            privs_str = ', '.join(sorted(privs))
            query = "GRANT {0} ON {1} TO '{2}'".format(privs_str, obj, self.grantee)
            query += get_on_cluster_clause(self.module, self.cluster)
            queries.append(query)

        # Handle partial revokes if specified
        if partial_revokes:
            server_version = get_server_version(self.module, self.client)

            if server_version['year'] < 25 or (server_version['year'] == 25 and server_version['feature'] < 8):
                self.module.fail_json(msg="Partial revokes not supported. Required 25.8 or later.")
            for revoke in partial_revokes:
                parsed_privs = {}
                for entry in revoke.get('privs', []):
                    parsed = self._parse_priv_entries(entry)
                    for stmt, cols in parsed.items():
                        # If any entry for stmt has no cols => unrestricted privilege
                        if not cols:
                            parsed_privs[stmt] = []
                        else:
                            if parsed_privs.get(stmt) == []:
                                continue
                            parsed_privs.setdefault(stmt, [])
                            parsed_privs[stmt].extend(cols)
                revoke_obj = revoke.get('object')
                # Check if the privilege is already present with the same revoke status
                if self._privilege_fully_present(revoke_obj, parsed_privs, revoke=1):
                    continue
                stmts = []
                # Handle each privilege and its associated columns
                for key, value in parsed_privs.items():
                    if value:
                        cols = list(dict.fromkeys(value))
                        stmts.append(f"{key}({', '.join(cols)})")
                    else:
                        stmts.append(key)
                query = "REVOKE {0} ON {1} FROM '{2}'".format(', '.join(stmts), revoke_obj, self.grantee)
                query += get_on_cluster_clause(self.module, self.cluster)
                queries.append(query)

        if not queries:
            return False  # No changes needed
        else:
            self.changed = True

        executed_statements.extend(queries)

        if self.module.check_mode:
            return self.changed

        for query in queries:
            execute_query(self.module, self.client, query)

        return self.changed

    def revoke(self):
        current = self.get()

        if not current:
            # No grants to revoke
            return self.changed

        self.changed = True

        # Build revoke queries for all current privileges
        queries = []
        for obj, privs in current.items():
            privs_str = ', '.join(sorted(privs))
            query = "REVOKE {0} ON {1} FROM '{2}'".format(privs_str, obj, self.grantee)
            query += get_on_cluster_clause(self.module, self.cluster)
            queries.append(query)

        executed_statements.extend(queries)

        if self.module.check_mode:
            return self.changed

        for query in queries:
            execute_query(self.module, self.client, query)

        return self.changed


def main():
    argument_spec = client_common_argument_spec()
    argument_spec.update(
        state=dict(type='str', choices=['present', 'absent'], default='present'),
        grantee=dict(type='str', required=True),
        exclusive=dict(type='bool', default=False),
        privileges=dict(type='list', elements='dict'),
        revokes=dict(type='list', elements='dict',
                     options=dict(
                         object=dict(type='str', required=True),
                         privs=dict(type='list', elements='str', required=True),
                     )),
    )

    argument_spec.update(cluster_argument_spec())

    # Instantiate an object of module class
    module = AnsibleModule(
        argument_spec=argument_spec,
        supports_check_mode=True,
        required_if=[
            ('state', 'present', ('privileges', 'revokes'), True),
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
    cluster = module.params['cluster']

    # Will fail if no driver informing the user
    check_clickhouse_driver(module)

    # Connect to DB
    client = connect_to_db_via_client(module, main_conn_kwargs, client_kwargs)

    # Do the job
    changed = False
    grants = ClickHouseGrants(module, client, grantee, cluster)
    # Get current grants
    start_grants = grants.get()

    if state == 'present':
        changed = grants.update()
    elif state == 'absent':
        changed = grants.revoke()

    # Get end grants - in check_mode, compute expected state
    if module.check_mode and changed:
        # In check mode, compute what the end state would be
        if state == 'absent':
            end_grants = {}
        else:  # state == 'present'
            desired_grants = grants._get_desired_grants()
            if module.params['exclusive']:
                end_grants = desired_grants
            else:
                # Merge current and desired grants (append mode)
                end_grants = dict(start_grants)
                for obj, privs in desired_grants.items():
                    if obj not in end_grants:
                        end_grants[obj] = {}
                    end_grants[obj].update(privs)
    else:
        # In normal mode or if no changes, query actual state
        end_grants = grants.get()

    # Close connection
    client.disconnect_connection()

    # Prepare result
    result = {
        'changed': changed,
        'executed_statements': executed_statements,
    }

    # Add diff if in diff mode or check mode
    if module._diff or module.check_mode:
        result['diff'] = {
            'before': start_grants,
            'after': end_grants,
        }

    # Users will get this in JSON output after execution
    module.exit_json(**result)


if __name__ == '__main__':
    main()
