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
      - It only applies to O(privileges), so O(privileges) must be set when this is C(true).
        It has no effect on O(partial_revokes). To remove all privileges, use C(state=absent) instead.
    type: bool
    default: false
  privileges:
    description:
      - Privileges to grant. This option is required when C(state) is C(present),
        unless O(partial_revokes) is specified.
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
  partial_revokes:
    description:
      - A list of privileges to partial revoke from the O(grantee).
      - At this moment this works only with state C(present). State C(absent) will not grant any privileges.
      - A privilege is only revoked when the O(grantee) actually holds it, either from an earlier run
        or because it is granted by O(privileges) in the same task.
      - Removing an entry from this list does not grant the privilege back.
        Add it to O(privileges) to restore it.
      - Partial revokes are not represented in the RV(diff) return value.
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

- name: Partially revoke a single table from a database-wide grant
  community.clickhouse.clickhouse_grants:
    grantee: david
    partial_revokes:
      - object: 'bar.foo'
        privs:
          - SELECT

- name: Partially revoke single columns
  community.clickhouse.clickhouse_grants:
    grantee: david
    partial_revokes:
      - object: 'bar.foo'
        privs:
          - SELECT(a,b)

- name: Partially revoke more than one statement
  community.clickhouse.clickhouse_grants:
    grantee: david
    partial_revokes:
      - object: 'bar.foo'
        privs:
          - SELECT, INSERT

- name: Partially revoke more than one statement passed as separate entries
  community.clickhouse.clickhouse_grants:
    grantee: david
    partial_revokes:
      - object: 'bar.foo'
        privs:
          - SELECT
          - INSERT

- name: Grant a whole database and partially revoke one table in the same task
  community.clickhouse.clickhouse_grants:
    grantee: david
    privileges:
      - object: 'bar.*'
        privs:
          "SELECT": false
    partial_revokes:
      - object: 'bar.foo'
        privs:
          - SELECT
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
  - Reflects the O(privileges) option only. Partial revokes made through
    O(partial_revokes) are not represented, so a run that only changes them
    reports the same before and after state.
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
    normalize_db_table,
    validate_identifier,
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

    def _pending_grant_rows(self, to_grant):
        """Represent privileges granted by the current run as system.grants-like rows.

        Only the fields needed to detect the source privilege of a partial
        revoke are filled in, as these rows are never used to decide whether
        a partial revoke itself is already in place.
        """
        rows = []
        for priv, obj, is_grant_option in to_grant:
            if '.' in obj:
                db, tbl = obj.split('.', 1)
                database = None if db == '*' else db
                table = None if tbl == '*' else tbl
                access_object = ''
            else:
                database = None
                table = None
                access_object = '' if obj == '*' else obj
            priv_extracted = self._parse_priv_entries(priv)
            for stmt, cols in priv_extracted.items():
                # Iterate over granted columns or fill single entry for all column grant.
                for col in cols or [None]:
                    rows.append({
                        'access_type': stmt,
                        'access_object': access_object,
                        'database': database,
                        'table': table,
                        'column': col,
                        'is_partial_revoke': 0,
                        'grant_option': is_grant_option,
                    })

        return rows

    def _normalize_revoke_object(self, obj):
        """Turn an object passed by the user into a quoted statement part.

        Quoting is what makes the value safe to interpolate, as
        validate_identifier rejects the backticks needed to break out of it.

        Ex:
            foo.bar   → `foo`.`bar`
            foo.*     → `foo`.*
            *.*       → *.*
            POSTGRES  → `POSTGRES`
            *         → *
        """
        if '.' not in obj:
            # An access object type, ex. POSTGRES, or * for all of them.
            if obj == '*':
                return obj

            validate_identifier(self.module, obj, "access object")
            return f"`{obj}`"

        database, table = obj.split('.', 1)

        if database == '*':
            if table != '*':
                self.module.fail_json(msg="Invalid object: '%s'. "
                                          "A wildcard database can only be used as '*.*'" % obj)
            return '*.*'

        return normalize_db_table(self.module, self.client, database, table)

    def _pending_grant_clears_revoke(self, grant, revoke):
        """When GRANT expands to glob: foo.bar → foo.*
        It will produce GRANT which will remove existing partial revoke, since
        it already exists.
        Return True if executing `grant` drops the partial revoke.
        """
        grant_type = grant['access_type'].upper()

        # Different statemnt skip. For now hardcoded ALL as parent
        # In future worth discovering parent privilege for passed priv if it already covers.
        if grant_type not in (revoke['access_type'].upper(), 'ALL'):
            return False

        if grant['access_object']:
            return (revoke['database'] is None
                    and revoke['access_object'] == grant['access_object'])

        # None means *. Check if grant covers revoke.
        for level in ('database', 'table', 'column'):
            if grant[level] is not None and grant[level] != revoke[level]:
                return False

        return True

    def _grants_after_pending(self, pending_grants):
        """Model grants after applying pending grants.

        Check if pending grants affect exisitng partial.
        If pending grants would drop partial revokes remove it from current privileges.
        Same partial revoke is passed in module and will be reapplied in different method.
        """
        current = []
        for grant in self.grants:
            is_partial_revoke = bool(int(grant.get('is_partial_revoke') or 0))
            if not is_partial_revoke:
                current.append(grant)
                continue

            cleared_by_pending = False
            for pending in pending_grants:
                if self._pending_grant_clears_revoke(pending, grant):
                    cleared_by_pending = True
                    break

            if not cleared_by_pending:
                current.append(grant)

        return current + pending_grants

    def _privilege_fully_present(self, obj, privs, revoke=0, extra_grants=None):
        """
        Check whether privileges described by `privs` on `obj` are already
        represented in current grants.

        `extra_grants` holds grants that are not in place yet but will be by
        the time the generated statements run.

        Returns True if no change is needed, False if change is required.
        """
        is_table_object = '.' in obj

        all_grants = self._grants_after_pending(extra_grants) if extra_grants else self.grants

        object_grants = [
            grant for grant in all_grants
            if self._grant_matches_object(grant, obj)
        ]

        requested_partial_flag = int(bool(revoke))

        for stmt, cols in privs.items():
            # For a partial revoke, there must be an underlying privilege
            # to revoke. If there isn't one, there is nothing to do.
            if revoke and not self._has_source_privilege(object_grants, stmt, cols, extra_grants):
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

        # Access object grants are never bound to a database, so a database
        # or table level grant must not be treated as a match here.
        if grant['database'] is not None:
            return False

        # ClickHouse represents wildcard access_object as an empty string.
        access_object = '' if obj == '*' else obj

        return (
            grant['access_object'] == access_object
            or grant['access_object'] == ''
        )

    def _has_source_privilege(self, grants, stmt, cols=None, extra_grants=None):
        """
        Return True if an underlying privilege exists for a partial revoke.

        ALL is considered a source for any specific privilege.
        """

        stmt = stmt.upper()
        cols = set(cols or [])

        for grant in grants:
            if grant['access_type'].upper() not in (stmt, 'ALL'):
                continue
            if int(grant.get('is_partial_revoke') or 0):
                continue

            # unrestricted grant is a valid source for any columns
            if not grant['column']:
                return True

            # column-specific source must be compatible with requested columns
            if cols and grant['column'] in cols:
                return True

        return False

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
        partial_revokes = self.module.params.get('partial_revokes', [])
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

            # Privileges granted by this very run are revocable too:
            # the REVOKE statements below are appended after the GRANT ones.
            pending_grants = self._pending_grant_rows(to_grant)

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
                if self._privilege_fully_present(revoke_obj, parsed_privs, revoke=1,
                                                 extra_grants=pending_grants):
                    continue
                stmts = []
                # Handle each privilege and its associated columns.
                # Privilege names are keywords and cannot be quoted, but
                # _parse_priv_entries only ever yields word characters for them.
                for key, value in parsed_privs.items():
                    if value:
                        cols = []
                        for col in dict.fromkeys(value):
                            validate_identifier(self.module, col, "column")
                            cols.append(f"`{col}`")
                        stmts.append(f"{key}({', '.join(cols)})")
                    else:
                        stmts.append(key)
                query = "REVOKE {0} ON {1} FROM '{2}'".format(
                    ', '.join(stmts), self._normalize_revoke_object(revoke_obj), self.grantee)
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
        partial_revokes=dict(
            type='list',
            elements='dict',
            options=dict(
                object=dict(type='str', required=True),
                privs=dict(type='list', elements='str', required=True),
            )
        ),
    )

    argument_spec.update(cluster_argument_spec())

    # Instantiate an object of module class
    module = AnsibleModule(
        argument_spec=argument_spec,
        supports_check_mode=True,
        required_if=[
            ('state', 'present', ('privileges', 'partial_revokes'), True),
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

    # Without privileges to compare against, exclusive would revoke
    # everything the grantee has, which state=absent already does explicitly.
    if state == 'present' and module.params['exclusive'] and not module.params['privileges']:
        module.fail_json(msg="exclusive=true requires privileges to be set. "
                             "Use state=absent to revoke all privileges.")

    if state == 'absent' and module.params['partial_revokes']:
        module.warn("The partial_revokes option is ignored when state=absent, "
                    "as all privileges are revoked anyway.")

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
