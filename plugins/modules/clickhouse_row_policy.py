#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright: (c) 2026, Rafal Kozlowski (@rkozlo) <rafalkozlowski07@gmail.com>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function

__metaclass__ = type

DOCUMENTATION = r'''
---
module: clickhouse_row_policy

short_description: Creates, removes or modify a ClickHouse row policy using the clickhouse-driver Client interface

description:
    - This module allows you to manage ClickHouse row policies. It can create, remove or
        modify row policies based on the provided parameters. The module uses the clickhouse-driver Client interface to interact with the ClickHouse database.

attributes:
  check_mode:
    description: Supports check_mode.
    support: full

author:
    - Rafal Kozlowski (@rkozlo) <rafalkozlowski07@gmail.com>

extends_documentation_fragment:
  - community.clickhouse.client_inst_opts

version_added: "2.3.0"

options:
  state:
    description:
      - Whether the row policy should be present or not.
      - If C(present), the module will create or update the row policy to match the provided parameters.
      - If C(absent), the module will remove the row policy if it exists.
    type: str
    choices: ["present", "absent"]
    default: "present"
  name:
    description:
      - The name of the row policy to manage.
    type: str
    required: true
  cluster:
    description:
      - The name of the cluster where the row policy is defined. If not provided, the row policy will be managed on the local server.
    type: str
    default: null
  database:
    description:
      - The name of the database where the row policy is defined.
    type: str
    required: true
  table:
    description:
      - The name of the table where the row policy is defined. Can be C(*),
    type: str
    default: "*"
  using:
    description:
      - The condition that defines the row policy. This is a required parameter when C(state=present).
    type: str
  restrictive:
    description:
      - Whether the row policy is restrictive.
    type: bool
    default: false
  apply_to:
    description:
      - The users or roles to which the row policy applies.
    type: list
    elements: str
    default: []
  apply_to_all:
    description:
      - Whether the row policy applies to all users or roles.
    type: bool
    default: false
  apply_to_except:
    description:
      - The users or roles to which the row policy does not apply. Can only be used if C(apply_to_all) is true.
    type: list
    elements: str
    default: []
'''

EXAMPLES = r'''
- name: Create a row policy for a specific table
  community.clickhouse.clickhouse_row_policy:
    name: "policy1"
    database: "test_db"
    table: "test_table"
    using: "a = 1"
    restrictive: true
    apply_to: ["user1", "user2"]

- name: Create a row policy that applies to all users except one
  community.clickhouse.clickhouse_row_policy:
    name: "policy2"
    database: "test_db"
    table: "test_table"
    using: "b = 1"
    apply_to_all: true
    apply_to_except: ["user3"]

- name: Remove a row policy
  community.clickhouse.clickhouse_row_policy:
    name: "policy1"
    database: "test_db"
    table: "test_table"
    state: "absent"

- name: Create a row policy for all tables in a database
  community.clickhouse.clickhouse_row_policy:
    name: "policy3"
    database: "test_db"
    table: "*"
    using: "c = 1"
'''

RETURN = r'''
executed_statements:
  description:
    - Data-modifying executed statements.
  returned: on success
  type: list
  sample: ["CREATE ROW POLICY `test_policy` ON `test_db`.`test_table` USING `c` = 1 AS PERMISSIVE TO `user1`, `user2`"]
'''

from ansible.module_utils.basic import AnsibleModule

from ansible_collections.community.clickhouse.plugins.module_utils.clickhouse import (
    get_main_conn_kwargs,
    connect_to_db_via_client,
    check_clickhouse_driver,
    client_common_argument_spec,
    execute_query,
    validate_identifier,
    get_on_cluster_clause,
    normalize_db_table,
)

executed_statements = []


class ClickHouseRowPolicy:
    def __init__(self, module, client, name, database, table):
        self.module = module
        self.client = client
        validate_identifier(module, name, "row policy name")
        validate_identifier(module, database, "database name")
        if table == "*":
            self.table = "*"
        else:
            validate_identifier(module, table, "table name")
            self.table = table
        self.name = name
        self.database = database
        self._exists = None
        self._select_filter = None
        self._is_restrictive = None
        self._apply_to_all = None
        self._apply_to_list = None
        self._apply_to_except = None

    @property
    def exists(self):
        self._load()
        return self._exists

    @property
    def select_filter(self):
        self._load()
        return self._select_filter

    @property
    def is_restrictive(self):
        self._load()
        return self._is_restrictive

    @property
    def apply_to_all(self):
        self._load()
        return self._apply_to_all

    @property
    def apply_to_list(self):
        self._load()
        return self._apply_to_list

    @property
    def apply_to_except(self):
        self._load()
        return self._apply_to_except

    def _load(self):
        """Fetch info about passed collection"""
        if self._exists is not None:
            return

        # In ClickHouse * is as empty string. We can't pass self.table.
        table_value = '' if self.table == '*' else self.table
        query = """SELECT
                        select_filter,
                        is_restrictive,
                        apply_to_all,
                        apply_to_list,
                        apply_to_except
                    FROM system.row_policies
                    WHERE
                        short_name = %(short_name)s
                        AND database = %(database)s
                        AND table = %(table)s"""
        query_parameters = {'short_name': self.name, 'database': self.database, 'table': table_value}
        result = execute_query(self.module, self.client, query, {'params': query_parameters})

        if not result:
            self._exists = False
            return

        self._select_filter, self._is_restrictive, self._apply_to_all, self._apply_to_list, self._apply_to_except = result[0]
        self._exists = True

    def _quote_identifier(self, identifier):
        validate_identifier(self.module, identifier)
        return f"`{identifier}`"

    def _build_to_clause(self, apply_to, apply_to_all, apply_to_except):
        if not apply_to and not apply_to_all:
            return ""
        result = " TO "
        if apply_to_all:
            result += "ALL"
            if apply_to_except:
                quoted_except = [self._quote_identifier(_item) for _item in apply_to_except]
                result += " EXCEPT "
                result += ", ".join(quoted_except)
        elif apply_to:
            quoted_except = [self._quote_identifier(_item) for _item in apply_to]
            result += ", ".join(quoted_except)
        return result

    def _get_restrictive_clause(self, restrictive):
        if restrictive:
            return " AS RESTRICTIVE"
        else:
            return " AS PERMISSIVE"

    def _normalize_using_parameter(self, input):
        """Best get what DB will create. Complex condition can easily brake idempotency."""
        query = "SELECT formatQuerySingleLine(CONCAT('SELECT 1 FROM _to_normalize WHERE ', %(cond)s))"
        execute_kwargs = {'params': {'cond': input}}
        result = execute_query(self.module, self.client, query, execute_kwargs)
        using = result[0][0].split(" WHERE ")[1]
        return using

    def _compare_apply_to(self, apply_to, apply_to_all, apply_to_except):
        if self.apply_to_all != apply_to_all:
            return False
        if self.apply_to_all and set(self.apply_to_except) != set(apply_to_except):
            return False
        if not self.apply_to_all and set(self.apply_to_list) != set(apply_to):
            return False
        return True

    def _needs_update(self, using, restrictive, apply_to, apply_to_all, apply_to_except):
        if self.select_filter != using:
            return True
        if self.is_restrictive != restrictive:
            return True
        if not self._compare_apply_to(apply_to, apply_to_all, apply_to_except):
            return True
        return False

    def create(self, using, restrictive, apply_to, apply_to_all, apply_to_except, cluster):
        query = f"CREATE ROW POLICY `{self.name}`"
        # Target table
        target_normalized = normalize_db_table(self.module, self.client, self.database + '.' + self.table)
        query += f" ON {target_normalized}"
        query += get_on_cluster_clause(self.module, cluster)
        using_normalized = self._normalize_using_parameter(using)
        query += f" USING {using_normalized}"
        query += self._get_restrictive_clause(restrictive)
        query += self._build_to_clause(apply_to, apply_to_all, apply_to_except)

        executed_statements.append(query)

        if not self.module.check_mode:
            execute_query(self.module, self.client, query)

        return True

    def alter(self, using, restrictive, apply_to, apply_to_all, apply_to_except, cluster):
        using_normalized = self._normalize_using_parameter(using)
        if not self._needs_update(using_normalized, restrictive, apply_to, apply_to_all, apply_to_except):
            return False
        query = f"ALTER ROW POLICY `{self.name}`"
        # Target table
        target_normalized = normalize_db_table(self.module, self.client, self.database + '.' + self.table)
        query += f" ON {target_normalized}"
        query += get_on_cluster_clause(self.module, cluster)
        query += f" USING {using_normalized}"
        query += self._get_restrictive_clause(restrictive)
        query += self._build_to_clause(apply_to, apply_to_all, apply_to_except)

        executed_statements.append(query)

        if not self.module.check_mode:
            execute_query(self.module, self.client, query)

        return True

    def drop(self, cluster):
        query = f"DROP ROW POLICY `{self.name}`"
        # Target table
        target_normalized = normalize_db_table(self.module, self.client, self.database + '.' + self.table)
        query += f" ON {target_normalized}"
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
        cluster=dict(type='str', default=None),
        database=dict(type='str', required=True),
        table=dict(type='str', default="*"),
        using=dict(type='str', default=None),
        restrictive=dict(type='bool', default=False),
        apply_to=dict(type='list', elements='str', default=[]),
        apply_to_all=dict(type='bool', default=False),
        apply_to_except=dict(type='list', elements='str', default=[])
    )

    # Conditional logic
    module = AnsibleModule(
        argument_spec=argument_spec,
        supports_check_mode=True,
        required_if=[
            ('state', 'present', ['using'], True),
        ],
        mutually_exclusive=[
            ('apply_to_all', 'apply_to'),
            ('apply_to_except', 'apply_to'),
        ],
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
    database = module.params["database"]
    table = module.params["table"]
    using = module.params["using"]
    restrictive = module.params["restrictive"]
    apply_to = module.params["apply_to"]
    apply_to_all = module.params["apply_to_all"]
    apply_to_except = module.params["apply_to_except"]

    # Will fail if no driver informing the user
    check_clickhouse_driver(module)

    # Connect to DB
    client = connect_to_db_via_client(module, main_conn_kwargs, client_kwargs)

    # Do the job
    changed = False
    row_policy = ClickHouseRowPolicy(module, client, name, database, table)
    if state == "present":
        if not row_policy.exists:
            changed = row_policy.create(using, restrictive, apply_to, apply_to_all, apply_to_except, cluster)
        else:
            changed = row_policy.alter(using, restrictive, apply_to, apply_to_all, apply_to_except, cluster)
    else:
        if row_policy.exists:
            changed = row_policy.drop(cluster)

    # Close connection
    client.disconnect_connection()

    # Users will get this in JSON output after execution
    module.exit_json(changed=changed, executed_statements=executed_statements)


if __name__ == "__main__":
    main()
