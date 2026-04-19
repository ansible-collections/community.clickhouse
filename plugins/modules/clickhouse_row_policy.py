#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright: (c) 2026, Rafal Kozlowski (@rkozlo) <rafalkozlowski07@gmail.com>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function

__metaclass__ = type

DOCUMENTATION = r'''
---
module: clickhouse_row_policy
'''

from ansible.module_utils.basic import AnsibleModule

from ansible_collections.community.clickhouse.plugins.module_utils.clickhouse import (
    check_clickhouse_driver,
    client_common_argument_spec,
    connect_to_db_via_client,
    execute_query,
    get_main_conn_kwargs,
    get_server_version
)

executed_statements = []


class ClickHouseRowPolicy:
    def __init__(self, module, client, name):
        self.module = module
        self.client = client
        self.name = name
        self._exists = None
        self._database = None
        self._table = None
        self._select_filter = None
        self._is_restrictive = None
        self._apply_to_all = None
        self._apply_to_list = None
        self._apply_to_except= None

    @property
    def exists(self):
        self._load()
        return self._exists

    @property
    def database(self):
        self._load()
        return self._database

    @property
    def table(self):
        self._load()
        return self._table

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

        query = f"SELECT database, table, select_filter, is_restrictive, apply_to_all, apply_to_list, apply_to_except FROM system.row_policies WHERE short_name = '{self.name}'"
        result = execute_query(self.module, self.client, query)

        if not result:
            self._exists = False
            return

        self._database, self._table, self._select_filter, self._is_restrictive, self._apply_to_all, self._apply_to_list, self._apply_to_except = result[0]
        self._exists = True
    
    def _validate_db_table(self, input):
        import re
        pattern = r'^[\w\-]+(\.[\w\-\*]+)?$'
        if not re.match(pattern, input):
            self.module.fail_json(msg=f"Invalid format: '{input}'. Expected 'table' or 'db.table'")

    def _build_target(self, input):
        """We want to make sure target is correct.
        When passed without db, default for session will apply and it can break idempotency.        
        """
        self._validate_db_table(input)
        if '.' in input:
            return input
        query = f"SELECT currentDatabase()"
        current_db = execute_query(self.module, self.client, query)[0]
        if not current_db:
            self.module.fail_json(msg="Error during fetch current db")
        return f"{current_db}.{input}"

    def _build_to_clause(self, apply_to, apply_to_all, apply_to_except):
        if not apply_to and not apply_to_all:
            return ""
        result = " TO "
        if apply_to_all:
            result += "ALL"
            if apply_to_except:
                result += " EXCEPT "
                result += ", ".join(apply_to_except)
        if apply_to:
            result += ", ".join(apply_to)
        return result


    def _normalize_using_parameter(self, input):
        """Best get what DB will create. Complex condition are easy to broke idempotency."""
        query = f"SELECT formatQuerySingleLine('SELECT 1 FROM _to_normalize WHERE {input}"
        result = execute_query(self.module, self.client, query)
        using = result[0].split(" WHERE ")[1]
        return using


    def create(self, target, using, restrictive, apply_to, apply_to_all, apply_to_except, cluster):
        query = f"CREATE ROW POLICY '{self.name}'"
        if cluster:
            query += f" ON CLUSTER '{cluster}'"
        target_normalized = self._build_target(target)
        query += f" ON {target_normalized}"
        using_normalized = self._normalize_using_parameter(using)
        query += f" USING {using_normalized}"
        if restrictive:
            query += " AS RESTRICTIVE"
        else:
            query += " AS PERMISSIVE"
        query += self._build_to_clause(apply_to, apply_to_all, apply_to_except)

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
        target=dict(type='str', required=True),
        using=dict(type='str', required=True),
        restrictive=dict(type='bool', default=False),
        apply_to=dict(type='list', elements='str', default=[]),
        applu_to_all=dict(type='bool', default=False),
        apply_to_except=dict(type='list', elements='str', default=[])
    )

    # Conditional logic
    module = AnsibleModule(
        argument_spec=argument_spec,
        supports_check_mode=True,
        required_if=[
            ('state', 'present', ['collection'], True)
        ],
    )