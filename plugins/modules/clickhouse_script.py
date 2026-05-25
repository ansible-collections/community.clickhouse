#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright: (c) 2026, Rafal Kozlowski (@rkozlo) <rafalkozlowski07@gmail.com>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)


from __future__ import absolute_import, division, print_function

__metaclass__ = type

DOCUMENTATION = r'''
---
module: clickhouse_script

short_description: Run SQL queries from a file

description:
    - This module allows to execute SQL queries from files.
    - Queries has to be separated with C(;)
    - Module uses sqlglot for parsing file.
    - Module uses server side parametrization. Ex {d:Date}, {id:String}.

version_added: '2.2.0'

author:
  - Rafal Kozlowski (@rkozlo)

requirements:
  - sqlglot
  - clickhouse-driver

extends_documentation_fragment:
  - community.clickhouse.client_inst_opts

options:
  path:
    description:
      - Path to a SQL file to execute.
    type: str
    required: true
  parameters:
    description:
      - Parameters to pass to substitute in script.
      - Note module uses server side parametrization.
    type: dict

attributes:
  check_mode:
    description:
      - In check mode I(executed_statements) and I(query_parameters) will be set, but none queire will be sent to database.
    support: full
  idempotency:
    description: Always changed=True.
    support: none
'''

EXAMPLES = r'''
- name: Execute plain sql script
  community.clickhouse.clickhouse_script:
    path: test_file.sql

- name: Execute sql script with parametrization
  community.clickhouse.clickhouse_script:
    path: test_file.sql
    parameters:
      d: "2026-05-19"
      c: "cl"
'''

RETURN = r'''
executed_statements:
  description:
    - Queries executed from file. In case of failure, queries executed prior to the failed one are returned via RV(custom_message).
  returned: on success
  type: list
  sample: [
    "CREATE DATABASE IF NOT EXISTS test_db",
    "SELECT 1 FROM test_db WHERE b = {a:String}"
  ]
query_parameters:
  description:
    - Query parameters passed to server.
  returned: on success
  type: list
  sample: [
    {
      "a": "cl",
      "d": "2026-05-19"
    }
  ]
custom_message:
  description:
    - Queries that was successfully executed in case if one of them fail. Normally, executed queries are returned via RV(executed_statements).
  returned: on failure
  type: dict
  sample:
    success_queries:
      - SELECT 1
      - SELECT 1
'''

from ansible.module_utils.basic import AnsibleModule, missing_required_lib
from ansible.module_utils.common.text.converters import to_native
from pathlib import Path
try:
    import sqlglot
    HAS_SQLGLOT = True
    SQLGLOT_IMPORT_ERROR = None
except ImportError:
    HAS_SQLGLOT = False

from ansible_collections.community.clickhouse.plugins.module_utils.clickhouse import (
    get_main_conn_kwargs,
    connect_to_db_via_client,
    check_clickhouse_driver,
    client_common_argument_spec,
    execute_query
)

executed_statements = []
query_parameters = []


class ClickHouseScript():
    def __init__(self, module, client, path):
        self.module = module
        self.client = client
        self.path = Path(path)
        self._exists = None
        self._is_file = None

    @property
    def exists(self):
        if self._exists is None:
            self._exists = self.path.exists()
        return self._exists

    @property
    def is_file(self):
        if self._is_file is None:
            self._is_file = self.path.is_file()
        return self._is_file

    def _validate(self):
        if not self.exists:
            self.module.fail_json(msg=f"Passed path does not exists: {str(self.path)}")
        elif not self.is_file:
            self.module.fail_json(msg=f"Passed path is not regular file: {str(self.path)}")

    def _get_statements(self):
        '''Method returns AST structure.'''
        try:
            content = self.path.read_text()
        except Exception as e:
            self.module.fail_json(msg="Failed reading file : %s" % to_native(e))
        try:
            return sqlglot.parse(content, dialect="clickhouse")
        except Exception as e:
            self.module.fail_json(msg="Failed parsing file: %s" % to_native(e))

    def execute(self, parameters=None):
        self._validate()

        statements = self._get_statements()
        execute_kwargs = None
        if parameters:
            execute_kwargs = {
                'params': parameters,
                # To keep consistency between sqlglot and server.
                # Without this server expects client side parametrization %(d)s% format.
                'settings': {'server_side_params': True},
            }
            query_parameters.append(parameters)
        for stmt in statements:
            sql = stmt.sql(dialect="clickhouse")
            if not self.module.check_mode:
                execute_query(self.module, self.client, sql, execute_kwargs=execute_kwargs, custom_message={'success_queries': executed_statements})
            executed_statements.append(sql)


def main():
    # Set up arguments.
    # If there are common arguments shared across several modules,
    # create the common_argument_spec() function under plugins/module_utils/*
    # and invoke here to return a dict with those arguments
    argument_spec = client_common_argument_spec()
    argument_spec.update(
        path=dict(type="str", required=True),
        parameters=dict(type="dict")
    )

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
    path = module.params["path"]
    parameters = module.params["parameters"]

    # Will fail if no driver informing the user
    check_clickhouse_driver(module)

    if not HAS_SQLGLOT:
        module.fail_json(msg=missing_required_lib('sqlglot'))

    # Connect to DB
    client = connect_to_db_via_client(module, main_conn_kwargs, client_kwargs)

    # Do the job
    clickhouse_script = ClickHouseScript(module=module, client=client, path=path)

    clickhouse_script.execute(parameters)

    # Close connection
    client.disconnect_connection()

    # Users will get this in JSON output after execution
    module.exit_json(changed=True, executed_statements=executed_statements, query_parameters=query_parameters)


if __name__ == "__main__":
    main()
