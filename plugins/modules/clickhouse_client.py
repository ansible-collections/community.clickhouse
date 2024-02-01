#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright: (c) 2024, Andrew Klychkov (@Andersson007) <andrew.a.klychkov@gmail.com>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

DOCUMENTATION = r'''
---
module: clickhouse_client

short_description: Execute queries in a ClickHouse database using the clickhouse-driver Client interface

description:
  - Execute arbitrary queries in a ClickHouse database using the
    L(clickhouse-driver,https://clickhouse-driver.readthedocs.io/en/latest) Client interface.
  - Always returns that the state changed.

version_added: '0.1.0'

author:
  - Andrew Klychkov (@Andersson007)

extends_documentation_fragment:
  - community.clickhouse.client_inst_opts

notes:
  - Does not support C(check_mode).

options:
  execute:
    description:
      - A query to the C(Client.execute()) method.
    type: str
    required: true

  execute_kwargs:
    description:
      - All additional keyword arguments you want to pass to
        the C(Client.execute()) method. For example, you can pass
        substitution parameters for the query you pass
        through the I(execute) argument.
    type: dict
    default: {}
'''

EXAMPLES = r'''
- name: Query DB using non-default user & DB to connect to
  register: result
  community.clickhouse.clickhouse_client:
    execute: SELECT version()
    login_host: localhost
    login_user: alice
    login_db: foo
    login_password: my_password

- name: Print returned server version
  ansible.builtin.debug:
    var: result.result

- name: Create table
  register: result
  community.clickhouse.clickhouse_client:
    execute: CREATE TABLE test_table_1 (x String) ENGINE = Memory

- name: Insert into test table using named parameters
  register: result
  community.clickhouse.clickhouse_client:
    execute: "INSERT INTO test_table_1 (x) VALUES (%(a)s), (%(b)s), (%(c)s)"
    execute_kwargs:
      params:
        a: one
        b: two
        c: three

- name: Check the result
  ansible.builtin.assert:
    that:
      - result.substituted_query == "INSERT INTO test_table_1 (x) VALUES ('one'), ('two'), ('three')"
      - result.statistics["processed_rows"] == 3

- name: Check rows were inserted into test table
  register: result
  community.clickhouse.clickhouse_client:
    execute: "SELECT * FROM test_table_1"

- name: Check return values
  ansible.builtin.assert:
    that:
    - result.result == [["one"], ["two"], ["three"]]
'''

RETURN = r'''
substituted_query:
  description:
  - Executed query with substituted arguments if any.
  returned: on success
  sample: SHOW version()
  type: str
result:
  description:
  - Result returned by Client.execute().
  returned: on success
  type: list
  sample: [["23.12.2.59"]]
statistics:
  description:
  - Last executed query statistics retrieved from the
    L(last_query,https://clickhouse-driver.readthedocs.io/en/latest/features.html#query-execution-statistics)
    attribute.
  - Returned items depend on server version.
  returned: on success
  type: dict
'''

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils._text import to_native

from ansible_collections.community.clickhouse.plugins.module_utils.connect import (
    check_driver,
    client_common_argument_spec,
)

Client = None
try:
    from clickhouse_driver import Client
    HAS_DB_DRIVER = True
except ImportError:
    HAS_DB_DRIVER = False


def get_main_conn_kwargs(module):
    """Retrieves main connection arguments values and translates
    them into corresponding clickhouse_driver.Client() arguments.

    Returns a dictionary of arguments with values to pass to Client().
    """
    main_conn_kwargs = {}
    main_conn_kwargs['host'] = module.params['login_host']  # Has a default value
    if module.params['login_port']:
        main_conn_kwargs['port'] = module.params['login_port']
    if module.params['login_db']:
        main_conn_kwargs['database'] = module.params['login_db']
    if module.params['login_user']:
        main_conn_kwargs['user'] = module.params['login_user']
    if module.params['login_password']:
        main_conn_kwargs['password'] = module.params['login_password']
    return main_conn_kwargs


def get_query_statistics(module, client):
    """Retrieve query statistics from the Client() object.

    Returns a dictionary with statistics.
    """
    statistics = {}
    try:
        # IMPORTANT: When adding new values, use hassattr() to check
        # if supported as below! This is important for compatibility reasons.
        statistics['elapsed'] = client.last_query.elapsed
        statistics['processed_rows'] = client.last_query.progress.rows
        statistics['processed_bytes'] = client.last_query.progress.bytes
        statistics['total_rows'] = client.last_query.progress.total_rows
        if hasattr(client.last_query.progress, 'written_rows'):
            # As supported since driver version 0.1.3
            statistics['written_rows'] = client.last_query.progress.written_rows

        if hasattr(client.last_query.progress, 'written_bytes'):
            # As supported since driver version 0.1.3
            statistics['written_bytes'] = client.last_query.progress.written_bytes

        if hasattr(client.last_query.progress, 'elapsed_ns'):
            # As supported since driver version 0.2.7
            statistics['elapsed_ns'] = client.last_query.progress.elapsed_ns

    except Exception as e:
        module.fail_json(msg="Failed to retrieve statistics: %s" % to_native(e))

    return statistics


def execute_query(module, client, query, execute_kwargs):
    """Execute query.

    Returns rows returned in response.
    """
    try:
        result = client.execute(query, **execute_kwargs)
    except Exception as e:
        module.fail_json(msg="Failed to execute query: %s" % to_native(e))

    return result


def get_substituted_query(module, client, query, execute_kwargs):
    """Substitute params in a query. If no params, just return the query as is.

    Returns a query with substituted params.
    """
    substituted_query = None
    if execute_kwargs.get("params"):
        try:
            substituted_query = client.substitute_params(query, execute_kwargs["params"],
                                                         context=client.connection.context)
        except Exception as e:
            module.fail_json(msg="Failed to substitute query params: %s" % to_native(e))
    else:
        # If no params were passed, no need to substitute.
        # Return query as is.
        substituted_query = query

    return substituted_query


def connect_to_db_via_client(module, main_conn_kwargs, client_kwargs):
    """Connects to DB using the Client() class.

    Returns Client() object.
    """
    try:
        # Merge the kwargs as Python 2 would through an error
        # when unpaking them separately to Client()
        client_kwargs.update(main_conn_kwargs)
        client = Client(**client_kwargs)
    except Exception as e:
        module.fail_json(msg="Failed to connect to database: %s" % to_native(e))

    return client


def main():
    # Set up arguments.
    # If there are common arguments shared across several modules,
    # create the common_argument_spec() function under plugins/module_utils/*
    # and invoke here to return a dict with those arguments
    argument_spec = client_common_argument_spec()
    argument_spec.update(
        execute=dict(type='str', required=True),
        execute_kwargs=dict(type='dict', default={}),
    )

    # Instantiate an object of module class
    module = AnsibleModule(
        argument_spec=argument_spec,
        supports_check_mode=False,
    )

    # Assign passed options to variables
    client_kwargs = module.params['client_kwargs']
    query = module.params['execute']
    execute_kwargs = module.params['execute_kwargs']
    # The reason why these arguments are separate from client_kwargs
    # is that we need to protect some sensitive data like passwords passed
    # to the module from logging (see the arguments above with no_log=True);
    # Such data must be passed as module arguments (not nested deep in values).
    main_conn_kwargs = get_main_conn_kwargs(module)

    # Will fail if no driver informing the user
    check_driver(module, HAS_DB_DRIVER)

    # Connect to DB
    client = connect_to_db_via_client(module, main_conn_kwargs, client_kwargs)

    # Substitute query params if needed for future return
    substituted_query = get_substituted_query(module, client, query, execute_kwargs)

    # Execute query
    result = execute_query(module, client, query, execute_kwargs)

    # Retreive statistics
    statistics = get_query_statistics(module, client)

    # Andersson007 doesn't see any way of checking if anything in DB was changed
    # or not by the query using the Client() interface
    # (considering all kinds of queries like DML, DDL, etc.)
    # It is safer and easier to always return changed=True.
    # Before changing this, think many times if you want to make
    # the code more complex and to support this complexity
    changed = True

    # Close connection
    client.disconnect_connection()

    # Users will get this in JSON output after execution
    kw = dict(
        changed=changed,
        result=result,
        substituted_query=substituted_query,
        statistics=statistics,
    )

    module.exit_json(**kw)


if __name__ == '__main__':
    main()
