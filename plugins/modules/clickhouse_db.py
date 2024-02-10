#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright: (c) 2024, Andrew Klychkov (@Andersson007) <andrew.a.klychkov@gmail.com>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

DOCUMENTATION = r'''
---
module: clickhouse_db

short_description: Creates or removes a ClickHouse database using the clickhouse-driver Client interface

description:
  - Creates or removes a ClickHouse database using the
    L(clickhouse-driver,https://clickhouse-driver.readthedocs.io/en/latest) Client interface.

attributes:
  check_mode:
    description: Supports check_mode.
    support: full

version_added: '0.3.0'

author:
  - Andrew Klychkov (@Andersson007)

extends_documentation_fragment:
  - community.clickhouse.client_inst_opts

options:
  state:
    description:
      - Database state.
      - If C(present), will create the database if not exists.
      - If C(absent), will drop the database if exists.
    type: str
    choices: ['present', 'absent']
    default: 'present'
  name:
    description:
      - Database name to add or remove.
    type: str
    required: true
  engine:
    description:
      - Database engine.
    type: str
'''

EXAMPLES = r'''
- name: Create database
  community.clickhouse.clickhouse_db:
    login_host: localhost
    login_user: alice
    login_db: foo
    login_password: my_password
    name: test_db
    engine: Memory
    state: present

- name: Drop database
  community.clickhouse.clickhouse_db:
    login_host: localhost
    login_user: alice
    login_db: foo
    login_password: my_password
    name: test_db
    engine: Memory
    state: absent
'''

RETURN = r'''
executed_statements:
  description:
  - Data-modifying executed statements.
  returned: on success
  type: list
  sample: ['CREATE DATABASE test_db']
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

executed_statements = []


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


def execute_query(module, client, query, execute_kwargs=None):
    """Execute query.

    Returns rows returned in response.
    """
    # Some modules do not pass this argument
    if execute_kwargs is None:
        execute_kwargs = {}

    try:
        result = client.execute(query, **execute_kwargs)
    except Exception as e:
        module.fail_json(msg="Failed to execute query: %s" % to_native(e))

    return result


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


class ClickHouseDB():
    def __init__(self, module, client, name):
        self.module = module
        self.client = client
        self.name = name
        self.exists, self.engine = self.__populate_info()

    def __populate_info(self):
        query = "SELECT engine FROM system.databases WHERE name = %(name)s"
        # Will move this function to the lib later and reuse
        exec_kwargs = {'params': {'name': self.name}}
        result = execute_query(self.module, self.client, query, exec_kwargs)

        # Assume the DB does not exist by default
        exists = False
        engine = None
        if result:
            # If exists
            exists = True
            engine = result[0][0]

        return exists, engine

    def create(self, engine):
        query = "CREATE DATABASE %s" % self.name
        if engine:
            query += " ENGINE = %s" % engine

        executed_statements.append(query)

        if not self.module.check_mode:
            execute_query(self.module, self.client, query)

        return True

    def update(self, engine):
        # There's no way to change the engine
        # so just inform the users they have to recreate
        # the DB in order to change them
        if engine and engine != self.engine:
            msg = ("The provided engine '%s' is different from "
                   "the current one '%s'. It is NOT possible to "
                   "change it. The recreation of the database is required "
                   "in order to change it." % (engine, self.engine))
            self.module.warn(msg)

        return False

    def drop(self):
        query = "DROP DATABASE %s" % self.name
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
        state=dict(type='str', choices=['present', 'absent'], default='present'),
        name=dict(type='str', required=True),
        engine=dict(type='str', default=None),
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
    name = module.params['name']
    engine = module.params['engine']

    # Will fail if no driver informing the user
    check_driver(module, HAS_DB_DRIVER)

    # Connect to DB
    client = connect_to_db_via_client(module, main_conn_kwargs, client_kwargs)

    # Do the job
    changed = False
    database = ClickHouseDB(module, client, name)

    if state == 'present':
        if not database.exists:
            changed = database.create(engine)
        else:
            # If database exists
            changed = database.update(engine)

    else:
        # If state is absent
        if database.exists:
            changed = database.drop()

    # Close connection
    client.disconnect_connection()

    # Users will get this in JSON output after execution
    module.exit_json(changed=changed, executed_statements=executed_statements)


if __name__ == '__main__':
    main()
