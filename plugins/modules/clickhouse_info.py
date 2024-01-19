#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright: (c) 2024, Andrew Klychkov (@Andersson007) <andrew.a.klychkov@gmail.com>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

DOCUMENTATION = r'''
---
module: clickhouse_info

short_description: Gather ClickHouse server information using the clickhouse-driver Client interface

description:
  - Gather ClickHouse server information using the
    L(clickhouse-driver,https://clickhouse-driver.readthedocs.io/en/latest) Client interface.
  - Does not change server state.

attributes:
  check_mode:
    description: Supports check_mode.
    support: full

requirements: ['clickhouse-driver']

version_added: '0.1.0'

author:
  - Andrew Klychkov (@Andersson007)

notes:
  - See the clickhouse-driver
    L(documentation,https://clickhouse-driver.readthedocs.io/en/latest)
    for more information about the driver interface.

options:
  login_host:
    description:
      - The same as the C(Client(host='...')) argument.
    type: str
    default: 'localhost'

  login_port:
    description:
      - The same as the C(Client(port='...')) argument.
      - If not passed, relies on the driver's default argument value.
    type: int

  login_db:
    description:
      - The same as the C(Client(database='...')) argument.
      - If not passed, relies on the driver's default argument value.
    type: str

  login_user:
    description:
      - The same as the C(Client(user='...')) argument.
      - If not passed, relies on the driver's default argument value.
    type: str

  login_password:
    description:
      - The same as the C(Client(password='...')) argument.
      - If not passed, relies on the driver's default argument value.
    type: str

  client_kwargs:
    description:
      - Any additional keyword arguments you want to pass
        to the Client interface when instantiating its object.
    type: dict
    default: {}
'''

EXAMPLES = r'''
- name: Get server information
  register: result
  community.clickhouse.clickhouse_info:
    login_host: localhost
    login_user: alice
    login_db: foo
    login_password: my_password

- name: Print returned data
  ansible.builtin.debug:
    var: result
'''

RETURN = r'''
driver:
  description: Python driver information.
  returned: success
  type: dict
  sample: { "version": "0.2.6" }
version:
  description: Clickhouse server version.
  returned: success
  type: dict
  sample: {"raw": "23.12.2.59", "year": 23, "feature": 12, "maintenance": 2, "build": 59, "type": null }
'''

Client = None
try:
    from clickhouse_driver import Client
    from clickhouse_driver import __version__ as driver_version
    HAS_DB_DRIVER = True
except ImportError:
    HAS_DB_DRIVER = False

from ansible.module_utils.basic import AnsibleModule, missing_required_lib
from ansible.module_utils._text import to_native


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


def get_server_version(module, client):
    """Get server version.

    Returns a dictionary with server version.
    """
    result = execute_query(module, client, "SELECT version()")

    raw = result[0][0]
    split_raw = raw.split('.')

    version = {}
    version["raw"] = raw

    version["year"] = int(split_raw[0])
    version["feature"] = int(split_raw[1])
    version["maintenance"] = int(split_raw[2])

    if '-' in split_raw[3]:
        tmp = split_raw[3].split('-')
        version["build"] = int(tmp[0])
        version["type"] = tmp[1]
    else:
        version["build"] = int(split_raw[3])
        version["type"] = None

    return version


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


def check_driver(module):
    """Checks if the driver is present.

    Informs user if no driver and fails.
    """
    if not HAS_DB_DRIVER:
        module.fail_json(msg=missing_required_lib('clickhouse_driver'))


def main():
    # Set up arguments.
    # If there are common arguments shared across several modules,
    # create the common_argument_spec() function under plugins/module_utils/*
    # and invoke here to return a dict with those arguments
    argument_spec = {}
    argument_spec.update(
        login_host=dict(type='str', default='localhost'),
        login_port=dict(type='int', default=None),
        login_db=dict(type='str', default=None),
        login_user=dict(type='str', default=None),
        login_password=dict(type='str', default=None, no_log=True),
        client_kwargs=dict(type='dict', default={}),
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

    # Will fail if no driver informing the user
    check_driver(module)

    # Connect to DB
    client = connect_to_db_via_client(module, main_conn_kwargs, client_kwargs)

    # Get server information
    srv_info = {'driver': {}}
    srv_info['driver']['version'] = driver_version
    srv_info['version'] = get_server_version(module, client)
    # srv_info['databases'] = get_databases(module, client)
    # srv_info['users'] = get_users(module, client)
    # srv_info['settings'] = get_settings(module, client)

    # Close connection
    client.disconnect_connection()

    # Users will get this in JSON output after execution
    module.exit_json(changed=False, **srv_info)


if __name__ == '__main__':
    main()
