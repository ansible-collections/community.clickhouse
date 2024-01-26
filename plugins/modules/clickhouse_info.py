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
      - Be sure your the user has permissions to read the system tables
        listed in the RETURN section.
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

  limit:
    description:
      - Limits a set of return values you want to get.
      - See the Return section for acceptable values.
    type: list
    elements: str
    version_added: '0.2.0'
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

- name: Limit return values with users and roles
  register: result
  community.clickhouse.clickhouse_info:
    filter:
      - users
      - roles
'''

# When adding new ret values,
# please add it to the ret_val_func_mapping dictionary!
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
databases:
  description:
    - The content of the system.databases table with names as keys.
    - Be sure your I(login_user) has permissions.
  returned: success
  type: dict
  sample: { "system": "..." }
users:
  description:
    - The content of the system.users table with names as keys.
    - Be sure your I(login_user) has permissions.
  returned: success
  type: dict
  sample: { "default": "..." }
roles:
  description:
    - The content of the system.roles table with names as keys.
    - Be sure your I(login_user) has permissions.
  returned: success
  type: dict
  sample: { "accountant": "..." }
settings:
  description:
    - The content of the system.settings table with names as keys.
  returned: success
  type: dict
  sample: { "zstd_window_log_max": "..." }
clusters:
  description:
    - The content of the system.clusters table with names as keys.
  returned: success
  type: dict
  sample: { "test_cluster_two_shards": "..." }
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

PRIV_ERR_CODE = 497


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
        if "Not enough privileges" in to_native(e):
            return PRIV_ERR_CODE
        module.fail_json(msg="Failed to execute query: %s" % to_native(e))

    return result


def get_databases(module, client):
    """Get databases.

    Returns a dictionary with database names as keys.
    """
    query = "SELECT name, engine, data_path, uuid FROM system.databases"
    result = execute_query(module, client, query)

    if result == PRIV_ERR_CODE:
        return {PRIV_ERR_CODE: "Not enough privileges"}

    db_info = {}
    for row in result:
        db_info[row[0]] = {
            "engine": row[1],
            "data_path": row[2],
            "uuid": str(row[3]),
        }

    return db_info


def get_clusters(module, client):
    """Get clusters.

    Returns a list with clusters names as top level keys.
    """
    query = ("SELECT cluster, shard_num, shard_weight, replica_num, host_name, "
             "host_address, port, is_local, user, default_database, errors_count, "
             "estimated_recovery_time FROM system.clusters")
    result = execute_query(module, client, query)

    if result == PRIV_ERR_CODE:
        return {PRIV_ERR_CODE: "Not enough privileges"}

    cluster_info = {}

    for row in result:
        cluster = row[0]
        shard_num = row[1]
        shard_weight = row[2]
        replica_num = row[3]
        host_name = row[4]
        host_address = row[5]
        port = row[6]
        is_local = row[7]
        user = row[8]
        default_database = row[9]
        errors_count = row[10]
        estimated_recovery_time = row[11]

        # Add cluster if not already there
        if cluster not in cluster_info:
            cluster_info[cluster] = {"shards": {}}

        # Add shard if not already there
        if shard_num not in cluster_info[cluster]["shards"]:
            cluster_info[cluster]["shards"][shard_num] = {
                "shard_weight": shard_weight,
                "replicas": {},
            }

        # Add replica if not already there
        if replica_num not in cluster_info[cluster]["shards"][shard_num]["replicas"]:
            cluster_info[cluster]["shards"][shard_num]["replicas"][replica_num] = {
                "host_name": host_name,
                "host_address": host_address,
                "port": port,
                "is_local": is_local,
                "user": user,
                "default_database": default_database,
                "errors_count": errors_count,
                "estimated_recovery_time": estimated_recovery_time,
            }

    return cluster_info


def get_roles(module, client):
    """Get roles.

    Returns a dictionary with roles names as keys.
    """
    query = "SELECT name, id, storage FROM system.roles"
    result = execute_query(module, client, query)

    if result == PRIV_ERR_CODE:
        return {PRIV_ERR_CODE: "Not enough privileges"}

    roles_info = {}
    for row in result:
        roles_info[row[0]] = {
            "id": str(row[1]),
            "storage": row[2],
        }

    return roles_info


def get_settings(module, client):
    """Get settings.

    Returns a dictionary with settings names as keys.
    """
    query = ("SELECT name, value, changed, description, min, max, readonly, "
             "type FROM system.settings")
    result = execute_query(module, client, query)

    if result == PRIV_ERR_CODE:
        return {PRIV_ERR_CODE: "Not enough privileges"}

    settings_info = {}
    for row in result:
        settings_info[row[0]] = {
            "value": row[1],
            "changed": row[2],
            "description": row[3],
            "min": row[4],
            "max": row[5],
            "readonly": row[6],
            "type": row[7],
        }

    return settings_info


def get_users(module, client):
    """Get users.

    Returns a dictionary with users names as keys.
    """
    query = ("SELECT name, id, storage, auth_type, auth_params, host_ip, host_names, "
             "host_names_regexp, host_names_like, default_roles_all, "
             "default_roles_list, default_roles_except FROM system.users")
    result = execute_query(module, client, query)

    if result == PRIV_ERR_CODE:
        return {PRIV_ERR_CODE: "Not enough privileges"}

    user_info = {}
    for row in result:
        user_info[row[0]] = {
            "id": str(row[1]),
            "storage": row[2],
            "auth_type": row[3],
            "auth_params": row[4],
            "host_ip": row[5],
            "host_names": row[6],
            "host_names_regexp": row[7],
            "host_names_like": row[8],
            "default_roles_all": row[9],
            "default_roles_list": row[10],
            "default_roles_except": row[11],
        }

    return user_info


def get_server_version(module, client):
    """Get server version.

    Returns a dictionary with server version.
    """
    result = execute_query(module, client, "SELECT version()")

    if result == PRIV_ERR_CODE:
        return {PRIV_ERR_CODE: "Not enough privileges"}

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


def get_driver(module, client):
    """Gets driver information.

    The module and client arguments are here
    to provide a common interface and are ignored.

    Returns its version for now.
    """
    return {"version": driver_version}


def check_driver(module):
    """Checks if the driver is present.

    Informs user if no driver and fails.
    """
    if not HAS_DB_DRIVER:
        module.fail_json(msg=missing_required_lib('clickhouse_driver'))


def handle_limit_values(module, supported_ret_vals, limit):
    """Checks if passed limit values match module return values.

    Prints a warning if do not match. Returns a list of stripped vals.
    """
    stripped_vals = []
    for wanted_val in limit:
        wanted_val = wanted_val.strip()

        if wanted_val not in supported_ret_vals:
            msg = ("The passed %s value does not exist in module return values: "
                   "please check the spelling and supported values, and try again" % wanted_val)
            module.warn(msg)
            continue

        stripped_vals.append(wanted_val)

    return stripped_vals


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
        limit=dict(type='list', elements='str'),
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

    # Create a mapping between ret values and functions.
    # When adding new values to return, add the value
    # and a corresponding function in this dictionary
    ret_val_func_mapping = {
        'driver': get_driver,
        'version': get_server_version,
        'databases': get_databases,
        'users': get_users,
        'roles': get_roles,
        'settings': get_settings,
        'clusters': get_clusters,
    }
    # Check if the limit is provided, it contains correct values
    limit = module.params['limit']
    if limit:
        limit = handle_limit_values(module, ret_val_func_mapping.keys(), limit)
    else:
        # If no limit, just gather all ret values
        limit = ret_val_func_mapping.keys()

    # Will fail if no driver informing the user
    check_driver(module)

    # Connect to DB
    client = connect_to_db_via_client(module, main_conn_kwargs, client_kwargs)

    # Get server information
    srv_info = {}
    for item in limit:
        # This will invoke a proper function
        srv_info[item] = ret_val_func_mapping[item](module, client)

    # Close connection
    client.disconnect_connection()

    # Users will get this in JSON output after execution
    module.exit_json(changed=False, **srv_info)


if __name__ == '__main__':
    main()
