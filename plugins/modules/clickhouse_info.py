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

version_added: '0.1.0'

author:
  - Andrew Klychkov (@Andersson007)
  - Aleksandr Vagachev (@aleksvagachev)

extends_documentation_fragment:
  - community.clickhouse.client_inst_opts

options:
  limit:
    description:
      - Limits a set of return values you want to get.
      - See the Return section for acceptable values.
      - If not specified, returns all supported return values.
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
    limit:
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
tables:
  description:
    - The content of the system.tables table with the table names as keys.
  returned: success
  type: dict
  sample: { "system": { "settings": "..." } }
  version_added: '0.3.0'
merge_tree_settings:
  description:
    - The content of the system.merge_tree_settings table with names as keys.
  returned: success
  type: dict
  sample: { "merge_max_block_size": "..." }
  version_added: '0.3.0'
dictionaries:
  description:
    - The content of the system.dictionaries table with dictionary names as keys.
  returned: success
  type: dict
  sample: { "database": { "dictionary": "..." } }
  version_added: '0.3.0'
quotas:
  description:
    - The content of the system.quotas table with quota names as keys.
  returned: success
  type: dict
  sample: { "default": "..." }
  version_added: '0.4.0'
settings_profiles:
  description:
    - The content of the system.settings_profiles table with profile names as keys.
  returned: success
  type: dict
  sample: { "readonly": "..." }
  version_added: '0.4.0'
functions:
  description:
    - The content of the system.functions table with function names as keys.
    - Works only for clickhouse-server versions >= 22.
    - Does not output functions on the 'System' origin.
  returned: success
  type: dict
  sample: { "test_function": "..." }
  version_added: '0.4.0'
storage_policies:
  description:
    - The content of the system.storage_policies table with storage_policies names as keys.
  returned: success
  type: dict
  sample: { "storage_policies": "..." }
  version_added: '0.4.0'
grants:
  description:
    - The content of the system.grants table with user_name and role_name names as keys.
  returned: success
  type: dict
  sample: { "role_name": {"..."}, "user_name": {"..."} }
  version_added: '0.7.0'
'''

from ansible.module_utils.basic import AnsibleModule

from ansible_collections.community.clickhouse.plugins.module_utils.clickhouse import (
    check_clickhouse_driver,
    client_common_argument_spec,
    connect_to_db_via_client,
    execute_query,
    get_main_conn_kwargs,
    get_server_version,
    version_clickhouse_driver,
)


PRIV_ERR_CODE = 497


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
        role_name = row[0]
        roles_info[role_name] = {
            "id": str(row[1]),
            "storage": row[2],
        }
        roles_info[role_name]["grants"] = get_grants(module, client, role_name)

    return roles_info


def get_tables(module, client):
    """Get tables.

    Returns a dictionary with databases name as dictionary,
    and the name of the table in this dictionary is the key.
    """
    query = ("SELECT database, name, uuid, engine, is_temporary, data_paths, "
             "metadata_path, metadata_modification_time, dependencies_database, "
             "dependencies_table, create_table_query, engine_full, partition_key, "
             "sorting_key, primary_key, sampling_key, storage_policy, total_rows, total_bytes, "
             "lifetime_rows, lifetime_bytes FROM system.tables")
    result = execute_query(module, client, query)

    if result == PRIV_ERR_CODE:
        return {PRIV_ERR_CODE: "Not enough privileges"}

    tables_info = {}
    for row in result:
        if row[0] not in tables_info:
            tables_info[row[0]] = {}
        tables_info[row[0]][row[1]] = {
            "uuid": str(row[2]),
            "engine": row[3],
            "is_temporary": row[4],
            "data_paths": row[5],
            "metadata_path": row[6],
            "metadata_modification_time": row[7],
            "dependencies_database": row[8],
            "dependencies_table": row[9],
            "create_table_query": row[10],
            "engine_full": row[11],
            "partition_key": row[12],
            "sorting_key": row[13],
            "primary_key": row[14],
            "sampling_key": row[15],
            "storage_policy": row[16],
            "total_rows": row[17],
            "total_bytes": row[18],
            "lifetime_rows": row[19],
            "lifetime_bytes": row[20],
        }

    return tables_info


def get_dictionaries(module, client):
    """Get dictionaries.

    Returns a dictionary with databases name as dictionary,
    and the name of the 'dictionary' in this dictionary is the key.
    """
    query = ("SELECT database, name, uuid, status, origin, type, key, "
             "attribute.names, attribute.types, bytes_allocated, query_count, "
             "hit_rate, element_count, load_factor, source, lifetime_min, "
             "lifetime_max, loading_start_time, last_successful_update_time, "
             "loading_duration, last_exception FROM system.dictionaries")
    result = execute_query(module, client, query)

    if result == PRIV_ERR_CODE:
        return {PRIV_ERR_CODE: "Not enough privileges"}

    dictionaries_info = {}
    for row in result:
        dict_database = row[0] if row[0] else 'dict'
        if dict_database not in dictionaries_info:
            dictionaries_info[dict_database] = {}
        dictionaries_info[dict_database][row[1]] = {
            "uuid": str(row[2]),
            "status": row[3],
            "origin": row[4],
            "type": row[5],
            "key": row[6],
            "attribute.names": row[7],
            "attribute.types": row[8],
            "bytes_allocated": row[9],
            "query_count": row[10],
            "hit_rate": row[11],
            "element_count": row[12],
            "load_factor": row[13],
            "source": row[14],
            "lifetime_min": row[15],
            "lifetime_max": row[16],
            "loading_start_time": row[17],
            "last_successful_update_time": row[18],
            "loading_duration": row[19],
            "last_exception": row[20],
        }

    return dictionaries_info


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


def get_merge_tree_settings(module, client):
    """Get merge_tree_settings.

    Returns a dictionary with merge_tree_settings names as keys.
    """
    query = ("SELECT name, value, changed, description, "
             "type FROM system.merge_tree_settings")
    result = execute_query(module, client, query)

    if result == PRIV_ERR_CODE:
        return {PRIV_ERR_CODE: "Not enough privileges"}

    merge_tree_settings_info = {}
    for row in result:
        merge_tree_settings_info[row[0]] = {
            "value": row[1],
            "changed": row[2],
            "description": row[3],
            "type": row[4],
        }

    return merge_tree_settings_info


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
        user_name = row[0]
        user_info[user_name] = {
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

        user_info[user_name]["roles"] = get_user_roles(module, client, user_name)
        user_info[user_name]["grants"] = get_grants(module, client, user_name)

    return user_info


def get_grants(module, client, name):
    """Get grants.

    Return a list of grants.
    """
    query = ("SHOW GRANTS FOR '%s'" % name)
    result = execute_query(module, client, query)
    return [row[0] for row in result]


def get_user_roles(module, client, user_name):
    """Get user roles.

    Returns a list of roles.
    """
    query = ("SELECT granted_role_name FROM system.role_grants "
             "WHERE user_name = '%s'" % user_name)
    result = execute_query(module, client, query)
    return [row[0] for row in result]


def get_settings_profiles(module, client):
    """Get settings profiles.

    Returns a dictionary with profile names as keys.
    """
    query = ("SELECT name, id, storage, num_elements, apply_to_all, apply_to_list, "
             "apply_to_except FROM system.settings_profiles")
    result = execute_query(module, client, query)

    if result == PRIV_ERR_CODE:
        return {PRIV_ERR_CODE: "Not enough privileges"}

    profile_info = {}
    for row in result:
        profile_info[row[0]] = {
            "id": str(row[1]),
            "storage": row[2],
            "num_elements": row[3],
            "apply_to_all": row[4],
            "apply_to_list": row[5],
            "apply_to_except": row[6],
        }

    return profile_info


def get_quotas(module, client):
    """Get quotas.

    Returns a dictionary with quota names as keys.
    """
    query = ("SELECT name, id, storage, keys, durations, apply_to_all, "
             "apply_to_list, apply_to_except FROM system.quotas")
    result = execute_query(module, client, query)

    if result == PRIV_ERR_CODE:
        return {PRIV_ERR_CODE: "Not enough privileges"}

    quota_info = {}
    for row in result:
        quota_info[row[0]] = {
            "id": str(row[1]),
            "storage": row[2],
            "keys": row[3],
            "durations": row[4],
            "apply_to_all": row[5],
            "apply_to_list": row[6],
            "apply_to_except": row[7],
        }

    return quota_info


def get_all_grants(module, client):
    """Get grants.

    Returns a dictionary with users and roles names as keys.
    """
    query = ("SELECT user_name, role_name, access_type, database, "
             "table, column, is_partial_revoke, grant_option FROM system.grants")

    result = execute_query(module, client, query)

    if result == PRIV_ERR_CODE:
        return {PRIV_ERR_CODE: "Not enough privileges"}

    grants_info = {'user_name': {},
                   'role_name': {},
                   }

    for row in result:
        if row[0] is not None:
            dict_name = 'user_name'
            name = row[0]
            if row[0] not in grants_info[dict_name]:
                grants_info[dict_name][name] = []
        else:
            dict_name = 'role_name'
            name = row[1]
            if row[1] not in grants_info[dict_name]:
                grants_info[dict_name][name] = []

        grants_info[dict_name][name].append({
            "access_type": row[2],
            "database": row[3],
            "table": row[4],
            "column": row[5],
            "is_partial_revoke": row[6],
            "grant_option": row[7],
        })

    return grants_info


def get_functions(module, client):
    """Get functions.

    Returns a dictionary with function names as keys.
    """
    srv_version = get_server_version(module, client)
    function_info = {}
    if srv_version['year'] >= 22:
        query = ("SELECT name, is_aggregate, case_insensitive, alias_to, "
                 "create_query, origin FROM system.functions "
                 "WHERE origin != 'System'")
        result = execute_query(module, client, query)

        if result == PRIV_ERR_CODE:
            return {PRIV_ERR_CODE: "Not enough privileges"}

        for row in result:
            function_info[row[0]] = {
                "is_aggregate": str(row[1]),
                "case_insensitive": row[2],
                "alias_to": row[3],
                "create_query": row[4],
                "origin": row[5],
            }

    return function_info


def get_storage_policies(module, client):
    """Get storage_policies.

    Returns a dictionary with storage_policies names as keys.
    """
    query = ("SELECT policy_name, volume_name, volume_priority, "
             "disks, volume_type, max_data_part_size, "
             "move_factor, prefer_not_to_merge FROM system.storage_policies")
    result = execute_query(module, client, query)

    if result == PRIV_ERR_CODE:
        return {PRIV_ERR_CODE: "Not enough privileges"}

    storage_policies_info = {}
    for row in result:
        storage_policies_info[row[0]] = {
            "volume_name": row[1],
            "volume_priority": row[2],
            "disks": row[3],
            "volume_type": row[4],
            "max_data_part_size": row[5],
            "move_factor": row[6],
            "prefer_not_to_merge": row[7],
        }

    return storage_policies_info


def get_driver(module, client):
    """Gets driver information.

    The module and client arguments are here
    to provide a common interface and are ignored.

    Returns its version for now.
    """
    return {"version": version_clickhouse_driver()}


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
    argument_spec = client_common_argument_spec()
    argument_spec.update(
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
        'dictionaries': get_dictionaries,
        'tables': get_tables,
        'merge_tree_settings': get_merge_tree_settings,
        'quotas': get_quotas,
        'settings_profiles': get_settings_profiles,
        'functions': get_functions,
        'storage_policies': get_storage_policies,
        'grants': get_all_grants,
    }
    # Check if the limit is provided, it contains correct values
    limit = module.params['limit']
    if limit:
        limit = handle_limit_values(module, ret_val_func_mapping.keys(), limit)
    else:
        # If no limit, just gather all ret values
        limit = ret_val_func_mapping.keys()

    # Will fail if no driver informing the user
    check_clickhouse_driver(module)

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
