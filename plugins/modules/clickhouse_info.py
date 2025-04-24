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
storage_policies:
  description:
    - The content of the system.storage_policies table with storage_policies names as keys.
  returned: success
  type: dict
  sample: { "storage_policies": "..." }
  version_added: '0.4.0'
grants:
  description:
    - The content of the system.grants table for users and roles as keys.
  returned: success
  type: dict
  sample: { "roles": {"..."}, "users": {"..."} }
  version_added: '0.7.0'
settings_profile_elements:
  description:
    - The content of the system.settings_profile_elements table for users, roles, profiles as keys.
  returned: success
  type: dict
  sample: { "roles": {"..."}, "users": {"..."}, "profiles": {"..."} }
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
    query = ("SELECT name, engine, data_path, metadata_path, uuid, "
             "engine_full, comment FROM system.databases")
    result = execute_query(module, client, query)

    if result == PRIV_ERR_CODE:
        return {str(PRIV_ERR_CODE): "Not enough privileges"}

    db_info = {}
    for row in result:
        db_info[row[0]] = {
            "engine": row[1],
            "data_path": row[2],
            "metadata_path": row[3],
            "uuid": str(row[4]),
            "engine_full": row[5],
            "comment": row[6],
        }

    return db_info


def get_clusters(module, client):
    """Get clusters.

    Returns a list with clusters names as top level keys.
    """
    query = ("SELECT cluster, shard_num, shard_weight, replica_num, host_name, "
             "host_address, port, is_local, user, default_database, errors_count, "
             "slowdowns_count, estimated_recovery_time FROM system.clusters")
    result = execute_query(module, client, query)

    if result == PRIV_ERR_CODE:
        return {str(PRIV_ERR_CODE): "Not enough privileges"}

    cluster_info = {}

    for row in result:
        cluster = row[0]
        shard_num = str(row[1])
        shard_weight = row[2]
        replica_num = str(row[3])
        host_name = row[4]
        host_address = row[5]
        port = row[6]
        is_local = row[7]
        user = row[8]
        default_database = row[9]
        errors_count = row[10]
        slowdowns_count = row[11]
        estimated_recovery_time = row[12]

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
                "slowdowns_count": slowdowns_count,
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
        return {str(PRIV_ERR_CODE): "Not enough privileges"}

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
             "dependencies_table, create_table_query, engine_full, as_select, partition_key, "
             "sorting_key, primary_key, sampling_key, storage_policy, total_rows, total_bytes, "
             "parts, active_parts, total_marks, lifetime_rows, lifetime_bytes, comment, "
             "has_own_data, loading_dependencies_database, loading_dependencies_table, "
             "loading_dependent_database, loading_dependent_table FROM system.tables")
    result = execute_query(module, client, query)

    if result == PRIV_ERR_CODE:
        return {str(PRIV_ERR_CODE): "Not enough privileges"}

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
            "as_select": row[12],
            "partition_key": row[13],
            "sorting_key": row[14],
            "primary_key": row[15],
            "sampling_key": row[16],
            "storage_policy": row[17],
            "total_rows": row[18],
            "total_bytes": row[19],
            "parts": row[20],
            "active_parts": row[21],
            "total_marks": row[22],
            "lifetime_rows": row[23],
            "lifetime_bytes": row[24],
            "comment": row[25],
            "has_own_data": row[26],
            "loading_dependencies_database": row[27],
            "loading_dependencies_table": row[28],
            "loading_dependent_database": row[29],
            "loading_dependent_table": row[30],
        }

    return tables_info


def get_dictionaries(module, client):
    """Get dictionaries.

    Returns a dictionary with databases name as dictionary,
    and the name of the 'dictionary' in this dictionary is the key.
    """
    query = ("SELECT database, name, uuid, status, origin, key.names, key.types, "
             "attribute.names, attribute.types, bytes_allocated, "
             "hierarchical_index_bytes_allocated, query_count, hit_rate, found_rate, "
             "element_count, load_factor, source, lifetime_min, "
             "lifetime_max, loading_start_time, last_successful_update_time, "
             "loading_duration, last_exception, comment FROM system.dictionaries")
    result = execute_query(module, client, query)

    if result == PRIV_ERR_CODE:
        return {str(PRIV_ERR_CODE): "Not enough privileges"}

    dictionaries_info = {}
    for row in result:
        dict_database = row[0] if row[0] else 'dict'
        if dict_database not in dictionaries_info:
            dictionaries_info[dict_database] = {}
        dictionaries_info[dict_database][row[1]] = {
            "uuid": str(row[2]),
            "status": row[3],
            "origin": row[4],
            "key.names": row[5],
            "key.types": row[6],
            "attribute.names": row[7],
            "attribute.types": row[8],
            "bytes_allocated": row[9],
            "hierarchical_index_bytes_allocated": row[10],
            "query_count": row[11],
            "hit_rate": row[12],
            "found_rate": row[13],
            "element_count": row[14],
            "load_factor": row[15],
            "source": row[16],
            "lifetime_min": row[17],
            "lifetime_max": row[18],
            "loading_start_time": row[19],
            "last_successful_update_time": row[20],
            "loading_duration": row[21],
            "last_exception": row[22],
        }

    return dictionaries_info


def get_settings(module, client):
    """Get settings.

    Returns a dictionary with settings names as keys.
    """
    query = ("SELECT name, value, changed, description, min, max, readonly, "
             "type, default, alias_for FROM system.settings")
    result = execute_query(module, client, query)

    if result == PRIV_ERR_CODE:
        return {str(PRIV_ERR_CODE): "Not enough privileges"}

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
            "default": row[8],
            "alias_for": row[9],
        }

    return settings_info


def get_merge_tree_settings(module, client):
    """Get merge_tree_settings.

    Returns a dictionary with merge_tree_settings names as keys.
    """
    query = ("SELECT name, value, changed, description, min, max, "
             "readonly, type FROM system.merge_tree_settings")
    result = execute_query(module, client, query)

    if result == PRIV_ERR_CODE:
        return {str(PRIV_ERR_CODE): "Not enough privileges"}

    merge_tree_settings_info = {}
    for row in result:
        merge_tree_settings_info[row[0]] = {
            "value": row[1],
            "changed": row[2],
            "description": row[3],
            "min": row[4],
            "max": row[5],
            "readonly": row[6],
            "type": row[7],
        }

    return merge_tree_settings_info


def get_users(module, client):
    """Get users.

    Returns a dictionary with users names as keys.
    """
    query = ("SELECT name, id, storage, auth_type, auth_params, host_ip, host_names, "
             "host_names_regexp, host_names_like, default_roles_all, "
             "default_roles_list, default_roles_except, grantees_any, "
             "grantees_list, grantees_except, default_database FROM system.users")
    result = execute_query(module, client, query)

    if result == PRIV_ERR_CODE:
        return {str(PRIV_ERR_CODE): "Not enough privileges"}

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
            "grantees_any": row[12],
            "grantees_list": row[13],
            "grantees_except": row[14],
            "default_database": row[15],

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
        return {str(PRIV_ERR_CODE): "Not enough privileges"}

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
        return {str(PRIV_ERR_CODE): "Not enough privileges"}

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
        return {str(PRIV_ERR_CODE): "Not enough privileges"}

    grants_info = {
        'users': {},
        'roles': {},
    }

    for row in result:
        if row[0] is not None:
            dict_name = 'users'
            name = row[0]
            if row[0] not in grants_info[dict_name]:
                grants_info[dict_name][name] = []
        else:
            dict_name = 'roles'
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


def get_settings_profile_elements(module, client):
    """Get settings_profile_elements.

    Returns a dictionary with roles, profiles and users names as keys.
    """
    query = ("SELECT profile_name, user_name, role_name, "
             "index, setting_name, value, min, max, writability, "
             "inherit_profile FROM system.settings_profile_elements")
    result = execute_query(module, client, query)

    if result == PRIV_ERR_CODE:
        return {str(PRIV_ERR_CODE): "Not enough privileges"}

    settings_profile_elements = {'profiles': {},
                                 'users': {},
                                 'roles': {},
                                 }

    for row in result:
        if row[0] is not None:
            dict_name = 'profiles'
            name = row[0]
            if row[0] not in settings_profile_elements[dict_name]:
                settings_profile_elements[dict_name][name] = []
        elif row[1] is not None:
            dict_name = 'users'
            name = row[1]
            if row[1] not in settings_profile_elements[dict_name]:
                settings_profile_elements[dict_name][name] = []
        else:
            dict_name = 'roles'
            name = row[2]
            if row[2] not in settings_profile_elements[dict_name]:
                settings_profile_elements[dict_name][name] = []

        settings_profile_elements[dict_name][name].append({
            "index": row[3],
            "setting_name": row[4],
            "value": row[5],
            "min": row[6],
            "max": row[7],
            "writability": row[8],
            "inherit_profile": row[9],
        })

    return settings_profile_elements


def get_storage_policies(module, client):
    """Get storage_policies.

    Returns a dictionary with storage_policies names as keys.
    """
    query = ("SELECT policy_name, volume_name, volume_priority, "
             "disks, volume_type, max_data_part_size, "
             "move_factor, prefer_not_to_merge FROM system.storage_policies")
    result = execute_query(module, client, query)

    if result == PRIV_ERR_CODE:
        return {str(PRIV_ERR_CODE): "Not enough privileges"}

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
        'settings_profile_elements': get_settings_profile_elements,
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
