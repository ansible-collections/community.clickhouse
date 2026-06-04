# This code is part of Ansible, but is an independent component.
# This particular file snippet, and this file snippet only, is BSD licensed.
# Modules you write using this snippet, which is embedded dynamically by Ansible
# still belong to the author of the module, and may assign their own license
# to the complete work.
#
# Simplified BSD License (see simplified_bsd.txt or https://opensource.org/licenses/BSD-2-Clause)

from __future__ import absolute_import, division, print_function

__metaclass__ = type

import re

from ansible.module_utils.basic import missing_required_lib
from ansible.module_utils.common.text.converters import to_native

Client = None
try:
    from clickhouse_driver import Client
    from clickhouse_driver.errors import ServerException
    from clickhouse_driver import __version__ as driver_version
    HAS_DB_DRIVER = True
except ImportError:
    HAS_DB_DRIVER = False

VALID_IDENTIFIER_PATTERN = re.compile(r'^[^`\\]+$')


def client_common_argument_spec():
    """
    Return a dictionary with connection options.

    The options are commonly used by many modules.
    """
    return dict(
        login_host=dict(type='str', default='localhost'),
        login_port=dict(type='int', default=None),
        login_db=dict(type='str', default=None),
        login_user=dict(type='str', default=None),
        login_password=dict(type='str', default=None, no_log=True),
        client_kwargs=dict(type='dict', default={}),
        success_on=dict(type='list', elements='int', default=[497]),
    )


def cluster_argument_spec():
    return dict(
        cluster=dict(type='str', default=None),
    )


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


def check_clickhouse_driver(module):
    """Checks if the driver is present.

    Informs user if no driver and fails.
    """
    if not HAS_DB_DRIVER:
        module.fail_json(msg=missing_required_lib('clickhouse_driver'))


def version_clickhouse_driver():
    """
    Returns the current version of clickhouse_driver.
    """
    return driver_version


def connect_to_db_via_client(module, main_conn_kwargs, client_kwargs):
    """Connects to DB using the Client() class.

    Returns Client() object.
    """
    try:
        # Merge the kwargs as Python 2 would throw an error
        # when unpacking them separately to Client()
        client_kwargs.update(main_conn_kwargs)
        client = Client(**client_kwargs)
        client.connection.connect()
    except Exception as e:
        module.fail_json(msg="Failed to connect to database: %s" % to_native(e))

    # Display warning about using unsuporrted server version.
    server_version = get_server_version(module, client)
    if server_version['year'] < 24 or server_version['year'] == 24 and server_version['feature'] < 8:
        module.warn("Used server version is not activately maintained with this collection. Some features may not work properly.")
    return client


def execute_query(module, client, query, execute_kwargs=None, set_settings=None, custom_message=None):
    """Execute query.

    Returns rows returned in response.

    set_settings - The list of settings that need to be set before executing the request.
    """
    # Some modules do not pass this argument
    if execute_kwargs is None:
        execute_kwargs = {}

    if set_settings is None:
        set_settings = {}

    try:
        if len(set_settings) != 0:
            for setting in set_settings:
                client.execute("SET %s = '%s'" % (setting, set_settings[setting]))
        result = client.execute(query, **execute_kwargs)
    except ServerException as e:
        if e.code in module.params['success_on']:
            module.exit_json(changed=False, msg="Code %i defined as success." % e.code, custom_message=custom_message)
        module.fail_json(
            msg="Failed to execute query.",
            exception=to_native(e),
            code=e.code,
            message=e.message,
            query=query,
            custom_message=custom_message
        )
    return result


def get_server_version(module, client):
    """Get server version.

    Returns a dictionary with server version.
    """
    result = client.connection.server_info.version_tuple()

    version = {}

    version["year"] = result[0]
    version["feature"] = result[1]
    version["maintenance"] = result[2]

    return version


def get_on_cluster_clause(module, cluster):
    if not cluster:
        return ""
    validate_identifier(module, cluster, "cluster name")
    return f" ON CLUSTER `{cluster}`"


def validate_identifier(module, name, context="identifier"):
    if not name:
        module.fail_json(msg=f"{context.capitalize()} cannot be empty")
    elif not VALID_IDENTIFIER_PATTERN.match(name):
        module.fail_json(msg=f"Invalid {context}: '{name}'")
    return name


def normalize_db_table(module, client, database, table):
    """We want to make sure target is correct.
    When passed without db, default for session will apply and it can break idempotency.
    Ex:
        db, table → `db`.`table`
        None, table    → `default`.`table`
        db, *     → `db`.*
        db, None → `db`.*
    """
    if not database:
        query = "SELECT currentDatabase()"
        database = execute_query(module, client, query)[0][0]
    else:
        validate_identifier(module, database)
    if not table or table == '*':
        return f"`{database}`.*"
    else:
        validate_identifier(module, table)
    return f"`{database}`.`{table}`"
