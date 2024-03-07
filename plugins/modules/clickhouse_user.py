#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright: (c) 2024, Aleksandr Vagachev (@aleksvagachev) <aleksvagachev@yandex.ru>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

DOCUMENTATION = r'''
---
module: clickhouse_user

short_description: Creates or removes a ClickHouse user using the clickhouse-driver Client interface

description:
  - Creates or removes a ClickHouse user using the
    L(clickhouse-driver,https://clickhouse-driver.readthedocs.io/en/latest) Client interface.
  - The module can only create and delete users, without any additional parameters.
    New features will be added in the future.

attributes:
  check_mode:
    description: Supports check_mode.
    support: full


author:
  - Aleksandr Vagachev (@aleksvagachev)

extends_documentation_fragment:
  - community.clickhouse.client_inst_opts

options:
  state:
    description:
      - Database state.
      - If C(present), will create the user if not exists.
      - If C(absent), will drop the user if exists.
    type: str
    choices: ['present', 'absent']
    default: 'present'
  name:
    description:
      - User name to add or remove.
    type: str
    required: true
  password:
    description:
      - Set the user's password.
      - Password can be passed unhashed or hashed.
    type: str
  type_password:
    description:
      - The type of password being transmitted(plaintext_password, sha256_password, sha256_hash...).
      - For more details, see U(https://clickhouse.com/docs/en/sql-reference/statements/create/user).
    type: str
    default: sha256_password
  cluster:
    description:
      - Run the command on all cluster hosts.
      - If the cluster is not configured, the command will crash with an error.
    type: str
'''

EXAMPLES = r'''
- name: Create user
  community.clickhouse.clickhouse_user:
    login_host: localhost
    login_user: alice
    login_db: foo
    login_password: my_password
    user: test_user
    password: qwerty
    type_password: sha256_password

- name: Create user
  community.clickhouse.clickhouse_user:
    login_host: localhost
    login_user: alice
    login_db: foo
    login_password: my_password
    user: test_user
    password: 9e69e7e29351ad837503c44a5971edebc9b7e6d8601c89c284b1b59bf37afa80
    type_password: sha256_hash
    cluster: test_cluster
    state: present

- name: Drop user
  community.clickhouse.clickhouse_user:
    login_host: localhost
    login_user: alice
    login_db: foo
    login_password: my_password
    user: test_user
    state: absent
'''

RETURN = r'''
executed_statements:
  description:
  - Data-modifying executed statements.
  returned: on success
  type: list
  sample: ["CREATE USER test_user IDENTIFIED WITH ***** BY '*****'"]
'''

from ansible.module_utils.basic import AnsibleModule

from ansible_collections.community.clickhouse.plugins.module_utils.clickhouse import (
    check_clickhouse_driver,
    client_common_argument_spec,
    connect_to_db_via_client,
    execute_query,
    get_main_conn_kwargs,
)

PRIV_ERR_CODE = 497
executed_statements = []


class ClickHouseUser():
    def __init__(self, module, client, user, password, type_password, cluster):
        self.module = module
        self.client = client
        self.user = user
        self.password = password
        self.type_password = type_password
        self.cluster = cluster
        # Set default values, then update
        self.user_exists = False
        self.__populate_info()

    def __populate_info(self):
        # Collecting user information
        query = ("SELECT name, storage, auth_type "
                 "FROM system.users "
                 "WHERE name = '%s'" % self.user)

        result = execute_query(self.module, self.client, query)

        if result == PRIV_ERR_CODE:
            login_user = self.module.params['login_user']
            msg = "Not enough privileges for user: %s" % login_user
            self.module.fail_json(msg=msg)

        if result != []:
            self.user_exists = True

    def create(self):
        query = "CREATE USER %s" % self.user

        if self.password is not None:
            query += (" IDENTIFIED WITH %s"
                      " BY '%s'") % (self.type_password, self.password)

        if self.cluster:
            query += " ON CLUSTER %s" % self.cluster

        executed_statements.append(query)

        if not self.module.check_mode:
            execute_query(self.module, self.client, query)

        return True

    def drop(self):
        query = "DROP USER %s" % self.user
        if self.cluster:
            query += " ON CLUSTER %s" % self.cluster

        executed_statements.append(query)

        if not self.module.check_mode:
            execute_query(self.module, self.client, query)

        return True


def main():
    argument_spec = client_common_argument_spec()
    argument_spec.update(
        state=dict(type='str', choices=['present', 'absent'], default='present'),
        user=dict(type='str', required=True),
        password=dict(type='str', default=None, no_log=True),
        type_password=dict(type='str', default='sha256_password', no_log=True),
        cluster=dict(type='str', default=None),
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
    user = module.params['user']
    password = module.params["password"]
    type_password = module.params["type_password"]
    cluster = module.params['cluster']

    # Will fail if no driver informing the user
    check_clickhouse_driver(module)

    # Connect to DB
    client = connect_to_db_via_client(module, main_conn_kwargs, client_kwargs)

    # Do the job
    changed = False
    user = ClickHouseUser(module, client, user, password,
                          type_password, cluster)

    if state == 'present':
        if not user.user_exists:
            changed = user.create()
        else:
            # If user exists
            pass
    else:
        # If state is absent
        if user.user_exists:
            changed = user.drop()

    # Close connection
    client.disconnect_connection()

    # Users will get this in JSON output after execution
    module.exit_json(changed=changed, executed_statements=executed_statements)


if __name__ == '__main__':
    main()
