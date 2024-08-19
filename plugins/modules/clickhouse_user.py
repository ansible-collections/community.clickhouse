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
  - Andrew Klychkov (@Andersson007)

extends_documentation_fragment:
  - community.clickhouse.client_inst_opts

version_added: '0.4.0'

options:
  state:
    description:
      - User state.
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
  update_password:
    description:
      - If C(on_create), will set the password only for newly created users.
        If the user already exists, a C(password) value will be ignored.
      - If C(always), will always update the password.
        This option is not idempotent and will update the password even
        if it is the same in the database. If in future ClickHouse will allow
        to retrieve password hashes and other necessary details, this behavior
        will be changed.
    type: str
    choices: [always, on_create]
    default: on_create
  settings:
    description:
      - Settings with their constraints applied by default at user login.
      - You can also specify the profile from which the settings will be inherited.
    type: list
    elements: str
    version_added: '0.5.0'
  roles:
    description:
      - Grants specified roles for the user.
      - To append specified roles to existing ones, also add I(append=true) to your task.
    type: list
    elements: str
    version_added: '0.6.0'
  default_roles:
    description:
      - Sets specified roles as default for the user.
      - The roles must be explicitly granted to the user whether manually
        before using this argument or by using the I(roles)
        argument in the same task.
      - To append specified roles to existing ones, also add I(append_roles=true) to your task.
    type: list
    elements: str
    version_added: '0.6.0'
  append_roles:
    description:
     - When set to C(true), appends roles specified in I(roles) to existing
       user roles instead of removing the user from not specified roles.
     - The default is C(false), which will remove the user from all not specified roles.
     - Requires I(roles) to be set in the task.
    type: bool
    default: false
    version_added: '0.6.0'
  append_default_roles:
    description:
     - When set to C(true), appends roles specified in I(default_roles) to existing
       default roles instead of unsetting not specified ones.
     - The default is C(false), which will unset all not specified roles.
     - Requires I(default_roles) to be set in the task.
    type: bool
    default: false
    version_added: '0.6.0'
'''

EXAMPLES = r'''
- name: Create user granting roles and setting default role
  community.clickhouse.clickhouse_user:
    login_host: localhost
    login_user: alice
    login_db: foo
    login_password: my_password
    name: test_user
    password: qwerty
    type_password: sha256_password
    roles:
    - accountant
    - manager
    default_roles:
    - accountant
    append_roles: true

- name: Append the sales role to alice's roles
  community.clickhouse.clickhouse_user:
    login_host: localhost
    login_user: alice
    login_db: foo
    login_password: my_password
    name: test_user
    roles:
    - sales
    append_roles: true

- name: If user exists, update password
  community.clickhouse.clickhouse_user:
    login_host: localhost
    login_user: alice
    login_db: foo
    login_password: my_password
    name: test_user
    password: qwerty123
    update_password: always

- name: Create user with specific settings
  community.clickhouse.clickhouse_user:
    login_host: localhost
    login_user: alice
    login_db: foo
    login_password: my_password
    name: test_user
    password: 9e69e7e29351ad837503c44a5971edebc9b7e6d8601c89c284b1b59bf37afa80
    type_password: sha256_hash
    cluster: test_cluster
    settings:
      - max_memory_usage = 15000 MIN 15000 MAX 16000 READONLY
      - PROFILE 'restricted'
    state: present

- name: Drop user
  community.clickhouse.clickhouse_user:
    login_host: localhost
    login_user: alice
    login_db: foo
    login_password: my_password
    name: test_user
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
    def __init__(self, module, client, name, password, type_password, cluster):
        self.changed = False
        self.module = module
        self.client = client
        self.name = name
        self.password = password
        self.type_password = type_password
        self.cluster = cluster
        # Set default values, then update
        self.user_exists = False
        self.current_default_roles = []
        self.current_roles = []
        # Fetch actual values from DB and
        # update the attributes with them
        self.__populate_info()

    def __populate_info(self):
        # Collecting user information
        query = ("SELECT name, storage, auth_type, default_roles_list "
                 "FROM system.users "
                 "WHERE name = '%s'" % self.name)

        result = execute_query(self.module, self.client, query)

        if result == PRIV_ERR_CODE:
            login_user = self.module.params['login_user']
            msg = "Not enough privileges for user: %s" % login_user
            self.module.fail_json(msg=msg)

        if result != []:
            self.user_exists = True
            self.current_default_roles = result[0][3]

        if self.user_exists:
            self.current_roles = self.__fetch_user_groups()

    def __fetch_user_groups(self):
        query = ("SELECT granted_role_name FROM system.role_grants "
                 "WHERE user_name = '%s'" % self.name)
        result = execute_query(self.module, self.client, query)
        return [row[0] for row in result]

    def create(self):
        list_settings = self.module.params['settings']
        query = "CREATE USER %s" % self.name

        if self.password is not None:
            query += (" IDENTIFIED WITH %s"
                      " BY '%s'") % (self.type_password, self.password)

        if self.cluster:
            query += " ON CLUSTER %s" % self.cluster

        if list_settings:
            query += " SETTINGS"
            for index, value in enumerate(list_settings):
                query += " %s" % value
                if index < len(list_settings) - 1:
                    query += ","

        executed_statements.append(query)

        if not self.module.check_mode:
            execute_query(self.module, self.client, query)

        if self.module.params['roles']:
            self.__grant_role(self.module.params['roles'])

        if self.module.params['default_roles']:
            self.__set_default_roles(self.module.params['default_roles'])

        return True

    def update(self, update_password):
        if self.module.params['roles']:
            desired_roles = self.module.params['roles']

            roles_to_grant = []
            for role in desired_roles:
                if role not in self.current_roles:
                    roles_to_grant.append(role)

            if roles_to_grant:
                self.__grant_roles(roles_to_grant)

            if not self.module.params['append_roles']:
                roles_to_revoke = []
                for role in self.current_roles:
                    if role not in self.desired_roles:
                        roles_to_revoke.append(role)

                if roles_to_revoke:
                    self.__revoke_roles(roles_to_revoke)

        if self.module.params['default_roles']:
            default_roles = self.module.params['default_roles']

            if self.module.params['append_roles']:
                roles_to_set = []
                for role in default_roles:
                    if role not in self.current_default_roles:
                        roles_to_set.append(role)

                if roles_to_set:
                    self.__set_default_roles(roles_to_set)

            elif not self.module.params['append_roles']:
                # Use sets to make a list of unique roles
                set1 = set(self.current_default_roles)
                set2 = set(default_roles)
                roles_to_set = list(set1.union(set2))
                self.__set_default_roles(roles_to_set)

        if update_password == 'on_create':
            return False or self.changed

        # If update_password is always
        # TODO: When ClickHouse will allow to retrieve password hashes,
        # make this idempotent, i.e. execute this only if the passwords don't match
        query = ("ALTER USER %s IDENTIFIED WITH %s "
                 "BY '%s'") % (self.name, self.type_password, self.password)

        executed_statements.append(query)

        if not self.module.check_mode:
            execute_query(self.module, self.client, query)

        return True

    def drop(self):
        query = "DROP USER %s" % self.name
        if self.cluster:
            query += " ON CLUSTER %s" % self.cluster

        executed_statements.append(query)

        if not self.module.check_mode:
            execute_query(self.module, self.client, query)

        return True

    def __grant_roles(self, roles_to_set):
        query = "GRANT %s TO %s" % (', '.join(roles_to_set), self.name)
        executed_statements.append(query)

        if not self.module.check_mode:
            execute_query(self.module, self.client, query)

        self.changed = True

    def __revoke_roles(self, roles_to_revoke):
        query = "REVOKE %s FROM %s" % (' ,'.join(roles_to_revoke), self.name)
        executed_statements.append(query)

        if not self.module.check_mode:
            execute_query(self.module, self.client, query)

        self.changed = True

    def __set_default_roles(self, roles_to_set):
        query = "ALTER USER %s DEFAULT ROLE %s" % (self.name, ', '.join(roles_to_set))
        executed_statements.append(query)

        if not self.module.check_mode:
            execute_query(self.module, self.client, query)

        self.changed = True


def main():
    argument_spec = client_common_argument_spec()
    argument_spec.update(
        state=dict(type='str', choices=['present', 'absent'], default='present'),
        name=dict(type='str', required=True),
        password=dict(type='str', default=None, no_log=True),
        type_password=dict(type='str', default='sha256_password', no_log=False),
        cluster=dict(type='str', default=None),
        update_password=dict(
            type='str', choices=['always', 'on_create'],
            default='on_create', no_log=False
        ),
        settings=dict(type='list', elements='str'),
        roles=dict(type='list', elements='str', default=None),
        default_roles=dict(type='list', elements='str', default=None),
        append_roles=dict(type='bool', default=False),
        append_default_roles=dict(type='bool', default=False),
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
    password = module.params["password"]
    type_password = module.params["type_password"]
    cluster = module.params['cluster']
    update_password = module.params['update_password']

    # Will fail if no driver informing the user
    check_clickhouse_driver(module)

    # Connect to DB
    client = connect_to_db_via_client(module, main_conn_kwargs, client_kwargs)

    # Do the job
    changed = False
    user = ClickHouseUser(module, client, name, password,
                          type_password, cluster)

    if state == 'present':
        if not user.user_exists:
            changed = user.create()
        else:
            # If user exists
            changed = user.update(update_password)
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
