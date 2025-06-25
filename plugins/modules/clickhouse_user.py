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
      - Grants specified roles to the user.
      - To append or remove roles, use the I(roles_mode) argument.
      - To revoke all roles, pass an empty list (C([])) and I(default_roles_mode=listed_only).
    type: list
    elements: str
    version_added: '0.6.0'
  default_roles:
    description:
      - Sets specified roles as default for the user.
      - The roles must be explicitly granted to the user whether manually
        before using this argument or by using the I(roles)
        argument in the same task.
      - To append or remove roles, use the I(default_roles_mode) argument.
      - To unset all roles as default, pass an empty list (C([])) and I(default_roles_mode=listed_only).
    type: list
    elements: str
    version_added: '0.6.0'
  roles_mode:
    description:
     - When C(listed_only) (default), makes the user a member of only roles specified in I(roles).
       It will remove the user from all other roles.
     - When C(append), appends roles specified in I(roles) to existing user roles.
     - When C(remove), removes roles specified in I(roles) from user roles.
     - The argument is ignored without I(roles) set.
    type: str
    choices: ['append', 'listed_only', 'remove']
    default: 'listed_only'
    version_added: '0.6.0'
  default_roles_mode:
    description:
     - When C(listed_only) (default), sets only roles specified in I(default_roles) as user default roles.
       It will unset all other roles as default roles.
     - When C(append), appends roles specified in I(default_roles) to existing user default roles.
       default roles instead of unsetting not specified ones.
     - When C(remove), removes roles specified in I(default_roles) from user default roles.
     - Ignored without I(default_roles) set.
    type: str
    choices: ['append', 'listed_only', 'remove']
    default: 'listed_only'
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

- name: Append the sales role to test_user's roles
  community.clickhouse.clickhouse_user:
    login_host: localhost
    login_user: alice
    login_db: foo
    login_password: my_password
    name: test_user
    roles:
    - sales
    roles_mode: append

- name: Unset all test_user's default roles
  community.clickhouse.clickhouse_user:
    login_host: localhost
    login_user: alice
    login_db: foo
    login_password: my_password
    name: test_user
    default_roles: []

- name: Revoke all roles from test_user
  community.clickhouse.clickhouse_user:
    login_host: localhost
    login_user: alice
    login_db: foo
    login_password: my_password
    name: test_user
    roles: []

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
    def __init__(self, module, client, name):
        self.changed = False
        self.module = module
        self.client = client
        self.name = name
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

    def create(self, type_password, password, cluster, settings,
               roles, roles_mode, default_roles, default_roles_mode):

        query = "CREATE USER '%s'" % self.name

        if password is not None:
            query += (" IDENTIFIED WITH %s BY '%s'") % (type_password, password)

        if cluster:
            query += " ON CLUSTER %s" % cluster

        if settings:
            query += " SETTINGS"
            for index, value in enumerate(settings):
                query += " %s" % value
                if index < len(settings) - 1:
                    query += ","

        executed_statements.append(query)

        if not self.module.check_mode:
            execute_query(self.module, self.client, query)

        if roles and roles_mode != 'remove':
            self.__grant_roles(roles, cluster)

        if default_roles and default_roles_mode != 'remove':
            self.__set_default_roles(default_roles, cluster)

        return True

    def update(self, update_password, type_password, password, cluster,
               roles, roles_mode, default_roles, default_roles_mode):

        if roles is not None:
            self.__update_roles(roles, roles_mode, cluster)

        if default_roles is not None:
            self.__update_default_roles(default_roles, default_roles_mode, cluster)

        self.__update_passwd(update_password, type_password, password, cluster)

        # TODO Add a possibility to update settings idempotently

        return self.changed

    def drop(self, cluster):
        query = "DROP USER '%s'" % self.name
        if cluster:
            query += " ON CLUSTER %s" % cluster

        executed_statements.append(query)

        if not self.module.check_mode:
            execute_query(self.module, self.client, query)

        return True

    def __update_roles(self, desired_roles, mode, cluster):
        des = set(desired_roles)
        cur = set(self.current_roles)

        if mode == 'remove':
            # Remove only roles already present in current roles
            roles_to_revoke = list(des & cur)
            if roles_to_revoke:
                self.__revoke_roles(roles_to_revoke, cluster)

        elif mode == 'append':
            # Grant only roles from decired that
            # are not already present in current roles
            roles_to_grant = list(des - cur)
            if roles_to_grant:
                self.__grant_roles(roles_to_grant, cluster)

        elif mode == 'listed_only':
            if desired_roles == [] and self.current_roles:
                self.__revoke_roles(self.current_roles, cluster)
            elif desired_roles != []:
                roles_to_grant = list(des - cur)
                roles_to_revoke = list(cur - des)
                if roles_to_grant:
                    self.__grant_roles(roles_to_grant, cluster)
                if roles_to_revoke:
                    self.__revoke_roles(roles_to_revoke, cluster)

    def __update_default_roles(self, desired_def_roles, mode, cluster):
        des = set(desired_def_roles)
        cur = set(self.current_default_roles)

        if mode == 'remove' and des & cur:
            if desired_def_roles != []:
                # In this case, "desired" means "desired to get removed"
                self.__set_default_roles(list(cur - des), cluster)

        elif mode == 'append':
            if desired_def_roles != [] and des != cur:
                self.__set_default_roles(list(cur.union(des)), cluster)

        elif mode == 'listed_only':
            if desired_def_roles == [] and self.current_roles:
                self.__unset_default_roles()

            elif desired_def_roles != [] and des != cur:
                self.__set_default_roles(desired_def_roles, cluster)

    def __update_passwd(self, update_password, type_pwd, pwd, cluster):
        if update_password == 'on_create':
            return False or self.changed

        # If update_password is always
        # TODO: When ClickHouse will allow to retrieve password hashes,
        # make this idempotent, i.e. execute this only if the passwords don't match
        query = ("ALTER USER '%s' IDENTIFIED WITH %s "
                 "BY '%s'") % (self.name, type_pwd, pwd)
        if cluster:
            query += " ON CLUSTER %s" % cluster

        executed_statements.append(query)

        if not self.module.check_mode:
            execute_query(self.module, self.client, query)

        self.changed = True

    def __grant_roles(self, roles_to_set, cluster):
        query = "GRANT %s TO '%s'" % (', '.join(roles_to_set), self.name)
        executed_statements.append(query)

        if cluster:
            query += " ON CLUSTER %s" % cluster

        if not self.module.check_mode:
            execute_query(self.module, self.client, query)

        self.changed = True

    def __revoke_roles(self, roles_to_revoke, cluster):
        query = "REVOKE %s FROM '%s'" % (', '.join(roles_to_revoke), self.name)
        executed_statements.append(query)

        if cluster:
            query += " ON CLUSTER %s" % cluster

        if not self.module.check_mode:
            execute_query(self.module, self.client, query)

        self.changed = True

    def __set_default_roles(self, roles_to_set, cluster):
        self.current_roles = self.__fetch_user_groups()
        for role in roles_to_set:
            if role not in self.current_roles and role not in self.module.params["roles"]:
                self.module.fail_json("User %s is not in %s role. Grant it explicitly first." % (self.name, role))

        query = "ALTER USER '%s' DEFAULT ROLE %s" % (self.name, ', '.join(roles_to_set))
        if cluster:
            query += " ON CLUSTER %s" % cluster
        executed_statements.append(query)

        if not self.module.check_mode:
            execute_query(self.module, self.client, query)

        self.changed = True

    def __unset_default_roles(self):
        query = "SET DEFAULT ROLE NONE TO '%s'" % self.name
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
        roles_mode=dict(type='str', choices=['listed_only', 'append', 'remove'],
                        default='listed_only'),
        default_roles_mode=dict(type='str', choices=['listed_only', 'append', 'remove'],
                                default='listed_only'),
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
    settings = module.params['settings']
    roles = module.params['roles']
    roles_mode = module.params['roles_mode']
    default_roles = module.params['default_roles']
    default_roles_mode = module.params['default_roles_mode']

    # Will fail if no driver informing the user
    check_clickhouse_driver(module)

    # Connect to DB
    client = connect_to_db_via_client(module, main_conn_kwargs, client_kwargs)

    # Do the job
    changed = False
    user = ClickHouseUser(module, client, name)

    if state == 'present':
        if not user.user_exists:
            changed = user.create(type_password, password, cluster, settings,
                                  roles, roles_mode, default_roles, default_roles_mode)
        else:
            # If user exists
            changed = user.update(update_password, type_password, password, cluster,
                                  roles, roles_mode, default_roles, default_roles_mode)
    else:
        # If state is absent
        if user.user_exists:
            changed = user.drop(cluster)

    # Close connection
    client.disconnect_connection()

    # Users will get this in JSON output after execution
    module.exit_json(changed=changed, executed_statements=executed_statements)


if __name__ == '__main__':
    main()
