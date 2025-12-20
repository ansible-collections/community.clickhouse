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
      - When specified for an existing user, settings will only be updated if they differ from current settings.
      - The module fetches current settings from C(system.settings_profile_elements) for comparison.
    type: list
    elements: str
    version_added: '0.5.0'
  roles:
    description:
      - Grants specified roles to the user.
      - To append or remove roles, use the O(roles_mode) argument.
      - To revoke all roles, pass an empty list (C([])) and O(default_roles_mode=listed_only).
    type: list
    elements: str
    version_added: '0.6.0'
  default_roles:
    description:
      - Sets specified roles as default for the user.
      - The roles must be explicitly granted to the user whether manually
        before using this argument or by using the O(roles)
        argument in the same task.
      - To append or remove roles, use the O(default_roles_mode) argument.
      - To unset all roles as default, pass an empty list (C([])) and O(default_roles_mode=listed_only).
    type: list
    elements: str
    version_added: '0.6.0'
  roles_mode:
    description:
     - When C(listed_only) (default), makes the user a member of only roles specified in O(roles).
       It will remove the user from all other roles.
     - When C(append), appends roles specified in O(roles) to existing user roles.
     - When C(remove), removes roles specified in O(roles) from user roles.
     - The argument is ignored without O(roles) set.
    type: str
    choices: ['append', 'listed_only', 'remove']
    default: 'listed_only'
    version_added: '0.6.0'
  default_roles_mode:
    description:
     - When C(listed_only) (default), sets only roles specified in O(default_roles) as user default roles.
       It will unset all other roles as default roles.
     - When C(append), appends roles specified in O(default_roles) to existing user default roles.
       default roles instead of unsetting not specified ones.
     - When C(remove), removes roles specified in O(default_roles) from user default roles.
     - Ignored without O(default_roles) set.
    type: str
    choices: ['append', 'listed_only', 'remove']
    default: 'listed_only'
    version_added: '0.6.0'
  user_hosts:
    description:
      - Host restrictions to apply to the user.
      - It's a list of dictionaries, where each dictionary specifies the type of restriction to apply to which hosts or pattern.
    type: list
    elements: dict
    suboptions:
      type:
        description:
          - The method used to validate which hosts that users are allowed to connect from (V(ANY), V(LOCAL), V(IP), V(LIKE), V(NAME), V(REGEXP)).
          - When specified for an existing user the previous host type and hosts will be updated.
          - For more details, see U(https://clickhouse.com/docs/en/sql-reference/statements/create/user).
        type: str
        required: true
      hosts:
        description:
          - A list of hosts or patterns from which the user will be allowed to connect.
          - This is required if O(user_hosts.type) is not V(ANY) or V(LOCAL).
        type: list
        elements: str
        required: false
    version_added: '1.0.0'
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

- name: Update user settings (idempotent - only updates if different)
  community.clickhouse.clickhouse_user:
    login_host: localhost
    login_user: alice
    login_db: foo
    login_password: my_password
    name: test_user
    settings:
      - max_memory_usage = 20000 READONLY
      - max_threads = 8

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

- name: Create a user that can only connect from a specified host
  community.clickhouse.clickhouse_user:
    login_host: localhost
    login_user: alice
    login_db: foo
    login_password: my_password
    name: test_user
    user_hosts:
      - type: NAME
        hosts:
          - 'host1'

- name: Update user host restrictions. Any previous host restrictions will be replaced. (idempotent - only updates if different)
  community.clickhouse.clickhouse_user:
    login_host: localhost
    login_user: alice
    login_db: foo
    login_password: my_password
    name: test_user
    user_hosts:
      - type: LIKE
        hosts:
          - '%.example.com'

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
        self.current_settings = {}
        self.current_user_hosts = {}
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
            self.current_settings = self.__fetch_user_settings()
            self.current_user_hosts = self.__fetch_user_hosts()

    def __fetch_user_groups(self):
        query = ("SELECT granted_role_name FROM system.role_grants "
                 "WHERE user_name = '%s'" % self.name)
        result = execute_query(self.module, self.client, query)
        return [row[0] for row in result]

    def __fetch_user_settings(self):
        """Fetch current user settings from system.settings_profile_elements"""
        query = ("SELECT setting_name, value, min, max, writability, inherit_profile "
                 "FROM system.settings_profile_elements "
                 "WHERE user_name = '%s'" % self.name)
        result = execute_query(self.module, self.client, query)

        # Build a dict of current settings with their full definition
        settings_dict = {}
        for row in result:
            setting_name, value, min_val, max_val, writability, inherit_profile = row

            # Handle PROFILE inheritance separately
            if inherit_profile is not None:
                settings_dict[inherit_profile] = "PROFILE %s" % inherit_profile
                continue

            # Skip if it's not a regular setting (setting_name could be None for profiles)
            if setting_name is None:
                continue

            # Build the setting string as it would appear in ALTER USER
            setting_parts = [setting_name]
            if value is not None:
                setting_parts.extend(["=", str(value)])
            if min_val is not None:
                setting_parts.extend(["MIN", str(min_val)])
            if max_val is not None:
                setting_parts.extend(["MAX", str(max_val)])
            if writability is not None and writability != 'WRITABLE':
                # Map writability enum to SQL keywords
                if writability == 'CONST':
                    setting_parts.append("READONLY")
                elif writability == 'CHANGEABLE_IN_READONLY':
                    setting_parts.append("CHANGEABLE_IN_READONLY")

            settings_dict[setting_name] = " ".join(setting_parts)

        return settings_dict

    def __fetch_user_hosts(self):
        """Fetch current user host restrictions from system.users"""
        query = ("SELECT host_ip, host_names, host_names_regexp, host_names_like FROM system.users "
                 "WHERE name = '%s'" % self.name)
        result = execute_query(self.module, self.client, query)

        user_hosts_dict = {
            'IP': set(result[0][0]),       # HOST ANY is represented by ['::/0'] in host_ip
            'NAME': set(result[0][1]),     # HOST LOCAL is represented by ['localhost'] in host_names
            'REGEXP': set(result[0][2]),
            'LIKE': set(result[0][3]),
        }

        return user_hosts_dict

    def create(self, type_password, password, cluster, user_hosts, settings,
               roles, roles_mode, default_roles, default_roles_mode):

        query = "CREATE USER '%s'" % self.name

        if password is not None:
            query += (" IDENTIFIED WITH %s BY '%s'") % (type_password, password)

        if user_hosts is not None:
            query += " %s" % self.__build_user_host_clause(user_hosts)

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

    def update(self, update_password, type_password, password, cluster, user_hosts,
               roles, roles_mode, default_roles, default_roles_mode, settings):

        if roles is not None:
            self.__update_roles(roles, roles_mode, cluster)

        if default_roles is not None:
            self.__update_default_roles(default_roles, default_roles_mode, cluster)

        self.__update_passwd(update_password, type_password, password, cluster)

        if user_hosts is not None:
            self.__update_host(user_hosts, cluster)

        if settings is not None:
            self.__update_settings(settings, cluster)

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

    def __update_host(self, user_hosts, cluster):
        des = self.__get_desired_user_hosts(user_hosts)
        cur = self.current_user_hosts

        if des == cur:
            return

        query = "ALTER USER '%s' %s" % (self.name, self.__build_user_host_clause(user_hosts))

        if cluster:
            query += " ON CLUSTER %s" % cluster

        executed_statements.append(query)

        if not self.module.check_mode:
            execute_query(self.module, self.client, query)

        self.changed = True

    def __get_desired_user_hosts(self, user_hosts):
        desired_hosts = {
            'IP': set(),
            'NAME': set(),
            'REGEXP': set(),
            'LIKE': set(),
        }
        for host_restriction in user_hosts:
            host_type = host_restriction['type'].upper()
            if host_type == 'ANY':
                # ANY overrides all other restrictions
                return {
                    'IP': set(['::/0']),
                    'NAME': set(),
                    'REGEXP': set(),
                    'LIKE': set(),
                }
            elif host_type.upper() == 'LOCAL':
                desired_hosts['NAME'].add('localhost')
            else:
                hosts = host_restriction.get('hosts', [])
                desired_hosts[host_type].update(hosts)

        return desired_hosts

    def __build_user_host_clause(self, user_hosts):
        # If ANY is specified among host restrictions, it overrides all others
        if 'ANY' in [hr['type'].upper() for hr in user_hosts]:
            return "HOST ANY"

        clauses = []
        for host_restriction in user_hosts:
            host_type = host_restriction['type']

            if host_type.upper() == 'LOCAL':
                clauses.append("HOST %s" % host_type)
            else:
                hosts = host_restriction.get('hosts')
                clauses.append("HOST %s %s" % (host_type, ', '.join("'{0}'".format(h) for h in hosts)))

        return ' '.join(clauses)

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

    def __update_settings(self, settings, cluster):
        """Update user settings idempotently by comparing with current settings"""
        # Parse desired settings into a comparable format
        desired_settings = {}
        desired_profiles = []

        for setting in settings:
            setting_upper = setting.upper()
            # Handle PROFILE separately as it's not a regular setting
            if 'PROFILE' in setting_upper:
                # Extract profile name (handle both PROFILE 'name' and PROFILE name)
                profile_part = setting.split(None, 1)[1].strip().strip("'\"")
                desired_profiles.append(profile_part)
            else:
                # Extract setting name (first word before = or space)
                setting_name = setting.split()[0].split('=')[0].strip()
                # Normalize the setting string for comparison
                normalized = ' '.join(setting.split())
                desired_settings[setting_name] = normalized

        # Compare current with desired
        needs_update = False

        # Check if any setting values differ
        for setting_name, desired_def in desired_settings.items():
            current_def = self.current_settings.get(setting_name, '')
            # Normalize both for comparison (case-insensitive, whitespace-normalized)
            current_normalized = ' '.join(current_def.upper().split())
            desired_normalized = ' '.join(desired_def.upper().split())

            if current_normalized != desired_normalized:
                needs_update = True
                break

        # Check if there are settings to remove (current has settings not in desired)
        if not needs_update:
            for setting_name in self.current_settings:
                if setting_name not in desired_settings:
                    # There's a setting currently applied that's not in desired
                    # We need to reapply to remove it
                    needs_update = True
                    break

        # Only update if settings actually differ
        if not needs_update:
            return

        # Build the ALTER USER query
        query = "ALTER USER '%s' SETTINGS" % self.name
        for index, value in enumerate(settings):
            query += " %s" % value
            if index < len(settings) - 1:
                query += ","

        if cluster:
            query += " ON CLUSTER %s" % cluster

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
        user_hosts=dict(type='list', elements='dict'),
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
    user_hosts = module.params['user_hosts']
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
            changed = user.create(type_password, password, cluster, user_hosts, settings,
                                  roles, roles_mode, default_roles, default_roles_mode)
        else:
            # If user exists
            changed = user.update(update_password, type_password, password, cluster, user_hosts,
                                  roles, roles_mode, default_roles, default_roles_mode, settings)
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
