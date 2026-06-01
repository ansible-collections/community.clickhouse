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

version_added: '0.3.0'

attributes:
  check_mode:
    description: Supports check_mode.
    support: full
  idempotent:
    support: partial
    description:
      - Obviously O(state=rename) will fail at second run.

author:
  - Andrew Klychkov (@Andersson007)
  - Aleksandr Vagachev (@aleksvagachev)
  - Rafal Kozlowski (@rkozlo)

extends_documentation_fragment:
  - community.clickhouse.client_inst_opts

options:
  state:
    description:
      - Database state.
      - If C(present), will create the database if not exists.
      - If C(absent), will drop the database if exists.
      - If C(rename), will rename the database if exists.
    type: str
    choices: ['present', 'absent', 'rename']
    default: 'present'
  name:
    description:
      - Database name to add or remove.
    type: str
    required: true
  engine:
    description:
      - Database engine. Once set, cannot be changed.
    type: str
  cluster:
    description:
      - Run the command on all cluster hosts.
      - If the cluster is not configured, the command will crash with an error.
    type: str
    version_added: '0.4.0'
  target:
    description:
      - Name for renaming the database.
    type: str
    version_added: '0.4.0'
  comment:
    description:
      - Database comment.
      - Since C(2.2.0) can change comments in existing databases (requires server 25.8 or later).
    type: str
    version_added: '0.4.0'
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
    comment: Test DB

- name: Drop database
  community.clickhouse.clickhouse_db:
    login_host: localhost
    login_user: alice
    login_db: foo
    login_password: my_password
    name: test_db
    state: absent

# Rename the database test_db to prod_db.
# If the database test_db exists, it will be renamed to prod_db.
# If the database test_db does not exist and the prod_db database exists,
# the module will report that nothing has changed.
# If both the databases exist, an error will be raised.
- name: Rename database
  community.clickhouse.clickhouse_db:
    login_host: localhost
    login_user: alice
    login_db: foo
    login_password: my_password
    name: test_db
    target: prod_db
    cluster: test_cluster
    state: rename
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

from ansible_collections.community.clickhouse.plugins.module_utils.clickhouse import (
    check_clickhouse_driver,
    client_common_argument_spec,
    connect_to_db_via_client,
    execute_query,
    get_main_conn_kwargs,
    get_server_version,
    validate_identifier,
    get_on_cluster_clause,
)


executed_statements = []


class ClickHouseDB():
    def __init__(self, module, client, name, cluster, comment):
        self.module = module
        self.client = client
        validate_identifier(module, name, "db name")
        self.name = name
        self.cluster = cluster
        # Set default values, then update
        self.exists = False
        self.engine = None
        self.comment = comment
        self.__populate_info()

    def __populate_info(self):
        # TODO: If anyone can determine the version when the comment feature
        # was added to database more precisely, you're welcome to adjust it here
        server_version = get_server_version(self.module, self.client)
        if server_version['year'] >= 22:
            # The comment is not supported in all versions
            query = ("SELECT engine, comment "
                     "FROM system.databases "
                     "WHERE name = %(name)s")
        else:
            self.module.deprecate(
                msg="Used server version is not maintained. Upgrade server version.",
                version="3.0.0",
                collection_name="community.clickhouse",
            )
            query = "SELECT engine FROM system.databases WHERE name = %(name)s"

        # Will move this function to the lib later and reuse
        exec_kwargs = {'params': {'name': self.name}}
        result = execute_query(self.module, self.client, query, exec_kwargs)

        # Assume the DB does not exist by default
        if result:
            # If exists
            self.exists = True
            self.engine = result[0][0]
            if server_version['year'] >= 22:
                self.comment = result[0][1]

    def _validate_db_engine(self, target_engine):
        '''Prevent executing incorrect queries.'''
        query = "SELECT 1 FROM system.database_engines WHERE name = %(engine)s"
        exec_kwargs = {'params': {'engine': target_engine}}
        result = execute_query(self.module, self.client, query, exec_kwargs)
        if not result:
            self.module.fail_json(msg=f"Passed engine {target_engine} is not supported by current version.")

    def create(self, engine, comment):
        query = "CREATE DATABASE `%s`" % self.name
        query += get_on_cluster_clause(self.module, self.cluster)
        if engine:
            self._validate_db_engine(engine)
            query += " ENGINE = %s" % engine

        if comment:
            query += " COMMENT '%s'" % comment

        executed_statements.append(query)

        if not self.module.check_mode:
            execute_query(self.module, self.client, query)

        return True

    def update(self, engine, comment):
        # IMPORTANT: In case in future any items here can change
        # please add check_mode handling

        # There's no way to change the engine
        # so just inform the users they have to recreate
        # the DB in order to change them
        if engine and engine != self.engine:
            msg = ("The provided engine '%s' is different from "
                   "the current one '%s'. It is NOT possible to "
                   "change it. The recreation of the database is required "
                   "in order to change it." % (engine, self.engine))
            self.module.warn(msg)
        server_version = get_server_version(self.module, self.client)
        # At this moment ALTER DATABASE supports only MODIFY COMMENT.
        # When it will support more options probably better
        # will be moving query builder above and here only link comment.
        if comment and comment != self.comment:
            if server_version['year'] < 22:
                msg = ('The module supports the comment feature for ClickHouse '
                       'versions equal to or higher than 22.*. Ignored.')
                self.module.warn(msg)
            elif (server_version['year'] == 25 and server_version['feature'] >= 8) or server_version['year'] >= 26:
                query = "ALTER DATABASE `%s`" % self.name
                if self.cluster:
                    query += get_on_cluster_clause(self.module, self.cluster)
                query += " MODIFY COMMENT '%s'" % comment

                executed_statements.append(query)

                if not self.module.check_mode:
                    execute_query(self.module, self.client, query)

                return True
            else:
                self.module.warn(
                    f"Server version {server_version['year']}.{server_version['feature']} "
                    f"does not support MODIFY COMMENT. Required: 25.8 or higher"
                )

        return False

    def rename(self, target):
        validate_identifier(self.module, target, "db name")
        query = "RENAME DATABASE `%s` TO `%s`" % (self.name, target)
        query += get_on_cluster_clause(self.module, self.cluster)

        executed_statements.append(query)

        if not self.module.check_mode:
            execute_query(self.module, self.client, query)

        return True

    def drop(self):
        query = "DROP DATABASE `%s`" % self.name
        query += get_on_cluster_clause(self.module, self.cluster)

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
        state=dict(type='str', choices=['present', 'absent', 'rename'], default='present'),
        name=dict(type='str', required=True),
        engine=dict(type='str', default=None),
        cluster=dict(type='str', default=None),
        target=dict(type='str', default=None),
        comment=dict(type='str', default=None),
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
    cluster = module.params['cluster']
    target = module.params['target']
    comment = module.params['comment']

    # Will fail if no driver informing the user
    check_clickhouse_driver(module)

    # Connect to DB
    client = connect_to_db_via_client(module, main_conn_kwargs, client_kwargs)

    # Do the job
    changed = False
    database = ClickHouseDB(module, client, name, cluster, comment)

    if state == 'present':
        if not database.exists:
            changed = database.create(engine, comment)
        else:
            # If database exists
            changed = database.update(engine, comment)
    elif state == 'rename':
        if database.exists:
            changed = database.rename(target)
        else:
            target_db = ClickHouseDB(module, client, target, cluster, comment)
            if target_db.exists:
                changed = False
                msg = "There is nothing to rename"
                module.warn(msg)
            else:
                msg = "The %s and %s databases do not exist" % (name, target)
                module.fail_json(msg=msg)
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
