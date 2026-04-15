#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright: (c) 2026, Rafal Kozlowski (@rkozlo) <rafalkozlowski07@gmail.com>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function

__metaclass__ = type

DOCUMENTATION = r'''
---
module: clickhouse_named_collection

short_description: Creates, removes or modify a ClickHouse named collection using the clickhouse-driver Client interface

description:
  - Creates, remove or modify a ClickHouse named collection using the
    L(clickhouse-driver,https://clickhouse-driver.readthedocs.io/en/latest) Client interface.
  - The module can only work if O(login_user) will have necessary grants.
  - An existing named collection can be modified only if server is properly configured and user is allowed to
    L(view secrets,https://clickhouse.com/docs/sql-reference/statements/grant#displaysecretsinshowandselect).
    There is an option to rewrite whole secret, but then it will not be idempotent.
  - Module is supported only on version 25.8 or later.

attributes:
  check_mode:
    description: Supports check_mode.
    support: full

author:
  - Rafal Kozlowski (@rkozlo)

extends_documentation_fragment:
  - community.clickhouse.client_inst_opts

version_added: '2.2.0'

options:
  state:
    description:
      - Named collection state.
      - If C(present), will create or modify the named collection.
      - If C(absent), will drop the named collection if exists.
    type: str
    choices: ['present', 'absent']
    default: 'present'
  name:
    description:
      - Named collection name to add or remove.
    type: str
    required: true
  rewrite:
    description:
      - Force to rewrite secret. It has only use when viewing secrets is disabled and user can't fetch secrets values.
      - If user can view secret values it will be fully idempotent. Will only alter collection if it differs.
    type: bool
    default: false
  collection:
    description:
      - Content of named collection.
    type: list
    elements: dict
    suboptions:
      name:
        description:
          - Name of secret.
        type: str
        required: true
      value:
        description:
          - Content of secret.
        type: str
        required: true
  cluster:
    description:
      - Run the command on all cluster hosts.
      - If the cluster is not configured, the command will crash with an error.
    type: str
'''

EXAMPLES = r'''
- name: Create named collection with 2 secrets
  community.clickhouse.clickhouse_named_collection:
    name: test_col
    collection:
      - name: user
        value: alice
      - name: password
        value: test_pass

- name: Create named collection with forcing rewrite
  community.clickhouse.clickhouse_named_collection:
    name: test_col
    collection:
      - name: user
        value: alice
      - name: password
        value: test_pass
    rewrite: true

- name: Drop named collection
  community.clickhouse.clickhouse_named_collection:
    name: test_col
    state: absent
'''

RETURN = r'''
---
executed_statements:
  description:
    - Data-modifying executed statements.
  returned: on success
  type: list
  sample: ["CREATE NAMED COLLECTION `test_col` AS user = '********', password = '********'"]
'''

from ansible.module_utils.basic import AnsibleModule

from ansible_collections.community.clickhouse.plugins.module_utils.clickhouse import (
    check_clickhouse_driver,
    client_common_argument_spec,
    connect_to_db_via_client,
    execute_query,
    get_main_conn_kwargs,
    get_server_version
)

executed_statements = []


class ClickHouseNamedCollection:
    def __init__(self, module, client, name):
        self.module = module
        self.client = client
        self.name = name
        self._exists = None
        self._source = None
        self._current = None

    @property
    def exists(self):
        self._load()
        return self._exists

    @property
    def source(self):
        self._load()
        return self._source

    @property
    def current(self):
        self._load()
        return self._current

    def _load(self):
        """Fetch info about passed collection"""
        if self._exists is not None:
            return

        query = f"SELECT collection, source FROM system.named_collections WHERE name = '{self.name}'"
        result = execute_query(self.module, self.client, query)

        if not result:
            self._exists = False
            self._source = None
            self._current = None
            return

        collection_data, source = result[0]
        self._exists = True
        self._source = source

        if source == 'SQL':
            self._current = self._normalize_current_collection_data(collection_data)
        else:
            self.module.fail_json(
                msg=f"Passed named collection isn't sourced by SQL. Got: {source}"
            )

    def fetch(self):
        """Get named collection paramters."""
        if not self.exists:
            return None

        query = "SELECT collection, source FROM system.named_collections WHERE name = '%s'" % self.name
        result = execute_query(self.module, self.client, query)
        if result:
            if result[0][1] != 'SQL':
                self.module.fail_json(msg="Passed named collection isn't sourced by SQL. Got: '%s'" % result[0][1])
            return result[0][0]
        return None

    def _normalize_current_collection_data(self, collection):
        """Normalize raw db output into normalized form used in module options."""
        """At this moment only support value field."""

        result = {}
        for key, value in collection.items():
            result[key] = {'value': value}
            self.module.no_log_values.add(value)
        return result

    def _check_if_hidden(self):
        """Check normalized collection if hasn't hidden fields."""
        """At this moment it can't be hidden in single key so we return on single element as whole."""
        for key in self._current.values():
            if key['value'] == '[HIDDEN]':
                return True
        return False

    def _build_list_collection(self, collection):
        parts = []
        for name, content in collection.items():
            for value in content.values():
                parts.append("%s = '%s'" % (name, value))
        return parts

    def _should_skip_alter(self, collection, rewrite):
        """Wheter preceed alter or not."""
        hidden = self._check_if_hidden()

        # Hidden means we can't determinie difference in key values.
        if hidden and not rewrite:
            return True

        if self.current == collection:
            return True
        return False

    def _build_alter_query(self, collection, cluster):
        parts = self._build_list_collection(collection)
        query = f"ALTER NAMED COLLECTION `{self.name}`"
        if cluster:
            query += f" ON CLUSTER '{cluster}'"
        query += " SET " + ", ".join(parts)
        return query

    def create(self, collection, cluster):
        if not collection:
            self.module.fail_json(msg="Collection not passed")
        query = "CREATE NAMED COLLECTION `%s`" % self.name
        if cluster:
            query += " ON CLUSTER '%s'" % cluster
        query += " AS "
        parts = self._build_list_collection(collection)
        query += ", ".join(parts)

        executed_statements.append(query)

        if not self.module.check_mode:
            execute_query(self.module, self.client, query)

        return True

    def drop(self, cluster):
        query = "DROP NAMED COLLECTION `%s`" % self.name
        if cluster:
            query += " ON CLUSTER '%s'" % cluster

        executed_statements.append(query)

        if not self.module.check_mode:
            execute_query(self.module, self.client, query)

        return True

    def alter(self, collection, cluster, rewrite):
        if not collection:
            self.module.fail_json(msg="Collection not passed")
        if self._should_skip_alter(collection, rewrite):
            return False

        query = self._build_alter_query(collection, cluster)

        executed_statements.append(query)

        if not self.module.check_mode:
            execute_query(self.module, self.client, query)
        return True

    def _normalize_collection_input(self, collection_list):
        """Convert list of {name, value} to dict of {name: {value}}"""
        if not collection_list:
            return {}

        normalized = {}
        for item in collection_list:
            name = item['name']
            value = item['value']

            # Handle duplicate names (last one wins or fail)
            if name in normalized:
                self.module.warn(f"Duplicate parameter '{name}', using last value")

            normalized[name] = {'value': value}

        return normalized


def main():
    # Set up arguments.
    # If there are common arguments shared across several modules,
    # create the common_argument_spec() function under plugins/module_utils/*
    # and invoke here to return a dict with those arguments
    argument_spec = client_common_argument_spec()
    argument_spec.update(
        state=dict(type="str", choices=["present", "absent"], default="present"),
        name=dict(type="str", required=True),
        cluster=dict(type='str', default=None),
        collection=dict(
            type='list',
            elements='dict',
            options=dict(
                name=dict(type='str', required=True),
                value=dict(type='str', required=True, no_log=True)
            )
        ),
        rewrite=dict(type='bool', default=False)
    )

    # Conditional logic
    module = AnsibleModule(
        argument_spec=argument_spec,
        supports_check_mode=True,
        required_if=[
            ('state', 'present', ['collection'], True)
        ],
    )

    # Assign passed options to variables
    client_kwargs = module.params["client_kwargs"]
    # The reason why these arguments are separate from client_kwargs
    # is that we need to protect some sensitive data like passwords passed
    # to the module from logging (see the arguments above with no_log=True);
    # Such data must be passed as module arguments (not nested deep in values).
    main_conn_kwargs = get_main_conn_kwargs(module)
    state = module.params["state"]
    name = module.params["name"]
    cluster = module.params["cluster"]
    collection = module.params["collection"]
    rewrite = module.params["rewrite"]

    # Will fail if no driver informing the user
    check_clickhouse_driver(module)

    # Connect to DB
    client = connect_to_db_via_client(module, main_conn_kwargs, client_kwargs)
    server_version = get_server_version(module, client)

    if server_version['year'] < 25 or (server_version['year'] == 25 and server_version['feature'] < 8):
        module.fail_json(msg="Server version not supported. Require 25.8 or later.")

    # Do the job
    changed = False
    named_collection = ClickHouseNamedCollection(module, client, name)
    normalized_collection = named_collection._normalize_collection_input(collection)
    if state == "present":
        if not named_collection.exists:
            changed = named_collection.create(normalized_collection, cluster)
        else:
            changed = named_collection.alter(normalized_collection, cluster, rewrite)
    else:
        # If state is absent
        if named_collection.exists:
            changed = named_collection.drop(cluster)

    # Close connection
    client.disconnect_connection()

    # Users will get this in JSON output after execution
    module.exit_json(changed=changed, executed_statements=executed_statements)


if __name__ == "__main__":
    main()
