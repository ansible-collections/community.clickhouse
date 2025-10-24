#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright: (c) 2024, Don Naro (@oranod) <dnaro@redhat.com>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function

__metaclass__ = type

DOCUMENTATION = r"""
---
module: clickhouse_role

short_description: Creates or removes a ClickHouse role.

description:
  - Creates or removes a ClickHouse role.

attributes:
  check_mode:
    description: Supports check_mode.
    support: full

version_added: '0.5.0'

author:
  - Don Naro (@oranod)
  - Aleksandr Vagachev (@aleksvagachev)
  - Andrew Klychkov (@Andersson007)

extends_documentation_fragment:
  - community.clickhouse.client_inst_opts

options:
  state:
    description:
      - Role state.
      - C(present) creates the role if it does not exist.
      - C(absent) deletes the role if it exists.
    type: str
    choices: ['present', 'absent']
    default: 'present'
  name:
    description:
      - Role name to add or remove.
    type: str
    required: true
  cluster:
    description:
      - Run the command on all cluster hosts.
      - If the cluster is not configured, the command will crash with an error.
    type: str
  settings:
    description:
      - Settings with their limitations that apply to the role.
      - You can also specify the profile from which the settings will be inherited.
    type: list
    elements: str
"""

EXAMPLES = r"""
- name: Create role
  community.clickhouse.clickhouse_role:
    name: test_role
    state: present

- name: Create a role with settings
  community.clickhouse.clickhouse_role:
    name: test_role
    state: present
    settings:
      - max_memory_usage = 15000 MIN 15000 MAX 16000 READONLY
      - PROFILE restricted
    cluster: test_cluster

- name: Remove role
  community.clickhouse.clickhouse_role:
    name: test_role
    state: absent
"""

RETURN = r"""
executed_statements:
  description:
  - Data-modifying executed statements.
  returned: on success
  type: list
  sample: ['CREATE ROLE test_role']
"""

import re

from ansible.module_utils.basic import AnsibleModule

from ansible_collections.community.clickhouse.plugins.module_utils.clickhouse import (
    check_clickhouse_driver,
    client_common_argument_spec,
    connect_to_db_via_client,
    execute_query,
    get_main_conn_kwargs,
)

executed_statements = []


class ClickHouseRole:
    def __init__(self, module, client, name):
        self.module = module
        self.client = client
        self.name = name
        self.exists = self.check_exists()

    def check_exists(self):
        query = "SELECT 1 FROM system.roles WHERE name = '%s' LIMIT 1" % self.name
        result = execute_query(self.module, self.client, query)
        return bool(result)

    def get_current_role_definition(self):
        """Get current role definition using SHOW CREATE ROLE"""
        if not self.exists:
            return None

        query = "SHOW CREATE ROLE %s" % self.name
        result = execute_query(self.module, self.client, query)
        if result:
            return result[0][0]  # SHOW CREATE ROLE returns single row with CREATE statement
        return None

    def parse_settings_from_create_statement(self, create_statement):
        """Parse settings from CREATE ROLE statement"""
        if not create_statement or 'SETTINGS' not in create_statement:
            return []

        # Extract settings part after SETTINGS keyword
        settings_part = create_statement.split('SETTINGS', 1)[1].strip()
        if not settings_part:
            return []

        # Parse individual settings (this is a simplified parser)
        # In real implementation, we might need more sophisticated parsing
        settings = []
        current_setting = ""
        paren_depth = 0
        quote_char = None

        i = 0
        while i < len(settings_part):
            char = settings_part[i]

            if quote_char:
                current_setting += char
                if char == quote_char and (i == 0 or settings_part[i - 1] != '\\'):
                    quote_char = None
            elif char in ("'", '"'):
                current_setting += char
                quote_char = char
            elif char == '(':
                current_setting += char
                paren_depth += 1
            elif char == ')':
                current_setting += char
                paren_depth -= 1
            elif char == ',' and paren_depth == 0:
                settings.append(current_setting.strip())
                current_setting = ""
            else:
                current_setting += char
            i += 1

        if current_setting.strip():
            settings.append(current_setting.strip())

        return settings

    def normalize_settings(self, settings_list):
        """Normalize settings for comparison"""
        if not settings_list:
            return []

        normalized = []
        for setting in settings_list:
            # Remove extra whitespace and standardize format
            normalized_setting = ' '.join(setting.split())

            # ClickHouse normalizes some constraint types
            # Different versions may use CONST or READONLY interchangeably
            # Since READONLY is an alias for CONST in ClickHouse, normalize both to CONST
            normalized_setting = re.sub(r'\bREADONLY\b', 'CONST', normalized_setting)

            # ClickHouse may handle profile names differently across versions
            # Normalize quoted and unquoted profile names
            # PROFILE 'default' -> PROFILE default, PROFILE "default" -> PROFILE default
            normalized_setting = re.sub(r"PROFILE\s+'([^']+)'", r"PROFILE \1", normalized_setting)
            normalized_setting = re.sub(r'PROFILE\s+"([^"]+)"', r"PROFILE \1", normalized_setting)

            # Also handle case where profile name might be output with backticks
            normalized_setting = re.sub(r"PROFILE\s+`([^`]+)`", r"PROFILE \1", normalized_setting)

            normalized.append(normalized_setting)

        return sorted(normalized)

    def settings_changed(self, current_settings, desired_settings):
        """Check if settings have changed"""
        current_normalized = self.normalize_settings(current_settings)
        desired_normalized = self.normalize_settings(desired_settings)

        # For debugging version compatibility issues
        if self.module._verbosity >= 3:  # Only show at high verbosity
            self.module.log(f"Current settings (normalized): {current_normalized}")
            self.module.log(f"Desired settings (normalized): {desired_normalized}")

        return current_normalized != desired_normalized

    def create(self):
        if not self.exists:
            query = "CREATE ROLE %s" % self.name

            if self.module.params['cluster']:
                query += " ON CLUSTER %s" % self.module.params['cluster']

            list_settings = self.module.params['settings']
            if list_settings:
                query += " SETTINGS"
                for index, value in enumerate(list_settings):
                    query += " %s" % value
                    if index < len(list_settings) - 1:
                        query += ","

            executed_statements.append(query)

            if not self.module.check_mode:
                execute_query(self.module, self.client, query)

            self.exists = True
            return True
        else:
            return False

    def alter(self, settings):
        """Update role settings using ALTER ROLE"""
        if not self.exists:
            return False

        query = "ALTER ROLE %s" % self.name

        if self.module.params['cluster']:
            query += " ON CLUSTER %s" % self.module.params['cluster']

        # Handle settings updates
        if settings is not None and len(settings) > 0:
            # Set these settings
            query += " SETTINGS"
            for index, value in enumerate(settings):
                query += " %s" % value
                if index < len(settings) - 1:
                    query += ","

        executed_statements.append(query)

        if not self.module.check_mode:
            execute_query(self.module, self.client, query)

        return True

    def drop(self):
        if self.exists:
            query = "DROP ROLE %s" % self.name
            executed_statements.append(query)

            if not self.module.check_mode:
                execute_query(self.module, self.client, query)

            self.exists = False
            return True
        else:
            return False


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
        settings=dict(type='list', elements='str'),
    )

    # Instantiate an object of module class
    module = AnsibleModule(
        argument_spec=argument_spec,
        supports_check_mode=True,
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

    # Will fail if no driver informing the user
    check_clickhouse_driver(module)

    # Connect to DB
    client = connect_to_db_via_client(module, main_conn_kwargs, client_kwargs)

    # Do the job
    changed = False
    role = ClickHouseRole(module, client, name)
    desired_settings = module.params.get("settings", [])

    if state == "present":
        if not role.exists:
            # Role doesn't exist, create it
            changed = role.create()
        else:
            # Role exists, check if settings need to be updated
            if desired_settings is not None and len(desired_settings) > 0:  # Only check settings if they are specified and not empty
                current_definition = role.get_current_role_definition()
                current_settings = role.parse_settings_from_create_statement(current_definition) if current_definition else []

                if role.settings_changed(current_settings, desired_settings):
                    changed = role.alter(desired_settings)
    else:
        # If state is absent
        if role.exists:
            changed = role.drop()

    # Close connection
    client.disconnect_connection()

    # Users will get this in JSON output after execution
    module.exit_json(changed=changed, executed_statements=executed_statements)


if __name__ == "__main__":
    main()
