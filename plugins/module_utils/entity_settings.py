from __future__ import absolute_import, division, print_function

__metaclass__ = type

from ansible.module_utils.basic import AnsibleModule

from ansible_collections.community.clickhouse.plugins.module_utils.clickhouse import (
    execute_query,
)


def get_settings_argument_spec():
    """
    Return a dictionary with connection options.

    The options are commonly used by many modules.
    """
    return dict(
        settings=dict(
            type='dict',
            required=False,
        ),
        profiles=dict(type='list', elements='str'),
    )


class EntitySettings():
    def __init__(self, module: AnsibleModule, client, entity_name: str, entity_type: str):
        self.module = module
        self.client = client
        self.name = entity_name
        self.entity_type = entity_type.lower()

    def _get_where_column(self):
        if self.entity_type == "user":
            return "user_name"
        elif self.entity_type == "role":
            return "role_name"
        elif self.entity_type == "profile":
            return "profile_name"
        else:
            self.module.fail_json(msg=f"entity_type must be 'user', 'role', or 'profile' got {self.entity_type}")

    def fetch(self):
        """Fetch current settings from system.settings_profile_elements."""
        """Returns raw result."""
        column = self._get_where_column()
        query = (
            "SELECT setting_name, value, min, max, writability, inherit_profile "
            f"FROM system.settings_profile_elements "
            f"WHERE {column} = '{self.name}'"
        )

        result = execute_query(self.module, self.client, query)
        return result

    def _normalize_current_settings(self, rows):
        """Convert DB rows into easy-to-compare structures."""
        current_settings = {}
        current_profiles = []

        for row in rows:
            setting_name, value, min, max, writability, inherit_profile = row
            if inherit_profile:
                current_profiles.append(inherit_profile)
            else:
                current_settings[setting_name] = {}
                current_settings[setting_name]['value'] = value
                if min:
                    current_settings[setting_name]['min'] = min
                if max:
                    current_settings[setting_name]['max'] = max
                if writability:
                    current_settings[setting_name]['writability'] = writability

        return current_settings, sorted(current_profiles)

    def compare_and_build_clause(self, desired_settings, desired_profiles):
        # In case only one is passed
        if not desired_settings:
            desired_settings = {}
        if not desired_profiles:
            desired_profiles = []
        current_rows = self.fetch()
        current_settings, current_profiles = self._normalize_current_settings(current_rows)
        desired_settings = self._normalize_settings(desired_settings)

        # Compare
        settings_changed = current_settings != desired_settings
        profiles_changed = set(current_profiles) != set(desired_profiles)
        changed = settings_changed or profiles_changed

        if not changed:
            return False, "", {}

        parts = []

        # PROFILE 'name'
        for profile in desired_profiles:
            if self.entity_type in ('role', 'user'):
                parts.append(f"PROFILE '{profile}'")
            else:
                parts.append(f"INHERIT `{profile}`")

        # SETTINGS key='val' [MIN 'min_val'] [MAX 'max_val'] [WRITABLE|CONST|CHANGEABLE_IN_READONLY]
        for key, setting in desired_settings.items():
            prepared = f"{key}='{setting['value']}'"
            if setting.get('min'):
                prepared += f" MIN '{setting['min']}'"
            if setting.get('max'):
                prepared += f" MAX '{setting['max']}'"
            if setting.get('writability'):
                prepared += f" {setting['writability']}"
            parts.append(f"{prepared}")

        if desired_profiles or desired_settings:
            clause = " SETTINGS " + ", ".join(parts) if parts else ""
        else:
            clause = " SETTINGS NONE"

        # Simple diff for Ansible output
        diff = {
            "before": {"settings": current_settings, "profiles": current_profiles},
            "after": {"settings": desired_settings, "profiles": desired_profiles}
        }

        return changed, clause, diff

    def _validate_setting_fields(self, name, setting):
        """
        Hard to validate nested objects in module.
        """
        if not isinstance(setting, dict):
            self.module.fail_json(
                msg=f"Setting {name} is not dictionary type."
            )
        required_one_of = ['value', 'min', 'max']
        if not any(k in setting for k in required_one_of):
            self.module.fail_json(
                msg=f"Setting '{name}' missing required field. Must have at least one of: {', '.join(required_one_of)}. Got: {setting}"
            )
        # Filter out unexpected keys
        allowed = set(required_one_of + ['writability'])
        extra = set(setting.keys()) - allowed
        if extra:
            self.module.fail_json(
                msg=f"Setting '{name}' has invalid keys: {', '.join(extra)}. Allowed: {', '.join(allowed)}"
            )

    def _normalize_settings(self, settings):
        if not settings:
            return {}
        # Map with DB aliases
        writability_map = {
            'READONLY': 'CONST',
            'CONST': 'CONST'
        }
        normalized = {}

        for setting_name, setting_config in settings.items():
            self._validate_setting_fields(setting_name, setting_config)
            normalized_config = setting_config.copy()

            # Convert numbers to strings (for all fields)
            for prop_name, prop_value in normalized_config.items():
                if isinstance(prop_value, (int, float)):
                    normalized_config[prop_name] = str(prop_value)

            # Writable uppercase and replace aliases
            if normalized_config.get('writability', ''):
                writability_value = normalized_config['writability'].upper()
                normalized_config['writability'] = writability_map.get(
                    writability_value,
                    writability_value
                )

            normalized[setting_name] = normalized_config

        return normalized
