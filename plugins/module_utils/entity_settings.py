from __future__ import absolute_import, division, print_function

__metaclass__ = type

from ansible.module_utils.basic import AnsibleModule

from ansible_collections.community.clickhouse.plugins.module_utils.clickhouse import (
    execute_query
)


class EntitySettings():
    def __init__(self, module: AnsibleModule, client, entity_name: str, entity_type: str):
        self.module = module
        self.client = client
        self.name = entity_name
        self.entity_type = entity_type.lower()

        if self.entity_type not in ("user", "role"):
            self.module.fail_json(msg=f"entity_type must be 'user' or 'role', got {entity_type}")

    def _get_where_column(self):
        return "user_name" if self.entity_type == "user" else "role_name"

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
            parts.append(f"PROFILE '{profile}'")

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
