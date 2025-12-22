#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright: (c) 2025, John Garland (@johnnyg) <johnnybg@gmail.com>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function

__metaclass__ = type

DOCUMENTATION = r"""
---
module: clickhouse_quota

short_description: Creates or removes a ClickHouse quota

description:
  - Creates or removes a ClickHouse quota.

attributes:
  check_mode:
    description: Supports check_mode.
    support: full

version_added: '1.1.0'

author:
  - John Garland (@johnnyg)

extends_documentation_fragment:
  - community.clickhouse.client_inst_opts

options:
  state:
    description:
      - Quota state.
      - V(present) creates the quota if it does not exist.
      - V(absent) deletes the quota if it exists.
    type: str
    choices: ['present', 'absent']
    default: 'present'
  name:
    description:
      - Quota name to add or remove.
    type: str
    required: true
  cluster:
    description:
      - Run the command on all cluster hosts.
      - If the cluster is not configured, the command will crash with an error.
    type: str
  keyed_by:
    description:
      - Keys the quota by the specified key (default is to not key).
    type: str
    choices:
      - user_name
      - ip_address
      - client_key
      - client_key,user_name
      - client_key,ip_address
  limits:
    description:
      - The limits that this quota should enforce.
    type: list
    elements: dict
    suboptions:
      randomized_start:
        description:
          - Whether this interval's start should be randomized.
          - Intervals always start at the same time if not randomized.
        type: bool
        default: false
      interval:
        description:
          - The interval to apply the following quotas on.
          - This is in the format C(<number> <unit>).
          - Where unit is one of second, minute, hour, day, week, month, quarter or year.
        type: str
        required: true
      max:
        description:
          - Maximum values to apply to this interval in this quota.
          - At least one key must be specified.
          - Mutually exclusive with O(limits.no_limits) and O(limits.tracking_only).
        type: dict
        suboptions:
          queries:
            description:
              - Maximum number of queries to enforce in this interval.
            type: int
          query_selects:
            description:
              - Maximum number of query selects to enforce in this interval.
            type: int
          query_inserts:
            description:
              - Maximum number of query inserts to enforce in this interval.
            type: int
          errors:
            description:
              - Maximum number of errors to enforce in this interval.
            type: int
          result_rows:
            description:
              - Maximum number of result rows to enforce in this interval.
            type: int
          result_bytes:
            description:
              - Maximum number of result bytes to enforce in this interval.
            type: int
          read_rows:
            description:
              - Maximum number of rows read to enforce in this interval.
            type: int
          read_bytes:
            description:
              - Maximum number of bytes read to enforce in this interval.
            type: int
          written_bytes:
            description:
              - Maximum number of bytes written to enforce in this interval.
            type: int
          execution_time:
            description:
              - Maximum number of execution time to enforce in this interval.
            type: float
          failed_sequential_authentications:
            description:
              - Maximum number of failed sequential authentications to enforce in this interval.
            type: int
      no_limits:
        description:
          - Don't apply any limits.
          - Mutually exclusive with O(limits.max) and O(limits.tracking_only).
        type: bool
      tracking_only:
        description:
          - Just track usage instead of enforcing.
          - Mutually exclusive with O(limits.max) and O(limits.no_limits).
        type: bool
  apply_to:
    description:
      - Apply this quota to the following list of users/roles dependent on O(apply_to_mode).
      - Can include special keywords of default and current_user or the name of an actual user or role.
      - Is an error to specify this if O(apply_to_mode=all).
    type: list
    elements: str
  apply_to_mode:
    description:
      - When V(listed_only) (default), the quota will only apply to the users/roles specified in O(apply_to).
      - When V(all), the quota will only apply to B(all) users/roles.
      - When V(all_except_listed), the quota will only apply to B(all) the users/roles except those specified in O(apply_to).
    type: str
    choices: ['listed_only', 'all', 'all_except_listed']
    default: 'listed_only'
"""

EXAMPLES = r"""
- name: Create quota
  community.clickhouse.clickhouse_quota:
    name: test_quota
    state: present

- name: Create a quota with limits
  community.clickhouse.clickhouse_quota:
    name: test_quota
    state: present
    limits:
      - interval: 5 minute
        max:
          queries: 100
          execution_time: 100
        apply_to:
          - one_role
          - another_role
    cluster: test_cluster

- name: Remove quota
  community.clickhouse.clickhouse_quota:
    name: test_quota
    state: absent
"""

RETURN = r"""
executed_statements:
  description:
  - Data-modifying executed statements.
  returned: on success
  type: list
  sample: ['CREATE QUOTA test_quota']
"""

import copy
import re
from operator import itemgetter

from ansible.module_utils.basic import AnsibleModule
from ansible_collections.community.clickhouse.plugins.module_utils.clickhouse import (
    check_clickhouse_driver,
    client_common_argument_spec,
    connect_to_db_via_client,
    execute_query,
    get_main_conn_kwargs,
)

_VALID_NAME_REGEX = re.compile(r"^[^'\"`;\0]+$")
_POSSIBLY_ESCAPED_NAME_REGEX = r"(?:`(?:[^`]+)`)|(?:\w+)"
_KEYED_BY_VALUES = [
    "user_name",
    "ip_address",
    "client_key, ?user_name",
    "client_key, ?ip_address",
    "client_key",  # needs to be after so it doesn't match first
]
_CREATE_QUOTA_REGEX = re.compile(
    rf"^CREATE QUOTA (?P<name>{_POSSIBLY_ESCAPED_NAME_REGEX})"
    rf"(?: ON CLUSTER (?P<cluster>{_POSSIBLY_ESCAPED_NAME_REGEX}))?"
    rf"(?: KEYED BY (?P<keyed_by>{'|'.join(_KEYED_BY_VALUES)}))?"
)

_NUMBER_REGEX = r"(?:-?\d+\.?\d*)"
_INTERVAL_UNITS = [
    "second",
    "minute",
    "hour",
    "day",
    "week",
    "month",
    "quarter",
    "year",
]
_MAX_LIMIT_TYPES = [
    "queries",
    "query_selects",
    "query_inserts",
    "errors",
    "result_rows",
    "result_bytes",
    "read_rows",
    "read_bytes",
    "written_bytes",
    "execution_time",
    "failed_sequential_authentications",
]
_LIMIT_TYPES = [
    rf"(?:MAX(?:,? (?:{'|'.join(_MAX_LIMIT_TYPES)}) = {_NUMBER_REGEX})+)",
    "NO LIMITS",
    "TRACKING ONLY",
]
_LIMITS_REGEX = re.compile(
    r"FOR (?:(?P<randomized>RANDOMIZED) )?"
    rf"INTERVAL (?P<interval_number>{_NUMBER_REGEX}) (?P<interval_unit>{'|'.join(_INTERVAL_UNITS)}) "
    rf"(?P<limit_type>{'|'.join(_LIMIT_TYPES)})"
)

_ROLES_REGEX = rf"(?:(?:{_POSSIBLY_ESCAPED_NAME_REGEX})(?:, )?)+"
_APPLY_TO_TYPES = [rf"(?:{_ROLES_REGEX})", "ALL", rf"ALL EXCEPT (?:{_ROLES_REGEX})"]
_APPLY_TO_REGEX = re.compile(rf" TO (?P<apply_to>{'|'.join(_APPLY_TO_TYPES)})$")
_USER_OR_ROLE_REGEX = re.compile(rf"(?P<name>{_POSSIBLY_ESCAPED_NAME_REGEX})(?:, ?)?")


_DEFAULT_LIMIT_PARAMS = {
    "randomized_start": False,
    "interval": None,
    "max": {},
    "no_limits": None,
    "tracking_only": None,
}

_DEFAULT_MAX_PARAMS = {
    "queries": None,
    "query_selects": None,
    "query_inserts": None,
    "errors": None,
    "result_rows": None,
    "result_bytes": None,
    "read_rows": None,
    "read_bytes": None,
    "written_bytes": None,
    "execution_time": None,
    "failed_sequential_authentications": None,
}

_DEFAULT_PARAMS = {
    "cluster": None,
    "keyed_by": None,
    "limits": [],
    "apply_to": [],
    "apply_to_mode": "listed_only",
}


class ClickHouseQuota:
    _type = "QUOTA"

    def __init__(self, module, client, name):
        if not _VALID_NAME_REGEX.match(name):
            raise ValueError(f"'{name}' is not a valid quota name")
        self.module = module
        self.client = client
        self.name = name
        self.executed_statements = []
        self.exists = self._check_exists()

    def _check_exists(self):
        query = f"SELECT 1 FROM system.{self._type.lower()}s WHERE name = '{self.name}' LIMIT 1"
        result = execute_query(self.module, self.client, query)
        return bool(result)

    def _get_create_statement(self):
        """Get current definition using SHOW CREATE X"""
        if not self.exists:
            return None

        query = f"SHOW CREATE {self._type} '{self.name}'"
        result = execute_query(self.module, self.client, query)
        if result:
            # SHOW CREATE X returns single row with CREATE statement
            return result[0][0]
        return None

    def _needs_altering(self):
        """Check if we need to alter to reach desired"""
        create_statement = self._get_create_statement()
        if create_statement is None:
            return True
        current_params = self._normalize(self._parse_create_statement(create_statement))
        desired_params = self._normalize(self.module.params)

        # For debugging version compatibility issues
        if self.module._verbosity >= 3:  # Only show at high verbosity
            self.module.log(f"Current params (normalized): {current_params}")
            self.module.log(f"Desired params (normalized): {desired_params}")

        return current_params != desired_params

    def _do(self, action):
        if action not in ("CREATE", "ALTER"):
            raise ValueError(
                f"Expected action to be CREATE or ALTER but got '{action}'"
            )

        query = " ".join(self._create_sql_clauses(action))

        self.executed_statements.append(query)

        if not self.module.check_mode:
            execute_query(self.module, self.client, query)

    def create(self):
        """
        Create entity using CREATE X
        Returns whether the entity was created or not
        """
        if self.exists:
            return False

        self._do("CREATE")
        self.exists = True
        return True

    def alter(self):
        """
        Update entity using ALTER X if it needs it
        Returns whether the entity was altered or not
        """
        if not self.exists or not self._needs_altering():
            return False

        self._do("ALTER")
        return True

    def drop(self):
        """Drop entity using DROP X"""
        if not self.exists:
            return False

        query = f"DROP {self._type} '{self.name}'"
        self.executed_statements.append(query)

        if not self.module.check_mode:
            execute_query(self.module, self.client, query)

        self.exists = False
        return True

    def ensure_state(self):
        state = self.module.params["state"]
        if state not in ("present", "absent"):
            raise ValueError(f"Unexpected state '{state}'")

        if state == "present":
            # create or alter role
            # will do nothing is nothing needs to be done
            changed = self.create() or self.alter()
        else:
            # drop if exists
            changed = self.drop()

        return changed

    @staticmethod
    def _parse_create_statement(create_statement):
        match = _CREATE_QUOTA_REGEX.match(create_statement)
        if not match:
            raise ValueError(f"Could not parse '{create_statement}'")
        params = {} | match.groupdict()
        params.pop("name")
        cluster = params["cluster"]
        if cluster is not None:
            params["cluster"] = cluster.strip("`")
        next_search_pos = match.end()
        limits = []
        for match in _LIMITS_REGEX.finditer(create_statement, pos=next_search_pos):
            limit = {}
            groups = match.groupdict()
            limit["randomized_start"] = groups["randomized"] == "RANDOMIZED"
            limit["interval"] = f"{groups['interval_number']} {groups['interval_unit']}"
            limit_type = groups["limit_type"]
            if limit_type == "NO LIMITS":
                limit["no_limits"] = True
            elif limit_type == "TRACKING ONLY":
                limit["tracking_only"] = True
            elif limit_type.startswith("MAX "):
                max_limits = {}
                for max_limit in limit_type[len("MAX ") :].split(", "):
                    key, _part, value = max_limit.partition(" = ")
                    type_fn = float if key == "execution_time" else int
                    max_limits[key] = type_fn(value)
                limit["max"] = max_limits
            else:
                raise ValueError(f"Invalid limit type '{limit_type}'")
            limits.append(limit)
            next_search_pos = match.end()
        params["limits"] = limits
        match = _APPLY_TO_REGEX.match(create_statement, pos=next_search_pos)
        if match:
            groups = match.groupdict()
            apply_to = groups["apply_to"]
            if apply_to == "ALL":
                params["apply_to_mode"] = "all"
                apply_to = ""
            elif apply_to.startswith("ALL EXCEPT "):
                params["apply_to_mode"] = "all_except_listed"
                apply_to = apply_to[len("ALL EXCEPT ") :]
            else:
                params["apply_to_mode"] = "listed_only"
            params["apply_to"] = [
                match.groupdict()["name"].strip("`")
                for match in _USER_OR_ROLE_REGEX.finditer(apply_to)
            ]
        return params

    @staticmethod
    def _normalize(params):
        normalized = _DEFAULT_PARAMS.copy()
        for key in normalized.keys() & params.keys():
            value = params[key]
            if value is not None:
                if key == "limits":
                    normalized_limits = []
                    for limit_params in value:
                        normalized_limit = _DEFAULT_LIMIT_PARAMS.copy()
                        for limit_key in normalized_limit.keys() & limit_params.keys():
                            limit_value = limit_params[limit_key]
                            if limit_value is not None:
                                normalized_limit[limit_key] = limit_value
                        normalized_limits.append(normalized_limit)
                    normalized[key] = normalized_limits
                else:
                    normalized[key] = copy.deepcopy(value)
        keyed_by = normalized["keyed_by"]
        if keyed_by:
            normalized["keyed_by"] = ",".join(
                key.strip() for key in keyed_by.split(",")
            )
        if (
            normalized["apply_to_mode"] == "all_except_listed"
            and not normalized["apply_to"]
        ):
            normalized["apply_to_mode"] = "all"
        # no limits is the default so they automatically get removed
        normalized["limits"] = [
            limit for limit in normalized["limits"] if not limit.get("no_limits")
        ]
        for limit in normalized["limits"]:
            max_limit = limit["max"]
            if max_limit:
                limit["max"] = _DEFAULT_MAX_PARAMS | max_limit
        normalized["limits"].sort(key=itemgetter("interval"))
        normalized["apply_to"].sort()
        return normalized

    def _create_sql_clauses(self, action):
        sql_clauses = [f"{action} {self._type} '{self.name}'"]

        cluster = self.module.params["cluster"]
        if cluster:
            sql_clauses.append(f"ON CLUSTER '{cluster}'")

        keyed_by = self.module.params.get("keyed_by")
        if keyed_by:
            sql_clauses.append(f"KEYED BY {keyed_by}")

        limits_sql_clauses = []
        for limit in self.module.params["limits"] or []:
            sql_clause = ["FOR"]
            if limit.get("randomized_start", False):
                sql_clause.append("RANDOMIZED")
            sql_clause.append(f"INTERVAL {limit['interval']}")
            max_limits = {
                key: value
                for key, value in (limit.get("max") or {}).items()
                if value is not None
            }
            if max_limits:
                sql_clause.append("MAX")
                sql_clause.append(
                    ", ".join(f"{key} = {value}" for key, value in max_limits.items())
                )
            elif limit.get("no_limits"):
                sql_clause.append("NO LIMITS")
            elif limit.get("tracking_only"):
                sql_clause.append("TRACKING ONLY")
            else:
                raise ValueError(
                    "One of max or no_limits or tracking_only needs to specified"
                )
            limits_sql_clauses.append(" ".join(sql_clause))
        if limits_sql_clauses:
            sql_clauses.append(", ".join(limits_sql_clauses))

        apply_to = self.module.params.get("apply_to", [])
        apply_to_mode = self.module.params["apply_to_mode"]
        if apply_to_mode == "all_except_listed" and not apply_to:
            apply_to_mode = "all"
        if apply_to and apply_to_mode == "all":
            raise ValueError(
                "Cannot specify list of user/roles to apply to when apply_to_mode == all"
            )
        if apply_to_mode == "all":
            sql_clauses.append("TO ALL")
        elif apply_to:
            sql_clauses.append("TO")
            if apply_to_mode == "all_except_listed":
                sql_clauses.append("ALL EXCEPT")
            sql_clauses.append(", ".join(apply_to))

        return sql_clauses


def main():
    # Set up arguments.
    # If there are common arguments shared across several modules,
    # create the common_argument_spec() function under plugins/module_utils/*
    # and invoke here to return a dict with those arguments
    argument_spec = client_common_argument_spec()
    argument_spec.update(
        state=dict(type="str", choices=["present", "absent"], default="present"),
        name=dict(type="str", required=True),
        cluster=dict(type="str", default=None),
        keyed_by=dict(
            type="str",
            choices=[
                "user_name",
                "ip_address",
                "client_key",
                "client_key,user_name",
                "client_key,ip_address",
            ],
        ),
        limits=dict(
            type="list",
            elements="dict",
            options=dict(
                randomized_start=dict(type="bool", default=False),
                interval=dict(type="str", required=True),
                max=dict(
                    type="dict",
                    options=dict(
                        queries=dict(type="int"),
                        query_selects=dict(type="int"),
                        query_inserts=dict(type="int"),
                        errors=dict(type="int"),
                        result_rows=dict(type="int"),
                        result_bytes=dict(type="int"),
                        read_rows=dict(type="int"),
                        read_bytes=dict(type="int"),
                        written_bytes=dict(type="int"),
                        execution_time=dict(type="float"),
                        failed_sequential_authentications=dict(type="int"),
                    ),
                    required_one_of=[
                        (
                            "queries",
                            "query_selects",
                            "query_inserts",
                            "errors",
                            "result_rows",
                            "result_bytes",
                            "read_rows",
                            "read_bytes",
                            "written_bytes",
                            "execution_time",
                            "failed_sequential_authentications",
                        )
                    ],
                ),
                no_limits=dict(type="bool"),
                tracking_only=dict(type="bool"),
            ),
            mutually_exclusive=[("max", "no_limits", "tracking_only")],
            required_one_of=[("max", "no_limits", "tracking_only")],
        ),
        apply_to=dict(type="list", elements="str"),
        apply_to_mode=dict(
            type="str",
            choices=["listed_only", "all", "all_except_listed"],
            default="listed_only",
        ),
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
    name = module.params["name"]

    # Will fail if no driver informing the user
    check_clickhouse_driver(module)

    # Connect to DB
    client = connect_to_db_via_client(module, main_conn_kwargs, client_kwargs)

    # Do the job
    quota = ClickHouseQuota(module, client, name)
    changed = quota.ensure_state()

    # Close connection
    client.disconnect_connection()

    # Users will get this in JSON output after execution
    module.exit_json(changed=changed, executed_statements=quota.executed_statements)


if __name__ == "__main__":
    main()
