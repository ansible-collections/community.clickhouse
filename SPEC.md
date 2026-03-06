# community.clickhouse — Technical Specification

## Collection Identity

| Field | Value |
|---|---|
| Namespace | `community` |
| Name | `clickhouse` |
| Full name | `community.clickhouse` |
| License | GPL-3.0-or-later |
| Required Python package | `clickhouse-driver` |
| Optional Python packages | `pyyaml` (for YAML config parsing), `xmltodict` (for XML config parsing) |

Current version, min ansible-core, and Python requirements are in `galaxy.yml` and `meta/runtime.yml`.

---

## Repository Layout

```
community/clickhouse/
├── galaxy.yml                         # Collection manifest (version, deps, tags)
├── meta/
│   ├── runtime.yml                    # min ansible-core version constraint
│   ├── execution-environment.yml      # EE build metadata
│   ├── ee-requirements.txt            # clickhouse-driver
│   └── ee-bindep.txt                  # (empty — no system deps)
├── plugins/
│   ├── doc_fragments/
│   │   └── client_inst_opts.py        # Shared connection param docs
│   ├── module_utils/
│   │   └── clickhouse.py              # Shared helper functions
│   └── modules/
│       ├── clickhouse_client.py       # Execute arbitrary SQL
│       ├── clickhouse_cfg_info.py     # Read config file as JSON
│       ├── clickhouse_db.py           # Manage databases
│       ├── clickhouse_grants.py       # Manage privileges
│       ├── clickhouse_info.py         # Gather server info
│       ├── clickhouse_quota.py        # Manage quotas
│       ├── clickhouse_role.py         # Manage roles
│       └── clickhouse_user.py         # Manage users
├── tests/
│   ├── integration/
│   │   ├── inventory                  # Static inventory for integration tests
│   │   └── targets/
│   │       ├── setup_clickhouse/      # Install ClickHouse (test dependency)
│   │       ├── clickhouse_client/
│   │       ├── clickhouse_cfg_info/
│   │       ├── clickhouse_db/
│   │       ├── clickhouse_grants/
│   │       ├── clickhouse_info/
│   │       ├── clickhouse_quota/
│   │       ├── clickhouse_role/
│   │       └── clickhouse_user/
│   └── unit/
│       └── plugins/
│           ├── modules/
│           │   └── test_clickhouse_client.py
│           └── module_utils/
│               ├── test_clickhouse.py
│               ├── test_clickhouse_grants.py
│               └── test_clickhouse_quota.py
├── changelogs/
│   ├── config.yaml                    # antsibull-changelog config
│   └── fragments/                     # Per-PR changelog entries
├── docs/docsite/
│   └── links.yml                      # Edit links and extra nav links
├── .github/
│   ├── workflows/
│   │   └── ansible-test-plugins.yml   # Main CI pipeline
│   ├── ISSUE_TEMPLATE/                # Bug/feature/docs templates
│   ├── CODEOWNERS
│   └── dependabot.yml
├── .ansible-lint                      # Lint config (excludes all non-role paths)
├── requirements.txt                   # clickhouse-driver
├── CONTRIBUTING.md
├── MAINTAINING.md
└── REVIEW_CHECKLIST.md
```

---

## Modules

All modules share connection parameters via the `client_inst_opts` doc fragment (see below). All modules except `clickhouse_client` support `check_mode`.

### clickhouse_client

**File:** `plugins/modules/clickhouse_client.py`
**check_mode:** No (always reports `changed=True`)
**Purpose:** Execute an arbitrary SQL query using the `clickhouse-driver` `Client.execute()` interface.

**Key parameters:**

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `execute` | str | required | SQL query to run |
| `execute_kwargs` | dict | `{}` | Extra kwargs passed to `Client.execute()` |
| `set_settings` | dict | `{}` | Session settings applied before execution |

**Return values:**

| Key | Description |
|---|---|
| `substituted_query` | The query after parameter substitution |
| `result` | List of rows returned by the query |
| `statistics` | Dict: `processed_rows`, `processed_bytes`, `elapsed_ns`, `server_duration_ms`, `client_duration_ms` |

**Caveats:**
- Cannot determine whether a query modified state, so always returns `changed=True`.
- Converts unsupported Python types in results: `UUID` → `str`, `Decimal` → `str`, `IPv4/IPv6Address` → `str`.

---

### clickhouse_cfg_info

**File:** `plugins/modules/clickhouse_cfg_info.py`
**check_mode:** Yes
**Purpose:** Read a ClickHouse config file (YAML or XML) from disk and return its content as a dict.

**Key parameters:**

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `path` | str | required | Absolute path to config file (`.yaml`/`.yml` or `.xml`) |

**Return values:**

| Key | Description |
|---|---|
| `config` | Dict representation of the config file |

**Caveats:**
- Requires `pyyaml` for YAML files; `xmltodict` for XML files.
- Does not connect to ClickHouse server; reads file directly from the managed host.

---

### clickhouse_db

**File:** `plugins/modules/clickhouse_db.py`
**check_mode:** Yes
**Purpose:** Create, drop, or rename a ClickHouse database.

**Key parameters:**

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `name` | str | required | Database name |
| `state` | str | `present` | `present` / `absent` / `rename` |
| `engine` | str | omit | Database engine (immutable after creation) |
| `cluster` | str | omit | Run with `ON CLUSTER <cluster>` |
| `target` | str | omit | New name when `state=rename` |
| `comment` | str | omit | Database comment |

**Return values:**

| Key | Description |
|---|---|
| `executed_statements` | List of SQL statements that were executed |

---

### clickhouse_grants

**File:** `plugins/modules/clickhouse_grants.py`
**check_mode:** Yes (also supports `diff` mode)
**Purpose:** Grant or revoke privileges for a ClickHouse user or role.

**Key parameters:**

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `grantee` | str | required | Target user or role name |
| `state` | str | `present` | `present` / `absent` |
| `privileges` | list of dicts | required for `present` | Each dict: `object` (`*.*`/`db.*`/`db.table`), `privs` (dict of priv→grant_option bool), `grant_option` (bool) |
| `exclusive` | bool | `false` | If true, revoke all existing grants before applying new ones |
| `cluster` | str | omit | `ON CLUSTER` support |

**Return values:**

| Key | Description |
|---|---|
| `executed_statements` | List of GRANT/REVOKE statements executed |
| `diff` | Before/after state (available in diff/check mode) |

**Caveats:**
- Supports column-level grants: `SELECT(col1, col2)`.
- Uses set-difference logic: only issues statements for changes.
- Grantee names with special characters are sanitized.

---

### clickhouse_info

**File:** `plugins/modules/clickhouse_info.py`
**check_mode:** Yes
**Purpose:** Gather ClickHouse server information from system tables; does not modify server state.

**Key parameters:**

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `limit` | list | omit | Restrict which info sections to return |

**Return values (all optional based on `limit`):**

| Key | Source / Notes |
|---|---|
| `driver` | clickhouse-driver version |
| `version` | Server version: `raw`, `year`, `feature`, `maintenance`, `build`, `type` |
| `databases` | `system.databases`, keyed by name |
| `users` | `system.users` with roles and grants |
| `roles` | `system.roles` with grants |
| `settings` | `system.settings` |
| `clusters` | `system.clusters` (hierarchical: cluster→shards→replicas) |
| `tables` | `system.tables` nested by database |
| `merge_tree_settings` | `system.merge_tree_settings` |
| `dictionaries` | `system.dictionaries` |
| `quotas` | `system.quotas` |
| `settings_profiles` | `system.settings_profiles` |
| `settings_profile_elements` | `system.settings_profile_elements` (by profiles/users/roles) |
| `storage_policies` | `system.storage_policies` |
| `grants` | `system.grants` organized by users and roles |

**Caveats:**
- Returns privilege errors (code 497) gracefully rather than failing.

---

### clickhouse_quota

**File:** `plugins/modules/clickhouse_quota.py`
**check_mode:** Yes
**Purpose:** Create or remove ClickHouse quotas with interval-based limits.

**Key parameters:**

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `name` | str | required | Quota name |
| `state` | str | `present` | `present` / `absent` |
| `cluster` | str | omit | `ON CLUSTER` support |
| `keyed_by` | str | omit | Key type: `user_name`, `ip_address`, `client_key`, combinations |
| `limits` | list of dicts | omit | Each dict: `interval` (e.g. `"1 day"`), `randomized_start` (bool), `max` (dict of limit types), `no_limits` (bool), `tracking_only` (bool) |
| `apply_to` | list | omit | Users/roles the quota applies to |
| `apply_to_mode` | str | `listed_only` | `listed_only` / `all` / `all_except_listed` |

**Return values:**

| Key | Description |
|---|---|
| `executed_statements` | List of SQL statements executed |

**Caveats:**
- Uses regex-based parsing to normalize and compare complex quota definitions for idempotency.
- `no_limits` and `tracking_only` are mutually exclusive with `max`.

---

### clickhouse_role

**File:** `plugins/modules/clickhouse_role.py`
**check_mode:** Yes
**Purpose:** Create or drop ClickHouse roles with optional settings constraints.

**Key parameters:**

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `name` | str | required | Role name |
| `state` | str | `present` | `present` / `absent` |
| `cluster` | str | omit | `ON CLUSTER` support |
| `settings` | list of str | omit | Settings with constraints, e.g. `"max_memory_usage = 15000 MIN 15000 MAX 16000 READONLY"` |

**Return values:**

| Key | Description |
|---|---|
| `executed_statements` | List of SQL statements executed |

**Caveats:**
- Parses `SHOW CREATE ROLE` output to detect existing settings and determine idempotency.
- Normalizes `READONLY` vs `CONST` and profile name casing.
- Uses `ALTER ROLE` to update settings in-place (rather than drop/recreate).

---

### clickhouse_user

**File:** `plugins/modules/clickhouse_user.py`
**check_mode:** Yes
**Purpose:** Create, drop, or modify ClickHouse users including passwords, host restrictions, roles, and settings.

**Key parameters:**

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `name` | str | required | User name |
| `state` | str | `present` | `present` / `absent` |
| `password` | str | omit | Plaintext or pre-hashed password |
| `type_password` | str | `sha256_password` | Password type/hash algorithm |
| `update_password` | str | `on_create` | `on_create` / `always` |
| `cluster` | str | omit | `ON CLUSTER` support |
| `user_hosts` | list of dicts | omit | Each dict: `type` (ANY/LOCAL/IP/LIKE/NAME/REGEXP), `value` |
| `settings` | list of dicts | omit | Settings at login with optional constraints |
| `roles` | list of str | omit | Roles to grant to user |
| `roles_mode` | str | `listed_only` | `listed_only` / `append` / `remove` |
| `default_roles` | list of str | omit | Roles set as default |
| `default_roles_mode` | str | `listed_only` | `listed_only` / `append` / `remove` |

**Return values:**

| Key | Description |
|---|---|
| `executed_statements` | List of SQL statements executed |

**Caveats:**
- Fetches current settings from `system.settings_profile_elements` for idempotent comparison.
- `update_password: on_create` skips password update if user already exists.

---

## Module Utilities

**File:** `plugins/module_utils/clickhouse.py`
**License:** BSD 2-Clause

### Constants

```python
PRIV_ERR_CODE = 497  # ClickHouse error code for insufficient privileges
```

### Functions

```python
def client_common_argument_spec() -> dict:
    """Returns AnsibleModule argument_spec dict with connection params:
       login_host (default: 'localhost'), login_port, login_db,
       login_user, login_password (no_log), client_kwargs (dict, default: {})
    """

def get_main_conn_kwargs(module: AnsibleModule) -> dict:
    """Translates module.params connection keys to clickhouse-driver Client() kwargs."""

def check_clickhouse_driver(module: AnsibleModule) -> None:
    """Calls module.fail_json() if clickhouse-driver is not installed."""

def version_clickhouse_driver() -> str:
    """Returns the installed clickhouse-driver version string."""

def connect_to_db_via_client(module, main_conn_kwargs, client_kwargs) -> Client:
    """Creates and returns a clickhouse-driver Client connection object."""

def execute_query(module, client, query, execute_kwargs=None, set_settings=None):
    """Executes a query; returns result list or PRIV_ERR_CODE (497) on privilege error.
       Other errors call module.fail_json().
    """

def get_server_version(module, client) -> dict:
    """Returns dict: {raw, year, feature, maintenance, build, type}"""
```

---

## Documentation Fragment

**File:** `plugins/doc_fragments/client_inst_opts.py`

All modules extend this fragment (`extends_documentation_fragment: community.clickhouse.client_inst_opts`). It documents the following shared connection parameters:

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `login_host` | str | `localhost` | ClickHouse server hostname |
| `login_port` | int | omit | Server port (driver default: 9000) |
| `login_db` | str | omit | Database to connect to |
| `login_user` | str | omit | Login username |
| `login_password` | str | omit | Login password (`no_log: true`) |
| `client_kwargs` | dict | `{}` | Extra keyword arguments passed directly to `clickhouse_driver.Client()` |

---

## Architectural Patterns & Conventions

### Idempotency Strategy
All state-management modules (db, user, role, quota, grants) follow this pattern:
1. Query the relevant `system.*` table to fetch current state.
2. Compare desired state with current state.
3. Only execute `CREATE`, `ALTER`, or `DROP` statements if a difference is detected.
4. Return the list of executed statements in `executed_statements`.

### Class-Based Module Pattern
Each state-management module encapsulates logic in a class (e.g., `ClickHouseDB`, `ClickHouseUser`, `ClickHouseRole`, `ClickHouseQuota`). These classes hold the client connection and implement methods like `get()`, `create()`, `drop()`, `update()`.

### Cluster Parameter Pattern
When `cluster` is specified, DDL statements are appended with `ON CLUSTER <cluster_name>`. This is consistent across all modules that support cluster deployments.

### Error Handling
- Privilege errors (ClickHouse error code 497) are caught and returned as `PRIV_ERR_CODE` rather than failing the module.
- All other query errors call `module.fail_json()` with a message using `to_native(e)`.
- Driver availability is checked at module start via `check_clickhouse_driver()`.

### Type Conversion (clickhouse_client only)
Before returning query results to Ansible, these Python types are converted to strings:
- `uuid.UUID` → `str`
- `decimal.Decimal` → `str`
- `ipaddress.IPv4Address` / `IPv6Address` → `str`

### `executed_statements` Return Pattern
All state-management modules return the list of SQL statements they executed. In check mode, the list contains the statements that *would* be executed. This provides a built-in audit trail.

### `version_added` Requirement
Every new module and every new parameter must include a `version_added: 'x.y.z'` field in its documentation block. This is enforced during PR review.

---

## Testing

### Integration Tests

**Tool:** `ansible-test integration`
**Location:** `tests/integration/targets/`
**Inventory:** `tests/integration/inventory` (static)

Each target directory contains:
```
<target>/
├── meta/main.yml      # Dependencies (e.g., requires setup_clickhouse)
└── tasks/
    ├── main.yml       # Entry point; includes other task files
    ├── initial.yml    # Core test cases
    └── *.yml          # Feature-specific files (e.g., data_types.yml, advanced.yml)
```

**Integration test targets:**

| Target | Tests |
|---|---|
| `setup_clickhouse` | Installs ClickHouse server (test infrastructure) |
| `clickhouse_client` | Query execution, data types, parameter substitution, nested fields, session settings |
| `clickhouse_cfg_info` | YAML and XML config file reading |
| `clickhouse_db` | Database create/drop/rename |
| `clickhouse_grants` | GRANT/REVOKE, exclusive mode, cluster grants |
| `clickhouse_info` | All info sections, limit filtering |
| `clickhouse_quota` | Quota create/drop, interval limits, keyed quotas |
| `clickhouse_role` | Role create/drop, settings constraints, advanced scenarios |
| `clickhouse_user` | User create/drop, host restrictions, roles, settings |

**Standard test task pattern:**
```yaml
- name: <description>
  register: result
  community.clickhouse.<module>:
    login_host: localhost
    <params>

- name: Assert expected outcome
  ansible.builtin.assert:
    that:
      - result is changed       # or `result is not changed`
      - result.<key> == <value>
```

### Unit Tests

**Tool:** `ansible-test units`
**Location:** `tests/unit/plugins/`

| File | Covers |
|---|---|
| `modules/test_clickhouse_client.py` | `is_uuid()`, `replace_val_in_tuple()`, `vals_to_supported()` type conversion |
| `module_utils/test_clickhouse.py` | Shared utility functions |
| `module_utils/test_clickhouse_grants.py` | `GRANT_REGEX` pattern matching, `ClickHouseGrants.get()` parsing of `SHOW GRANTS` output |
| `module_utils/test_clickhouse_quota.py` | Quota parsing and normalization logic |

### Local Testing Commands

```bash
# Sanity checks for a specific file
ansible-test sanity plugins/modules/clickhouse_db.py --docker -v

# Unit tests
ansible-test units tests/unit/plugins/modules/test_clickhouse_client.py --docker -v

# Integration test for a specific module
ansible-test integration clickhouse_db --docker <container> -v
```

---

## CI/CD

**File:** `.github/workflows/ansible-test-plugins.yml`
**Triggers:** Push/PR on `plugins/**`, `tests/**`; daily scheduled run.

Three job types run in a matrix (see workflow file for current version lists):
- **Sanity** — `ansible-test sanity --docker`
- **Units** — `ansible-test units` with coverage uploaded to codecov (not required for merge)
- **Integration** — Docker-based, matrix covers multiple ansible-core and ClickHouse versions

---

## Development Conventions

### Changelog Fragments
Every PR that changes module behavior (not docs/tests/refactoring) must include a changelog fragment in `changelogs/fragments/<description>.yaml`. Format managed by `antsibull-changelog`. Fragments are consumed and deleted at release time (`keep_fragments: false`).

Fragment sections: `major_changes`, `minor_changes`, `breaking_changes`, `deprecated_features`, `removed_features`, `security_fixes`, `bugfixes`, `known_issues`.

### Versioning
The collection follows SemVer:
- Major: breaking changes (e.g., dropping Ansible version support)
- Minor: new modules, new non-breaking parameters
- Patch: bug fixes

### PR Requirements
All three test types (sanity, units, integration) must pass before merging. New modules and new parameters must have `version_added` set to the next planned release version.
