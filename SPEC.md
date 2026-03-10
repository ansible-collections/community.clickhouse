# community.clickhouse — Technical Specification

## Collection Identity

| Field | Value |
|---|---|
| Namespace | `community` |
| Name | `clickhouse` |
| Full name | `community.clickhouse` |
| License | GPL-3.0-or-later |
| Required Python package | `clickhouse-driver` |
| Optional Python packages | `pyyaml` (YAML config parsing), `xmltodict` (XML config parsing) |

Current version, min ansible-core, and Python requirements are in `galaxy.yml` and `meta/runtime.yml`.

---

## Repository Layout

- `plugins/modules/` — modules
- `plugins/module_utils/clickhouse.py` — shared utilities
- `plugins/doc_fragments/client_inst_opts.py` — shared connection parameter docs
- `tests/integration/targets/` — integration test targets mirroring module names; `setup_clickhouse/` installs the ClickHouse server and is declared as a dependency by every other target
- `tests/unit/plugins/` — unit tests

---

## Modules

- All modules share connection parameters via the `client_inst_opts` doc fragment.
- All modules except `clickhouse_client` support `check_mode`.
- For full parameter and return value reference, read each module's `DOCUMENTATION` and `RETURN` blocks directly.

| Module | Purpose | Notable caveats |
|---|---|---|
| `clickhouse_client` | Execute arbitrary SQL via `Client.execute()` | No `check_mode`; always `changed=True`; converts `UUID`, `Decimal`, `IPv4/IPv6Address` to `str` |
| `clickhouse_cfg_info` | Read a ClickHouse config file (YAML/XML) as a dict | Requires `pyyaml` or `xmltodict`; reads from disk, no server connection |
| `clickhouse_db` | Create, drop, or rename a database | Engine is immutable after creation |
| `clickhouse_grants` | Grant or revoke privileges for a user or role | Set-difference logic — only issues statements for actual changes; supports column-level grants |
| `clickhouse_info` | Gather server info from system tables | Returns privilege errors (code 497) gracefully rather than failing |
| `clickhouse_quota` | Create or remove quotas with interval-based limits | Uses regex-based parsing to normalize complex quota definitions for idempotency |
| `clickhouse_role` | Create or drop roles with optional settings constraints | Parses `SHOW CREATE ROLE`; uses `ALTER ROLE` (not drop/recreate) to update settings |
| `clickhouse_user` | Create, drop, or modify users (passwords, hosts, roles, settings) | `update_password: on_create` skips password update if user already exists |

---

## Module Utilities

**File:** `plugins/module_utils/clickhouse.py`

```python
PRIV_ERR_CODE = 497  # ClickHouse error code for insufficient privileges

def client_common_argument_spec() -> dict
    # Returns argument_spec with connection params:
    # login_host (default: 'localhost'), login_port, login_db,
    # login_user, login_password (no_log), client_kwargs (dict, default: {})

def get_main_conn_kwargs(module) -> dict
    # Translates module.params connection keys to clickhouse-driver Client() kwargs

def check_clickhouse_driver(module) -> None
    # Calls module.fail_json() if clickhouse-driver is not installed

def version_clickhouse_driver() -> str
    # Returns installed clickhouse-driver version string

def connect_to_db_via_client(module, main_conn_kwargs, client_kwargs) -> Client
    # Creates and returns a clickhouse-driver Client connection object

def execute_query(module, client, query, execute_kwargs=None, set_settings=None)
    # Executes a query; returns result list or PRIV_ERR_CODE on privilege error
    # All other errors call module.fail_json()

def get_server_version(module, client) -> dict
    # Returns {raw, year, feature, maintenance, build, type}
```

---

## Documentation Fragment

**File:** `plugins/doc_fragments/client_inst_opts.py`

All modules extend this via `extends_documentation_fragment: community.clickhouse.client_inst_opts`. Shared connection parameters:

| Parameter | Default | Notes |
|---|---|---|
| `login_host` | `localhost` | ClickHouse server hostname |
| `login_port` | omit | Server port (driver default: 9000) |
| `login_db` | omit | Database to connect to |
| `login_user` | omit | Login username |
| `login_password` | omit | `no_log: true` |
| `client_kwargs` | `{}` | Extra kwargs passed directly to `clickhouse_driver.Client()` |

---

## Architectural Patterns

### Module Bootstrap Pattern

Every module follows the same structure:

```python
argument_spec = client_common_argument_spec()
argument_spec.update({...})                     # module-specific params
module = AnsibleModule(argument_spec=argument_spec, ...)
check_clickhouse_driver(module)
client = connect_to_db_via_client(module, ...)
```

### Idempotency Strategy

State-management modules (db, user, role, quota, grants) all follow:
1. Query the relevant `system.*` table to fetch current state.
2. Compare desired state with current state.
3. Execute `CREATE`, `ALTER`, or `DROP` only if a difference is detected.
4. Return executed statements in `executed_statements` (populated in check_mode too).

### Class-Based Module Pattern

Each state-management module encapsulates logic in a class (e.g., `ClickHouseDB`, `ClickHouseUser`). Classes hold the client connection and implement methods like `get()`, `create()`, `drop()`, `update()`.

### Error Handling

- Privilege errors (code 497) are caught and returned as `PRIV_ERR_CODE` rather than failing.
- All other query errors call `module.fail_json()` with `to_native(e)`.
- Driver availability checked at module start via `check_clickhouse_driver()`.

### Cluster Parameter

When `cluster` is specified, DDL statements are appended with `ON CLUSTER <cluster_name>`. Consistent across all modules that accept the parameter.

---

## Testing

### Integration Test Pattern

Each target in `tests/integration/targets/` declares `setup_clickhouse` as a dependency in `meta/main.yml`. Standard task pattern:

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

Verify database state by following up with `community.clickhouse.clickhouse_client`, registering the result, and asserting on it.

### Test Commands

```bash
# Sanity
ansible-test sanity plugins/modules/clickhouse_db.py --docker -v

# Units
ansible-test units tests/unit/plugins/modules/test_clickhouse_client.py --docker -v

# Integration
ansible-test integration clickhouse_db --docker default -v
```

---

## Development Conventions

### Changelog Fragments

Every PR that changes module behavior needs a fragment in `changelogs/fragments/<description>.yaml`. Docs, tests, and refactoring PRs are exempt. Fragments are consumed at release time (`keep_fragments: false`).

Valid sections:
- `major_changes`, `minor_changes`, `bugfixes`
- `breaking_changes`, `deprecated_features`, `removed_features`
- `security_fixes`, `known_issues`

### Versioning

- **Major** — breaking changes (e.g., dropping Ansible version support)
- **Minor** — new modules, new non-breaking parameters
- **Patch** — bug fixes

### PR Requirements

All three test types (sanity, units, integration) must pass. New modules and new parameters must have `version_added: 'x.y.z'` set to the next planned release version.
