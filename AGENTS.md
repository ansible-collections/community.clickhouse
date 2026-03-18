# AGENTS.md

This file is intended for AI coding agents. It is kept human-readable so contributors can also use it as a quick-reference guide.

When planning or reviewing changes, always check with `REVIEW_CHECKLIST.md` file.

When official documentation is not explicitly provided or it's insufficient, you MUST delegate to the `docs-explorer` subagent (see `.agents/subagents/docs-explorer.md`) to look up current official documentation for the relevant libraries and technologies.

## What This Project Is

An Ansible collection (`community.clickhouse`) providing modules for managing ClickHouse databases. No roles exist — only modules and shared utilities.
To save your session context, see `SPEC.md` for full technical reference only if needed or asked explicitly.

## Development Environment

The collection must reside at `ansible_collections/community/clickhouse/` (relative to a directory on `ANSIBLE_COLLECTIONS_PATHS`) for imports to resolve correctly.

All required packages are listed in `requirements.txt`.

For test commands, patterns, and requirements see `.agents/skills/run-tests/SKILL.md`.

## Architecture

### Shared Foundation

Every module follows the same bootstrap pattern:

```python
argument_spec = client_common_argument_spec()   # from module_utils/clickhouse.py
argument_spec.update({...})                     # module-specific params
module = AnsibleModule(argument_spec=argument_spec, ...)
check_clickhouse_driver(module)
client = connect_to_db_via_client(module, ...)
```

Connection parameters (`login_host`, `login_port`, `login_user`, `login_password`, `login_db`, `client_kwargs`) are defined once in `plugins/doc_fragments/client_inst_opts.py` and shared via `extends_documentation_fragment`.

### Idempotency Pattern

State-management modules (db, user, role, quota, grants) all follow: query `system.*` table → compare current vs desired → execute DDL only if different → return `executed_statements`. The comparison logic lives in module-level classes (e.g., `ClickHouseDB`, `ClickHouseUser`).

### check_mode

All modules support `check_mode` except `clickhouse_client` (which executes arbitrary SQL and cannot know whether it changes state). In check_mode, modules populate `executed_statements` with what *would* run.

### clickhouse_client Type Conversion

Query results containing `uuid.UUID`, `decimal.Decimal`, or `ipaddress.IPv4/IPv6Address` are converted to `str` before returning to Ansible (these types are not JSON-serializable by Ansible's output layer).

## Coding Guidelines

- Follow these software development principles: KISS (Keep It Simple, Stupid), DRY (Don't Repeat Yourself), YAGNI (You Aren't Gonna Need It), Separation of Concerns, Composition over Inheritance, and Convention Over Configuration.
- Prioritize code simplicity and readability over flexibility.
- Favor simple, short, and easily testable functions with no side effects over classes. Use classes only when they naturally fit the problem and help avoid boilerplate code while grouping tightly related functionality.
- Use `snake_case` for all variable and parameter names.
- Shared code used by multiple modules belongs in `plugins/module_utils/clickhouse.py` (DRY principle). Do not duplicate connection or utility logic in individual modules.
- Do not add connection parameters to individual modules. Extend the `client_inst_opts` doc fragment in `plugins/doc_fragments/client_inst_opts.py` instead.
- New modules and new parameters require `version_added: 'x.y.z'` in their DOCUMENTATION block, set to the next planned release version.
- All modules must pass sanity, unit, and integration tests before merging.

## Development Conventions

- Every new module parameter and new module requires `version_added: 'x.y.z'` in its DOCUMENTATION block, set to the next planned release version.
- Every PR that changes module behavior needs a changelog fragment in `changelogs/fragments/<something>.yaml`. Docs/tests/refactoring PRs are exempt. Valid fragment sections: `major_changes`, `minor_changes`, `bugfixes`, `breaking_changes`, `deprecated_features`, `removed_features`, `security_fixes`, `known_issues`. Fragments are consumed (deleted) at release time (`keep_fragments: false` in `changelogs/config.yaml`).
- Tests are required for code changes; see `.agents/skills/run-tests/SKILL.md` for test commands, patterns, and requirements.

## Subagents

Reusable subagent definitions live in the `.agents/subagents/` directory. Each file describes a specialized agent: when to invoke it, how it should behave, and what format to return results in.

When a task matches a subagent's trigger conditions, delegate that part of the work to the subagent rather than handling it inline.

Available subagents:

- `.agents/subagents/docs-explorer.md` — looks up documentation for any external library, framework, or tool

## Agent Skills

Reusable agent skills live in the `.agents/skills/` directory. Each skill is a self-contained directory containing a `SKILL.md` file with YAML frontmatter (`name`, `description`) followed by the skill instructions.

**At session start, read every `.agents/skills/*/SKILL.md` file and register the skills found.** When a user request matches a skill's trigger conditions, load and apply that `SKILL.md` before responding.
