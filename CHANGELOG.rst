===============================================================
Ansible community.clickhouse collection changelog Release Notes
===============================================================

.. contents:: Topics

v2.0.0
======

Release Summary
---------------

This is a major release of the ``community.clickhouse`` collection.
This changelog contains all changes to the modules and plugins in this collection
that have been made after the previous release.

Breaking Changes / Porting Guide
--------------------------------

- ansible-core support - the collection supports ansible-core >= 2.17 and no longer supports older versions. If you use an older version, please upgrade to at least 2.17 (https://github.com/ansible-collections/community.clickhouse/pull/169).

v1.1.0
======

Release Summary
---------------

This is a minor release of the ``community.clickhouse`` collection.
This changelog contains all changes to the modules and plugins in this collection
that have been made after the previous release.

Bugfixes
--------

- clickhouse_user - Fixed idempotency issue when user hosts were provided in different orders. Now compares pre/post user host settings using a set. (https://github.com/ansible-collections/community.clickhouse/issues/152)

New Modules
-----------

- clickhouse_quota - Creates or removes a ClickHouse quota

v1.0.0
======

Release Summary
---------------

This is a major release of the ``community.clickhouse`` collection.
This changelog contains all changes to the modules and plugins in this collection
that have been made after the previous release.

Minor Changes
-------------

- clickhouse_user - add new user_hosts parameter to clickhouse_user module to allow restricting hosts for users  (https://github.com/ansible-collections/community.clickhouse/issues/146).

v0.12.1
=======

Release Summary
---------------

This is a patch release of the ``community.clickhouse`` collection.
This changelog contains all changes to the modules and plugins in this collection
that have been made after the previous release.

Bugfixes
--------

- clickhouse_grants - sanitize grantee by enclosing it in single qoutes to support special characters in grantee names (https://github.com/ansible-collections/community.clickhouse/issues/139).

v0.12.0
=======

Release Summary
---------------

This is a minor release of the ``community.clickhouse`` collection.
This changelog contains all changes to the modules and plugins in this collection
that have been made after the previous release.

Minor Changes
-------------

- clickhouse_role - add support for updating existing role settings using ALTER ROLE, making the module fully idempotent for role management operations (https://github.com/ansible-collections/community.clickhouse/issues/62).

v0.11.0
=======

Release Summary
---------------

This is a minor release of the ``community.clickhouse`` collection.
This changelog contains all changes to the modules and plugins in this collection
that have been made after the previous release.

Minor Changes
-------------

- clickhouse_grants - add ``cluster`` argument (https://github.com/ansible-collections/community.clickhouse/issues/130).

v0.10.0
=======

Release Summary
---------------

This is a minor release of the ``community.clickhouse`` collection.
This changelog contains all changes to the modules and plugins in this collection
that have been made after the previous release.

Minor Changes
-------------

- clickhouse_user - the settings argument can now update user settings idempotently at any use, not only upon user creation as before (https://github.com/ansible-collections/community.clickhouse/issues/73).

Bugfixes
--------

- clickhouse_user - quote names in queries to prevent errors (https://github.com/ansible-collections/community.clickhouse/pull/110).

v0.9.0
======

Release Summary
---------------

This is a minor release of the ``community.clickhouse`` collection.
This changelog contains all changes to the modules and plugins in this collection
that have been made after the previous release.

New Modules
-----------

- clickhouse_grants - Manage grants for ClickHouse users and roles

v0.8.5
======

Release Summary
---------------

This is a patch release of the ``community.clickhouse`` collection.
This changelog contains all changes to the modules and plugins in this collection
that have been made after the previous release.

Bugfixes
--------

- clickhouse_client - the module fails when executing DDL queries that return nothing via client object (https://github.com/ansible-collections/community.clickhouse/issues/116).

v0.8.4
======

Release Summary
---------------

This is a patch release of the ``community.clickhouse`` collection.
This changelog contains all changes to the modules and plugins in this collection
that have been made after the previous release.

Bugfixes
--------

- clickhouse_cfg_info - Prevent infinite loop when parsing YAML files with recursive anchors by validating JSON serializability (https://github.com/ansible-collections/community.clickhouse/pull/114).

v0.8.3
======

Release Summary
---------------

This is a patch release of the ``community.clickhouse`` collection.
This changelog contains all changes to the modules and plugins in this collection
that have been made after the previous release.

Bugfixes
--------

- clickhouse_db - fix the module to correct position "ON CLUSTER" when create db with specifying engine type (https://github.com/ansible-collections/community.clickhouse/pull/108).

v0.8.2
======

Release Summary
---------------

This is a patch release of the ``community.clickhouse`` collection.
This changelog contains all changes to the modules and plugins in this collection
that have been made after the previous release.

Bugfixes
--------

- clickhouse_user - fix the module to correct work clause "ON CLUSTER" when updating user attributes such as roles and passwords (https://github.com/ansible-collections/community.clickhouse/pull/105).

v0.8.1
======

Release Summary
---------------

This is a patch release of the ``community.clickhouse`` collection.
This changelog contains all changes to the modules and plugins in this collection
that have been made after the previous release.

Bugfixes
--------

- clickhouse_info - ensure the module works with ansible-core 2.19 and higher.

v0.8.0
======

Release Summary
---------------

This is a minor release of the ``community.clickhouse`` collection.
This changelog contains all changes to the modules and plugins in this collection
that have been made after the previous release.

Major Changes
-------------

- clickhouse_info - removed support for clickhouse versions 21 and 22 (https://github.com/ansible-collections/community.clickhouse/pull/93).

Minor Changes
-------------

- clickhouse_info - columns are extracted from clickhouse version 23 system tables, the affected system tables are - databases, clusters, tables, dictionaries, settings, merge_tree_settings, users, settings_profile_elements (https://github.com/ansible-collections/community.clickhouse/pull/93).

Bugfixes
--------

- clickhouse_user - fixes failure when creating a new user and role_mode is not remove (https://github.com/ansible-collections/community.clickhouse/issues/97).

v0.7.0
======

Release Summary
---------------

This is a minor release of the ``community.clickhouse`` collection.
This changelog contains all changes to the modules and plugins in this collection
that have been made after the previous release.

Minor Changes
-------------

- clickhouse_info - add the ``grants`` return value for users and roles.
- clickhouse_info - add the ``grants`` returns all grants for users and roles.
- clickhouse_info - add the ``settings_profile_elements`` returns all settings for users, profiles and roles.

Breaking Changes / Porting Guide
--------------------------------

- clickhouse_info - removed ``functions`` for collecting information of created functions. A rare and unpopular feature.

New Modules
-----------

- clickhouse_cfg_info - Retrieves ClickHouse config file content and returns it as JSON

v0.6.0
======

Release Summary
---------------

This is a minor release of the ``community.clickhouse`` collection.
This changelog contains all changes to the modules and plugins in this collection
that have been made after the previous release.

Minor Changes
-------------

- clickhouse_info - add the ``roles`` field to user information.
- clickhouse_user - add the ``default_roles_mode`` argument to specify how to handle roles passed through ``default_roles`` argument (https://github.com/ansible-collections/community.clickhouse/pull/70).
- clickhouse_user - add the ``default_roles`` argument to set default roles (https://github.com/ansible-collections/community.clickhouse/pull/70).
- clickhouse_user - add the ``roles_mode`` argument to specify how to handle roles passed through ``roles`` argument (https://github.com/ansible-collections/community.clickhouse/pull/70).
- clickhouse_user - add the ``roles`` argument to grant roles (https://github.com/ansible-collections/community.clickhouse/pull/70).

v0.5.0
======

Release Summary
---------------

This is a minor release of the ``community.clickhouse`` collection.
This changelog contains all changes to the modules and plugins in this collection
that have been made after the previous release.

Minor Changes
-------------

- clickhouse_client - added the ``set_settings`` argument (https://github.com/ansible-collections/community.clickhouse/pull/63).
- clickhouse_user - added the ability to add settings with their restrictions applied by default when a user logs in.

New Modules
-----------

- clickhouse_role - Creates or removes a ClickHouse role.

v0.4.0
======

Release Summary
---------------

This is the minor release of the ``community.clickhouse`` collection.
This changelog contains all changes to the modules and plugins in this collection
that have been made after the previous release.

Minor Changes
-------------

- clickhouse_db - add the ``cluster`` argument to execute commands on all cluster hosts.
- clickhouse_db - add the ``comment`` argument to set a comment on databases.
- clickhouse_db - add the ``target`` argument to rename the database.
- clickhouse_db - added the ability to rename databases.
- clickhouse_info - added the ability to collect information from system.functions.
- clickhouse_info - added the ability to collect information from system.quotas, system.settings_profiles.
- clickhouse_info - added the ability to collect information from system.storage_policies.

New Modules
-----------

- clickhouse_user - Creates or removes a ClickHouse user using the clickhouse-driver Client interface

v0.3.0
======

Release Summary
---------------

This is the minor release of the ``community.clickhouse`` collection.
This changelog contains all changes to the modules and plugins in this collection
that have been made after the previous release.

Minor Changes
-------------

- clickhouse_info - added the ability to collect information from system.tables, system.dictionaries, system.merge_tree_settings.

Bugfixes
--------

- clickhouse_client - Add support for returned values of types ``IPv4Address`` and ``IPv6Address``.
- clickhouse_client - Add support for returned values of types ``UUID`` and ``decimal``.

New Modules
-----------

- clickhouse_db - Creates or removes a ClickHouse database using the clickhouse-driver Client interface

v0.2.0
======

Release Summary
---------------

This is a minor release of the ``community.clickhouse`` collection.
This changelog contains all changes to the modules and plugins in this collection
that have been made after the previous release.

Minor Changes
-------------

- clickhouse_info - add the ``limit`` argument to limit the returned values.

v0.1.1
======

Release Summary
---------------

This is a patch release of the community.clickhouse collections. It fixes the ``clickhouse_info`` module to work with older versions of the ClickHouse server.

Bugfixes
--------

- clickhouse_info - fix the module to work with older server versions (https://github.com/ansible-collections/community.clickhouse/pull/10).

v0.1.0
======

Release Summary
---------------

This is the first release of the community.clickhouse collection.

Minor Changes
-------------

- clickhouse_client - add the module.
- clickhouse_info - add the module.

New Modules
-----------

- clickhouse_client - Execute queries in a ClickHouse database using the clickhouse-driver Client interface
- clickhouse_info - Gather ClickHouse server information using the clickhouse-driver Client interface
