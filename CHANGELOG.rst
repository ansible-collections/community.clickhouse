===============================================================
Ansible community.clickhouse collection changelog Release Notes
===============================================================

.. contents:: Topics

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
