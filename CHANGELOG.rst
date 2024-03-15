====================================================
CHANGE THIS IN changelogs/config.yaml! Release Notes
====================================================

.. contents:: Topics

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
