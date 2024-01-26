====================================================
CHANGE THIS IN changelogs/config.yaml! Release Notes
====================================================

.. contents:: Topics


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
