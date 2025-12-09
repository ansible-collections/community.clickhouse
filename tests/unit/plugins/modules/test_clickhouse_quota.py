from __future__ import absolute_import, division, print_function

__metaclass__ = type
import pytest
from ansible_collections.community.clickhouse.plugins.modules.clickhouse_quota import (
    _APPLY_TO_REGEX,
    _CREATE_QUOTA_REGEX,
    _LIMITS_REGEX,
    _VALID_NAME_REGEX,
    ClickHouseQuota,
)
from ansible_collections.community.clickhouse.plugins.modules.clickhouse_quota import (
    _DEFAULT_PARAMS as DEFAULT_NORMALIZE_PARAMS,
)


@pytest.mark.parametrize(
    argnames="name,is_valid",
    argvalues=[
        ("", False),
        ("test_quota", True),
        ("test quota", True),
        ("test-quota", True),
        ("test.quota", True),
        ("tést quota", True),
        ("'test quota'", False),
        ('"test quota"', False),
        ("`test quota`", False),
        ("test;quota", False),
        ("test\0quota", False),
    ],
)
def test_valid_name_regex(name, is_valid):
    match = _VALID_NAME_REGEX.match(name)
    if is_valid:
        assert match is not None
    else:
        assert match is None


@pytest.mark.parametrize(
    argnames="search",
    argvalues=[
        "",
        "CREATE NOT_QUOTA foo",
    ],
)
def test_negative_create_quota_regex(search):
    match = _CREATE_QUOTA_REGEX.match(search)
    assert match is None


DEFAULT_CREATE_QUOTA_GROUPS = dict(
    name=None,
    cluster=None,
    keyed_by=None,
)


@pytest.mark.parametrize(
    argnames="search,expected",
    argvalues=[
        ("CREATE QUOTA test_quota", dict(name="test_quota")),
        ("CREATE QUOTA `test_quota`", dict(name="`test_quota`")),
        ("CREATE QUOTA `test quota`", dict(name="`test quota`")),
        ("CREATE QUOTA `tést_quota`", dict(name="`tést_quota`")),
        ("CREATE QUOTA `tést quota`", dict(name="`tést quota`")),
        (
            "CREATE QUOTA test_quota ON CLUSTER test_cluster",
            dict(name="test_quota", cluster="test_cluster"),
        ),
        (
            "CREATE QUOTA test_quota ON CLUSTER `test cluster`",
            dict(name="test_quota", cluster="`test cluster`"),
        ),
        (
            "CREATE QUOTA test_quota ON CLUSTER `tést_cluster`",
            dict(name="test_quota", cluster="`tést_cluster`"),
        ),
        (
            "CREATE QUOTA test_quota ON CLUSTER `tést cluster`",
            dict(name="test_quota", cluster="`tést cluster`"),
        ),
        (
            "CREATE QUOTA `tést quota` ON CLUSTER `tést cluster`",
            dict(name="`tést quota`", cluster="`tést cluster`"),
        ),
        (
            "CREATE QUOTA test_quota KEYED BY user_name",
            dict(name="test_quota", keyed_by="user_name"),
        ),
        ("CREATE QUOTA test_quota KEYED BY non_existent_key", dict(name="test_quota")),
        (
            "CREATE QUOTA `tést quota` KEYED BY ip_address",
            dict(name="`tést quota`", keyed_by="ip_address"),
        ),
        (
            "CREATE QUOTA `tést quota` ON CLUSTER `tést cluster` KEYED BY client_key,ip_address",
            dict(
                name="`tést quota`",
                cluster="`tést cluster`",
                keyed_by="client_key,ip_address",
            ),
        ),
    ],
)
def test_create_quota_regex(search, expected):
    match = _CREATE_QUOTA_REGEX.match(search)
    assert match is not None
    actual = match.groupdict()
    assert actual == (DEFAULT_CREATE_QUOTA_GROUPS | expected)


DEFAULT_LIMITS_GROUPS = dict(
    randomized=None,
    interval_number=None,
    interval_unit=None,
    limit_type=None,
)


@pytest.mark.parametrize(
    argnames="search,expected",
    argvalues=[
        (
            "FOR INTERVAL 5 minute NO LIMITS",
            {
                "interval_number": "5",
                "interval_unit": "minute",
                "limit_type": "NO LIMITS",
            },
        ),
        (
            "FOR RANDOMIZED INTERVAL 0.25 year TRACKING ONLY",
            {
                "randomized": "RANDOMIZED",
                "interval_number": "0.25",
                "interval_unit": "year",
                "limit_type": "TRACKING ONLY",
            },
        ),
        (
            "FOR INTERVAL 1 day MAX queries = 100",
            {
                "interval_number": "1",
                "interval_unit": "day",
                "limit_type": "MAX queries = 100",
            },
        ),
        (
            "FOR INTERVAL 1 day MAX query_selects = 80, query_inserts = 20",
            {
                "interval_number": "1",
                "interval_unit": "day",
                "limit_type": "MAX query_selects = 80, query_inserts = 20",
            },
        ),
        (
            "FOR RANDOMIZED INTERVAL 1000 second MAX execution_time = 100.5, failed_sequential_authentications = 10",
            {
                "randomized": "RANDOMIZED",
                "interval_number": "1000",
                "interval_unit": "second",
                "limit_type": "MAX execution_time = 100.5, failed_sequential_authentications = 10",
            },
        ),
    ],
)
def test_limits_regex(search, expected):
    match = _LIMITS_REGEX.match(search)
    assert match is not None
    actual = match.groupdict()
    assert actual == (DEFAULT_LIMITS_GROUPS | expected)


@pytest.mark.parametrize(
    argnames="search,expected",
    argvalues=[
        (" TO DEFAULT", dict(apply_to="DEFAULT")),
        (
            " TO CURRENT_USER, test_user, `tést user`",
            dict(apply_to="CURRENT_USER, test_user, `tést user`"),
        ),
        (
            " TO DEFAULT, `tést user`, CURRENT_USER",
            dict(apply_to="DEFAULT, `tést user`, CURRENT_USER"),
        ),
        (" TO ALL", dict(apply_to="ALL")),
        (" TO ALL EXCEPT CURRENT_USER", dict(apply_to="ALL EXCEPT CURRENT_USER")),
        (
            " TO ALL EXCEPT CURRENT_USER, test_user, `tést user`",
            dict(apply_to="ALL EXCEPT CURRENT_USER, test_user, `tést user`"),
        ),
    ],
)
def test_apply_to_regex(search, expected):
    match = _APPLY_TO_REGEX.match(search)
    assert match is not None
    actual = match.groupdict()
    assert actual == expected


DEFAULT_PARSE_PARAMS = dict(
    cluster=None,
    keyed_by=None,
    limits=[],
)


@pytest.mark.parametrize(
    argnames="create_statement,expected",
    argvalues=[
        (
            "CREATE QUOTA test_quota FOR INTERVAL 1 hour TRACKING ONLY TO DEFAULT, `tést user`, CURRENT_USER",
            {
                "limits": [
                    {
                        "randomized_start": False,
                        "interval": "1 hour",
                        "tracking_only": True,
                    },
                ],
                "apply_to": ["DEFAULT", "tést user", "CURRENT_USER"],
                "apply_to_mode": "listed_only",
            },
        ),
        (
            "CREATE QUOTA test_quota FOR RANDOMIZED INTERVAL 1 hour NO LIMITS TO ALL",
            {
                "limits": [
                    {
                        "randomized_start": True,
                        "interval": "1 hour",
                        "no_limits": True,
                    },
                ],
                "apply_to": [],
                "apply_to_mode": "all",
            },
        ),
        (
            "CREATE QUOTA test_quota ON CLUSTER `tést cluster` KEYED BY client_key,user_name"
            " FOR RANDOMIZED INTERVAL 1 minute MAX queries = 100, query_selects = 80, query_inserts = 10,"
            " FOR INTERVAL 1 day MAX execution_time = 3000.5, read_rows = 1024"
            " TO ALL EXCEPT `tést user`",
            {
                "cluster": "tést cluster",
                "keyed_by": "client_key,user_name",
                "limits": [
                    {
                        "randomized_start": True,
                        "interval": "1 minute",
                        "max": {
                            "queries": 100,
                            "query_selects": 80,
                            "query_inserts": 10,
                        },
                    },
                    {
                        "randomized_start": False,
                        "interval": "1 day",
                        "max": {
                            "execution_time": 3000.5,
                            "read_rows": 1024,
                        },
                    },
                ],
                "apply_to": ["tést user"],
                "apply_to_mode": "all_except_listed",
            },
        ),
        (
            "CREATE QUOTA tracking_only KEYED BY user_name FOR INTERVAL 15 minute TRACKING ONLY TO ALL",
            {
                "keyed_by": "user_name",
                "limits": [
                    {
                        "randomized_start": False,
                        "interval": "15 minute",
                        "tracking_only": True,
                    },
                ],
                "apply_to": [],
                "apply_to_mode": "all",
            },
        ),
    ],
)
def test_parse_create_statement(create_statement, expected):
    actual = ClickHouseQuota._parse_create_statement(create_statement)
    assert actual == (DEFAULT_PARSE_PARAMS | expected)


@pytest.mark.parametrize(
    argnames="params,expected",
    argvalues=[
        ({}, {}),
        (
            {"apply_to": ["test_user", "current_user"]},
            {"apply_to": ["current_user", "test_user"]},
        ),
        ({"apply_to_mode": "all_except_listed"}, {"apply_to_mode": "all"}),
        ({"extra_args": "foo"}, {}),
        ({"keyed_by": "user_name"}, {"keyed_by": "user_name"}),
        ({"keyed_by": "client_key,user_name"}, {"keyed_by": "client_key,user_name"}),
        ({"keyed_by": "client_key, user_name"}, {"keyed_by": "client_key,user_name"}),
        (
            {"limits": [{"interval": "5 minute"}, {"interval": "1 minute"}]},
            {
                "limits": [
                    {
                        "interval": "1 minute",
                        "randomized_start": False,
                        "max": {},
                        "no_limits": None,
                        "tracking_only": None,
                    },
                    {
                        "interval": "5 minute",
                        "randomized_start": False,
                        "max": {},
                        "no_limits": None,
                        "tracking_only": None,
                    },
                ]
            },
        ),
        (
            {
                "limits": [
                    {
                        "interval": "1 day",
                        "max": {
                            "queries": 10,
                        },
                    }
                ]
            },
            {
                "limits": [
                    {
                        "randomized_start": False,
                        "interval": "1 day",
                        "max": {
                            "queries": 10,
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
                        },
                        "no_limits": None,
                        "tracking_only": None,
                    }
                ]
            },
        ),
        (
            {
                "limits": [
                    {
                        "interval": "1 day",
                        "max": {
                            "errors": 1,
                        },
                    }
                ]
            },
            {
                "limits": [
                    {
                        "randomized_start": False,
                        "interval": "1 day",
                        "max": {
                            "errors": 1,
                            "queries": None,
                            "query_selects": None,
                            "query_inserts": None,
                            "result_rows": None,
                            "result_bytes": None,
                            "read_rows": None,
                            "read_bytes": None,
                            "written_bytes": None,
                            "execution_time": None,
                            "failed_sequential_authentications": None,
                        },
                        "no_limits": None,
                        "tracking_only": None,
                    }
                ]
            },
        ),
        (
            {
                "limits": [
                    {
                        "interval": "1 day",
                        "no_limits": True,
                    }
                ]
            },
            {"limits": []},
        ),
        (
            {
                "limits": [
                    {
                        "interval": "1 day",
                        "tracking_only": True,
                    }
                ]
            },
            {
                "limits": [
                    {
                        "randomized_start": False,
                        "interval": "1 day",
                        "max": {},
                        "no_limits": None,
                        "tracking_only": True,
                    }
                ]
            },
        ),
        ({"limits": None}, {"limits": []}),
        ({"apply_to": None}, {"apply_to": []}),
        (
            {
                "limits": [
                    {
                        "interval": "15 minute",
                        "max": None,
                        "no_limits": None,
                        "tracking_only": True,
                        "random_extra_key": True,
                    }
                ]
            },
            {
                "limits": [
                    {
                        "interval": "15 minute",
                        "max": {},
                        "no_limits": None,
                        "tracking_only": True,
                        "randomized_start": False,
                    }
                ]
            },
        ),
    ],
)
def test_normalize(params, expected):
    actual = ClickHouseQuota._normalize(params)
    assert actual == (DEFAULT_NORMALIZE_PARAMS | expected)


DEFAULT_PARAMS = dict(
    state="present",
    cluster=None,
    keyed_by=None,
    limits=[],
    apply_to_mode="listed_only",
)


@pytest.mark.parametrize(
    argnames="name,params,existing_quota,expected_executed",
    argvalues=[
        # create queries
        ("tést_quota", {}, None, ["CREATE QUOTA 'tést_quota'"]),
        ("tést_quota", {}, "CREATE QUOTA `tést_quota`", []),
        (
            "test_quota",
            {
                "cluster": "test_cluster",
                "keyed_by": "user_name",
                "limits": [
                    {
                        "interval": "5 minute",
                        "max": {
                            "queries": 100,
                        },
                    }
                ],
                "apply_to": ["CURRENT_USER"],
            },
            None,
            [
                "CREATE QUOTA 'test_quota' ON CLUSTER 'test_cluster' KEYED BY user_name FOR INTERVAL 5 minute MAX queries = 100 TO CURRENT_USER"
            ],
        ),
        # drop queries
        ("test_quota", dict(state="absent"), None, []),
        (
            "test_quota",
            dict(state="absent"),
            "CREATE QUOTA 'test_quota'",
            ["DROP QUOTA 'test_quota'"],
        ),
        # alter queries
        (
            "test_quota",
            {
                "cluster": "test_cluster",
                "keyed_by": "user_name",
                "limits": [
                    {
                        "interval": "5 minute",
                        "max": {
                            "queries": 100,
                        },
                    }
                ],
                "apply_to": ["CURRENT_USER"],
            },
            "CREATE QUOTA test_quota",
            [
                "ALTER QUOTA 'test_quota' ON CLUSTER 'test_cluster' KEYED BY user_name FOR INTERVAL 5 minute MAX queries = 100 TO CURRENT_USER"
            ],
        ),
        (
            "test_quota",
            {
                "limits": [
                    {
                        "interval": "5 minute",
                        "max": {
                            "queries": 100,
                        },
                    }
                ],
                "apply_to": ["test_user", "CURRENT_USER"],
            },
            "CREATE QUOTA test_quota FOR INTERVAL 5 minute MAX queries = 100 TO CURRENT_USER, test_user",
            [],
        ),
        (
            "tést quota",
            {
                "keyed_by": "client_key",
                "limits": [
                    {
                        "randomized_start": True,
                        "interval": "5 minute",
                        "max": {
                            "queries": 100,
                            "execution_time": 1.1,
                        },
                    },
                    {
                        "interval": "1 minute",
                        "max": {
                            "errors": 10,
                        },
                    },
                ],
                "apply_to": ["default"],
            },
            "CREATE QUOTA `tést quota` KEYED BY client_key, user_name FOR INTERVAL 1 minute MAX errors = 10, "
            "FOR RANDOMIZED INTERVAL 5 minute MAX queries = 100, execution_time = 1.1 TO default",
            [
                "ALTER QUOTA 'tést quota' KEYED BY client_key FOR RANDOMIZED INTERVAL 5 minute MAX queries = 100, execution_time = 1.1, "
                "FOR INTERVAL 1 minute MAX errors = 10 TO default"
            ],
        ),
        (
            "tést quota",
            {
                "keyed_by": "client_key,user_name",
                "limits": [
                    {
                        "randomized_start": True,
                        "interval": "5 minute",
                        "max": {
                            "queries": 100,
                            "execution_time": 1.1,
                        },
                    },
                    {
                        "interval": "1 minute",
                        "max": {
                            "errors": 10,
                        },
                    },
                ],
                "apply_to": ["default"],
            },
            "CREATE QUOTA `tést quota` KEYED BY client_key, user_name FOR INTERVAL 1 minute MAX errors = 10, "
            "FOR RANDOMIZED INTERVAL 5 minute MAX queries = 100, execution_time = 1.1 TO default",
            [],
        ),
        (
            "test_quota",
            {
                "keyed_by": "user_name",
                "limits": [
                    {
                        "interval": "5 minute",
                        "no_limits": True,
                    },
                ],
            },
            "CREATE QUOTA test_quota KEYED BY user_name",
            [],
        ),
        (
            "tracking_only",
            {
                "keyed_by": "user_name",
                "limits": [
                    {
                        "interval": "15 minute",
                        "tracking_only": True,
                        "no_limits": None,
                        "max": None,
                    },
                ],
                "apply_to": [],
                "apply_to_mode": "all",
            },
            "CREATE QUOTA tracking_only KEYED BY user_name FOR INTERVAL 15 minute TRACKING ONLY TO ALL",
            [],
        ),
    ],
)
def test_ensure_state(mocker, name, params, existing_quota, expected_executed):
    for check_mode in (True, False):
        client = mocker.MagicMock()
        client.execute.return_value = ["1"] if existing_quota else []
        module = mocker.MagicMock()
        module._verbosity = 1
        module.check_mode = check_mode
        module.params = DEFAULT_PARAMS | params
        will_run_show_create = existing_quota and module.params["state"] == "present"
        quota = ClickHouseQuota(module, client, name)
        client.execute.assert_called_once_with(
            f"SELECT 1 FROM system.quotas WHERE name = '{name}' LIMIT 1"
        )
        client.execute.reset_mock()
        client.execute.return_value = [[existing_quota]] if will_run_show_create else []
        changed = quota.ensure_state()
        if check_mode:
            if will_run_show_create:
                client.execute.assert_called_once_with(f"SHOW CREATE QUOTA '{name}'")
            else:
                client.execute.assert_not_called()
        else:
            expected_call_args_list = []
            if will_run_show_create:
                expected_call_args_list.append(((f"SHOW CREATE QUOTA '{name}'",),))
            expected_call_args_list.extend(((sql,),) for sql in expected_executed)
            assert client.execute.call_args_list == expected_call_args_list
        assert quota.executed_statements == expected_executed
        assert changed is bool(expected_executed)
