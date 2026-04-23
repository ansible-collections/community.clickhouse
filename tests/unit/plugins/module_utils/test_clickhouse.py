from __future__ import (absolute_import, division, print_function)

__metaclass__ = type

import pytest
from importlib.util import find_spec

from ansible_collections.community.clickhouse.plugins.module_utils.clickhouse import (
    check_clickhouse_driver,
    version_clickhouse_driver,
    client_common_argument_spec,
    get_main_conn_kwargs,
    validate_identifier,
    validate_db_table,
    normalize_db_table
)

REASON = "The clickhouse_driver module is not installed"


class FakeAnsibleModule:
    def __init__(self):
        self.params = {
            "login_host": "localhost",
            "login_port": None,
            "login_user": None,
            "login_db": None,
            "login_password": None,
            "client_kwargs": {},
            "success_on": [497],
        }

    def fail_json(self, msg):
        print(msg)


def test_client_common_argument_spec():
    EXPECTED = {
        'login_db': {'type': 'str', 'default': None},
        'login_port': {'type': 'int', 'default': None},
        'login_user': {'type': 'str', 'default': None},
        'login_host': {'type': 'str', 'default': 'localhost'},
        'login_password': {'type': 'str', 'default': None, 'no_log': True},
        'client_kwargs': {'type': 'dict', 'default': {}},
        'success_on': {'type': 'list', 'elements': 'int', 'default': [497]},
    }

    assert client_common_argument_spec() == EXPECTED


@pytest.mark.parametrize(
    'input_params,output_params',
    [
        ('', {'host': 'localhost'},),
        ({'login_host': 'test_host', 'login_port': 8000},
         {'host': 'test_host', 'port': 8000},
         ),
        ({'login_host': '127.0.0.1',
          'login_db': 'test_database',
          'login_user': 'test_user',
          'login_port': 9000,
          'login_password': 'qwerty',
          },
         {'host': '127.0.0.1',
          'database': 'test_database',
          'user': 'test_user',
          'port': 9000,
          'password': 'qwerty',
          },
         ),
    ]
)
def test_get_main_conn_kwargs(input_params, output_params):
    fake_module = FakeAnsibleModule()
    fake_module.params.update(input_params)

    assert get_main_conn_kwargs(fake_module) == output_params


@pytest.mark.skipif(find_spec('clickhouse_driver') is None, reason=REASON)
def test_version_clickhouse_driver():
    from clickhouse_driver import __version__

    assert __version__ == version_clickhouse_driver()


def test_check_clickhouse_driver():
    fake_module = FakeAnsibleModule()
    result = check_clickhouse_driver(fake_module)

    assert result is None or "clickhouse_driver" in result


@pytest.mark.parametrize("wrong_db_table", [
    "1.tab",
    "1a.tab",
    "db.tab.tab",
    "db.**",
    "db..tab",
    "db.tab*extra",
    "db.*extra",
    "db.*.extra",
    ".tab",
    "db.",
    "db*",
])
def test_validate_db_table_against_not_allowed_names(mocker, wrong_db_table):
    mock_module = mocker.MagicMock()
    mock_module.fail_json = mocker.MagicMock()

    validate_db_table(mock_module, wrong_db_table)

    call_msg = mock_module.fail_json.call_args[1]['msg']

    mock_module.fail_json.assert_called_once()
    assert "Invalid format:" in call_msg


@pytest.mark.parametrize("correct_db_table", [
    "db.tab",
    "d.tab",
    "tab",
    "_db.tab",
    "db._tab",
    "db.*",
    "_db_.tab_",
])
def test_validate_db_table_against_allowed_names(mocker, correct_db_table):
    mock_module = mocker.MagicMock()
    mock_module.fail_json = mocker.MagicMock()

    validate_db_table(mock_module, correct_db_table)

    mock_module.fail_json.assert_not_called()


@pytest.mark.parametrize("input_name,expected", [
    ("table", "`current_db`.`table`"),
    ("_table", "`current_db`.`_table`"),
])
def test_normalize_db_table_with_db_call(mocker, input_name, expected):
    mock_module = mocker.MagicMock()
    mock_module.fail_json = mocker.MagicMock()
    mock_client = mocker.MagicMock()
    mock_execute_query = mocker.patch("ansible_collections.community.clickhouse.plugins.module_utils.clickhouse.execute_query",
                                      return_value=[('current_db',)])

    result = normalize_db_table(mock_module, mock_client, input_name)

    assert result == expected
    mock_execute_query.assert_called_once()


@pytest.mark.parametrize("input_name,expected", [
    ("db.tab", "`db`.`tab`"),
    ("db.*", "`db`.*"),
    ("_db.tab", "`_db`.`tab`"),
])
def test_normalize_db_table_without_db_call(mocker, input_name, expected):
    mock_module = mocker.MagicMock()
    mock_module.fail_json = mocker.MagicMock()
    mock_client = mocker.MagicMock()
    mock_execute_query = mocker.patch("ansible_collections.community.clickhouse.plugins.module_utils.clickhouse.execute_query",
                                      return_value=[('current_db',)])

    result = normalize_db_table(mock_module, mock_client, input_name)

    assert result == expected
    mock_execute_query.assert_not_called()


@pytest.mark.parametrize("malicious_name", [
    "test; DROP TABLE users; --",
    "test`; DROP TABLE users; --",
    "test' OR '1'='1",
    'test"; DROP TABLE users; --',
    "123_test",
    "test.test",
    "`test`",
    "test`collection",
])
def test_validate_function_against_malicious_names(mocker, malicious_name):
    mock_module = mocker.MagicMock()
    mock_module.fail_json = mocker.MagicMock()

    validate_identifier(mock_module, malicious_name)

    call_msg = mock_module.fail_json.call_args[1]['msg']

    mock_module.fail_json.assert_called_once()
    assert "Invalid identifier:" in call_msg


def test_validate_function_with_empty_name(mocker):
    mock_module = mocker.MagicMock()
    mock_module.fail_json = mocker.MagicMock()

    validate_identifier(mock_module, "")

    call_msg = mock_module.fail_json.call_args[1]['msg']

    mock_module.fail_json.assert_called_once()
    assert "cannot be empty" in call_msg


@pytest.mark.parametrize("correct_name", [
    "test",
    "test_name",
    "test_name1",
    "test1",
    "test123"
])
def test_validate_function_with_correct_names(mocker, correct_name):
    mock_module = mocker.MagicMock()
    mock_module.fail_json = mocker.MagicMock()

    result = validate_identifier(mock_module, correct_name)

    mock_module.fail_json.assert_not_called()

    assert result == correct_name


def test_validate_identifier_with_custom_context(mocker):
    mock_module = mocker.MagicMock()
    mock_module.fail_json = mocker.MagicMock()

    validate_identifier(mock_module, "invalid!", "cluster name")

    mock_module.fail_json.assert_called_once()
    call_msg = mock_module.fail_json.call_args[1]['msg']
    assert "Invalid cluster name" in call_msg
