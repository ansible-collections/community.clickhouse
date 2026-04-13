from __future__ import absolute_import, division, print_function
import pytest
from ansible_collections.community.clickhouse.plugins.modules.clickhouse_role import ClickHouseRole


@pytest.fixture
def role(mocker):
    mocker.patch("ansible_collections.community.clickhouse.plugins.modules.clickhouse_role.execute_query"
                 , return_value=[(1)])

    mock_module = mocker.MagicMock()
    mock_module.check_mode = False
    mock_client = mocker.MagicMock()

    return ClickHouseRole(module=mock_module, client=mock_client, name="test_role")


@pytest.fixture
def mock_execute(mocker):
    return mocker.patch("ansible_collections.community.clickhouse.plugins.modules.clickhouse_role.execute_query", return_value=[])


def get_executed_query(mock_execute):
    return mock_execute.call_args[0][2]


def test_alter_role_no_changes_new_setting(role, mock_execute, mocker):
    mocker.patch("ansible_collections.community.clickhouse.plugins.module_utils.entity_settings.execute_query"
                 , return_value=[])
    changed = role.alter({}, [], None)
    assert changed is False
    assert mock_execute.call_count == 0


def test_alter_role_one_setting_old_setting(role, mock_execute):
    changed = role.alter(["max_memory_usage='10G'"], [], None)
    actual_query = get_executed_query(mock_execute)
    assert changed is True
    assert actual_query == "ALTER ROLE test_role SETTINGS max_memory_usage='10G'"


def test_alter_role_one_setting_new_setting(role, mock_execute, mocker):
    mocker.patch(
        "ansible_collections.community.clickhouse.plugins.module_utils.entity_settings.execute_query",
        return_value=[]
    )
    changed = role.alter({'max_memory_usage': {'value': '10G'}}, [], None)
    actual_query = get_executed_query(mock_execute)
    assert changed is True
    assert actual_query == "ALTER ROLE test_role SETTINGS max_memory_usage='10G'"


def test_alter_role_one_profile_new_setting(role, mock_execute):
    changed = role.alter({}, ['web'], None)
    actual_query = get_executed_query(mock_execute)
    assert changed is True
    assert actual_query == "ALTER ROLE test_role SETTINGS PROFILE 'web'"


def test_alter_role_one_profile_one_setting_new_setting(role, mock_execute):
    changed = role.alter({'max_memory_usage': {'value': '10G'}}, ['web'], None)
    actual_query = get_executed_query(mock_execute)
    assert changed is True
    assert actual_query == "ALTER ROLE test_role SETTINGS PROFILE 'web', max_memory_usage='10G'"


def test_drop(role, mock_execute):
    changed = role.drop()
    actual_query = get_executed_query(mock_execute)
    assert changed is True
    assert actual_query == "DROP ROLE test_role"


def test_alter_role_none_settings_nor_profile(role, mock_execute, mocker):
    mocker.patch("ansible_collections.community.clickhouse.plugins.module_utils.entity_settings.execute_query"
                 , return_value=[('max_memory_usage', '10G', None, None, 'WRITABLE', False)])
    changed = role.alter({}, [], None)
    actual_query = get_executed_query(mock_execute)
    assert changed is True
    assert actual_query == "ALTER ROLE test_role SETTINGS NONE"


def test_alter_role_del_settings_add_profile(role, mock_execute, mocker):
    mocker.patch("ansible_collections.community.clickhouse.plugins.module_utils.entity_settings.execute_query"
                 , return_value=[('max_memory_usage', '10G', None, None, 'WRITABLE', False)])
    changed = role.alter({}, ['web'], None)
    actual_query = get_executed_query(mock_execute)
    assert changed is True
    assert actual_query == "ALTER ROLE test_role SETTINGS PROFILE 'web'"


def test_alter_role_add_settings_del_profile(role, mock_execute, mocker):
    mocker.patch("ansible_collections.community.clickhouse.plugins.module_utils.entity_settings.execute_query"
                 , return_value=[('max_memory_usage', '10G', None, None, 'WRITABLE', 'web')])
    changed = role.alter({'max_memory_usage': {'value': '10G'}}, [], None)
    actual_query = get_executed_query(mock_execute)
    assert changed is True
    assert actual_query == "ALTER ROLE test_role SETTINGS max_memory_usage='10G'"
