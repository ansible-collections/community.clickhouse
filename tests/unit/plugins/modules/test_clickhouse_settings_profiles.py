from __future__ import absolute_import, division, print_function
import pytest
from ansible_collections.community.clickhouse.plugins.modules.clickhouse_settings_profile import ClickHouseSettingsProfile


@pytest.fixture
def settings_profile(mocker):
    mock_module = mocker.MagicMock()
    mock_module.check_mode = False
    mock_client = mocker.MagicMock()

    return ClickHouseSettingsProfile(module=mock_module, client=mock_client, name="test_profile")


def get_executed_query(mock_execute):
    # Assumes execute_query(module, client, query, params)
    # args, kwargs = mock_execute.call_args
    # return args[2] if len(args) > 2 else kwargs.get('query')
    return mock_execute.call_args[0][2]


@pytest.fixture
def mock_execute(mocker):
    return mocker.patch(
        "ansible_collections.community.clickhouse.plugins.modules.clickhouse_settings_profile.execute_query",
        return_value=[]
    )


def test_setup_object(settings_profile):
    assert settings_profile.name == 'test_profile'
    assert settings_profile._exists is None


@pytest.mark.parametrize(
    'settings,profiles,cluster,expected',
    [
        (None, None, None, 'CREATE SETTINGS PROFILE `test_profile`'),
        (None, ['web'], None, 'CREATE SETTINGS PROFILE `test_profile` SETTINGS INHERIT `web`'),
        (None, ['web', 'app'], None, 'CREATE SETTINGS PROFILE `test_profile` SETTINGS INHERIT `web`, INHERIT `app`'),
        ({'max_memory': {'value': '1G'}}, None, None, 'CREATE SETTINGS PROFILE `test_profile` SETTINGS max_memory=\'1G\''),
        (
            {'max_memory': {'value': '1G'}, 'max_threads': {'value': '1'}},
            None, None,
            'CREATE SETTINGS PROFILE `test_profile` SETTINGS max_memory=\'1G\', max_threads=\'1\''),
        (
            {'max_memory': {'value': '1G'}, 'max_threads': {'value': '1'}},
            ['web'], None,
            'CREATE SETTINGS PROFILE `test_profile` SETTINGS INHERIT `web`, max_memory=\'1G\', max_threads=\'1\''),
        (
            {'max_memory': {'value': '1G'}, 'max_threads': {'value': '1'}},
            ['web', 'app'], None,
            'CREATE SETTINGS PROFILE `test_profile` SETTINGS INHERIT `web`, INHERIT `app`, max_memory=\'1G\', max_threads=\'1\''),
        (
            {'max_memory': {'value': '1G'}, 'max_threads': {'value': '1'}},
            ['web', 'app'], 'cluster',
            'CREATE SETTINGS PROFILE `test_profile` ON CLUSTER `cluster` SETTINGS INHERIT `web`, INHERIT `app`, max_memory=\'1G\', max_threads=\'1\''),
    ]
)
def test_create_profile(mock_execute, settings_profile, settings, profiles, cluster, expected):
    settings_profile._exists = False
    changed = settings_profile.create(settings, profiles, cluster)
    actuall_query = get_executed_query(mock_execute)
    assert changed is True
    assert mock_execute.call_count == 1
    assert actuall_query == expected


@pytest.mark.parametrize(
    'cluster,expected',
    [
        (None, 'DROP SETTINGS PROFILE `test_profile`'),
        ('cluster', 'DROP SETTINGS PROFILE `test_profile` ON CLUSTER `cluster`'),
    ],
)
def test_drop_profile(mock_execute, settings_profile, cluster, expected):
    settings_profile._exists = True
    changed = settings_profile.drop(cluster)
    actuall_query = get_executed_query(mock_execute)
    assert changed is True
    assert mock_execute.call_count == 1
    assert actuall_query == expected


def test_alter_profile_no_changes(settings_profile, mock_execute, mocker):
    settings_profile._exists = True
    changed = settings_profile.alter({}, [])
    assert changed is False
    assert mock_execute.call_count == 0


def test_alter_profile_changed_inherit_and_settings(settings_profile, mock_execute):
    settings_profile._exists = True
    changed = settings_profile.alter({'max_memory_usage': {'value': '10G'}}, ['web'], None)
    actual_query = get_executed_query(mock_execute)
    assert changed is True
    assert mock_execute.call_count == 1
    assert actual_query == "ALTER SETTINGS PROFILE `test_profile` SETTINGS INHERIT `web`, max_memory_usage='10G'"


def test_alter_profile_changed_many_inherit_and_settings_with_cluster(settings_profile, mock_execute):
    settings_profile._exists = True
    changed = settings_profile.alter({'max_memory_usage': {'value': '10G'}, 'max_threads': {'value': '1'}}, ['web', 'app'], 'cluster')
    actual_query = get_executed_query(mock_execute)
    assert changed is True
    assert mock_execute.call_count == 1
    assert actual_query == (
        "ALTER SETTINGS PROFILE `test_profile` ON CLUSTER `cluster` "
        "SETTINGS INHERIT `web`, INHERIT `app`, max_memory_usage='10G', max_threads='1'"
    )


def test_alter_profile_reset_settings(settings_profile, mocker, mock_execute):
    settings_profile._exists = True
    mocker.patch("ansible_collections.community.clickhouse.plugins.module_utils.entity_settings.EntitySettings.fetch"
                 , return_value=[('max_memory_usage', '10G', None, None, 'WRITABLE', False)])
    changed = settings_profile.alter({}, [], None)
    actual_query = get_executed_query(mock_execute)
    assert changed is True
    assert mock_execute.call_count == 1
    assert actual_query == "ALTER SETTINGS PROFILE `test_profile` SETTINGS NONE"
