from __future__ import absolute_import, division, print_function
import pytest
from ansible_collections.community.clickhouse.plugins.module_utils.entity_settings import EntitySettings


@pytest.fixture
def user_ent(mocker):
    mocker.patch(
        "ansible_collections.community.clickhouse.plugins.module_utils.entity_settings.execute_query",
        return_value=[
            ('max_memory_usage', '15000', '15000', '16000', 'CONST', None)
        ]
    )
    mock_module = mocker.MagicMock()
    mock_module.check_mode = False
    mock_client = mocker.MagicMock()
    return EntitySettings(module=mock_module, client=mock_client, entity_name="test_user", entity_type="user")


@pytest.fixture
def role_ent(mocker):
    mocker.patch(
        "ansible_collections.community.clickhouse.plugins.module_utils.entity_settings.execute_query",
        return_value=[
            ('max_memory_usage', '15000', '15000', '16000', 'CONST', None)
        ]
    )
    mock_module = mocker.MagicMock()
    mock_module.check_mode = False
    mock_client = mocker.MagicMock()
    return EntitySettings(module=mock_module, client=mock_client, entity_name="test_role", entity_type="role")


def test_where_clause_for_user(user_ent):
    result = user_ent._get_where_column()
    assert result == "user_name"


def test_where_clause_for_role(role_ent):
    result = role_ent._get_where_column()
    assert result == "role_name"


def test_result_for_fetching_current_setings(user_ent):
    result = user_ent.fetch()
    assert result == [('max_memory_usage', '15000', '15000', '16000', 'CONST', None)]


def test_normalizing_current_settings_one_setting_no_profile(role_ent):
    result_settings, result_profiles = role_ent._normalize_current_settings(role_ent.fetch())

    assert result_settings == {'max_memory_usage': {'value': '15000', 'min': '15000', 'max': '16000', 'writability': 'CONST'}}
    assert result_profiles == []


def test_normalizing_current_settings_no_setting_one_profile(role_ent, mocker):
    mocker.patch(
        "ansible_collections.community.clickhouse.plugins.module_utils.entity_settings.execute_query",
        return_value=[(None, None, None, None, None, 'web')]
    )
    result_settings, result_profiles = role_ent._normalize_current_settings(role_ent.fetch())

    assert result_settings == {}
    assert result_profiles == ['web']


def test_normalizing_current_settings_one_setting_one_profile(role_ent, mocker):
    mocker.patch(
        "ansible_collections.community.clickhouse.plugins.module_utils.entity_settings.execute_query",
        return_value=[
            (None, None, None, None, None, 'test_profile'),
            ('max_memory_usage', '15000', '15000', '16000', None, None)
        ]
    )
    result_settings, result_profiles = role_ent._normalize_current_settings(role_ent.fetch())

    assert result_settings == {'max_memory_usage': {'value': '15000', 'min': '15000', 'max': '16000'}}
    assert result_profiles == ['test_profile']


def test_normalizing_current_settings_many_setting_many_profile(role_ent, mocker):
    mocker.patch("ansible_collections.community.clickhouse.plugins.module_utils.entity_settings.execute_query", return_value=[
        (None, None, None, None, None, 'test_profile'),
        (None, None, None, None, None, 'app'),
        (None, None, None, None, None, 'web'),
        (None, None, None, None, None, 'web2'),
        ('max_memory_usage', '15000', '15000', '16000', 'CONST', None),
        ('allow_experimental_analyzer', '0', None, None, None, None),
        ('allow_experimental_variant_type', '1', None, None, None, None)]
    )
    result_settings, result_profiles = role_ent._normalize_current_settings(role_ent.fetch())

    assert result_settings == {
        'max_memory_usage': {
            'value': '15000',
            'min': '15000',
            'max': '16000',
            'writability': 'CONST'
        },
        'allow_experimental_variant_type': {
            'value': '1'
        },
        'allow_experimental_analyzer': {
            'value': '0'
        }
    }
    assert result_profiles == sorted(['test_profile', 'web', 'web2', 'app'])


def test_normalizing_passed_settings_none(user_ent):
    settings = None
    result = user_ent._normalize_settings(settings)
    assert result == {}


def test_normalizing_passed_settings_empty(user_ent):
    settings = {}
    result = user_ent._normalize_settings(settings)
    assert result == {}


def test_normalizing_passed_settings_lower_string(user_ent):
    settings = {
        'max_memory_usage': {
            'value': '15000',
            'min': '15000',
            'max': '16000',
            'writability': 'const'
        }
    }
    result = user_ent._normalize_settings(settings)
    assert result == {
        'max_memory_usage': {
            'value': '15000',
            'min': '15000',
            'max': '16000',
            'writability': 'CONST'
        }
    }


def test_normalizing_passed_settings_lower_alias(user_ent):
    settings = {
        'max_memory_usage': {
            'value': '15000',
            'min': '15000',
            'max': '16000',
            'writability': 'readonly'
        }
    }
    result = user_ent._normalize_settings(settings)
    assert result == {
        'max_memory_usage': {
            'value': '15000',
            'min': '15000',
            'max': '16000',
            'writability': 'CONST'
        }
    }


def test_normalizing_passed_settings_many_settings(user_ent):
    settings = {
        'max_memory_usage': {
            'value': '15000',
            'min': '15000',
            'max': '16000',
            'writability': 'readonly'
        },
        'allow_experimental_analyzer': {
            'value': '0',
            'min': None,
            'max': None,
            'writability': None
        },
        'session_timezone': {
            'value': 'Asia/Novosibirsk',
            'min': None,
            'max': None,
            'writability': 'changeable_in_readonly'
        }
    }
    result = user_ent._normalize_settings(settings)
    assert result == {
        'max_memory_usage': {
            'value': '15000',
            'min': '15000',
            'max': '16000',
            'writability': 'CONST'
        },
        'allow_experimental_analyzer': {
            'value': '0',
            'min': None,
            'max': None,
            'writability': None
        },
        'session_timezone': {
            'value': 'Asia/Novosibirsk',
            'min': None,
            'max': None,
            'writability': 'CHANGEABLE_IN_READONLY'
        }
    }


def test_normalizing_passed_settings_quota_numbers(user_ent):
    result = user_ent._normalize_settings(
        {
            'max_memory_usage': {
                'value': 15000,
                'min': 15000,
                'max': 16000,
                'writability': 'CONST'
            }
        }
    )
    assert result == {
        'max_memory_usage': {
            'value': '15000',
            'min': '15000',
            'max': '16000',
            'writability': 'CONST'
        }
    }


def test_normalizing_passed_settings_quota_floats(user_ent):
    result = user_ent._normalize_settings(
        {
            'async_insert_busy_timeout_increase_rate': {
                'value': 0.2,
                'min': 0.1,
                'max': 0.5,
                'writability': 'CONST'
            }
        }
    )
    assert result == {
        'async_insert_busy_timeout_increase_rate': {
            'value': '0.2',
            'min': '0.1',
            'max': '0.5',
            'writability': 'CONST'
        }
    }


def test_returning_entity_no_changes(user_ent):
    result = user_ent.compare_and_build_clause(
        {
            'max_memory_usage': {
                'value': '15000',
                'min': '15000',
                'max': '16000',
                'writability': 'CONST'
            }
        },
        []
    )
    assert result == (False, '', {})


def test_returning_entity_writability_changed(user_ent):
    result = user_ent.compare_and_build_clause(
        {
            'max_memory_usage': {
                'value': '15000',
                'min': '15000',
                'max': '16000',
                'writability': 'WRITABLE'
            }
        },
        []
    )
    assert result[0] is True
    assert result[1] == " SETTINGS max_memory_usage='15000' MIN '15000' MAX '16000' WRITABLE"
    assert result[2] == {
        'before': {
            'settings': {
                'max_memory_usage': {
                    'value': '15000',
                    'min': '15000',
                    'max': '16000',
                    'writability': 'CONST'
                }
            },
            'profiles': []
        },
        'after': {
            'settings': {
                'max_memory_usage': {
                    'value': '15000',
                    'min': '15000',
                    'max': '16000',
                    'writability': 'WRITABLE'
                }
            },
            'profiles': []
        }
    }


def test_returning_entity_new_profile_appear(user_ent):
    result = user_ent.compare_and_build_clause(
        {
            'max_memory_usage': {
                'value': '15000',
                'min': '15000',
                'max': '16000',
                'writability': 'WRITABLE'
            }
        },
        ['web']
    )
    assert result[0] is True
    assert result[1] == " SETTINGS PROFILE 'web', max_memory_usage='15000' MIN '15000' MAX '16000' WRITABLE"
    assert result[2] == {
        'before': {
            'settings': {
                'max_memory_usage': {
                    'value': '15000',
                    'min': '15000',
                    'max': '16000',
                    'writability': 'CONST'
                }
            },
            'profiles': []
        },
        'after': {
            'settings': {
                'max_memory_usage': {
                    'value': '15000',
                    'min': '15000',
                    'max': '16000',
                    'writability': 'WRITABLE'
                }
            },
            'profiles': ['web']
        }
    }


def test_returning_entity_new_setting_appear(user_ent):
    result = user_ent.compare_and_build_clause(
        {
            'max_memory_usage': {
                'value': '15000',
                'min': '15000',
                'max': '16000',
                'writability': 'WRITABLE'
            },
            'allow_experimental_analyzer': {
                'value': '0',
                'min': None,
                'max': None,
                'writability': None
            }

        },
        []
    )
    assert result[0] is True
    assert result[1] == " SETTINGS max_memory_usage='15000' MIN '15000' MAX '16000' WRITABLE, allow_experimental_analyzer='0'"
    assert result[2] == {
        'before': {
            'settings': {
                'max_memory_usage': {
                    'value': '15000',
                    'min': '15000',
                    'max': '16000',
                    'writability': 'CONST'
                }
            },
            'profiles': []
        },
        'after': {
            'settings': {
                'max_memory_usage': {
                    'value': '15000',
                    'min': '15000',
                    'max': '16000',
                    'writability': 'WRITABLE'
                },
                'allow_experimental_analyzer': {
                    'value': '0',
                    'min': None,
                    'max': None,
                    'writability': None
                }
            },
            'profiles': []
        }
    }


def test_returning_entity_purge_settings(user_ent):
    result = user_ent.compare_and_build_clause(
        {},
        []
    )
    assert result[0] is True
    assert result[1] == " SETTINGS NONE"
    assert result[2] == {
        'before': {
            'settings': {
                'max_memory_usage': {
                    'value': '15000',
                    'min': '15000',
                    'max': '16000',
                    'writability': 'CONST'
                }
            },
            'profiles': []
        },
        'after': {
            'settings': {},
            'profiles': []
        }
    }


def test_returning_entity_set_only_profiles(user_ent):
    result = user_ent.compare_and_build_clause(
        {},
        ['web', 'app', 'front']
    )
    assert result[0] is True
    assert result[1] == " SETTINGS PROFILE 'web', PROFILE 'app', PROFILE 'front'"
    assert result[2] == {
        'before': {
            'settings': {
                'max_memory_usage': {
                    'value': '15000',
                    'min': '15000',
                    'max': '16000',
                    'writability': 'CONST'
                }
            },
            'profiles': []
        },
        'after': {
            'settings': {},
            'profiles': ['web', 'app', 'front']
        }
    }
