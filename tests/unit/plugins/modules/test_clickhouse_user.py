from __future__ import absolute_import, division, print_function
import pytest
from ansible_collections.community.clickhouse.plugins.modules.clickhouse_user import ClickHouseUser


@pytest.fixture
def user(mocker):
    mocker.patch("ansible_collections.community.clickhouse.plugins.modules.clickhouse_user.execute_query", return_value=[])

    mock_module = mocker.MagicMock()
    mock_module.check_mode = False
    mock_client = mocker.MagicMock()

    return ClickHouseUser(module=mock_module, client=mock_client, name="alice")


@pytest.fixture
def user_exist(mocker):
    mocker.patch(
        "ansible_collections.community.clickhouse.plugins.modules.clickhouse_user.execute_query",
        side_effect=[
            [('alice', 'local_directory', ['sha256_hash'], [])],
            [],
            [],
            [([], [], [], [])]
        ])

    mock_module = mocker.MagicMock()
    mock_module.check_mode = False
    mock_client = mocker.MagicMock()

    return ClickHouseUser(module=mock_module, client=mock_client, name="alice")


@pytest.fixture
def mock_execute(mocker):
    return mocker.patch("ansible_collections.community.clickhouse.plugins.modules.clickhouse_user.execute_query", return_value=[])


def get_executed_query(mock_execute):
    return mock_execute.call_args[0][2]


def test_user_not_exists(user):
    assert user.user_exists is False


@pytest.mark.parametrize(
    "auth,leg_type,leg_pass,expected,param",
    [
        (None, 'sha256_password', 'test_pass', " IDENTIFIED WITH sha256_password BY 'test_pass'", None),
        ({'type': 'sha256_password', 'password': 'test_pass'}, None, None, " IDENTIFIED WITH sha256_password BY %(password)s", {'password': 'test_pass'}),
        ({'type': 'sha256_hash', 'password': 'test_hash'}, None, None, " IDENTIFIED WITH sha256_hash BY %(password)s", {'password': 'test_hash'}),
        ({'type': 'not_identified'}, None, None, " NOT IDENTIFIED", None),
        ({'type': 'no_password'}, None, None, " IDENTIFIED WITH NO_PASSWORD", None),
        ({'type': 'ldap', 'server': 'ad'}, None, None, " IDENTIFIED WITH ldap SERVER %(server)s", {'server': 'ad'}),
    ]
)
def test_build_ident_clause_correct(user, auth, leg_type, leg_pass, expected, param):
    result, result_param = user._build_identified_clause(auth, leg_type, leg_pass)

    user.module.fail_json.assert_not_called()
    assert result == expected
    assert param == result_param


@pytest.mark.parametrize("auth", [{'type': 'ldap'},])
def test_build_ident_clause_incorrect(user, auth):
    user._build_identified_clause(auth)

    user.module.fail_json.assert_called_once()


def test_create_ldap_user(user, mock_execute):
    user.create(
        None, None, None, None, None, None,
        'listed_only', None, 'listed_only',
        {'type': 'ldap', 'server': 'ad'}, [])
    actual_query = get_executed_query(mock_execute)
    assert actual_query == "CREATE USER `alice` IDENTIFIED WITH ldap SERVER %(server)s"


def test_create_sha256_pass_user(user, mock_execute):
    user.create(
        None, None, None, None, None, None,
        'listed_only', None, 'listed_only',
        {'type': 'sha256_password', 'password': 'password1'}, [])
    actual_query = get_executed_query(mock_execute)
    assert actual_query == "CREATE USER `alice` IDENTIFIED WITH sha256_password BY %(password)s"


def test_create_sha256_hash_user(user, mock_execute):
    user.create(
        None, None, None, None, None, None,
        'listed_only', None, 'listed_only',
        {'type': 'sha256_hash', 'password': '0b14d501a594442a01c6859541bcb3e8164d183d32937b851835442f69d5c94e'},
        []
    )
    actual_query = get_executed_query(mock_execute)
    assert actual_query == "CREATE USER `alice` IDENTIFIED WITH sha256_hash BY %(password)s"


def test_create_sha256_hash_user_old_way(user, mock_execute):
    user.create(
        'sha256_hash', '0b14d501a594442a01c6859541bcb3e8164d183d32937b851835442f69d5c94e',
        None, [], None, None, 'listed_only',
        None, 'listed_only', None, [])
    actual_query = get_executed_query(mock_execute)
    assert actual_query == "CREATE USER `alice` IDENTIFIED WITH sha256_hash BY '0b14d501a594442a01c6859541bcb3e8164d183d32937b851835442f69d5c94e'"


def test_create_sha256_pass_user_old_way(user, mock_execute):
    user.create(
        'sha256_password', 'password1',
        None, None, None, None,
        'listed_only', None, 'listed_only', None, [])
    actual_query = get_executed_query(mock_execute)
    assert actual_query == "CREATE USER `alice` IDENTIFIED WITH sha256_password BY 'password1'"


def test_create_not_identified_user(user, mock_execute):
    user.create(
        None, None, None, None, None, None,
        'listed_only', None, 'listed_only',
        {'type': 'not_identified'}, [])
    actual_query = get_executed_query(mock_execute)
    assert actual_query == "CREATE USER `alice` NOT IDENTIFIED"


def test_create_no_password_user(user, mock_execute):
    user.create(
        None, None, None, None, None, None,
        'listed_only', None, 'listed_only',
        {'type': 'no_password'}, [])
    actual_query = get_executed_query(mock_execute)
    assert actual_query == "CREATE USER `alice` IDENTIFIED WITH NO_PASSWORD"


def test_create_user_with_settings(user, mock_execute):
    user.create(
        None, None, None, None,
        ["max_concurrent_queries=3", "max_threads=8"],
        None, 'listed_only', None, 'listed_only',
        {'type': 'sha256_password', 'password': 'password1'}, [])
    actual_query = get_executed_query(mock_execute)
    assert actual_query == "CREATE USER `alice` IDENTIFIED WITH sha256_password BY %(password)s SETTINGS max_concurrent_queries=3, max_threads=8"


def test_create_user_with_host_name_passed(user, mock_execute):
    user.create(
        None, None, None,
        [{'type': 'NAME', 'hosts': ['host1']}],
        None, None, 'listed_only',
        None, 'listed_only',
        {'type': 'sha256_password', 'password': 'password1'}, [])
    actual_query = get_executed_query(mock_execute)
    assert actual_query == "CREATE USER `alice` IDENTIFIED WITH sha256_password BY %(password)s HOST NAME 'host1'"


def test_create_user_with_host_ip_passed(user, mock_execute):
    user.create(
        None, None, None,
        [{'type': 'IP', 'hosts': ['127.0.0.1']}],
        None, None,
        'listed_only', None, 'listed_only',
        {'type': 'sha256_password', 'password': 'password1'}, [])
    actual_query = get_executed_query(mock_execute)
    assert actual_query == "CREATE USER `alice` IDENTIFIED WITH sha256_password BY %(password)s HOST IP '127.0.0.1'"


def test_create_user_with_host_cidr_passed(user, mock_execute):
    user.create(
        None, None, None,
        [{'type': 'IP', 'hosts': ['10.0.0.0/8']}],
        None, None,
        'listed_only', None,
        'listed_only', {'type': 'sha256_password', 'password': 'password1'}, [])
    actual_query = get_executed_query(mock_execute)
    assert actual_query == "CREATE USER `alice` IDENTIFIED WITH sha256_password BY %(password)s HOST IP '10.0.0.0/8'"


def test_create_user_with_host_complex_passed(user, mock_execute):
    user.create(
        None, None, None,
        [{'type': 'IP', 'hosts': ['10.0.0.0/8']}, {'type': 'NAME', 'hosts': ['host1']}],
        None, None,
        'listed_only', None, 'listed_only',
        {'type': 'sha256_password', 'password': 'password1'}, [])
    actual_query = get_executed_query(mock_execute)
    assert actual_query == "CREATE USER `alice` IDENTIFIED WITH sha256_password BY %(password)s HOST IP '10.0.0.0/8' HOST NAME 'host1'"


def test_create_user_using_dict_settings(user, mock_execute):
    user.create(
        None, None, None, None,
        {'max_memory_usage': {'value': '10G'}}, None,
        'listed_only', None, 'listed_only',
        None, [])
    actual_query = get_executed_query(mock_execute)
    assert actual_query == "CREATE USER `alice` SETTINGS max_memory_usage='10G'"


def test_create_user_using_profiles_prameter(user, mock_execute):
    user.create(
        None, None, None, None,
        {}, None,
        'listed_only', None, 'listed_only',
        None, ['app', 'web'])
    actual_query = get_executed_query(mock_execute)
    assert actual_query == "CREATE USER `alice` SETTINGS PROFILE 'app', PROFILE 'web'"


def test_create_user_using_profiles_parameter_and_settings(user, mock_execute):
    user.create(
        None, None, None, None,
        {'max_memory_usage': {'value': '10G'}}, None,
        'listed_only', None, 'listed_only',
        None, ['app', 'web'])
    actual_query = get_executed_query(mock_execute)
    assert actual_query == "CREATE USER `alice` SETTINGS PROFILE 'app', PROFILE 'web', max_memory_usage='10G'"


def test_create_user_using_profiles_parameter_and_settings_complex(user, mock_execute):
    user.create(
        None, None, None, None,
        {
            'max_memory_usage': {'value': '10G', 'max': '100G', 'min': '1G', 'writability': 'const'},
            'allow_experimental_variant_type': {'value': '1', 'min': None, 'max': None, 'writability': 'WRITABLE'}
        }, None,
        'listed_only', None, 'listed_only',
        None, ['app', 'web', 'test_profile', 'web2']
    )
    actual_query = get_executed_query(mock_execute)
    assert actual_query == (
        "CREATE USER `alice` SETTINGS PROFILE 'app', "
        "PROFILE 'web', PROFILE 'test_profile', "
        "PROFILE 'web2', "
        "max_memory_usage='10G' MIN '1G' MAX '100G' CONST, "
        "allow_experimental_variant_type='1' WRITABLE")


def test_alter_user_nothing_changed(user_exist, mock_execute):
    changed = user_exist.update(
        'on_create', None, None, None, [],
        None, 'listed_only', None, 'listed_only',
        {},
        {'type': 'sha256_password'}, []
    )
    assert changed is False
    assert mock_execute.call_count == 0
    # assert mock_execute.assert_not_called()


def test_alter_user_profile_added(user_exist, mock_execute):
    changed = user_exist.update(
        'on_create', None, None, None, [],
        [], 'listed_only', None, 'listed_only',
        {},
        {'type': 'sha256_password'}, ['web']
    )
    actual_query = get_executed_query(mock_execute)
    assert changed is True
    assert actual_query == "ALTER USER `alice` SETTINGS PROFILE 'web'"


def test_alter_user_setting_added(user_exist, mock_execute):
    changed = user_exist.update(
        'on_create', None, None, None, [],
        [], 'listed_only', None, 'listed_only',
        {'max_memory_usage': {'value': '10G', 'min': '1G', 'max': '100G', 'writability': 'const'}},
        {'type': 'sha256_password'}, []
    )
    actual_query = get_executed_query(mock_execute)
    assert changed is True
    assert actual_query == (
        "ALTER USER `alice` SETTINGS "
        "max_memory_usage='10G' MIN '1G' MAX '100G' CONST")


def test_alter_user_old_setting_added(user_exist, mock_execute):
    changed = user_exist.update(
        'on_create', None, None, None, [],
        [], 'listed_only', None, 'listed_only',
        ["max_memory_usage='10G' MIN = '1G' MAX = '100G' CONST"],
        {'type': 'sha256_password'}, []
    )
    actual_query = get_executed_query(mock_execute)
    assert changed is True
    assert actual_query == "ALTER USER `alice` SETTINGS max_memory_usage='10G' MIN = '1G' MAX = '100G' CONST"


def test_alter_user_old_setting_with_profile_added(user_exist, mock_execute):
    changed = user_exist.update(
        'on_create', None, None, None, [],
        [], 'listed_only', None, 'listed_only',
        ["max_memory_usage='10G' MIN = '1G' MAX = '100G' CONST", "PROFILE web"],
        {'type': 'sha256_password'}, []
    )
    actual_query = get_executed_query(mock_execute)
    assert changed is True
    assert actual_query == "ALTER USER `alice` SETTINGS max_memory_usage='10G' MIN = '1G' MAX = '100G' CONST, PROFILE web"


def test_drop(user_exist, mock_execute):
    changed = user_exist.drop(None)
    actual_query = get_executed_query(mock_execute)
    assert changed is True
    assert actual_query == "DROP USER `alice`"
