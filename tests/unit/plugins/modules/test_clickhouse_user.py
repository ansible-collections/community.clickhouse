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
def mock_execute(mocker):
    return mocker.patch("ansible_collections.community.clickhouse.plugins.modules.clickhouse_user.execute_query", return_value=[])


def get_executed_query(mock_execute):
    return mock_execute.call_args[0][2]


def test_user_not_exists(user):
    assert user.user_exists is False


def test_create_ldap_user(user, mock_execute):
    user.create(
        None, None, None, None, None, None,
        'listed_only', None, 'listed_only',
        {'type': 'ldap', 'server': 'ad'})
    actual_query = get_executed_query(mock_execute)
    assert actual_query == "CREATE USER 'alice' IDENTIFIED WITH ldap SERVER 'ad'"


def test_create_ldap_user_without_server(user, mock_execute):
    user.create(
        None, None, None, None, None, None,
        'listed_only', None, 'listed_only',
        {'type': 'ldap'})
    user.module.fail_json.assert_called_once()


def test_create_sha256_pass_user(user, mock_execute):
    user.create(
        None, None, None, None, None, None,
        'listed_only', None, 'listed_only',
        {'type': 'sha256_password', 'password': 'password1'})
    actual_query = get_executed_query(mock_execute)
    assert actual_query == "CREATE USER 'alice' IDENTIFIED WITH sha256_password BY 'password1'"


def test_create_sha256_hash_user(user, mock_execute):
    user.create(
        None, None, None, None, None, None,
        'listed_only', None, 'listed_only',
        {'type': 'sha256_hash', 'password': '0b14d501a594442a01c6859541bcb3e8164d183d32937b851835442f69d5c94e'})
    actual_query = get_executed_query(mock_execute)
    assert actual_query == "CREATE USER 'alice' IDENTIFIED WITH sha256_hash BY '0b14d501a594442a01c6859541bcb3e8164d183d32937b851835442f69d5c94e'"


def test_create_sha256_hash_user_old_way(user, mock_execute):
    user.create(
        'sha256_hash', '0b14d501a594442a01c6859541bcb3e8164d183d32937b851835442f69d5c94e',
        None, [], None, None, 'listed_only', None, 'listed_only', None)
    actual_query = get_executed_query(mock_execute)
    assert actual_query == "CREATE USER 'alice' IDENTIFIED WITH sha256_hash BY '0b14d501a594442a01c6859541bcb3e8164d183d32937b851835442f69d5c94e'"


def test_create_sha256_pass_user_old_way(user, mock_execute):
    user.create(
        'sha256_password', 'password1',
        None, None, None, None, 'listed_only', None, 'listed_only', None)
    actual_query = get_executed_query(mock_execute)
    assert actual_query == "CREATE USER 'alice' IDENTIFIED WITH sha256_password BY 'password1'"


def test_create_not_identified_user(user, mock_execute):
    user.create(
        None, None, None, None, None, None,
        'listed_only', None, 'listed_only',
        {'type': 'not_identified'})
    actual_query = get_executed_query(mock_execute)
    assert actual_query == "CREATE USER 'alice' NOT IDENTIFIED"


def test_create_no_password_user(user, mock_execute):
    user.create(
        None, None, None, None, None, None,
        'listed_only', None, 'listed_only',
        {'type': 'no_password'})
    actual_query = get_executed_query(mock_execute)
    assert actual_query == "CREATE USER 'alice' IDENTIFIED WITH NO_PASSWORD"


def test_create_user_with_settings(user, mock_execute):
    user.create(
        None, None, None, None,
        ["max_concurrent_queries=3", "max_threads=8"],
        None, 'listed_only', None, 'listed_only',
        {'type': 'sha256_password', 'password': 'password1'})
    actual_query = get_executed_query(mock_execute)
    assert actual_query == "CREATE USER 'alice' IDENTIFIED WITH sha256_password BY 'password1' SETTINGS max_concurrent_queries=3, max_threads=8"


def test_create_user_with_host_name_passed(user, mock_execute):
    user.create(
        None, None, None,
        [{'type': 'NAME', 'hosts': ['host1']}],
        None, None, 'listed_only',
        None, 'listed_only',
        {'type': 'sha256_password', 'password': 'password1'})
    actual_query = get_executed_query(mock_execute)
    assert actual_query == "CREATE USER 'alice' IDENTIFIED WITH sha256_password BY 'password1' HOST NAME 'host1'"


def test_create_user_with_host_ip_passed(user, mock_execute):
    user.create(
        None, None, None,
        [{'type': 'IP', 'hosts': ['127.0.0.1']}],
        None, None,
        'listed_only', None, 'listed_only',
        {'type': 'sha256_password', 'password': 'password1'})
    actual_query = get_executed_query(mock_execute)
    assert actual_query == "CREATE USER 'alice' IDENTIFIED WITH sha256_password BY 'password1' HOST IP '127.0.0.1'"


def test_create_user_with_host_cidr_passed(user, mock_execute):
    user.create(
        None, None, None,
        [{'type': 'IP', 'hosts': ['10.0.0.0/8']}],
        None, None,
        'listed_only', None,
        'listed_only', {'type': 'sha256_password', 'password': 'password1'})
    actual_query = get_executed_query(mock_execute)
    assert actual_query == "CREATE USER 'alice' IDENTIFIED WITH sha256_password BY 'password1' HOST IP '10.0.0.0/8'"


def test_create_user_with_host_complex_passed(user, mock_execute):
    user.create(
        None, None, None,
        [{'type': 'IP', 'hosts': ['10.0.0.0/8']}, {'type': 'NAME', 'hosts': ['host1']}],
        None, None,
        'listed_only', None, 'listed_only',
        {'type': 'sha256_password', 'password': 'password1'})
    actual_query = get_executed_query(mock_execute)
    assert actual_query == "CREATE USER 'alice' IDENTIFIED WITH sha256_password BY 'password1' HOST IP '10.0.0.0/8' HOST NAME 'host1'"
