from __future__ import absolute_import, division, print_function
import pytest
from ansible_collections.community.clickhouse.plugins.modules.clickhouse_row_policy import ClickHouseRowPolicy


@pytest.fixture
def row_policy(mocker):
    mock_module = mocker.MagicMock()
    mock_module.check_mode = False
    mock_client = mocker.MagicMock()

    return ClickHouseRowPolicy(module=mock_module, client=mock_client, name="test_policy")


def get_executed_query(mock_execute):
    # Assumes execute_query(module, client, query, params)
    args, kwargs = mock_execute.call_args
    return args[2] if len(args) > 2 else kwargs.get('query')


@pytest.fixture
def mock_execute(mocker):
    return mocker.patch(
        "ansible_collections.community.clickhouse.plugins.modules.clickhouse_row_policy.execute_query",
        return_value=[]
    )


def test_fetch_row_policy_not_exists(row_policy, mock_execute):
    row_policy._load()

    assert row_policy.exists is False
    assert mock_execute.call_count == 1


def test_fetch_row_policy_exists(row_policy, mocker):
    mock_execute = mocker.patch(
        "ansible_collections.community.clickhouse.plugins.modules.clickhouse_row_policy.execute_query",
        return_value=[('mydb', 'table1', 'b = 1', 0, 0, ['test_user'], [])]

    )
    row_policy._load()

    assert mock_execute.call_count == 1
    assert row_policy.exists is True
    assert row_policy.database == 'mydb'
    assert row_policy.table == 'table1'
    assert row_policy.select_filter == 'b = 1'
    assert row_policy.is_restrictive == 0
    assert row_policy.apply_to_all == 0
    assert row_policy.apply_to_list == ['test_user']
    assert row_policy.apply_to_except == []


def test_build_to_clause_nothing_passed(row_policy):
    result = row_policy._build_to_clause([], 0, [])
    assert result == ""


def test_build_to_clause_apply_to_all(row_policy):
    result = row_policy._build_to_clause([], 1, [])
    assert result == " TO ALL"


def test_build_to_clause_apply_to_all_except_one(row_policy):
    result = row_policy._build_to_clause([], 1, ['except1'])
    assert result == " TO ALL EXCEPT except1"


def test_build_to_clause_apply_to_all_except_many(row_policy):
    result = row_policy._build_to_clause([], 1, ['except1', 'except2'])
    assert result == " TO ALL EXCEPT except1, except2"


def test_build_to_clause_apply_to_list_one(row_policy):
    result = row_policy._build_to_clause(['test_user'], 0, [])
    assert result == " TO test_user"


def test_build_to_clause_apply_to_list_many(row_policy):
    result = row_policy._build_to_clause(['test_user', 'test_user2'], 0, [])
    assert result == " TO test_user, test_user2"


def test_build_target_passed_db_table(row_policy, mocker):
    mock_execute = mocker.patch("ansible_collections.community.clickhouse.plugins.modules.clickhouse_row_policy.execute_query",
        return_value=[('default_db')])
    result = row_policy._build_target("db.table")
    assert result == "db.table"
    assert mock_execute.call_count == 0


def test_build_target_passed_db_asterix(row_policy, mocker):
    mock_execute =mocker.patch("ansible_collections.community.clickhouse.plugins.modules.clickhouse_row_policy.execute_query",
        return_value=[('default_db')])
    result = row_policy._build_target("db.*")
    assert result == "db.*"
    assert mock_execute.call_count == 0


def test_build_target_passed_table_only(row_policy, mocker):
    mock_execute = mocker.patch("ansible_collections.community.clickhouse.plugins.modules.clickhouse_row_policy.execute_query",
        return_value=[('default_db')])
    result = row_policy._build_target("table")
    assert result == "default_db.table"
    assert mock_execute.call_count == 1


def test_build_using_paramter_simple(row_policy, mocker):
    mock_execute = mocker.patch("ansible_collections.community.clickhouse.plugins.modules.clickhouse_row_policy.execute_query",
        return_value=[('SELECT 1 FROM _to_normalize WHERE a = 1')])
    result = row_policy._normalize_using_parameter('a=1')
    assert result == 'a = 1'
    assert mock_execute.call_count == 1


def test_build_using_paramter_complex (row_policy, mocker):
    mock_execute = mocker.patch("ansible_collections.community.clickhouse.plugins.modules.clickhouse_row_policy.execute_query",
        return_value=[('SELECT 1 FROM _to_normalize WHERE (((b >= 1) AND (c <= 2)) OR 1 OR 2) OR 3')])
    result = row_policy._normalize_using_parameter('(((b>=1)    AND (c<=2))OR 1 OR 2)OR 3')
    assert result == '(((b >= 1) AND (c <= 2)) OR 1 OR 2) OR 3'
    assert mock_execute.call_count == 1

def test_create_row_policy_no_assign(row_policy, mocker):
    mock_execute = mocker.patch("ansible_collections.community.clickhouse.plugins.modules.clickhouse_row_policy.execute_query",
        return_value=[('SELECT 1 FROM _to_normalize WHERE b = a')])
    row_policy._exist = False
    changed = row_policy.create("db.tab", "b = a", 0, [], 0, [], None)
    actuall_query = get_executed_query(mock_execute)
    assert changed is True
    assert mock_execute.call_count == 2
    assert actuall_query == "CREATE ROW POLICY 'test_policy' ON db.tab USING b = a AS PERMISSIVE"

def test_create_row_policy_on_cluster(row_policy, mocker):
    mock_execute = mocker.patch("ansible_collections.community.clickhouse.plugins.modules.clickhouse_row_policy.execute_query",
        return_value=[('SELECT 1 FROM _to_normalize WHERE b = a')])
    row_policy._exist = False
    changed = row_policy.create("db.tab", "b = a", 0, [], 0, [], 'test_cluster')
    actuall_query = get_executed_query(mock_execute)
    assert changed is True
    assert mock_execute.call_count == 2
    assert actuall_query == "CREATE ROW POLICY 'test_policy' ON CLUSTER 'test_cluster' ON db.tab USING b = a AS PERMISSIVE"


def test_create_row_policy_restrictive_no_assign(row_policy, mocker):
    mock_execute = mocker.patch("ansible_collections.community.clickhouse.plugins.modules.clickhouse_row_policy.execute_query",
        return_value=[('SELECT 1 FROM _to_normalize WHERE b = a')])
    row_policy._exist = False
    changed = row_policy.create("db.tab", "b = a", 1, [], 0, [], None)
    actuall_query = get_executed_query(mock_execute)
    assert changed is True
    assert mock_execute.call_count == 2
    assert actuall_query == "CREATE ROW POLICY 'test_policy' ON db.tab USING b = a AS RESTRICTIVE"


def test_create_row_policy_assign_all(row_policy, mocker):
    mock_execute = mocker.patch("ansible_collections.community.clickhouse.plugins.modules.clickhouse_row_policy.execute_query",
        return_value=[('SELECT 1 FROM _to_normalize WHERE b = a')])
    row_policy._exist = False
    changed = row_policy.create("db.tab", "b = a", 0, [], 1, [], None)
    actuall_query = get_executed_query(mock_execute)
    assert changed is True
    assert mock_execute.call_count == 2
    assert actuall_query == "CREATE ROW POLICY 'test_policy' ON db.tab USING b = a AS PERMISSIVE TO ALL"


def test_create_row_policy_assign_all_except(row_policy, mocker):
    mock_execute = mocker.patch("ansible_collections.community.clickhouse.plugins.modules.clickhouse_row_policy.execute_query",
        return_value=[('SELECT 1 FROM _to_normalize WHERE b = a')])
    row_policy._exist = False
    changed = row_policy.create("db.tab", "b = a", 0, [], 1, ['except1', 'except2'], None)
    actuall_query = get_executed_query(mock_execute)
    assert changed is True
    assert mock_execute.call_count == 2
    assert actuall_query == "CREATE ROW POLICY 'test_policy' ON db.tab USING b = a AS PERMISSIVE TO ALL EXCEPT except1, except2"


def test_create_row_policy_assign_list(row_policy, mocker):
    mock_execute = mocker.patch("ansible_collections.community.clickhouse.plugins.modules.clickhouse_row_policy.execute_query",
        return_value=[('SELECT 1 FROM _to_normalize WHERE b = a')])
    row_policy._exist = False
    changed = row_policy.create("db.tab", "b = a", 0, ['test_user', 'test_role'], 0, [], None)
    actuall_query = get_executed_query(mock_execute)
    assert changed is True
    assert mock_execute.call_count == 2
    assert actuall_query == "CREATE ROW POLICY 'test_policy' ON db.tab USING b = a AS PERMISSIVE TO test_user, test_role"


