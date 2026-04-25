from __future__ import absolute_import, division, print_function
import pytest
from ansible_collections.community.clickhouse.plugins.modules.clickhouse_row_policy import ClickHouseRowPolicy


@pytest.fixture
def row_policy(mocker):
    mock_module = mocker.MagicMock()
    mock_module.check_mode = False
    mock_client = mocker.MagicMock()

    return ClickHouseRowPolicy(module=mock_module, client=mock_client, name="test_policy", database="test_db", table="test_table")


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
        return_value=[('b = 1', 0, 0, ['test_user'], [])]

    )
    row_policy._load()

    assert mock_execute.call_count == 1
    assert row_policy.exists is True
    assert row_policy.database == 'test_db'
    assert row_policy.table == 'test_table'
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
    assert result == " TO ALL EXCEPT `except1`"


def test_build_to_clause_apply_to_all_except_many(row_policy):
    result = row_policy._build_to_clause([], 1, ['except1', 'except2'])
    assert result == " TO ALL EXCEPT `except1`, `except2`"


def test_build_to_clause_apply_to_list_one(row_policy):
    result = row_policy._build_to_clause(['test_user'], 0, [])
    assert result == " TO `test_user`"


def test_build_to_clause_apply_to_list_many(row_policy):
    result = row_policy._build_to_clause(['test_user', 'test_user2'], 0, [])
    assert result == " TO `test_user`, `test_user2`"


def test_build_using_parameter_simple(row_policy, mocker):
    mock_execute = mocker.patch("ansible_collections.community.clickhouse.plugins.modules.clickhouse_row_policy.execute_query",
                                return_value=[[('SELECT 1 FROM _to_normalize WHERE a = 1')]])
    result = row_policy._normalize_using_parameter('a=1')
    assert result == 'a = 1'
    assert mock_execute.call_count == 1


def test_build_using_parameter_complex(row_policy, mocker):
    mock_execute = mocker.patch("ansible_collections.community.clickhouse.plugins.modules.clickhouse_row_policy.execute_query",
                                return_value=[[('SELECT 1 FROM _to_normalize WHERE (((b >= 1) AND (c <= 2)) OR 1 OR 2) OR 3')]])
    result = row_policy._normalize_using_parameter('(((b>=1)    AND (c<=2))OR 1 OR 2)OR 3')
    assert result == '(((b >= 1) AND (c <= 2)) OR 1 OR 2) OR 3'
    assert mock_execute.call_count == 1


def test_compare_apply_to_no_differ(row_policy):
    row_policy._exists = True
    row_policy._apply_to_all = False
    row_policy._apply_to_except = []
    row_policy._apply_to_list = ['test_user']

    result = row_policy._compare_apply_to(['test_user'], False, [])

    assert result is True


def test_compare_apply_to_to_all_differs(row_policy):
    row_policy._exists = True
    row_policy._apply_to_all = False
    row_policy._apply_to_except = []
    row_policy._apply_to_list = []

    result = row_policy._compare_apply_to([], True, [])

    assert result is False


def test_compare_apply_to_except_differs(row_policy):
    row_policy._exists = True
    row_policy._apply_to_all = True
    row_policy._apply_to_except = []
    row_policy._apply_to_list = []

    result = row_policy._compare_apply_to([], True, ['test_user'])

    assert result is False


def test_compare_apply_to_to_differs(row_policy):
    row_policy._exists = True
    row_policy._apply_to_all = False
    row_policy._apply_to_except = []
    row_policy._apply_to_list = ['test_user']

    result = row_policy._compare_apply_to(['test_user2'], False, [])

    assert result is False


def test_needs_update_no_changes(row_policy):
    row_policy._exists = True
    row_policy._select_filter = 'b = 1'
    row_policy._apply_to_all = False
    row_policy._apply_to_except = []
    row_policy._apply_to_list = []
    row_policy._is_restrictive = False

    result = row_policy._needs_update('b = 1', False, [], False, [])

    assert result is False


def test_needs_update_filter_differs(row_policy):
    row_policy._exists = True
    row_policy._select_filter = 'b = 1'
    row_policy._apply_to_all = False
    row_policy._apply_to_except = []
    row_policy._apply_to_list = []
    row_policy._is_restrictive = False

    result = row_policy._needs_update('b = 2', False, [], False, [])

    assert result is True


def test_needs_update_is_restrictive_differs(row_policy):
    row_policy._exists = True
    row_policy._select_filter = 'b = 1'
    row_policy._apply_to_all = False
    row_policy._apply_to_except = []
    row_policy._apply_to_list = []
    row_policy._is_restrictive = False

    result = row_policy._needs_update('b = 2', True, [], False, [])

    assert result is True


def test_needs_update_compare_apply_to_differs(mocker, row_policy):
    row_policy._exists = True
    row_policy._select_filter = 'b = 1'
    row_policy._apply_to_all = False
    row_policy._apply_to_except = []
    row_policy._apply_to_list = []
    row_policy._is_restrictive = False

    result = row_policy._needs_update('b = 1', False, ['test_user'], False, [])

    assert result is True


def test_create_row_policy_no_assign(row_policy, mocker):
    mock_execute = mocker.patch("ansible_collections.community.clickhouse.plugins.modules.clickhouse_row_policy.execute_query",
                                return_value=[[('SELECT 1 FROM _to_normalize WHERE b = a')]])
    row_policy._exist = False
    changed = row_policy.create("b = a", 0, [], 0, [], None)
    actuall_query = get_executed_query(mock_execute)
    assert changed is True
    assert mock_execute.call_count == 2
    assert actuall_query == "CREATE ROW POLICY `test_policy` ON `test_db`.`test_table` USING b = a AS PERMISSIVE"


def test_create_row_policy_on_cluster(row_policy, mocker):
    mock_execute = mocker.patch("ansible_collections.community.clickhouse.plugins.modules.clickhouse_row_policy.execute_query",
                                return_value=[[('SELECT 1 FROM _to_normalize WHERE b = a')]])
    row_policy._exist = False
    changed = row_policy.create("b = a", 0, [], 0, [], 'test_cluster')
    actuall_query = get_executed_query(mock_execute)
    assert changed is True
    assert mock_execute.call_count == 2
    assert actuall_query == "CREATE ROW POLICY `test_policy` ON `test_db`.`test_table` ON CLUSTER `test_cluster` USING b = a AS PERMISSIVE"


def test_create_row_policy_restrictive_no_assign(row_policy, mocker):
    mock_execute = mocker.patch("ansible_collections.community.clickhouse.plugins.modules.clickhouse_row_policy.execute_query",
                                return_value=[[('SELECT 1 FROM _to_normalize WHERE b = a')]])
    row_policy._exist = False
    changed = row_policy.create("b = a", 1, [], 0, [], None)
    actuall_query = get_executed_query(mock_execute)
    assert changed is True
    assert mock_execute.call_count == 2
    assert actuall_query == "CREATE ROW POLICY `test_policy` ON `test_db`.`test_table` USING b = a AS RESTRICTIVE"


def test_create_row_policy_assign_all(row_policy, mocker):
    mock_execute = mocker.patch("ansible_collections.community.clickhouse.plugins.modules.clickhouse_row_policy.execute_query",
                                return_value=[[('SELECT 1 FROM _to_normalize WHERE b = a')]])
    row_policy._exist = False
    changed = row_policy.create("b = a", 0, [], 1, [], None)
    actuall_query = get_executed_query(mock_execute)
    assert changed is True
    assert mock_execute.call_count == 2
    assert actuall_query == "CREATE ROW POLICY `test_policy` ON `test_db`.`test_table` USING b = a AS PERMISSIVE TO ALL"


def test_create_row_policy_assign_all_except(row_policy, mocker):
    mock_execute = mocker.patch("ansible_collections.community.clickhouse.plugins.modules.clickhouse_row_policy.execute_query",
                                return_value=[[('SELECT 1 FROM _to_normalize WHERE b = a')]])
    row_policy._exist = False
    changed = row_policy.create("b = a", 0, [], 1, ['except1', 'except2'], None)
    actuall_query = get_executed_query(mock_execute)
    assert changed is True
    assert mock_execute.call_count == 2
    assert actuall_query == "CREATE ROW POLICY `test_policy` ON `test_db`.`test_table` USING b = a AS PERMISSIVE TO ALL EXCEPT `except1`, `except2`"


def test_create_row_policy_assign_list(row_policy, mocker):
    mock_execute = mocker.patch("ansible_collections.community.clickhouse.plugins.modules.clickhouse_row_policy.execute_query",
                                return_value=[[('SELECT 1 FROM _to_normalize WHERE b = a')]])
    row_policy._exist = False
    changed = row_policy.create("b = a", 0, ['test_user', 'test_role'], 0, [], None)
    actuall_query = get_executed_query(mock_execute)
    assert changed is True
    assert mock_execute.call_count == 2
    assert actuall_query == "CREATE ROW POLICY `test_policy` ON `test_db`.`test_table` USING b = a AS PERMISSIVE TO `test_user`, `test_role`"


def test_drop_row_policy(row_policy, mocker):
    mock_execute = mocker.patch("ansible_collections.community.clickhouse.plugins.modules.clickhouse_row_policy.execute_query")
    row_policy._exist = True
    changed = row_policy.drop(None)
    actuall_query = get_executed_query(mock_execute)
    assert changed is True
    assert mock_execute.call_count == 1
    assert actuall_query == "DROP ROW POLICY `test_policy` ON `test_db`.`test_table`"


def test_drop_row_policy_on_cluster(row_policy, mocker):
    mock_execute = mocker.patch("ansible_collections.community.clickhouse.plugins.modules.clickhouse_row_policy.execute_query")
    row_policy._exist = True
    changed = row_policy.drop('test_cluster')
    actuall_query = get_executed_query(mock_execute)
    assert changed is True
    assert mock_execute.call_count == 1
    assert actuall_query == "DROP ROW POLICY `test_policy` ON `test_db`.`test_table` ON CLUSTER `test_cluster`"


def test_alter_row_policy_no_changes(row_policy, mocker, mock_execute):
    row_policy._exists = True
    row_policy._select_filter = 'b = 1'
    row_policy._apply_to_all = False
    row_policy._apply_to_except = []
    row_policy._apply_to_list = []
    row_policy._is_restrictive = False

    mocker.patch.object(row_policy, '_normalize_using_parameter', return_value='b = 1')
    mocker.patch.object(row_policy, '_needs_update', return_value=False)

    changed = row_policy.alter("b = 1", False, [], False, [], None)
    assert changed is False
    assert mock_execute.call_count == 0


def test_alter_row_policy_changed(row_policy, mocker, mock_execute):
    row_policy._exists = True
    row_policy._select_filter = 'b = 1'
    row_policy._apply_to_all = True
    row_policy._apply_to_except = ['test_user']
    row_policy._apply_to_list = []
    row_policy._is_restrictive = False

    mocker.patch.object(row_policy, '_normalize_using_parameter', return_value='b = 2')
    mocker.patch.object(row_policy, '_needs_update', return_value=True)

    changed = row_policy.alter("b = 2", False, [], True, ['test_user'], None)
    actuall_query = get_executed_query(mock_execute)
    assert changed is True
    assert mock_execute.call_count == 1
    assert actuall_query == "ALTER ROW POLICY `test_policy` ON `test_db`.`test_table` USING b = 2 AS PERMISSIVE TO ALL EXCEPT `test_user`"


def test_alter_row_policy_changed_on_cluster(row_policy, mocker, mock_execute):
    row_policy._exists = True
    row_policy._select_filter = 'b = 1'
    row_policy._apply_to_all = True
    row_policy._apply_to_except = ['test_user']
    row_policy._apply_to_list = []
    row_policy._is_restrictive = False

    mocker.patch.object(row_policy, '_normalize_using_parameter', return_value='b = 2')
    mocker.patch.object(row_policy, '_needs_update', return_value=True)

    changed = row_policy.alter("b = 2", False, [], True, ['test_user'], 'test_cluster')
    actuall_query = get_executed_query(mock_execute)
    assert changed is True
    assert mock_execute.call_count == 1
    assert actuall_query == (
        "ALTER ROW POLICY `test_policy` ON `test_db`.`test_table` "
        "ON CLUSTER `test_cluster` USING b = 2 AS PERMISSIVE TO ALL EXCEPT `test_user`")
