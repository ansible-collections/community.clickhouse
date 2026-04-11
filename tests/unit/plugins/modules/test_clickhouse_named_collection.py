from __future__ import absolute_import, division, print_function
import pytest
from ansible_collections.community.clickhouse.plugins.modules.clickhouse_named_collection import ClickHouseNamedCollection


@pytest.fixture
def collection(mocker):
    mock_module = mocker.MagicMock()
    mock_module.check_mode = False
    mock_client = mocker.MagicMock()

    return ClickHouseNamedCollection(module=mock_module, client=mock_client, name="test_collection")


def get_executed_query(mock_execute):
    # Assumes execute_query(module, client, query, params)
    args, kwargs = mock_execute.call_args
    return args[2] if len(args) > 2 else kwargs.get('query')


@pytest.fixture
def mock_execute(mocker):
    return mocker.patch(
        "ansible_collections.community.clickhouse.plugins.modules.clickhouse_named_collection.execute_query",
        return_value=[]
    )


def test_fetch_collection_not_exists(collection, mock_execute):
    collection._load()

    assert collection.exists is False
    assert mock_execute.call_count == 1


def test_fetch_collection_exists(collection, mocker):
    mock_execute = mocker.patch(
        "ansible_collections.community.clickhouse.plugins.modules.clickhouse_named_collection.execute_query",
        return_value=[({'a': 'b'}, 'SQL')]
    )
    collection._load()

    assert mock_execute.call_count == 1
    assert collection.exists is True
    assert collection.source == 'SQL'
    assert collection.current == {'a': {'value': 'b'}}


def test_fetch_collection_exists_not_supported(collection, mocker):
    mocker.patch(
        "ansible_collections.community.clickhouse.plugins.modules.clickhouse_named_collection.execute_query",
        return_value=[
            ({'password': '[HIDDEN]', 'user': '[HIDDEN]'}, 'XML')
        ]
    )
    collection.module.fail_json.side_effect = Exception("fail_json called")

    with pytest.raises(Exception, match="fail_json called"):
        collection._load()
    collection.module.fail_json.assert_called_once()

    assert "Passed named collection isn't sourced by SQL. Got: XML" in collection.module.fail_json.call_args[1]['msg']


def test_normalize_current_collection(collection, mocker):
    mocker.patch(
        "ansible_collections.community.clickhouse.plugins.modules.clickhouse_named_collection.execute_query",
        return_value=[
            ({'password': '[HIDDEN]', 'user': '[HIDDEN]'}, 'SQL')
        ]
    )
    collection._load()

    assert collection._current == {'password': {'value': '[HIDDEN]'}, 'user': {'value': '[HIDDEN]'}}


def test_normalized_input_collection(collection):

    result = collection._normalize_collection_input(
        [
            {'name': 'user', 'value': 'alice'},
            {'name': 'password', 'value': 'test_pass'}
        ]
    )
    assert result == {'user': {'value': 'alice'}, 'password': {'value': 'test_pass'}}


def test_check_hidden_passed_hidden(collection):
    collection._exists = True
    collection._source = 'SQL'
    collection._current = {'password': {'value': '[HIDDEN]'}, 'user': {'value': '[HIDDEN]'}}
    result = collection._check_if_hidden()
    assert result is True


def test_check_hidden_passed_not_hidden(collection):
    collection._exists = True
    collection._source = 'SQL'
    collection._current = {'password': {'value': 'test_pass'}, 'user': {'value': 'alice'}}
    result = collection._check_if_hidden()
    assert result is False


def test_should_skip_alter_hidden(collection):
    collection._exists = True
    collection._source = 'SQL'
    collection._current = {'password': {'value': '[HIDDEN]'}, 'user': {'value': '[HIDDEN]'}}
    skipped = collection._should_skip_alter({'user': {'value': 'alice'}}, False)
    assert skipped is True


def test_should_skip_alter_rewrite(collection):
    collection._exists = True
    collection._source = 'SQL'
    collection._current = {'password': {'value': '[HIDDEN]'}, 'user': {'value': '[HIDDEN]'}}
    skipped = collection._should_skip_alter({'user': {'value': 'alice'}}, True)
    assert skipped is False


def test_should_skip_alter_equal(collection):
    collection._exists = True
    collection._source = 'SQL'
    collection._current = {'password': {'value': 'test_pass'}, 'user': {'value': 'alice'}}
    skipped = collection._should_skip_alter({'user': {'value': 'alice'}, 'password': {'value': 'test_pass'}}, False)
    assert skipped is True


def test_should_skip_alter_not_equal(collection):
    collection._exists = True
    collection._source = 'SQL'
    collection._current = {'password': {'value': 'test_pass'}, 'user': {'value': 'alice'}}
    skipped = collection._should_skip_alter({'user': {'value': 'alice'}, 'password': {'value': 'test_pass2'}}, False)
    assert skipped is False


def test_build_alter_query_single(collection):
    query = collection._build_alter_query({'user': {'value': 'alice'}}, None)
    assert query == "ALTER NAMED COLLECTION `test_collection` SET user = 'alice'"


def test_build_alter_query_double(collection):
    query = collection._build_alter_query({'user': {'value': 'alice'}, 'password': {'value': 'test_pass'}}, None)
    assert query == "ALTER NAMED COLLECTION `test_collection` SET user = 'alice', password = 'test_pass'"


def test_build_alter_query_double_plus_cluster(collection):
    query = collection._build_alter_query({'user': {'value': 'alice'}, 'password': {'value': 'test_pass'}}, 'north')
    assert query == "ALTER NAMED COLLECTION `test_collection` ON CLUSTER 'north' SET user = 'alice', password = 'test_pass'"


def test_create_collection_empty_params(collection):
    collection._exist = False

    collection.module.fail_json.side_effect = Exception("fail_json called")

    with pytest.raises(Exception, match="fail_json called"):
        collection.create({}, None)
    collection.module.fail_json.assert_called_once()

    assert "Collection not passed" in collection.module.fail_json.call_args[1]['msg']


def test_create_collection_with_one_param(collection, mock_execute):
    collection._exist = False
    changed = collection.create({'user': {'value': 'alice'}}, None)
    actuall_query = get_executed_query(mock_execute)
    assert changed is True
    assert mock_execute.call_count == 1
    assert actuall_query == "CREATE NAMED COLLECTION `test_collection` AS user = 'alice'"


def test_create_collection_with_two_param(collection, mock_execute):
    changed = collection.create({'user': {'value': 'alice'}, 'password': {'value': 'test_pass'}}, None)
    actuall_query = get_executed_query(mock_execute)
    assert changed is True
    assert mock_execute.call_count == 1
    assert actuall_query == "CREATE NAMED COLLECTION `test_collection` AS user = 'alice', password = 'test_pass'"


def test_create_collection_with_three_param_plus_cluster(collection, mock_execute):
    changed = collection.create({'user': {'value': 'alice'}, 'password': {'value': 'test_pass'}, 'host': {'value': 'host1'}}, 'north')
    actuall_query = get_executed_query(mock_execute)
    assert changed is True
    assert mock_execute.call_count == 1
    assert actuall_query == "CREATE NAMED COLLECTION `test_collection` ON CLUSTER 'north' AS user = 'alice', password = 'test_pass', host = 'host1'"


def test_alter_collection_empty_params(collection):
    collection._exist = True
    collection._source = 'SQL'
    collection._current = {'user': {'value': 'alice'}, 'password': {'value': 'test_pass'}}

    collection.module.fail_json.side_effect = Exception("fail_json called")

    with pytest.raises(Exception, match="fail_json called"):
        collection.alter({}, None, False)
    collection.module.fail_json.assert_called_once()

    assert "Collection not passed" in collection.module.fail_json.call_args[1]['msg']


def test_alter_collection_no_hidden_with_one_param(collection, mock_execute):
    collection._exists = True
    collection._source = 'SQL'
    collection._current = {'user': {'value': 'alice'}, 'password': {'value': 'test_pass'}}
    changed = collection.alter({'user': {'value': 'alice'}}, None, False)
    actuall_query = get_executed_query(mock_execute)
    assert changed is True
    assert mock_execute.call_count == 1
    assert actuall_query == "ALTER NAMED COLLECTION `test_collection` SET user = 'alice'"


def test_alter_collection_no_hidden_with_two_param(collection, mock_execute):
    collection._exists = True
    collection._source = 'SQL'
    collection._current = {'user': {'value': 'alice'}, 'password': {'value': 'test_pass'}}
    changed = collection.alter({'user': {'value': 'alice'}, 'password': {'value': 'test_pass2'}}, None, False)
    actuall_query = get_executed_query(mock_execute)
    assert changed is True
    assert mock_execute.call_count == 1
    assert actuall_query == "ALTER NAMED COLLECTION `test_collection` SET user = 'alice', password = 'test_pass2'"


def test_alter_collection_no_hidden_no_changes(collection, mock_execute):
    collection._exists = True
    collection._source = 'SQL'
    collection._current = {'user': {'value': 'alice'}, 'password': {'value': 'test_pass'}}
    changed = collection.alter({'user': {'value': 'alice'}, 'password': {'value': 'test_pass'}}, None, False)
    assert changed is False
    assert mock_execute.call_count == 0


def test_alter_collection_hidden(collection, mock_execute):
    collection._exists = True
    collection._source = 'SQL'
    collection._current = {'user': {'value': '[HIDDEN]'}, 'password': {'value': '[HIDDEN]'}}
    changed = collection.alter({'user': {'value': 'alice'}, 'password': {'value': 'test_pass'}}, None, False)
    assert changed is False
    assert mock_execute.call_count == 0


def test_alter_collection_hidden_rewrite(collection, mock_execute):
    collection._exists = True
    collection._source = 'SQL'
    collection._current = {'user': {'value': '[HIDDEN]'}, 'password': {'value': '[HIDDEN]'}}
    changed = collection.alter({'user': {'value': 'alice'}, 'password': {'value': 'test_pass'}}, None, True)
    actuall_query = get_executed_query(mock_execute)
    assert changed is True
    assert mock_execute.call_count == 1
    assert actuall_query == "ALTER NAMED COLLECTION `test_collection` SET user = 'alice', password = 'test_pass'"


def test_drop_collection(collection, mock_execute):
    collection._exists = True
    collection._source = 'SQL'
    collection._current = {'user': {'value': '[HIDDEN]'}, 'password': {'value': '[HIDDEN]'}}
    changed = collection.drop(None)
    actuall_query = get_executed_query(mock_execute)
    assert changed is True
    assert mock_execute.call_count == 1
    assert actuall_query == "DROP NAMED COLLECTION `test_collection`"
