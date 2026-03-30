from __future__ import absolute_import, division, print_function
import pytest
from ansible_collections.community.clickhouse.plugins.modules.clickhouse_db import ClickHouseDB


@pytest.fixture
def mock_execute(mocker):
    return mocker.patch(
        "ansible_collections.community.clickhouse.plugins.modules.clickhouse_db.execute_query",
        return_value=[]
    )


def _create_db(mocker, name="test_db", cluster=None, comment=None):
    mock_module = mocker.MagicMock()
    mock_module.check_mode = False
    mock_client = mocker.MagicMock()

    return ClickHouseDB(
        module=mock_module,
        client=mock_client,
        name=name,
        cluster=cluster,
        comment=comment
    )


@pytest.fixture
def db_factory(mocker):
    def _make(cluster=None, comment=None, name="test_db", version=None):
        # Patch version before creating DB
        version_to_use = version if version else {'year': 26, 'feature': 1}
        mocker.patch(
            "ansible_collections.community.clickhouse.plugins.modules.clickhouse_db.get_server_version",
            return_value=version_to_use
        )
        return _create_db(mocker, name=name, cluster=cluster, comment=comment)
    return _make


def get_executed_query(mock_execute):
    return mock_execute.call_args[0][2]


def test_plain_db_with_default_options(db_factory, mock_execute):
    db = db_factory()
    db.create(engine=None, comment=None)
    actual_query = get_executed_query(mock_execute)
    assert actual_query == "CREATE DATABASE test_db"


def test_plain_db_with_options(db_factory, mock_execute):
    db = db_factory()
    db.create(engine='Ordinary', comment='test comment')
    actual_query = get_executed_query(mock_execute)
    assert actual_query == "CREATE DATABASE test_db ENGINE = Ordinary COMMENT 'test comment'"


def test_altering_existing_database_comment(db_factory, mock_execute):
    db = db_factory()
    db.update(engine=None, comment='test comment2')
    actual_query = get_executed_query(mock_execute)
    assert actual_query == "ALTER DATABASE test_db MODIFY COMMENT 'test comment2'"


def test_renaming_database(db_factory, mock_execute):
    db = db_factory()
    db.rename(target='test_db2')
    actual_query = get_executed_query(mock_execute)
    assert actual_query == "RENAME DATABASE test_db TO test_db2"


def test_droping_database(db_factory, mock_execute):
    db = db_factory()
    db.drop()
    actual_query = get_executed_query(mock_execute)
    assert actual_query == "DROP DATABASE test_db"


def test_altering_database_comment_not_supported(db_factory, mocker):
    # Patch old server version
    db = db_factory(
        version={'year': 25, 'feature': 3}
    )

    db.module.warn = mocker.MagicMock()
    mock_execute = mocker.patch(
        "ansible_collections.community.clickhouse.plugins.modules.clickhouse_db.execute_query"
    )

    db.update(engine=None, comment='test comment2')

    db.module.warn.assert_called_once()
    mock_execute.assert_not_called()
