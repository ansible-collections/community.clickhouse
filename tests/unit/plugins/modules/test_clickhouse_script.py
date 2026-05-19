from __future__ import absolute_import, division, print_function
import pytest
from ansible_collections.community.clickhouse.plugins.modules.clickhouse_script import ClickHouseScript


@pytest.fixture
def script(mocker):
    mock_module = mocker.MagicMock()
    mock_module.check_mode = False
    mock_client = mocker.MagicMock()

    return ClickHouseScript(module=mock_module, client=mock_client, path="test.sql")


def test_path_not_exists(script):
    script._exists = False
    script._is_file = False
    script._validate()

    script.module.fail_json.assert_called_once()


def test_path_not_regular_file(script):
    script._exists = True
    script._is_file = False
    script._validate()

    script.module.fail_json.assert_called_once()


def test_path_correct(script):
    script._exists = True
    script._is_file = True
    script._validate()

    script.module.fail_json.assert_not_called()
