from __future__ import (absolute_import, division, print_function)

__metaclass__ = type

import pytest
from importlib.util import find_spec

from ansible_collections.community.clickhouse.plugins.module_utils.clickhouse import (
    check_clickhouse_driver,
    version_clickhouse_driver,
    client_common_argument_spec,
    get_main_conn_kwargs,
)

REASON = "The clickhouse_driver module is not installed"


class FakeAnsibleModule:
    def __init__(self):
        self.params = {
            "login_host": "localhost",
            "login_port": None,
            "login_user": None,
            "login_db": None,
            "login_password": None,
            "client_kwargs": {},
        }

    def fail_json(self, msg):
        print(msg)


def test_client_common_argument_spec():
    EXPECTED = {
        'login_db': {'type': 'str', 'default': None},
        'login_port': {'type': 'int', 'default': None},
        'login_user': {'type': 'str', 'default': None},
        'login_host': {'type': 'str', 'default': 'localhost'},
        'login_password': {'type': 'str', 'default': None, 'no_log': True},
        'client_kwargs': {'type': 'dict', 'default': {}}
    }

    assert client_common_argument_spec() == EXPECTED


@pytest.mark.parametrize(
    'input_params,output_params',
    [
        ('', {'host': 'localhost'},),
        ({'login_host': 'test_host', 'login_port': 8000},
         {'host': 'test_host', 'port': 8000},
         ),
        ({'login_host': '127.0.0.1',
          'login_db': 'test_database',
          'login_user': 'test_user',
          'login_port': 9000,
          'login_password': 'qwerty',
          },
         {'host': '127.0.0.1',
          'database': 'test_database',
          'user': 'test_user',
          'port': 9000,
          'password': 'qwerty',
          },
         ),
    ]
)
def test_get_main_conn_kwargs(input_params, output_params):
    fake_module = FakeAnsibleModule()
    fake_module.params.update(input_params)

    assert get_main_conn_kwargs(fake_module) == output_params


@pytest.mark.skipif(find_spec('clickhouse_driver') is None, reason=REASON)
def test_version_clickhouse_driver():
    from clickhouse_driver import __version__

    assert __version__ == version_clickhouse_driver()


def test_check_clickhouse_driver():
    fake_module = FakeAnsibleModule()
    result = check_clickhouse_driver(fake_module)

    assert result is None or "clickhouse_driver" in result
