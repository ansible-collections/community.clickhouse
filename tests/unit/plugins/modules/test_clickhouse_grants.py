from __future__ import (absolute_import, division, print_function)

__metaclass__ = type

import pytest
from unittest.mock import MagicMock, patch

from ansible_collections.community.clickhouse.plugins.modules.clickhouse_grants import (
    ClickHouseGrants,
    GRANT_REGEX,
    executed_statements,
)


class TestGrantRegex:
    """Test the GRANT statement regex pattern"""

    @pytest.mark.parametrize(
        'grant_statement,expected',
        [
            ('GRANT SELECT ON foo.* TO alice',
             ('SELECT', 'foo.*', None)),
            ('GRANT SELECT, INSERT ON foo.* TO alice',
             ('SELECT, INSERT', 'foo.*', None)),
            ('GRANT SELECT ON foo.* TO alice WITH GRANT OPTION',
             ('SELECT', 'foo.*', ' WITH GRANT OPTION')),
            ('GRANT CREATE USER ON *.* TO alice',
             ('CREATE USER', '*.*', None)),
            ('GRANT ALTER DELETE ON db.table TO bob WITH GRANT OPTION',
             ('ALTER DELETE', 'db.table', ' WITH GRANT OPTION')),
            ('GRANT SELECT(x, y) ON foo.test_table TO carol',
             ('SELECT(x, y)', 'foo.test_table', None)),
            ('GRANT SELECT ON foo.* TO alice WITH GRANT OPTION ON CLUSTER test_cluster',
             ('SELECT', 'foo.*', ' WITH GRANT OPTION')),
            ('GRANT SELECT ON foo.* TO alice ON CLUSTER test_cluster',
             ('SELECT', 'foo.*', None)),
            ('GRANT SELECT ON foo.* TO alice-with-dash',
             ('SELECT', 'foo.*', None)),
        ]
    )
    def test_grant_regex_match(self, grant_statement, expected):
        """Test that GRANT_REGEX correctly parses various grant statements"""
        match = GRANT_REGEX.match(grant_statement)
        assert match is not None
        assert match.groups() == expected

    @pytest.mark.parametrize(
        'invalid_statement',
        [
            'INVALID GRANT STATEMENT',
            'SELECT * FROM table',
            'REVOKE SELECT ON foo.* FROM alice',
            '',
        ]
    )
    def test_grant_regex_no_match(self, invalid_statement):
        """Test that GRANT_REGEX doesn't match invalid statements"""
        match = GRANT_REGEX.match(invalid_statement)
        assert match is None


class TestClickHouseGrantsGet:
    """Test the get() method that parses SHOW GRANTS output"""

    def setup_method(self):
        """Set up mock module and client for each test"""
        self.mock_module = MagicMock()
        self.mock_module.params = {'login_user': 'default'}
        self.mock_client = MagicMock()

    @patch('ansible_collections.community.clickhouse.plugins.modules.clickhouse_grants.execute_query')
    def test_get_simple_grants(self, mock_execute):
        """Test parsing simple grant statements"""
        mock_execute.return_value = [
            ('GRANT SELECT ON foo.* TO alice',),
            ('GRANT INSERT ON foo.* TO alice',),
        ]

        grants_obj = ClickHouseGrants(self.mock_module, self.mock_client, 'alice')
        # Override the grantee_exists check
        grants_obj.grantee_exists = True

        result = grants_obj.get()

        assert result == {
            'foo.*': {
                'SELECT': False,
                'INSERT': False,
            }
        }

    @patch('ansible_collections.community.clickhouse.plugins.modules.clickhouse_grants.execute_query')
    def test_get_grants_with_grant_option(self, mock_execute):
        """Test parsing grants with GRANT OPTION"""
        mock_execute.return_value = [
            ('GRANT SELECT ON foo.* TO alice WITH GRANT OPTION',),
            ('GRANT INSERT ON foo.* TO alice',),
        ]

        grants_obj = ClickHouseGrants(self.mock_module, self.mock_client, 'alice')
        grants_obj.grantee_exists = True

        result = grants_obj.get()

        assert result == {
            'foo.*': {
                'SELECT': True,  # WITH GRANT OPTION
                'INSERT': False,  # Without GRANT OPTION
            }
        }

    @patch('ansible_collections.community.clickhouse.plugins.modules.clickhouse_grants.execute_query')
    def test_get_multiple_objects(self, mock_execute):
        """Test parsing grants on multiple database objects"""
        mock_execute.return_value = [
            ('GRANT SELECT ON foo.* TO alice',),
            ('GRANT INSERT ON bar.* TO alice',),
            ('GRANT CREATE USER ON *.* TO alice',),
        ]

        grants_obj = ClickHouseGrants(self.mock_module, self.mock_client, 'alice')
        grants_obj.grantee_exists = True

        result = grants_obj.get()

        assert result == {
            'foo.*': {'SELECT': False},
            'bar.*': {'INSERT': False},
            '*.*': {'CREATE USER': False},
        }

    @patch('ansible_collections.community.clickhouse.plugins.modules.clickhouse_grants.execute_query')
    def test_get_multiple_privileges_in_one_statement(self, mock_execute):
        """Test parsing multiple privileges in a single GRANT statement"""
        mock_execute.return_value = [
            ('GRANT SELECT, INSERT, DELETE ON foo.* TO alice WITH GRANT OPTION',),
        ]

        grants_obj = ClickHouseGrants(self.mock_module, self.mock_client, 'alice')
        grants_obj.grantee_exists = True

        result = grants_obj.get()

        assert result == {
            'foo.*': {
                'SELECT': True,
                'INSERT': True,
                'DELETE': True,
            }
        }

    @patch('ansible_collections.community.clickhouse.plugins.modules.clickhouse_grants.execute_query')
    def test_get_column_level_privileges(self, mock_execute):
        """Test parsing column-level privileges"""
        mock_execute.return_value = [
            ('GRANT SELECT(x) ON foo.test_table TO alice',),
        ]

        grants_obj = ClickHouseGrants(self.mock_module, self.mock_client, 'alice')
        grants_obj.grantee_exists = True

        result = grants_obj.get()

        assert result == {
            'foo.test_table': {
                'SELECT(X)': False,  # Note: uppercase conversion
            }
        }

    @patch('ansible_collections.community.clickhouse.plugins.modules.clickhouse_grants.execute_query')
    def test_get_normalizes_global_object(self, mock_execute):
        """Test that '*' is normalized to '*.*' for ClickHouse 25.x compatibility"""
        mock_execute.return_value = [
            ('GRANT CREATE USER ON * TO alice',),  # ClickHouse 25.x format
        ]

        grants_obj = ClickHouseGrants(self.mock_module, self.mock_client, 'alice')
        grants_obj.grantee_exists = True

        result = grants_obj.get()

        # Should normalize '*' to '*.*'
        assert result == {
            '*.*': {'CREATE USER': False},
        }
        # Should NOT have '*' as key
        assert '*' not in result

    @patch('ansible_collections.community.clickhouse.plugins.modules.clickhouse_grants.execute_query')
    def test_get_empty_grants(self, mock_execute):
        """Test parsing when no grants exist"""
        mock_execute.return_value = []

        grants_obj = ClickHouseGrants(self.mock_module, self.mock_client, 'alice')
        grants_obj.grantee_exists = True

        result = grants_obj.get()

        assert result == {}

    @patch('ansible_collections.community.clickhouse.plugins.modules.clickhouse_grants.execute_query')
    def test_get_grants_with_cluster_setting(self, mock_execute):
        """Test parsing grants with ON CLUSTER"""
        mock_execute.return_value = [
            ('GRANT SELECT ON foo.* TO alice WITH GRANT OPTION ON CLUSTER test_cluster',),
            ('GRANT INSERT ON foo.* TO alice ON CLUSTER test_cluster',),
        ]

        grants_obj = ClickHouseGrants(self.mock_module, self.mock_client, 'alice', 'test_cluster')
        grants_obj.grantee_exists = True

        result = grants_obj.get()

        assert result == {
            'foo.*': {
                'SELECT': True,   # WITH GRANT OPTION
                'INSERT': False,  # Without GRANT OPTION
            }
        }

    @patch('ansible_collections.community.clickhouse.plugins.modules.clickhouse_grants.execute_query')
    def test_get_multiple_objects(self, mock_execute):
        """Test parsing grants with dashes in grantee name"""
        mock_execute.return_value = [
            ('GRANT SELECT ON foo.* TO alice-with-dash',),
            ('GRANT INSERT ON bar.* TO alice-with-dash',),
            ('GRANT CREATE USER ON *.* TO alice-with-dash',),
        ]

        grants_obj = ClickHouseGrants(self.mock_module, self.mock_client, 'alice-with-dash')
        grants_obj.grantee_exists = True

        result = grants_obj.get()

        assert result == {
            'foo.*': {'SELECT': False},
            'bar.*': {'INSERT': False},
            '*.*': {'CREATE USER': False},
        }


class TestClickHouseGrantsGetDesiredGrants:
    """Test the _get_desired_grants() method"""

    def setup_method(self):
        """Set up mock module and client for each test"""
        self.mock_module = MagicMock()
        self.mock_client = MagicMock()

    @patch('ansible_collections.community.clickhouse.plugins.modules.clickhouse_grants.execute_query')
    def test_get_desired_grants_simple(self, mock_execute):
        """Test converting privileges parameter to desired grants format"""
        self.mock_module.params = {
            'login_user': 'default',
            'privileges': [
                {
                    'object': 'foo.*',
                    'privs': {
                        'SELECT': True,
                        'INSERT': False,
                    }
                }
            ]
        }

        mock_execute.return_value = [('1',)]  # Mock user exists check
        grants_obj = ClickHouseGrants(self.mock_module, self.mock_client, 'alice')

        result = grants_obj._get_desired_grants()

        assert result == {
            'foo.*': {
                'SELECT': True,
                'INSERT': False,
            }
        }

    @patch('ansible_collections.community.clickhouse.plugins.modules.clickhouse_grants.execute_query')
    def test_get_desired_grants_multiple_objects(self, mock_execute):
        """Test multiple database objects in privileges"""
        self.mock_module.params = {
            'login_user': 'default',
            'privileges': [
                {
                    'object': 'foo.*',
                    'privs': {'SELECT': True}
                },
                {
                    'object': 'bar.*',
                    'privs': {'INSERT': False}
                }
            ]
        }

        mock_execute.return_value = [('1',)]
        grants_obj = ClickHouseGrants(self.mock_module, self.mock_client, 'alice')

        result = grants_obj._get_desired_grants()

        assert result == {
            'foo.*': {'SELECT': True},
            'bar.*': {'INSERT': False},
        }

    @patch('ansible_collections.community.clickhouse.plugins.modules.clickhouse_grants.execute_query')
    def test_get_desired_grants_with_grant_option_override(self, mock_execute):
        """Test grant_option parameter overrides individual privilege settings"""
        self.mock_module.params = {
            'login_user': 'default',
            'privileges': [
                {
                    'object': 'foo.*',
                    'grant_option': True,  # Override
                    'privs': {
                        'SELECT': False,  # Should be overridden to True
                        'INSERT': False,  # Should be overridden to True
                    }
                }
            ]
        }

        mock_execute.return_value = [('1',)]
        grants_obj = ClickHouseGrants(self.mock_module, self.mock_client, 'alice')

        result = grants_obj._get_desired_grants()

        assert result == {
            'foo.*': {
                'SELECT': True,  # Overridden by grant_option
                'INSERT': True,  # Overridden by grant_option
            }
        }

    @patch('ansible_collections.community.clickhouse.plugins.modules.clickhouse_grants.execute_query')
    def test_get_desired_grants_privilege_name_uppercase(self, mock_execute):
        """Test that privilege names are converted to uppercase"""
        self.mock_module.params = {
            'login_user': 'default',
            'privileges': [
                {
                    'object': 'foo.*',
                    'privs': {
                        'select': True,  # lowercase
                        'InSeRt': False,  # mixed case
                    }
                }
            ]
        }

        mock_execute.return_value = [('1',)]
        grants_obj = ClickHouseGrants(self.mock_module, self.mock_client, 'alice')

        result = grants_obj._get_desired_grants()

        assert result == {
            'foo.*': {
                'SELECT': True,  # Uppercase
                'INSERT': False,  # Uppercase
            }
        }

    @patch('ansible_collections.community.clickhouse.plugins.modules.clickhouse_grants.execute_query')
    def test_get_desired_grants_empty_privileges(self, mock_execute):
        """Test with empty privileges list"""
        self.mock_module.params = {
            'login_user': 'default',
            'privileges': None
        }

        mock_execute.return_value = [('1',)]
        grants_obj = ClickHouseGrants(self.mock_module, self.mock_client, 'alice')

        result = grants_obj._get_desired_grants()

        assert result == {}


class TestClickHouseGrantsPartialRevokes:
    """Test the helpers backing the revokes option"""

    def setup_method(self):
        self.mock_module = MagicMock()
        self.mock_client = MagicMock()
        patcher = patch('ansible_collections.community.clickhouse.plugins.modules.'
                        'clickhouse_grants.execute_query')
        self.mock_execute_query = patcher.start()
        # Let __check_grantee_exists find the grantee
        self.mock_execute_query.return_value = [(1,)]
        self.obj = ClickHouseGrants(module=self.mock_module, client=self.mock_client, grantee="test")

    def teardown_method(self):
        patch.stopall()

    @pytest.mark.parametrize(
        'priv,expected',
        [
            ("SELECT", ({"SELECT": []})),
            ("SELECT(a)", ({"SELECT": ['a']})),
            ("SELECT (a)", ({"SELECT": ['a']})),
            ("SELECT (a2)", ({"SELECT": ['a2']})),
            ("SELECT (a,b)", ({"SELECT": ['a', 'b']})),
            ("SELECT (a, b)", ({"SELECT": ['a', 'b']})),
            ("SELECT(a, b)", ({"SELECT": ['a', 'b']})),
            ("dictGet", ({"dictGet": []})),
            ("SELECT, INSERT", ({"SELECT": [], "INSERT": []})),
            ("SELECT , INSERT", ({"SELECT": [], "INSERT": []})),
            ("SELECT(a), INSERT(b)", ({"SELECT": ['a'], "INSERT": ['b']})),
            ("SELECT(a, b), INSERT(b)", ({"SELECT": ['a', 'b'], "INSERT": ['b']})),
            ("SELECT(a, c), INSERT(b,d)", ({"SELECT": ['a', 'c'], "INSERT": ['b', 'd']})),
            ("SELECT (very_longColumn-mess_column)", ({"SELECT": ['very_longColumn-mess_column']})),
            ("SELECT (very_longColumn-mess_column, very_longColumn-mess_column2)",
             ({"SELECT": ['very_longColumn-mess_column', 'very_longColumn-mess_column2']})),
            ("ALTER COMMENT COLUMN(a, b)", ({"ALTER COMMENT COLUMN": ['a', 'b']})),
        ]
    )
    def test_parse_privs(self, priv, expected):
        result = self.obj._parse_priv_entries(priv)
        assert result == expected

    @pytest.mark.parametrize(
        'grant,obj,expected',
        [
            (
                {'access_type': 'SELECT', 'access_object': '', 'database': None, 'table': None,
                 'column': None, 'is_partial_revoke': False, 'grant_option': False},
                "foo.*",
                True,
            ),
            (
                {'access_type': 'SELECT', 'access_object': '', 'database': 'foo', 'table': None,
                 'column': None, 'is_partial_revoke': False, 'grant_option': False},
                "foo.*",
                True,
            ),
            (
                {'access_type': 'SELECT', 'access_object': '', 'database': 'foo', 'table': None,
                 'column': None, 'is_partial_revoke': False, 'grant_option': False},
                "foo2.*",
                False,
            ),
            (
                {'access_type': 'SELECT', 'access_object': '', 'database': 'foo2', 'table': None,
                 'column': None, 'is_partial_revoke': False, 'grant_option': False},
                "foo.*",
                False,
            ),
            (
                {'access_type': 'SELECT', 'access_object': 'POSTGRES', 'database': None, 'table': None,
                 'column': None, 'is_partial_revoke': False, 'grant_option': False},
                "*",
                False,
            ),
            (
                {'access_type': 'SELECT', 'access_object': '', 'database': None, 'table': None,
                 'column': None, 'is_partial_revoke': False, 'grant_option': False},
                "*",
                True,
            ),
            (
                {'access_type': 'SELECT', 'access_object': 'POSTGRES', 'database': None, 'table': None,
                 'column': None, 'is_partial_revoke': False, 'grant_option': False},
                "POSTGRES",
                True,
            ),
            (
                {'access_type': 'SELECT', 'access_object': '', 'database': None, 'table': None,
                 'column': None, 'is_partial_revoke': False, 'grant_option': False},
                "POSTGRES",
                True,
            ),
        ]
    )
    def test_grant_matches_object(self, grant, obj, expected):
        result = self.obj._grant_matches_object(grant, obj)
        assert result == expected

    def test_priv_empty(self):
        self.obj._grants = []
        result = self.obj._privilege_fully_present('foo.*', {'SELECT': []})
        assert result is False

    def test_privilege_fully_present_db_table_glob(self):
        self.obj._grants = [
            {'access_type': 'SELECT', 'access_object': '', 'database': 'foo', 'table': None,
             'column': None, 'is_partial_revoke': False, 'grant_option': False}
        ]
        result = self.obj._privilege_fully_present('foo.*', {'SELECT': []})
        assert result is True

    def test_privilege_fully_present_db_table_column(self):
        self.obj._grants = [
            {'access_type': 'SELECT', 'access_object': '', 'database': 'foo', 'table': 'bar',
             'column': None, 'is_partial_revoke': False, 'grant_option': False}
        ]
        result = self.obj._privilege_fully_present('foo.bar', {'SELECT': []})
        assert result is True

    def test_privilege_fully_present_db_table_column_different(self):
        self.obj._grants = [
            {'access_type': 'SELECT', 'access_object': '', 'database': 'foo', 'table': 'bar1',
             'column': None, 'is_partial_revoke': False, 'grant_option': False}
        ]
        result = self.obj._privilege_fully_present('foo.bar', {'SELECT': []})
        assert result is False

    def test_privilege_fully_present_db_table_column_with_column(self):
        self.obj._grants = [
            {'access_type': 'SELECT', 'access_object': '', 'database': 'foo', 'table': 'bar',
             'column': 'col1', 'is_partial_revoke': False, 'grant_option': False}
        ]
        result = self.obj._privilege_fully_present('foo.bar', {'SELECT': ['col1']})
        assert result is True

    def test_privilege_fully_present_db_table_column_with_column_different(self):
        self.obj._grants = [
            {'access_type': 'SELECT', 'access_object': '', 'database': 'foo', 'table': 'bar',
             'column': 'col1', 'is_partial_revoke': False, 'grant_option': False}
        ]
        result = self.obj._privilege_fully_present('foo.bar', {'SELECT': ['col2']})
        assert result is False

    def test_privilege_fully_present_db_table_column_with_column_multiple(self):
        self.obj._grants = [
            {'access_type': 'SELECT', 'access_object': '', 'database': 'foo', 'table': 'bar',
             'column': 'col1', 'is_partial_revoke': False, 'grant_option': False}
        ]
        result = self.obj._privilege_fully_present('foo.bar', {'SELECT': ['col1', 'col2']})
        assert result is False

    def test_privilege_fully_present_db_table_column_with_column_multiple_all_present(self):
        self.obj._grants = [
            {'access_type': 'SELECT', 'access_object': '', 'database': 'foo', 'table': 'bar',
             'column': 'col1', 'is_partial_revoke': False, 'grant_option': False},
            {'access_type': 'SELECT', 'access_object': '', 'database': 'foo', 'table': 'bar',
             'column': 'col2', 'is_partial_revoke': False, 'grant_option': False}
        ]
        result = self.obj._privilege_fully_present('foo.bar', {'SELECT': ['col1', 'col2']})
        assert result is True

    def test_privilege_fully_present_db_table_column_with_column_multiple_some_present(self):
        self.obj._grants = [
            {'access_type': 'SELECT', 'access_object': '', 'database': 'foo', 'table': 'bar',
             'column': 'col1', 'is_partial_revoke': False, 'grant_option': False}
        ]
        result = self.obj._privilege_fully_present('foo.bar', {'SELECT': ['col1', 'col2']})
        assert result is False

    def test_privilege_fully_present_overlap(self):
        self.obj._grants = [
            {'access_type': 'SELECT', 'access_object': '', 'database': 'foo', 'table': None,
             'column': None, 'is_partial_revoke': False, 'grant_option': False}
        ]
        result = self.obj._privilege_fully_present('foo.bar', {'SELECT': []})
        assert result is True

    def test_privilege_fully_present_overlap_partial_revoke(self):
        self.obj._grants = [
            {'access_type': 'SELECT', 'access_object': '', 'database': 'foo', 'table': None,
             'column': None, 'is_partial_revoke': False, 'grant_option': False}
        ]
        result = self.obj._privilege_fully_present('foo.bar', {'SELECT': []}, revoke=1)
        assert result is False

    def test_privilege_fully_present_revoke_not_present(self):
        self.obj._grants = [
            {'access_type': 'SELECT', 'access_object': '', 'database': 'foo', 'table': None,
             'column': None, 'is_partial_revoke': False, 'grant_option': False}
        ]
        result = self.obj._privilege_fully_present('foo.bar', {'INSERT': []}, revoke=1)
        assert result is True

    def test_privilege_fully_present_revoke_column(self):
        self.obj._grants = [
            {'access_type': 'SELECT', 'access_object': '', 'database': 'foo', 'table': 'bar',
             'column': 'col1', 'is_partial_revoke': True, 'grant_option': False}
        ]
        result = self.obj._privilege_fully_present('foo.bar', {'SELECT': ['col1']}, revoke=1)
        assert result is True

    def test_priv_already_absent_revoke_column(self):
        self.obj._grants = [
            {'access_type': 'SELECT', 'access_object': '', 'database': 'foo', 'table': 'bar',
             'column': 'col1', 'is_partial_revoke': False, 'grant_option': False}
        ]
        result = self.obj._privilege_fully_present('foo.bar', {'SELECT': ['col1']}, revoke=1)
        assert result is False

    def test_privilege_fully_present_object_type_empty(self):
        self.obj._grants = [
            {'access_type': 'READ', 'access_object': '', 'database': None, 'table': None, 'column': None, 'is_partial_revoke': False, 'grant_option': False}
        ]
        result = self.obj._privilege_fully_present('*', {'READ': []})
        assert result is True

    def test_privilege_fully_present_object_type_filled_no_grants(self):
        self.obj._grants = []
        result = self.obj._privilege_fully_present('POSTGRES', {'READ': []})
        assert result is False

    def test_privilege_fully_present_object_type_wildcard_no_grants(self):
        self.obj._grants = []
        result = self.obj._privilege_fully_present('*', {'READ': []})
        assert result is False

    def test_privilege_fully_present_object_type_subpart(self):
        self.obj._grants = [
            {'access_type': 'READ', 'access_object': '', 'database': None, 'table': None, 'column': None, 'is_partial_revoke': False, 'grant_option': False}
        ]
        result = self.obj._privilege_fully_present('POSTGRES', {'READ': []})
        assert result is True

    def test_privilege_fully_present_object_type_expand(self):
        self.obj._grants = [
            {'access_type': 'READ', 'access_object': 'POSTGRES', 'database': None, 'table': None,
             'column': None, 'is_partial_revoke': False, 'grant_option': False}
        ]
        result = self.obj._privilege_fully_present('*', {'READ': []})
        assert result is False

    def test_privilege_fully_present_object_type_partial_revoke(self):
        self.obj._grants = [
            {'access_type': 'READ', 'access_object': '', 'database': None, 'table': None, 'column': None, 'is_partial_revoke': False, 'grant_option': False}
        ]
        result = self.obj._privilege_fully_present('POSTGRES', {'READ': []}, revoke=1)
        assert result is False

    def test_privilege_partial_revoke_from_all(self):
        self.obj._grants = [
            {'access_type': 'ALL', 'access_object': '', 'database': None, 'table': None,
             'column': None, 'is_partial_revoke': False, 'grant_option': False}
        ]
        result = self.obj._privilege_fully_present('system.*', {'SELECT': []}, revoke=1)
        assert result is False

    def test_privilege_grant_all_already_cover_by_asterix(self):
        self.obj._grants = [
            {'access_type': 'ALL', 'access_object': '', 'database': None, 'table': None,
             'column': None, 'is_partial_revoke': False, 'grant_option': False}
        ]
        result = self.obj._privilege_fully_present('system.*', {'SELECT': []})
        assert result is True

    def test_privilege_all_already_cover_by_database(self):
        self.obj._grants = [
            {'access_type': 'ALL', 'access_object': '', 'database': 'system', 'table': None,
             'column': None, 'is_partial_revoke': False, 'grant_option': False}
        ]
        result = self.obj._privilege_fully_present('system.*', {'SELECT': []})
        assert result is True

    def test_pending_grant_rows(self):
        result = self.obj._pending_grant_rows({
            ('SELECT', 'foo.*', False),
        })
        assert result == [
            {'access_type': 'SELECT', 'access_object': '', 'database': 'foo', 'table': None,
             'column': None, 'is_partial_revoke': 0, 'grant_option': 0}
        ]

    def test_pending_grant_rows_object_types(self):
        result = self.obj._pending_grant_rows([
            ('SELECT(a, b)', 'foo.bar', False),
            ('CREATE USER', '*.*', True),
            ('READ', 'POSTGRES', False),
            ('READ', '*', False),
        ])
        assert result == [
            {'access_type': 'SELECT', 'access_object': '', 'database': 'foo', 'table': 'bar',
             'column': 'a', 'is_partial_revoke': 0, 'grant_option': False},
            {'access_type': 'SELECT', 'access_object': '', 'database': 'foo', 'table': 'bar',
             'column': 'b', 'is_partial_revoke': 0, 'grant_option': False},
            {'access_type': 'CREATE USER', 'access_object': '', 'database': None, 'table': None,
             'column': None, 'is_partial_revoke': 0, 'grant_option': True},
            {'access_type': 'READ', 'access_object': 'POSTGRES', 'database': None, 'table': None,
             'column': None, 'is_partial_revoke': 0, 'grant_option': False},
            {'access_type': 'READ', 'access_object': '', 'database': None, 'table': None,
             'column': None, 'is_partial_revoke': 0, 'grant_option': False},
        ]

    def test_privilege_fully_present_revoke_source_granted_in_same_run(self):
        # Nothing is granted yet, the source privilege comes from
        # the GRANT statements the same run is about to execute.
        self.obj._grants = []
        pending = self.obj._pending_grant_rows({('SELECT', 'foo.*', False)})
        result = self.obj._privilege_fully_present('foo.bar', {'SELECT': []}, revoke=1,
                                                   extra_grants=pending)
        assert result is False

    def test_privilege_fully_present_revoke_no_source_in_same_run(self):
        # The pending grant is for another object, so there is nothing to revoke.
        self.obj._grants = []
        pending = self.obj._pending_grant_rows({('SELECT', 'other.*', False)})
        result = self.obj._privilege_fully_present('foo.bar', {'SELECT': []}, revoke=1,
                                                   extra_grants=pending)
        assert result is True

    def test_privilege_fully_present_revoke_already_done_with_pending_grants(self):
        # The partial revoke is already in place, a pending grant must not
        # make the module revoke it again.
        self.obj._grants = [
            {'access_type': 'SELECT', 'access_object': '', 'database': 'foo', 'table': None,
             'column': None, 'is_partial_revoke': 0, 'grant_option': 0},
            {'access_type': 'SELECT', 'access_object': '', 'database': 'foo', 'table': 'bar',
             'column': None, 'is_partial_revoke': 1, 'grant_option': 0},
        ]
        pending = self.obj._pending_grant_rows({('INSERT', 'foo.*', False)})
        result = self.obj._privilege_fully_present('foo.bar', {'SELECT': []}, revoke=1,
                                                   extra_grants=pending)
        assert result is True

    def test_privilege_fully_present_revoke_and_grant_same_table_diff_column(self):
        self.obj._grants = []
        pending = self.obj._pending_grant_rows({('SELECT(A)', 'foo.bar', False)})
        result = self.obj._privilege_fully_present('foo.bar', {'SELECT': ['B']}, revoke=1,
                                                   extra_grants=pending)
        assert result is True

    def test_privilege_covered_access_object_grant_short_circuits(self):
        grants = [
            {'access_type': 'READ', 'column': None, 'is_partial_revoke': 0},
        ]
        assert self.obj._privilege_covered(grants, 'READ', [], 0, False) is True

    def test_privilege_covered_table_grant_with_unrestricted_row_covers_all_columns(self):
        grants = [
            {'access_type': 'SELECT', 'column': None, 'is_partial_revoke': 0},
        ]
        assert self.obj._privilege_covered(grants, 'SELECT', ['name'], 0, True) is True

    def test_privilege_covered_rejects_all_grant_for_partial_revoke(self):
        grants = [
            {'access_type': 'ALL', 'column': None, 'is_partial_revoke': 0},
        ]
        assert self.obj._privilege_covered(grants, 'SELECT', [], 1, True) is False

    @pytest.mark.parametrize(
        'obj,expected',
        [
            ('foo.bar', '`foo`.`bar`'),
            ('foo.*', '`foo`.*'),
            ('*.*', '*.*'),
            ('POSTGRES', '`POSTGRES`'),
            ('*', '*'),
        ]
    )
    def test_normalize_revoke_object(self, obj, expected):
        assert self.obj._normalize_revoke_object(obj) == expected

    def test_normalize_revoke_object_rejects_partial_wildcard_db(self):
        self.obj._normalize_revoke_object('*.bar')
        self.mock_module.fail_json.assert_called_once()
        assert "'*.*'" in self.mock_module.fail_json.call_args.kwargs['msg']

    def test_grant_matches_object_ignores_table_grants_for_access_objects(self):
        # A table level grant carries an empty access_object too, but it
        # must not be treated as a match for an access object request.
        grant = {'access_type': 'SELECT', 'access_object': '', 'database': 'foo', 'table': 'bar',
                 'column': None, 'is_partial_revoke': 0, 'grant_option': 0}
        assert self.obj._grant_matches_object(grant, 'POSTGRES') is False
        assert self.obj._grant_matches_object(grant, '*') is False


class TestClickHouseGrantsUpdateRevokes:
    """Test the statements update() builds for the revokes option"""

    def setup_method(self):
        executed_statements.clear()

        self.mock_module = MagicMock()
        self.mock_module.check_mode = False
        self.mock_module._diff = False
        self.mock_module.params = {
            'state': 'present',
            'exclusive': False,
            'privileges': None,
            'partial_revokes': None,
            'cluster': None,
        }
        self.mock_client = MagicMock()

        patcher = patch('ansible_collections.community.clickhouse.plugins.modules.'
                        'clickhouse_grants.execute_query')
        self.mock_execute_query = patcher.start()
        self.mock_execute_query.return_value = [(1,)]

        version_patcher = patch('ansible_collections.community.clickhouse.plugins.modules.'
                                'clickhouse_grants.get_server_version')
        self.mock_version = version_patcher.start()
        self.mock_version.return_value = {'year': 25, 'feature': 8, 'maintenance': 0}

        self.obj = ClickHouseGrants(module=self.mock_module, client=self.mock_client,
                                    grantee="alice")

    def teardown_method(self):
        patch.stopall()
        executed_statements.clear()

    def test_revoke_statement_quotes_object(self):
        self.mock_module.params['partial_revokes'] = [{'object': 'foo.bar', 'privs': ['SELECT']}]
        # SHOW GRANTS output consumed by get()
        self.mock_execute_query.return_value = [('GRANT SELECT ON foo.* TO alice',)]
        self.obj._grants = [
            {'access_type': 'SELECT', 'access_object': '', 'database': 'foo', 'table': None,
             'column': None, 'is_partial_revoke': 0, 'grant_option': 0}
        ]

        changed = self.obj.update()

        assert changed is True
        assert executed_statements == ["REVOKE SELECT ON `foo`.`bar` FROM 'alice'"]

    def test_revoke_statement_quotes_columns(self):
        self.mock_module.params['partial_revokes'] = [{'object': 'foo.bar', 'privs': ['SELECT(a, b)']}]
        self.mock_execute_query.return_value = [('GRANT SELECT ON foo.* TO alice',)]
        self.obj._grants = [
            {'access_type': 'SELECT', 'access_object': '', 'database': 'foo', 'table': None,
             'column': None, 'is_partial_revoke': 0, 'grant_option': 0}
        ]

        self.obj.update()

        assert executed_statements == ["REVOKE SELECT(`a`, `b`) ON `foo`.`bar` FROM 'alice'"]

    def test_grant_and_revoke_in_one_run_are_ordered(self):
        self.mock_module.params['privileges'] = [{'object': 'foo.*', 'privs': {'SELECT': False}}]
        self.mock_module.params['partial_revokes'] = [{'object': 'foo.bar', 'privs': ['SELECT']}]
        # Nothing granted yet
        self.mock_execute_query.return_value = []
        self.obj._grants = []

        changed = self.obj.update()

        assert changed is True
        assert executed_statements == [
            "GRANT SELECT ON foo.* TO 'alice'",
            "REVOKE SELECT ON `foo`.`bar` FROM 'alice'",
        ]

    def test_expand_grant_and_revoke_stays_in_place(self):
        self.mock_module.params['privileges'] = [{'object': 'foo.*', 'privs': {'SELECT': False}}]
        self.mock_module.params['partial_revokes'] = [{'object': 'foo.bar', 'privs': ['SELECT(a)']}]
        # Nothing granted yet
        self.mock_execute_query.return_value = []
        self.obj._grants = [
            {'access_type': 'SELECT', 'access_object': '', 'database': 'foo', 'table': 'bar',
             'column': None, 'is_partial_revoke': 0, 'grant_option': 0},
            {'access_type': 'SELECT', 'access_object': '', 'database': 'foo', 'table': 'bar',
             'column': 'a', 'is_partial_revoke': 1, 'grant_option': 0}
        ]

        changed = self.obj.update()

        assert changed is True
        assert executed_statements == [
            "GRANT SELECT ON foo.* TO 'alice'",
            "REVOKE SELECT(`a`) ON `foo`.`bar` FROM 'alice'",
        ]

    def test_no_statement_when_revoke_already_in_place(self):
        self.mock_module.params['partial_revokes'] = [{'object': 'foo.bar', 'privs': ['SELECT']}]
        self.mock_execute_query.return_value = [('GRANT SELECT ON foo.* TO alice',)]
        self.obj._grants = [
            {'access_type': 'SELECT', 'access_object': '', 'database': 'foo', 'table': None,
             'column': None, 'is_partial_revoke': 0, 'grant_option': 0},
            {'access_type': 'SELECT', 'access_object': '', 'database': 'foo', 'table': 'bar',
             'column': None, 'is_partial_revoke': 1, 'grant_option': 0},
        ]

        changed = self.obj.update()

        assert changed is False
        assert executed_statements == []

    def test_unsupported_server_version_fails(self):
        self.mock_module.params['partial_revokes'] = [{'object': 'foo.bar', 'privs': ['SELECT']}]
        self.mock_execute_query.return_value = []
        self.mock_version.return_value = {'year': 25, 'feature': 3, 'maintenance': 0}
        self.obj._grants = []

        self.obj.update()

        self.mock_module.fail_json.assert_called_once()
        assert '25.8' in self.mock_module.fail_json.call_args.kwargs['msg']

    def test_check_mode_builds_but_does_not_execute(self):
        self.mock_module.check_mode = True
        self.mock_module.params['partial_revokes'] = [{'object': 'foo.bar', 'privs': ['SELECT']}]
        self.mock_execute_query.return_value = [('GRANT SELECT ON foo.* TO alice',)]
        self.obj._grants = [
            {'access_type': 'SELECT', 'access_object': '', 'database': 'foo', 'table': None,
             'column': None, 'is_partial_revoke': 0, 'grant_option': 0}
        ]
        self.mock_execute_query.reset_mock()

        changed = self.obj.update()

        assert changed is True
        assert executed_statements == ["REVOKE SELECT ON `foo`.`bar` FROM 'alice'"]
        # Only the SHOW GRANTS lookup, never the REVOKE
        assert self.mock_execute_query.call_count == 1
