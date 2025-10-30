from __future__ import (absolute_import, division, print_function)

__metaclass__ = type

import pytest
from unittest.mock import MagicMock, patch

from ansible_collections.community.clickhouse.plugins.modules.clickhouse_grants import (
    ClickHouseGrants,
    GRANT_REGEX,
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
