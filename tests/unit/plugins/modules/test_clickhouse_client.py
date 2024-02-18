from __future__ import (absolute_import, division, print_function)

__metaclass__ = type

import pytest
from uuid import UUID
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address


from ansible_collections.community.clickhouse.plugins.modules.clickhouse_client import (
    is_uuid,
    vals_to_supported,
    replace_val_in_tuple,
)


@pytest.mark.parametrize(
    'input_params,result',
    [
        ('e66c72d8-fbd2-c174-0df3-7cbfd0c3d635', True),
        ('qwerty123', False),
        ('meaningless text', False),
        ('d1659f3e-83fe-3845-f1be-5fada6046b67', True),
    ]
)
def test_is_uuid(input_params, result):
    # Testing the function is_uuid
    assert is_uuid(input_params) == result


@pytest.mark.parametrize(
    'input_params,result',
    [
        (((1, 2, 3, 4), 3, 5),
         (1, 2, 3, 5)
         ),
        (((1, 2, 3, 4), 2, 5),
         (1, 2, 5, 4)
         ),
        (((1, 2, 3, 4), 1, 5),
         (1, 5, 3, 4)
         ),
        (((1, 2), 0, 5),
         (5, 2)
         ),
    ]
)
def test_replace_val_in_tuple(input_params, result):
    # Testing the function replace_val_in_tuple
    assert replace_val_in_tuple(*input_params) == result


@pytest.mark.parametrize(
    'input_params,result',
    [
        ([
            ('default', UUID('e66c72d8-fbd2-c174-0df3-7cbfd0c3d635')),
            ('test', UUID('4bfbe653-9137-0ea6-b97d-dc391ec9a919')),
        ], [
            ('default', 'e66c72d8-fbd2-c174-0df3-7cbfd0c3d635'),
            ('test', '4bfbe653-9137-0ea6-b97d-dc391ec9a919'),
        ],),
        ([
            ('localhost', IPv4Address('127.0.0.1')),
            ('non_routable', IPv4Address('0.0.0.0')),
        ], [
            ('localhost', '127.0.0.1'),
            ('non_routable', '0.0.0.0'),
        ],),
        ([
            ('localhost', IPv6Address('::1')),
            ('non_routable', IPv6Address('::')),
        ], [
            ('localhost', '::1'),
            ('non_routable', '::'),
        ],),
        ([
            ('pi', Decimal('3.14159')),
            ('e', Decimal('2.71828')),
        ], [
            ('pi', 3.14159),
            ('e', 2.71828),
        ],),
    ]
)
def test_vals_to_supported(input_params, result):
    # Testing the function vals_to_supported
    assert vals_to_supported(input_params) == result
