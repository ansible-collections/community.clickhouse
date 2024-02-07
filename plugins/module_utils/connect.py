# This code is part of Ansible, but is an independent component.
# This particular file snippet, and this file snippet only, is BSD licensed.
# Modules you write using this snippet, which is embedded dynamically by Ansible
# still belong to the author of the module, and may assign their own license
# to the complete work.
#
# Simplified BSD License (see simplified_bsd.txt or https://opensource.org/licenses/BSD-2-Clause)

from __future__ import absolute_import, division, print_function

__metaclass__ = type

from ansible.module_utils.basic import missing_required_lib


def client_common_argument_spec():
    """
    Return a dictionary with connection options.

    The options are commonly used by many modules.
    """
    return dict(
        login_host=dict(type='str', default='localhost'),
        login_port=dict(type='int', default=None),
        login_db=dict(type='str', default=None),
        login_user=dict(type='str', default=None),
        login_password=dict(type='str', default=None, no_log=True),
        client_kwargs=dict(type='dict', default={}),
    )


def check_driver(module, has_db_driver):
    """Checks if the driver is present.

    Informs user if no driver and fails.
    """
    if not has_db_driver:
        module.fail_json(msg=missing_required_lib('clickhouse_driver'))
