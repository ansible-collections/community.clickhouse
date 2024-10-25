#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright: (c) 2024, Andrew Klychkov (@Andersson007) <andrew.a.klychkov@gmail.com>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

DOCUMENTATION = r'''
---
module: clickhouse_cfg_info

short_description: Retrieves ClickHouse config file content and returns it as JSON

description:
  - Retrieves ClickHouse config file content and returns it as JSON.
  - Only config files in YAML format are supported at the moment.
  - Does not change server state.

attributes:
  check_mode:
    description: Supports check_mode.
    support: full

author:
  - Andrew Klychkov (@Andersson007)

options:
  path:
    description:
    - Path to the config file.
    type: path
    required: true

requirements:
  - pyyaml
'''

# TODO: it should also handle xml configs

EXAMPLES = r'''
- name: Get server information
  register: result
  community.clickhouse.clickhouse_cfg_info:
    path: /etc/clickhouse-server/config.yaml

- name: Print returned data
  ansible.builtin.debug:
    var: result
'''

RETURN = r''' # '''

try:
    import yaml
    HAS_PYYAML = True
except ImportError:
    HAS_PYYAML = False

from ansible.module_utils.basic import (
    AnsibleModule,
    missing_required_lib,
)


def load_from_yaml(path):
    with open(path, 'r') as f:
        content = yaml.safe_load(f)
    return content


def main():
    argument_spec = {}
    argument_spec.update(
        path=dict(type='path', required=True),
    )

    # Instantiate an object of module class
    module = AnsibleModule(
        argument_spec=argument_spec,
        supports_check_mode=True,
    )

    if not HAS_PYYAML:
        module.fail_json(msg=missing_required_lib('pyyaml'))

    cfg_content = load_from_yaml(module.params['path'])

    # Users will get this in JSON output after execution
    module.exit_json(changed=False, **cfg_content)


if __name__ == '__main__':
    main()
