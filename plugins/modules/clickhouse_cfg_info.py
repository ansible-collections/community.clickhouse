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

version_added: '0.7.0'

description:
  - Retrieves ClickHouse config file content and returns it as JSON.
  - Supports config files in the YAML and XML formats.
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
  - pyyaml (for YAML config files)
  - xmltodict (for XML config files)
'''

EXAMPLES = r'''
- name: Get server information
  register: result
  community.clickhouse.clickhouse_cfg_info:
    path: /etc/clickhouse-server/config.yaml

- name: Print returned data
  ansible.builtin.debug:
    var: result
'''

RETURN = r'''
config:
  description:
    - The content of the config file.
  returned: success
  type: dict
'''

try:
    import yaml
    HAS_PYYAML = True
except ImportError:
    HAS_PYYAML = False

try:
    import xmltodict
    HAS_XMLTODICT = True
except ImportError:
    HAS_XMLTODICT = False

from ansible.module_utils.basic import (
    AnsibleModule,
    missing_required_lib,
)
import json


def load_config(module, load_func, path):
    try:
        f = open(path, 'r')
        content = load_func(f)
    except Exception as e:
        fail_msg = "Could not open/load from file %s: %s" % (path, e)
        module.fail_json(msg=fail_msg)
    else:
        f.close()
        return content


def load_from_yaml(f):
    item = yaml.safe_load(f)
    # Ensure structure is JSON-serializable to prevent hangs from recursive aliases
    # See: https://github.com/ansible-collections/community.clickhouse/issues/111
    json.dumps(item)
    return item


def load_from_xml(f):
    content = xmltodict.parse(f.read())['clickhouse']
    # This lib loads all values including boolean
    # or numerical as strings. Let's convert if possible.
    return convert_str_vals_in_dict(content)


def is_xml(path):
    return True if len(path) > 4 and path[-4:] == '.xml' else False


def convert_str_vals_in_dict(d):
    """Recursively traverse a dict and covert
    string values to appropirate types.
    """
    for key, val in d.items():
        if isinstance(val, dict):
            convert_str_vals_in_dict(val)

        elif isinstance(val, list):
            for i, v in enumerate(val):
                if isinstance(v, dict):
                    convert_str_vals_in_dict(val[i])
        else:
            d[key] = convert(val)

    return d


def convert(val):
    # Try to convert or just return it back
    try:
        if val == 'false':
            val = False
        elif val == 'true':
            val = True
        elif val.isnumeric():
            val = int(val)
        else:
            val = float(val)
    except Exception:
        return val

    return val


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

    path = module.params['path']

    # When XML
    if is_xml(path):
        if not HAS_XMLTODICT:
            module.fail_json(msg=missing_required_lib('xmltodict'))
        load_func = load_from_xml

    # When YAML, the default
    else:
        if not HAS_PYYAML:
            module.fail_json(msg=missing_required_lib('pyyaml'))
        load_func = load_from_yaml

    # Load content as dict
    cfg_content = load_config(module, load_func, path)

    # Users will get this in JSON output after execution
    module.exit_json(changed=False, config=cfg_content)


if __name__ == '__main__':
    main()
