# -*- coding: utf-8 -*-

# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function

__metaclass__ = type


class ModuleDocFragment(object):
    DOCUMENTATION = r'''
options:
  settings:
    description:
      - Settings assigned to an object.
      - Settings are passed as dict where key is the setting name.
      - Each setting takes the following options value, min, max, writability.
    type: dict
    suboptions:
      value:
        description:
          - The value for the setting.
        type: str
        required: true
      min:
        description:
          - Minimum allowed value for the setting.
        type: str
        required: false
      max:
        description:
          - Maximum allowed value for the setting.
        type: str
        required: false
      writability:
        description:
          - Whether the setting can be changed.
          - Example values you can found L(profiles,https://clickhouse.com/docs/sql-reference/statements/create/settings-profile)
        type: str
        required: false
  profiles:
    description:
      - List of profiles assigned to an object.
    type: list
    elements: str
    required: false
notes:
  - For possible settings, check
    L(session settings,https://clickhouse.com/docs/operations/settings/settings).
'''
