# -*- coding: utf-8 -*-

# Copyright (c) Ansible Project
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import (absolute_import, division, print_function)
__metaclass__ = type


class ModuleDocFragment(object):

    # Standard documentation fragment
    DOCUMENTATION = r"""
options: {}
attributes:
  check_mode:
    support: full
    description: Can run in C(check_mode) and return changed status prediction without modifying target.
  idempotent:
    support: full
    description:
      - When run twice in a row outside check mode, with the same arguments, the second invocation indicates no change.
      - This assumes that the system controlled/queried by the module has not changed in a relevant way.
"""

    # Should be used together with the standard fragment
    IDEMPOTENT_NOT_MODIFY_STATE = r"""
options: {}
attributes:
  idempotent:
    support: full
    details:
      - This action does not modify state.
"""
