# -*- coding: utf-8 -*-

# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function

__metaclass__ = type


class ModuleDocFragment(object):
    DOCUMENTATION = r'''
options:
  login_host:
    description:
      - The same as the V(Client(host='...'\)) argument.
    type: str
    default: 'localhost'

  login_port:
    description:
      - The same as the V(Client(port='...'\)) argument.
      - If not passed, relies on the driver's default argument value.
    type: int

  login_db:
    description:
      - The same as the V(Client(database='...'\)) argument.
      - If not passed, relies on the driver's default argument value.
    type: str

  login_user:
    description:
      - The same as the V(Client(user='...'\)) argument.
      - If not passed, relies on the driver's default argument value.
      - Be sure your the user has permissions to read the system tables
        listed in the RETURN section.
    type: str

  login_password:
    description:
      - The same as the V(Client(password='...'\)) argument.
      - If not passed, relies on the driver's default argument value.
    type: str

  client_kwargs:
    description:
      - Any additional keyword arguments you want to pass
        to the Client interface when instantiating its object.
    type: dict
    default: {}

requirements: [ 'clickhouse-driver' ]

notes:
  - See the clickhouse-driver
    L(documentation,https://clickhouse-driver.readthedocs.io/en/latest)
    for more information about the driver interface.
'''
