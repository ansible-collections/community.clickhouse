# ClickHouse collection for Ansible

[![Plugins CI](https://github.com/ansible-collections/community.clickhouse/workflows/Plugins%20CI/badge.svg?event=push)](https://github.com/ansible-collections/community.clickhouse/actions?query=workflow%3A"Plugins+CI") [![Codecov](https://img.shields.io/codecov/c/github/ansible-collections/community.clickhouse)](https://codecov.io/gh/ansible-collections/community.clickhouse)

## Our mission

At the `community.clickhouse` Ansible collection project,
our mission is to produce and maintain simple, flexible,
and powerful open-source software tailored to automating ClickHouse-related tasks.

We welcome members from all skill levels to participate actively in our open, inclusive, and vibrant community.
Whether you are an expert or just beginning your journey with Ansible and ClickHouse,
you are encouraged to contribute, share insights, and collaborate with fellow enthusiasts.

We strive to make managing ClickHouse deployments as effortless and efficient as possible with automation,
enabling users to focus on their core objectives.

## External requirements

- [clickhouse-driver](https://clickhouse-driver.readthedocs.io/en/latest/) Python connector installed on a target machine.

## Using this collection

### Installing the Collection from Ansible Galaxy

Before using the ClickHouse collection, you need to install it with the Ansible Galaxy CLI:

```bash
ansible-galaxy collection install community.clickhouse
```

You can also include it in a `requirements.yml` file and install it via `ansible-galaxy collection install -r requirements.yml`, using the format:

```yaml
---
collections:
  - name: community.clickhouse
```

Note that if you install the collection from Ansible Galaxy, it will not be upgraded automatically if you upgrade the Ansible package.
To upgrade the collection to the latest available version, run the following command:

```bash
ansible-galaxy collection install community.clickhouse --upgrade
```

You can also install a specific version of the collection, for example, if you need to downgrade when something is broken in the latest version (please report an issue in this repository). Use the following syntax:

```bash
ansible-galaxy collection install community.clickhouse:==0.1.0
```

See [Ansible Using collections](https://docs.ansible.com/ansible/latest/user_guide/collections_using.html) for more details.

### Usage example

```yaml
- name: Create database
  community.clickhouse.clickhouse_db:
    name: test_db
    engine: Memory
    state: present
    comment: Test DB

- name: Get server information
  register: result
  community.clickhouse.clickhouse_info:

- name: Print server information
  ansible.builtin.debug:
    var: result

# Note: it runs SELECT version() just for the sake of example.
# You can get it with the task above much more conveniently.
- name: Query DB using non-default user & DB to connect to
  register: result
  community.clickhouse.clickhouse_client:
    execute: SELECT version()
    login_host: localhost
    login_user: alice
    login_db: foo
    login_password: my_password

- name: Print returned server version
  ansible.builtin.debug:
    var: result.result
```

## Code of Conduct

We follow the [Ansible Code of Conduct](https://docs.ansible.com/ansible/latest/community/code_of_conduct.html) in all our interactions within this project.

If you encounter abusive behavior violating the [Ansible Code of Conduct](https://docs.ansible.com/ansible/latest/community/code_of_conduct.html), please refer to the [policy violations](https://docs.ansible.com/ansible/latest/community/code_of_conduct.html#policy-violations) section of the Code of Conduct for information on how to raise a complaint.

## Communication

> `GitHub Discussions` feature is disabled in this repository. Use the `clickhouse` tag on the forum in the [Project Discussions](https://forum.ansible.com/new-topic?title=topic%20title&body=topic%20body&category=project&tags=postgresql) or [Get Help](https://forum.ansible.com/new-topic?title=topic%20title&body=topic%20body&category=help&tags=clickhouse) category instead.

### Asynchronous channels

* Join the Ansible forum:
    * [ClickHouse Team](https://forum.ansible.com/g/ClickHouseTeam): by joining the team you will automatically get subscribed to the posts tagged with [clickhouse](https://forum.ansible.com/tag/clickhouse).
    * [Get Help](https://forum.ansible.com/c/help/6/none): get help or help others.
    * [Posts tagged with 'postgresql'](https://forum.ansible.com/tag/clickhouse): leverage tags to narrow the scope.
    * [Social Spaces](https://forum.ansible.com/c/chat/4): gather and interact with fellow enthusiasts.
    * [News & Announcements](https://forum.ansible.com/c/news/5/none): track project-wide announcements including social events.

* The Ansible [Bullhorn newsletter](https://forum.ansible.com/t/about-the-newsletter-category/166): used to announce releases and important changes.

### Real-time channels

* Matrix rooms:
    * `TBD` [#clickhouse:ansible.com](https://matrix.to/#/#clickhouse:ansible.com): questions on how to contribute and use this collection.
    * [#users:ansible.com](https://matrix.to/#/#users:ansible.com): general use questions and support.
    * [#social:ansible.com](https://matrix.to/#/#social:ansible.com): say hello or share a funny joke and let's laugh together;)
    * [#ansible-community:ansible.com](https://matrix.to/#/#community:ansible.com): community and collection development questions.
    * other Matrix rooms or corresponding bridged Libera.Chat channels. See the [Ansible Communication Guide](https://docs.ansible.com/ansible/devel/community/communication.html) for details.

For more information about communication, including how to join these channels, see the [Ansible communication guide](https://docs.ansible.com/ansible/devel/community/communication.html).


We announce important development changes and releases through Ansible's [The Bullhorn newsletter](https://docs.ansible.com/ansible/devel/community/communication.html#the-bullhorn). If you are a collection developer, be sure you are subscribed.

We can create a [Forum](https://forum.ansible.com/) group and a Matrix channel in future if there's an interest.

For now, refer to general channels you can find in the [Ansible communication guide](https://docs.ansible.com/ansible/devel/community/communication.html).

## Contributing to this collection

The content of this collection is made by [people](https://github.com/ansible-collections/community.clickhouse/graphs/contributors) just like you: a community of individuals collaborating on making the world better through developing automation software.

We are actively accepting new contributors and all types of contributions are very welcome.

You don't know how to start? Refer to our [contribution guide](https://github.com/ansible-collections/community.clickhouse/blob/main/CONTRIBUTING.md)!

We use the following guidelines:

* [CONTRIBUTING.md](https://github.com/ansible-collections/community.clickhouse/blob/main/CONTRIBUTING.md)
* [Ansible Community Guide](https://docs.ansible.com/ansible/latest/community/index.html)
* [Ansible Development Guide](https://docs.ansible.com/ansible/devel/dev_guide/index.html)
* [Ansible Collection Development Guide](https://docs.ansible.com/ansible/devel/dev_guide/developing_collections.html#contributing-to-collections)

## Collection maintenance

The current maintainers (contributors with `write` or higher access) are listed in the [MAINTAINERS](https://github.com/ansible-collections/community.clickhouse/blob/main/MAINTAINERS) file. If you have questions or need help, feel free to mention them in the proposals.

To learn how to maintain / become a maintainer of this collection, refer to the [Maintainer guidelines](https://github.com/ansible-collections/community.clickhouse/blob/main/MAINTAINING.md).

It is necessary for maintainers of this collection to be subscribed to:

* The collection itself (the `Watch` button -> `All Activity` in the upper right corner of the repository's homepage).
* The [news-for-maintainers repository](https://github.com/ansible-collections/news-for-maintainers).

They also should be subscribed to Ansible's [The Bullhorn newsletter](https://docs.ansible.com/ansible/devel/community/communication.html#the-bullhorn).

## Governance

The process of decision making in this collection is based on discussing and finding consensus among participants.

## Releases Support Timeline

We maintain each major release version (1.x.y, 2.x.y, ...) for two years after the next major version is released.

Here is the table for the support timeline:
- 1.x.y: to be released

## Included content

See the list of included modules and their documentation for your installed version on the [collection Galaxy page](https://galaxy.ansible.com/ui/repo/published/community/clickhouse/docs/).

## Tested with

Tested with the following `ansible-core` releases:
- 2.14
- 2.15
- 2.16
- current development version

Tested with ClickHouse:
- 21.8.15.7
- 22.8.9.24
- 23.8.9.54

## More information

- [Ansible User guide](https://docs.ansible.com/ansible/latest/user_guide/index.html)
- [Ansible Developer guide](https://docs.ansible.com/ansible/latest/dev_guide/index.html)
- [Ansible Community code of conduct](https://docs.ansible.com/ansible/latest/community/code_of_conduct.html)
