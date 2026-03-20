---
name: release
description: Guides the release of an Ansible collection following the upstream process (without release branches). Outputs step-by-step instructions with commands for changelog generation, release PR, tagging, Galaxy publication, version bump, and GitHub release. Use when asked to release, publish, or tag a new collection version.
---

# Skill: release

## Purpose

Guide the release of an Ansible collection. This skill is collection-generic — it derives namespace, name, and version from `galaxy.yml`.

## When to Invoke

TRIGGER when:
- A user asks to release, publish, or tag a new collection version
- A user asks about the release process or release checklist

DO NOT TRIGGER when:
- Reviewing a PR (use `pr-review` skill instead)
- Running tests (use `run-tests` skill instead)
- General changelog or versioning questions unrelated to performing a release

## Inputs

- `version` (required): the target release version, e.g. `2.1.0`

## Prerequisites

- `antsibull-changelog` installed (`pip install antsibull-changelog`)
- `gh` CLI installed and authenticated
- Push access to the upstream remote

## Human Confirmation Gates

**Do not proceed past a confirmation gate without explicit human approval.** Present the relevant information and wait for the human to confirm before continuing to the next step. Gates are marked with **CONFIRM** below.

## Release Steps

### Step 0 — Read collection context

Extract collection identity from `galaxy.yml`:

```bash
grep -E '^(namespace|name|version):' galaxy.yml
```

Use the extracted values as `NAMESPACE`, `COLLECTION`, and `CURRENT_VERSION` in all subsequent steps. Use the user-provided version as `VERSION`.

**CONFIRM:** Present the extracted `NAMESPACE`, `COLLECTION`, `CURRENT_VERSION`, and the target `VERSION` to the human. Ask them to confirm these values are correct before proceeding.

### Step 1 — Pre-flight checks

```bash
git status
git checkout main
git pull --rebase upstream main
```

Verify before continuing:
- Working tree is clean (no uncommitted changes)
- `version` in `galaxy.yml` matches `VERSION`
- Changelog fragments exist: `ls changelogs/fragments/`

### Step 2 — Create release branch

```bash
git checkout -b release_VERSION
```

### Step 3 — Generate changelog

Determine the release type from `VERSION` and suggest a release summary using this template:

- **Major** (`X.0.0`): `This is a major release of the ``NAMESPACE.COLLECTION`` collection.`
- **Minor** (`X.Y.0`): `This is a minor release of the ``NAMESPACE.COLLECTION`` collection.`
- **Patch** (`X.Y.Z`): `This is a patch release of the ``NAMESPACE.COLLECTION`` collection.`

Followed by:
`This changelog contains all changes to the modules and plugins in this collection that have been made after the previous release.`

Create the release summary fragment:

```bash
cat > changelogs/fragments/VERSION.yml << 'EOF'
release_summary: |-
  This is a <major/minor/patch> release of the ``NAMESPACE.COLLECTION`` collection.
  This changelog contains all changes to the modules and plugins in this collection
  that have been made after the previous release.
EOF
```

**CONFIRM:** Present the suggested release summary and the list of changelog fragments that will be included. Ask the human to approve or edit the text before writing the fragment.

Generate the changelog:

```bash
antsibull-changelog release --reload-plugins
```

**CONFIRM:** Show the human the generated `CHANGELOG.rst` diff and ask them to confirm the content is correct before continuing.

### Step 4 — Commit and push release branch

```bash
git add -A
git commit -m "Release VERSION"
git push origin release_VERSION
```

### Step 5 — Create pull request

```bash
gh pr create --title "Release VERSION" --body "Release VERSION of NAMESPACE.COLLECTION."
```

**CONFIRM:** Wait for the human to confirm that CI has passed and the PR has been reviewed and merged before continuing.

### Step 6 — Update local main

After the PR is merged:

```bash
git checkout main
git pull --rebase upstream main
```

### Step 7 — Tag and push

**CONFIRM:** Ask the human to confirm before creating and pushing the tag. This action is irreversible.

```bash
git tag -a VERSION -m "NAMESPACE.COLLECTION: VERSION"
git push upstream VERSION
```

### Step 8 — Create GitHub release

```bash
gh release create VERSION --title "VERSION" --notes "See [CHANGELOG.rst](https://github.com/NAMESPACE/COLLECTION/blob/main/CHANGELOG.rst) for details."
```

## Output Format

Present each step as a numbered section containing:
1. What the step does (one line)
2. The exact command(s) to run (with placeholders replaced by actual values)
3. What to verify before proceeding to the next step
