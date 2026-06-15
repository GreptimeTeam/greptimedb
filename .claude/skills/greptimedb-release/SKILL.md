---
name: greptimedb-release
description: Runbook for publishing a new GreptimeDB version (tag + GitHub release + docs release-note PR) on the upstream GreptimeTeam/greptimedb repo. Use when asked to "release" / "publish" a GreptimeDB version (e.g. v1.1.0, v1.0.3).
---

# GreptimeDB Release Runbook

Publish a formal GreptimeDB release on **`GreptimeTeam/greptimedb`**. Always operate against
that repo (for `gh`, pass `--repo GreptimeTeam/greptimedb`). Changelog generation is a
separate, involved task — use the **`greptimedb-release-note`** skill for it.

This whole flow touches public infrastructure. Confirm with the user before the
outward-facing step (creating the release, which creates the tag and triggers CI).

## Prerequisites & remote

- Tools: `gh` (check with `gh auth status`) and `git`. Changelog generation additionally
  needs `git cliff` and Python — see the `greptimedb-release-note` skill.
- **Resolve the remote first.** This runbook writes `<remote>` for the git remote pointing
  at `GreptimeTeam/greptimedb`; substitute your actual name (often `upstream`, sometimes
  `origin` on a direct clone):
  ```
  git remote -v | grep -i 'GreptimeTeam/greptimedb' | awk '{print $1}' | head -1
  ```

## 0. Inputs: version and branch

- Ask the user for the **version** to release (e.g. `1.1.0`, `1.0.3`) and, if needed, the
  **branch**.
- **Branch is inferred from version**: `MAJOR.MINOR` → `release/v<MAJOR>.<MINOR>`.
  - `v1.0.0 / v1.0.1 / v1.0.2` live on `release/v1.0`; `v1.1.0` lives on `release/v1.1`.
  - Infer it, then double-check with the user.
- **New minor/major (`X.Y.0`)** is cut from `main`. If `release/vX.Y` does not exist on
  the remote, offer to create it from the intended `main` commit (with the user's consent):
  `git push <remote> <commit>:refs/heads/release/vX.Y`.
- **Patch (`X.Y.Z`, Z>0)** must use the existing `release/vX.Y` branch (it carries
  cherry-picked commits).
- Check what exists on the remote: `git ls-remote --heads <remote> 'release/*'`.

## 1. Verify the Cargo version

The workspace version on the release branch **must** equal the version being released, or
stop:
```
git show <remote>/release/vX.Y:Cargo.toml | grep -A30 '\[workspace.package\]' | grep -m1 version
```
(For a fresh minor cut from main, `<remote>/main` and `release/vX.Y` are usually the same
commit.)

## 2. Generate and curate the changelog

Use the **`greptimedb-release-note`** skill. It produces `CHANGELOG-vX.Y.Z.md` (uncommitted)
and the docs-blog variant. **Review the highlights with the user and iterate** before
publishing.

## 3. Create the GitHub release (creates the tag + triggers CI)

**Do NOT pre-create the tag.** Creating the release creates the tag and fires the
tag-push CI (`.github/workflows/release.yml`) that builds all binaries (**~hours**).
Confirm with the user, then:
```
gh release create vX.Y.Z \
  --repo GreptimeTeam/greptimedb \
  --target release/vX.Y \
  --title "Release vX.Y.Z" \
  --notes-file CHANGELOG-vX.Y.Z.md \
  --prerelease
```
- Title convention: `Release vX.Y.Z`.
- Always create as **`--prerelease`**: prerelease here is just a "build in progress" marker.
  The CI clears it on success (see §5). (The user creating it on the web works too.)
- Verify: `gh release view vX.Y.Z --repo GreptimeTeam/greptimedb --json name,tagName,isPrerelease,draft,targetCommitish`.

## 4. Open the docs release-note PR (do not wait for CI)

Right after triggering the release, open the docs draft PR (see the docs section of the
`greptimedb-release-note` skill). It only needs the finalized changelog.

Then **delete the local `CHANGELOG-vX.Y.Z.md`** (its content now lives in the release body
and the docs blog post).

## 5. After the CI build finishes (~hours)

CI's `publish-github-release` action, for a tag matching `^vX.Y.Z$`, runs
`ncipollo/release-action` with `allowUpdates: true`, **`prerelease=false`,
`makeLatest=true`, `omitBody=true`** (so it keeps our changelog body but finalizes the
flags). These flags are described from the current `.github/workflows/release.yml` —
**verify against the workflow** if behavior differs. Verify success:
```
gh release view vX.Y.Z --repo GreptimeTeam/greptimedb --json isPrerelease,assets
```
**Latest handling is conditional on whether this is the newest version:**
- **Releasing the latest version** (e.g. v1.1.0 when nothing newer exists) → CI's finalized
  state (non-prerelease, latest) is correct; **do nothing**.
- **Releasing a non-latest line** (e.g. a patch `v1.0.3` while `v1.1.0` is already latest) →
  set it back to non-latest after CI finishes:
  `gh release edit vX.Y.Z --repo GreptimeTeam/greptimedb --latest=false`.

## 6. Rollback (only on failure, with double confirmation)

Show the user exactly what will be removed first; never delete blindly.
```
gh release delete vX.Y.Z --repo GreptimeTeam/greptimedb   # remove the release
git push <remote> :refs/tags/vX.Y.Z                        # remove the tag
```

## Conventions / gotchas

- Remotes: `<remote>` is whatever points at `GreptimeTeam/greptimedb` (see Prerequisites).
  `gh` defaults can be unreliable in a repo with many remotes — always pass
  `--repo GreptimeTeam/greptimedb`.
- Release title is always `Release vX.Y.Z`.
- When reasoning about "previous version", skip nightly / build-suffixed tags
  (`*-nightly-*`, `vX.Y.Z-rc.N-<sha>-<date>-*`).
