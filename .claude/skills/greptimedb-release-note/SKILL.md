---
name: greptimedb-release-note
description: Generate a GreptimeDB release changelog with git cliff (correct range, subtract already-released patch PRs, rebuild contributors, add human-curated highlights), output to a file, and prepare the docs-repo blog PR. Use when asked to write/generate a GreptimeDB release note or changelog.
---

# GreptimeDB Release Note / Changelog

Generate the changelog for a GreptimeDB release. Tooling: **`git cliff`** (config:
`cliff.toml` in the greptimedb repo) and **`gh`**. Run inside the greptimedb checkout.

**Prerequisites:** `git cliff`, `gh` (check `gh auth status`), and **Python** (the
subtract-PRs / rebuild-contributors step in §3–§4 is scripted). This skill writes
`<remote>` for the git remote pointing at `GreptimeTeam/greptimedb` (often `upstream`,
sometimes `origin`); resolve it with
`git remote -v | grep -i 'GreptimeTeam/greptimedb' | awk '{print $1}' | head -1`.

## 1. GitHub token (never print it)

`git cliff` enriches commits with PR titles/authors via the GitHub API; for hundreds of
commits you need a token or you hit rate limits. Pass it inline and **never read or echo the
token**:
```
GITHUB_TOKEN=$(gh auth token) git cliff ...
```
Alternatively the user sources an env file that exports `GITHUB_TOKEN` (don't read it). Without
a token it still runs, but may be rate-limited / incomplete.

## 2. Determine the previous version (skip nightlies)

From `gh release list --repo GreptimeTeam/greptimedb`, ignore `*-nightly-*`, `-rc.*`, `-beta.*`,
and build-suffixed tags; focus on formal `vX.Y.Z`.
- Patch `vX.Y.Z` (Z>0): previous = `vX.Y.(Z-1)`.
- New minor `vX.Y.0`: previous = the latest `vX.(Y-1).*` (for `v1.0.0`, the biggest `0.x`).
Double-check with the user.

## 3. Pick the range and generate

Key topology fact: a **minor tag** (e.g. `v1.0.0`) is an ancestor of `main`; **patch tags**
(`v1.0.1`, `v1.0.2`) live on the `release/v1.0` branch and are **NOT** ancestors of `main`
(they are cherry-picks with different SHAs). Verify with
`git merge-base --is-ancestor <tag> <remote>/main`.

### New minor (`X.Y.0`, cut from main)
Note the two different tags here: the `git cliff` **base is the previous minor `.0` tag**
(e.g. `v1.0.0`, an ancestor of `main`) — **not** the "previous version" from §2 (the latest
patch, e.g. `v1.0.2`), which is only used below to decide which patch PRs to subtract.

Base = the **previous minor tag** (e.g. `v1.0.0`); tip = the release commit (= `release/vX.Y`
tip, usually `<remote>/main`):
```
GITHUB_TOKEN=$(gh auth token) git cliff <prev-minor-tag>..<release-commit> --tag vX.Y.0 \
  -o /path/CHANGELOG-vX.Y.0.md
```
`cliff.toml`'s `ignore_tags` folds in-range nightly tags into the single section.

**Then subtract PRs already shipped in the intermediate patch releases** (`vX.(Y-1).1`,
`.2`, …) — their main-branch commits are inside the range and would duplicate, and the
audience cares about what's new vs the latest patch. Collect the PR set from the patch release
bodies and remove matching lines:
```
gh release view vX.(Y-1).Z --repo GreptimeTeam/greptimedb | grep -oE 'pull/[0-9]+'
```
Remove every changelog bullet whose `#NNNN` is in that combined set (a small Python script is
the reliable way — match `pull/<n>)` on lines starting with `*`).

### Patch (`X.Y.Z`, Z>0, cherry-picks on the release branch)
The previous patch tag is an ancestor of the release branch, so a plain range works **when the
cherry-picks land as individual commits**:
```
GITHUB_TOKEN=$(gh auth token) git cliff <prev-patch-tag>..<release-branch-tip> --tag vX.Y.Z -o ...
```
No extra subtraction (the branch only contains the new cherry-picks).

**Common case — squashed pick commit.** Patch branches are often a single squashed commit like
`chore: pick fixes and bump version to vX.Y.Z (#NNNN)`, so the plain range above yields just
that one bullet. First **find the picked PRs**: read the squash commit message
(`git log -1 --format=%B <release-branch-tip>`) — each `* fix: ... (#NN)` line is a picked PR.
Then pick one of two ways to build the changelog (a patch is usually only a few PRs — **ask the
user which they prefer**):

- **(a) Generate on `main` and filter** — lets `git cliff` produce correctly formatted bullets
  (titles, authors, categories):
  1. Locate each picked PR's commit on `main` and order them:
     ```
     git log --oneline <remote>/main --grep '#NN'   # per PR → its main commit SHA
     git merge-base --is-ancestor <older> <newer> && echo "older is older"
     ```
  2. Run cliff over a `main` range covering all of them (base = parent of the oldest picked
     commit, tip = the newest), then keep only the picked `#NN` bullets:
     ```
     GITHUB_TOKEN=$(gh auth token) git cliff <oldest-pick>~1..<newest-pick> --tag vX.Y.Z -o /tmp/raw.md
     ```
     The raw output includes unrelated PRs in the main range that were **not** picked — drop
     them, then rebuild contributors (§4).
- **(b) Hand-write the bullets** — a fine fallback for a small patch (and when the API/network is
  flaky). For each picked PR fetch its title + author (`gh api repos/GreptimeTeam/greptimedb/pulls/<n>`)
  and write the `* <title> by [@user] in [#NN](...)` lines plus the contributor list yourself.

If the branch history is otherwise messy, the same "identify the picked PRs and keep only those"
rule applies.

> Flaky-network note for (a): `git cliff` fetches the repo's full commit history from the GitHub
> API for author enrichment (paginates back thousands of commits, even for a tiny range). On a
> flaky connection it can time out mid-pagination; it caches pages, so just **re-run until it
> completes** — each retry resumes from the cache. If it keeps failing, fall back to (b).

## 4. Rebuild contributor sections after subtracting

`cliff` computes `New Contributors` / `All Contributors` over the full range, so after removing
lines, recompute from the **remaining** commit bullets:
- **All Contributors** = sorted set of `@user` from remaining `* ... by [@user] ... in [#NN]`
  lines (drop bots like `dependabot[bot]`).
- **New Contributors** = drop any entry whose first-contribution PR was removed (their only
  in-range PR shipped in a patch).
Do this in the same script that subtracts the PRs.

## 5. Verify

Cross-check the result against `git log <base>..<tip>`: no subtracted PR remains, and no
genuine new PR was dropped. Optionally drop the mechanical `chore: bump version to vX.Y.Z`
line (ask the user; some prefer to keep it).

## 6. Human-curated sections (insert after the `Release date:` line)

Mirror past release bodies (`gh release view v1.0.0 --repo GreptimeTeam/greptimedb`):
- **Short intro**, terse, engineer-written tone (no marketing adjectives).
- **`### 👍 Highlights`** — *few, deep* highlights, each with a **working example**
  (SQL / TOML config). To get examples right, **read the highlight PR and the related docs
  PR/page** in the docs repo (`GreptimeTeam/docs`, often checked out locally) — verify syntax
  in `docs/reference/sql/*.md`, `config/*.example.toml`, `config/config.md`. Do **not** mention
  implementation details, tiny features, or unfinished/experimental-but-not-ready features.
  Always have the user review and edit the highlights; iterate.
- **Dashboard** subsection — don't just bump the version. Read the dashboard PRs in the bundled
  release (`gh release view <ver> --repo GreptimeTeam/dashboard`; then the linked PRs) and
  describe the user-facing change.

## 7. Output

Write to `CHANGELOG-vX.Y.Z.md` (title `# vX.Y.Z`, which `--tag` produces) and **do not commit
it**. The release runbook deletes it after the release + docs PR are done.

## 8. Docs-repo blog variant + draft PR

The release note is also published as a blog post in **`GreptimeTeam/docs`**.
- **Ask the user for their local `GreptimeTeam/docs` checkout path**; if they have none, offer
  to clone (`git clone git@github.com:GreptimeTeam/docs.git <path>`).
- File: `blog/release-X-Y-Z.md` (version with dashes; see `blog/release-1-0-0.md`).
- Content = docs frontmatter (below) followed by the GitHub release body, **keeping the
  `# vX.Y.Z` H1 directly under the frontmatter**. The blog frontmatter has no `title:`, so
  Docusaurus uses that H1 as the page title and sidebar label — omitting it makes the page
  render as the filename (e.g. `release-1-1-0` instead of `v1.1.0`). Match the shape of
  `blog/release-1-0-0.md`:
  ```
  ---
  keywords: [release, GreptimeDB, changelog, vX.Y.Z]
  description: GreptimeDB vX.Y.Z Changelog
  date: YYYY-MM-DD
  ---

  # vX.Y.Z

  Release date: ...
  ```
- The docs working tree may have unrelated WIP — **don't disturb it**: use a `git worktree` off
  `origin/main`:
  ```
  git -C <docs> fetch origin main
  git -C <docs> worktree add -b chore/X.Y.Z-release-note /tmp/docs-release-note origin/main
  ```
- **The PR body must follow the docs repo's template** (`.github/pull_request_template.md` —
  "What's Changed in this PR" + a Checklist). Fill in the description; leave checklist boxes
  for the reviewer.
- Commit with sign-off, push, open a **draft** PR, then remove the worktree:
  ```
  git -C /tmp/docs-release-note add blog/release-X-Y-Z.md
  git -C /tmp/docs-release-note commit -s -m "docs: add X.Y.Z release note"
  git -C /tmp/docs-release-note push -u origin chore/X.Y.Z-release-note
  gh pr create --draft --repo GreptimeTeam/docs --base main --head chore/X.Y.Z-release-note \
    --title "docs: add X.Y.Z release note" --body-file <template-filled body>
  git -C <docs> worktree remove /tmp/docs-release-note
  ```
- Gotcha: `gh pr edit/create` may fail with an org-scope error if the token lacks `read:org`.
  Edit the body via REST instead: `gh api repos/GreptimeTeam/docs/pulls/<n> -X PATCH -F body=@body.md`.
