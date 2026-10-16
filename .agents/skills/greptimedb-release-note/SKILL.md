---
name: greptimedb-release-note
description: Generate a GreptimeDB release changelog (minor, patch, or alpha/beta/rc pre-release) with git cliff (correct range, subtract already-released patch PRs, rebuild contributors, add human-curated highlights and upgrade notes), output to a file, and prepare the docs-repo blog PR. Use when asked to write/generate a GreptimeDB release note or changelog.
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

From `gh release list --repo GreptimeTeam/greptimedb`, ignore `*-nightly-*` and build-suffixed
tags (e.g. `v1.3.0-alpha.1-3c0e2a8d5-20260928-1790639248`).
- Patch `vX.Y.Z` (Z>0): previous = `vX.Y.(Z-1)`.
- New minor `vX.Y.0`: previous = the latest `vX.(Y-1).*` (for `v1.0.0`, the biggest `0.x`).
- Pre-release `vX.Y.Z-{alpha,beta,rc}.N`: previous = the latest pre-release tag of the same
  `vX.Y.Z` (e.g. `v1.3.0-beta.1` for `v1.3.0-beta.2`). If there is none (the first alpha),
  previous = the previous minor `.0` tag and the range is built as a new minor (§3), with the
  pre-release tag as the tip.
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
  1. Locate and order the picked PRs' commits on `main` chronologically:
     ```
     git log --oneline <remote>/main --grep='#NN1' --grep='#NN2' --reverse
     ```
     The first line is the oldest pick, and the last line is the newest pick.
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

### Pre-release (`X.Y.Z-{alpha,beta,rc}.N` after an earlier pre-release)
**The tip is the tagged commit, not the release branch tip.** Resolve it from the remote and
write this SHA in the docs PR body (§8) as what the note was checked against:
```
git ls-remote --tags <remote> 'vX.Y.Z-beta.N*'
```
Use the `refs/tags/vX.Y.Z-beta.N` line (its `^{}` line if the tag is annotated), not the
build-suffixed tags it also lists. PRs that land on `release/vX.Y` after the tag
(`git log --oneline <tag>..<remote>/release/vX.Y`) belong to the next release, even if they are
already merged when you write the note.

The range `<prev-pre-release>..<tag>` usually mixes two kinds of commits: individual backports
(`... [Backport release/vX.Y] (#NNNN)`) and squashed pick commits (`chore: backport selected
changes for ... (#NNNN)`). In both, the original PRs are the `(#NN)` at the end of body lines;
a backport commit can also carry prerequisite PRs. Collect the PR set, falling back to the
subject for commits whose body lists none:
```
for c in $(git rev-list <prev-pre-release>..<tag>); do
  prs=$(git log -1 --format=%b "$c" | grep -oE '\(#[0-9]+\)$')
  [ -n "$prs" ] || prs=$(git log -1 --format=%s "$c" | grep -oE '\(#[0-9]+\)$')
  echo "$prs"
done | tr -dc '0-9\n' | sort -un
```
List the bullets under the **original** PRs (title and author of the PR merged to `main`), not
the backport PRs. Build them with patch (a) or (b) above; for (b), fetch
`gh api repos/GreptimeTeam/greptimedb/pulls/<n> --jq '{number,title,author:.user.login,merged_at}'`,
group by the `cliff.toml` parsers (first match wins), order by `merged_at`, and put `!` titles
under Breaking changes (§6).

A squashed pick PR can also contain release-only commits that have no PR. List them:
```
gh api repos/GreptimeTeam/greptimedb/pulls/<squash-pr>/commits --paginate \
  --jq '.[].commit.message | split("\n")[0]' | grep -vE '\(#[0-9]+\)$'
```
Mechanical ones (version bump, toolchain or lint adaptations; example:
`fix: adapt release OptionMap iteration to Rust 1.98`) get no bullet. A release-only
commit that changes behavior gets a bullet in its group.

> Flaky-network note for (a): `git cliff` fetches the repo's full commit history from the GitHub
> API for author enrichment (paginates back thousands of commits, even for a tiny range). On a
> flaky connection it can time out mid-pagination; it caches pages, so just **re-run until it
> completes** — each retry resumes from the cache. If it keeps failing, fall back to (b).

### Groups (all paths)
Use only the groups in `cliff.toml`. `git cliff` prints a `### Build` section for `build:` commits
because no parser matches them; move a user-facing `build(deps)` fix to Bug Fixes and the rest to
Miscellaneous Tasks. Keep each bullet list free of blank lines; a blank line turns it into a
loose list that renders with different spacing.

## 4. Rebuild contributor sections after subtracting

`cliff` computes `New Contributors` / `All Contributors` over the full range, so after removing
lines, recompute from the **remaining** commit bullets:
- **All Contributors** = sorted set of `@user` from remaining `* ... by [@user] ... in [#NN]`
  lines (drop bots like `dependabot[bot]`).
- **New Contributors** = drop any entry whose first-contribution PR was removed (their only
  in-range PR shipped in a patch).
Do this in the same script that subtracts the PRs.

When the bullets are built by hand (§3 (b), pre-releases), `git cliff` computes no New
Contributors, and GitHub's generated notes don't either (they only see the backport PRs). An
author is new if none of their merged PRs shipped before, i.e. none appears in the history of the
base tag:
```
git log <base-tag> --format=%B | grep -oE '\(#[0-9]+\)$' | tr -dc '0-9\n' | sort -u > /tmp/shipped.txt
gh search prs --repo GreptimeTeam/greptimedb --author <user> --merged --limit 1000 \
  --json number --jq '.[].number' | sort -u > /tmp/author.txt
comm -12 /tmp/author.txt /tmp/shipped.txt   # empty => new contributor
```
Run it for every non-bot author in the release, and cite their earliest-merged PR in this
release. "Earliest merged PR is in range" is not the same test: a contributor whose first PR
merged on `main` but was not picked into any release is still new when a later PR of theirs
ships.

## 5. Verify

Cross-check the result against `git log <base>..<tip>`: no subtracted PR remains, and no
genuine new PR was dropped. Optionally drop the mechanical `chore: bump version to vX.Y.Z`
line (ask the user; some prefer to keep it).

## 6. Human-curated sections (insert after the `Release date:` line)

**Style.** Follow the Google developer documentation style guide (apply the
`tech-doc-style-english` skill if it is installed): sentence-case headings, active voice, present
tense for behavior, American spelling, serial comma, code font for identifiers, bold only for UI
names. Keep it short:
- Intro: two sentences at most, what the release covers and its main theme.
- Highlight: one paragraph of up to four sentences, plus an example if it earns one (see below).
- Upgrade note or breaking change: one bullet, one or two sentences: what changes, who is
  affected, what to do.
- Every sentence states a fact the reader acts on or checks. Delete sentences that restate the
  heading or the previous sentence, transitions, and summaries.
- No marketing words (`significantly`, `powerful`, `seamless`, `robust`, `enhanced`), no vague
  verbs (`supports`, `handles`, `improves`; say what happens), no `several`/`many` when the number
  is known. Keep the source's hedges; don't add new ones.

Example:
> Bad: This release significantly enhances write reliability by introducing a robust retry
> mechanism that seamlessly handles transient failures.
>
> Good: In distributed clusters, the frontend retries a failed write up to three times with
> exponential backoff. Set `max_retries = 0` to disable retries.

Mirror past release bodies (`gh release view v1.0.0 --repo GreptimeTeam/greptimedb`):
- **Short intro** (see Style).
- **`### 👍 Highlights`** — *few, deep* highlights, ranked by user impact. Fixes for silently
  wrong query results, lost or invisible data, and security issues rank with new features and
  can be highlights.
  - Write a fix as its symptom and the affected deployments or usage, not the PR title. Example:
    "with no traffic, `errors / requests > 0.05` was true, so the alert fired".
  - State whether a feature is on by default. If an example config uses the default values, say
    so.
  - Give an example (SQL / TOML config) only where it helps readers act. Don't pick highlights,
    or the example, by what is easy to demonstrate.
  - One highlight covers one change or one user-visible problem. Don't merge unrelated PRs into
    one entry because they touch the same component.
  - Sources: the PR body's sections on impact, compatibility, and behavior changes, plus the
    code and config it touches, not the title alone. Verify examples against the related docs
    PR/page in the docs repo (`GreptimeTeam/docs`, often checked out locally),
    `docs/reference/sql/*.md`, `config/*.example.toml`, `config/config.md`.
  Do **not** mention implementation details, tiny features, or unfinished/experimental-but-not-ready
  features. Always have the user review and edit the highlights; iterate.
- **Dashboard** subsection — don't just bump the version. Read the dashboard PRs in the bundled
  release (`gh release view <ver> --repo GreptimeTeam/dashboard`; then the linked PRs) and
  describe the user-facing change. Use the names the UI shows, not the PR titles; check them in
  the dashboard's locale strings at the bundled tag:
  ```
  gh api 'repos/GreptimeTeam/dashboard/contents/src/locale/en-US/menu.ts?ref=<dashboard-tag>' \
    --jq .content | base64 -d
  ```
  Screenshots need absolute `https://` URLs: the body is copied to the GitHub release, where
  relative and site-root (`/img/...`) paths don't resolve.
- **`### Upgrade notes`** and **`### Breaking changes`** — check every PR in the release for:
  `!` titles; `Compatibility`, `Behavior change`, `Breaking` paragraphs in the body (often a
  plain `Compatibility:` line, not a heading); unchecked "API changes are backward compatible" /
  "Schema or data changes are backward compatible" boxes; new config options and their defaults;
  changed defaults; persisted format changes; rolling-upgrade order; new hard limits. These
  signals set reading order, not a filter: a limit can sit in plain prose with every box
  checked, so read the body of every user-facing `feat`/`fix` PR. Examples of what is easy to
  miss: a 512 MiB cap on decompressed request bodies (#9264); a default step for stepless
  subqueries (#9481).
  - **Upgrade notes**: steps operators take — upgrade order, index rebuilds, settings to review.
  - **Breaking changes**: behavior that used to work and now errors or returns different
    results. Put the `!` PR bullets here too, after these entries (see `v1.3.0-beta.1`).
- **Benchmark results** (optional) — state the baseline version (often the previous formal
  release, not the previous beta), data size, cache state, and hardware. Don't attribute results
  to single PRs. Leave out results the benchmark marks as not for publication, and smoke-level
  runs.

## 7. Output

Write to `CHANGELOG-vX.Y.Z.md` (title `# vX.Y.Z`, which `--tag` produces) and **do not commit
it**. The release runbook deletes it after the release + docs PR are done.

## 8. Docs-repo blog variant + draft PR

The release note is also published as a blog post in **`GreptimeTeam/docs`**.
- **Ask the user for their local `GreptimeTeam/docs` checkout path**; if they have none, offer
  to clone (`git clone git@github.com:GreptimeTeam/docs.git <path>`).
- File: `blog/release-X-Y-Z.md` (version with dashes; see `blog/release-1-0-0.md`). Pre-releases
  use dashes throughout: `blog/release-1-3-0-beta-2.md`.
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
  The docs CI check `check-release-note-sync` requires the post minus its frontmatter to equal
  the GitHub release body byte for byte, so the H1 goes on the line right after the closing
  `---`, with no blank line.
- The docs working tree may have unrelated WIP — **don't disturb it**: use a `git worktree` off
  `origin/main`:
  ```
  git -C <docs> fetch origin main
  git -C <docs> worktree add -b chore/X.Y.Z-release-note /tmp/docs-release-note origin/main
  ```
- **Read and follow the current docs repo PR template**
  (`.github/pull_request_template.md`) from the checkout used for the release.
  Fill in every required section; do not rely on hard-coded section names from an older
  template. The checklist items are the author's to confirm: check the ones you did, and for one
  that doesn't apply, check it and say why. For pre-releases, state the tagged SHA (§3) under
  Verification.
- **Build the site before opening the PR.** A new worktree has no `node_modules`, and an old
  install can miss dependencies added since. Use the pnpm version pinned in `package.json`
  (`packageManager`); a newer pnpm fails the frozen install with `ERR_PNPM_LOCKFILE_CONFIG_MISMATCH`:
  ```
  cd /tmp/docs-release-note
  pm=$(jq -r .packageManager package.json)   # e.g. pnpm@8.6.0
  npx -y "$pm" install --frozen-lockfile
  npx -y "$pm" build
  ```
  The docs repo's husky hook sources `.husky/_/husky.sh`, an ignored file that exists only in a
  checkout where husky was installed; if `git commit` fails on it, copy `<docs>/.husky/_` into
  the worktree. If the build fails, fix it or stop; don't mark the PR ready for review.
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
