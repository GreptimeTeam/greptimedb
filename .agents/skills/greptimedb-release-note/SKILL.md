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
  see "First pre-release" in §3: the previous minor's `.0` may not be published yet, and since
  v1.2 a `.0` tag sits on `release/vX.Y`, not on `main`.
Double-check with the user.

## 3. Pick the range and generate

For every release type, use the release tag's commit as `<tip>` when the tag exists.
Resolve the exact tag from the remote (use the peeled `^{}` commit for annotated tags)
and exclude commits added to the release branch afterward. Before tagging, pin the
intended release commit and record its SHA. Pass that SHA as `--target` when creating
the release, then verify the resulting tag before publishing the docs copy.

Key topology fact: **patch tags** (`v1.0.1`, `v1.0.2`) live on `release/vX.Y` and are **NOT**
ancestors of `main` (they are cherry-picks with different SHAs). Up to v1.1 the `.0` tag was
the point where `release/vX.Y` left `main`; from v1.2 the branch is cut before the `.0`, and
pre-releases and the `.0` itself are tagged on the branch with backports. Verify with
`git merge-base --is-ancestor <tag> <remote>/main`.

### New minor (`X.Y.0`)
The note compares against the whole previous line: everything new since `release/vX.(Y-1)`
left `main`, minus what any published `vX.(Y-1).*` release (pre-releases, `.0`, patches)
already shipped. The line's own pre-releases (`vX.Y.0-beta.*`) are not subtracted; the `.0`
note covers the whole minor.

Base = the **previous minor tag** (e.g. `v1.1.0`); tip = the release commit resolved above.
Split the range at the point where `release/vX.Y` left `main`:
```
bp=$(git merge-base <remote>/release/vX.Y <remote>/main)
```
- `<prev-minor-tag>..$bp` is on `main`; generate it with `git cliff`:
  ```
  GITHUB_TOKEN=$(gh auth token) git cliff <prev-minor-tag>..$bp --tag vX.Y.0 \
    -o /path/CHANGELOG-vX.Y.0.md
  ```
  `cliff.toml`'s `ignore_tags` folds in-range nightly tags into the single section.
- `$bp..<tip>` is the release branch: backports, squashed picks, and commits without a PR.
  `git cliff` fails on such commits (`Filter call 'sort' failed`), so build this part as a
  pre-release: collect the original PRs with the loop in "Pre-release" and add their bullets.
  List the commits that carry no PR with
  `git log --format='%h %s' $bp..<tip> | grep -vE '\(#[0-9]+\)$'`; drop mechanical ones, and
  for a manual port of a `main` PR (the body says `cherry picked from commit` a SHA that is not
  on `main`), find that PR by title and cite it.

**Then subtract PRs already shipped in published `vX.(Y-1).*` releases**: their `main` commits
are inside the range. Build the set as for the first pre-release (below): the PR links in
every published `vX.(Y-1).*` release body, plus the `(#N)` line endings in the history of the
newest published `vX.(Y-1).*` tag. Remove every changelog bullet whose `#NNNN` is in that set (a
small Python script is the reliable way — match `pull/<n>)` on lines starting with `*`).

### Patch (`X.Y.Z`, Z>0, cherry-picks on the release branch)
The previous patch tag is an ancestor of the release branch, so a plain range works **when the
cherry-picks land as individual commits**:
```
GITHUB_TOKEN=$(gh auth token) git cliff <prev-patch-tag>..<tip> --tag vX.Y.Z -o ...
```
No extra subtraction (the branch only contains the new cherry-picks).

**Common case — squashed pick commit.** Patch branches are often a single squashed commit like
`chore: pick fixes and bump version to vX.Y.Z (#NNNN)`, so the plain range above yields just
that one bullet. First **find the picked PRs**: read the squash commit message
(`git log -1 --format=%B <tip>`) — each `* fix: ... (#NN)` line is a picked PR.
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

### First pre-release (`X.Y.0-alpha.1`, cut from main)
The base is the commit where the previous line's release branch left `main`. It exists when the
alpha is cut, even if `vX.(Y-1).0` does not:
```
base=$(git merge-base <remote>/release/vX.(Y-1) <tag>)
```
If there is no `release/vX.(Y-1)`, use the newest published release tag that is an ancestor of
the tag (`git merge-base --is-ancestor <candidate> <tag>`). Run `git cliff <base>..<tag>`, then
subtract every PR that already shipped in a published `vX.(Y-1).*` release, pre-releases
included. Take the union of the PR links in those release bodies and the `(#N)` line endings in
the history of the newest published `vX.(Y-1).*` tag:
```
gh release view <published-vX.(Y-1)-tag> --repo GreptimeTeam/greptimedb | grep -oE 'pull/[0-9]+'
git log <newest-published-vX.(Y-1)-tag> --format=%B | grep -oE '\(#[0-9]+\)$'
```
Either source alone misses PRs: a release body can omit a backport that its tag contains, and a
backport commit can name only the backport PR.

### Pre-release (`X.Y.Z-{alpha,beta,rc}.N` after an earlier pre-release)
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
group by the `cliff.toml` parsers (first match wins), order by `merged_at`, and review `!` titles
for user-visible compatibility impact (§6) before assigning a category.

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
`git cliff` drops PRs whose title isn't a conventional commit (`filter_unconventional`). List
them and add the user-facing ones by hand:
```
git log --format=%s <range> | grep -E '\(#[0-9]+\)$' | grep -vE '^[a-z]+(\([^)]*\))?!?: '
```
Review generated breaking-change bullets using the same user-impact criteria as §6. Move
internal-only entries to ordinary groups, preserving genuine incompatibilities, PR links,
and authors.

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

Write concise, factual English. Rank changes by their impact and explain what changed,
why it matters, and any required action. Verify claims against the release's PRs, code,
and documentation; preserve their conditions and limitations.

- **Short intro** — summarize the release in one or two sentences.
- **`### 👍 Highlights`** — select the most consequential changes, including new features,
  correctness, security, performance, and substantial architectural improvements. Internal changes can
  be highlights when their technical significance or benefit is supported by evidence;
  change size alone is not a reason to feature them. Explain enough of the mechanism to
  make the benefit clear, without turning the note into an implementation walkthrough.
  Keep related changes together and distinguish shipped benefits from future possibilities.
  Include usage examples only when needed, and verify them against the released version.
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
- **`### Upgrade and compatibility notes`** — distinguish internal changes from changes
  visible to database users and operators. Include only user-visible compatibility changes
  and required upgrade actions; state who is affected, what changes, and what to do.
  Read PR bodies to establish impact: `!` titles and compatibility checkboxes alone do not
  determine whether a change belongs here. Internal changes without upgrade impact belong in
  highlights or the ordinary changelog, according to significance. Preserve PR links and
  contributor attribution. Omit empty sections and avoid repeating information across
  curated sections.
- **Benchmark results** (optional) — state the baseline version (often the previous formal
  release, not the previous beta), workload, data size, cache state, and hardware. Attribute results
  to individual changes only when the measurements isolate their effect. Leave out results the
  benchmark marks as not for publication, and smoke-level runs.

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
  that doesn't apply, check it and say why. State the release SHA (§3) under
  Verification.
- **Build the site before opening the PR.** A new worktree has no `node_modules`. Install with
  the pnpm version pinned in `package.json` (`packageManager`), as CI does:
  ```
  cd /tmp/docs-release-note
  pm=$(jq -r .packageManager package.json)   # e.g. pnpm@8.6.0
  npx -y "$pm" install --frozen-lockfile
  npx -y "$pm" build
  ```
  If the build fails, fix it or stop; don't mark the PR ready for review.
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
