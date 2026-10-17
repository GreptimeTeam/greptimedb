---
name: greptimedb-create-pr
description: Create or update a GreptimeDB pull request for the current branch. Runs the pre-PR checks, writes a conventional title and a body with fixed fields (problem, non-goals, constraints, verification) in plain English, opens it as a draft, then publishes the author's rationale and agent session to the private review repository when the author has access. Use when asked to open, create, or update a GreptimeDB PR.
argument-hint: "[notes, e.g. ready, related issue, extra sessions codex:<id>]"
---

# Create a GreptimeDB PR

**Usage**: `/greptimedb-create-pr [notes]`

Invoking this skill is the request to push and open the PR. Marking it ready,
labels, and reviewers only when the notes or the user say so.

The PR carries two outputs. The public PR body holds what defines correctness:
the problem, non-goals, constraints, and verification. The author's reasoning
(design choices, rejected options, open doubts) and the agent session go to the
private review repository, where reviewers read them only after a blind review.
Reasoning in the PR body anchors reviewers to the author's view, so keep it out.

Requires Python 3.9+, `git`, `gh` (authenticated), and `gitleaks` for the
private part. The script is `python3` on macOS and Linux, `py -3` or `python` on
Windows.

```
S=.agents/review-context/review_context.py
```

## Step 1: Gather

1. Upstream: the remote whose URL points to `GreptimeTeam/greptimedb`
   (case-insensitive; it may be named `origin`, `upstream`, or anything else).
   The push remote is the author's fork. If ambiguous, ask.
2. `git fetch <upstream> main`, then compare against `<upstream>/main`, not the
   fork's `main`, which may be stale: `git log --oneline <upstream>/main..HEAD`,
   `git diff <upstream>/main...HEAD --stat`.
3. Existing PR: `gh pr list --repo GreptimeTeam/greptimedb --head <branch>
   --json number,headRefName,headRepositoryOwner` and keep only the one whose
   owner is the author's fork; branch names collide across forks. If it
   exists, update it instead of creating another.
4. Read `.github/pull_request_template.md` and the "Before opening a PR" list
   in `AGENTS.md`.
5. Intent: linked issue, design doc, the session's plan, commit messages. Read
   the full diff; the body describes what the code does.

## Step 2: Pre-flight

- Review the branch diff against
  `.agents/skills/greptimedb-review-pr/references/review-criteria.md`, unless
  a review already ran over the same changes. Moderate or complex findings wait
  for the user's decision.
- Run the checks in `AGENTS.md` that apply to the change (format, lint, tests
  for touched packages, license headers, generated files). Fix what you
  introduced; report the rest. Do not open the PR over failing checks without
  saying so.
- Commits: `git commit -s`; conventional-commit messages; no `Co-Authored-By`
  or any AI or tool attribution.
- Do not sweep unrelated uncommitted files into the PR. Ask when unclear.
- If the PR is under review, new fixes go in new commits; do not amend or
  force-push reviewed commits.

## Step 3: Title

`<type>(<scope>): <what changes>`, lowercase after the colon, no trailing
period, under ~72 characters. Name the change, not the activity ("cut
allocations when building bloom indexes", not "improve index performance").

## Step 4: Body

The title and body are public and always in English.

Keep the template's CLA line, headings, and checklist. Put the issue link under
"Refer to a related PR or issue link" (`Closes #N` when it closes it). Under
"What's changed and what's your intention?" write these fields, all present,
in this order:

```
### Problem
What is wrong or missing, in one to three sentences.

### Change
What the code now does, grouped by component when there are several.

### Non-goals
What this PR deliberately does not handle. "None." if nothing a reviewer
would expect is left out.

### Constraints
Interface contracts, compatibility (persisted and wire formats, config, APIs),
and performance requirements the change must hold. State breaks and known
limitations here plainly. "None." if there are none.

### Verification
What was run and the result, one line each: tests, sqlness cases, benchmarks,
manual scenarios. Facts only.
```

Do not write design reasoning and do not argue correctness. No "we chose X
because", "this is safe because", rejected alternatives, or how the bug was
found. Those go into `rationale.md` in Step 6.

When a reviewer needs a map, end Change with a `Review focus:` list of three to
five items: a file, function, or algorithm plus the invariant to check there.
Name where to look and what must hold, not why it was written that way. Around
1000 changed lines (excluding lockfiles, generated files, and fixtures) usually
needs one; a small diff with one subtle invariant can too. Add one line naming
what can be skimmed (moved code, renames) if that is a large part of the diff.

Draw a Mermaid diagram (under ~15 nodes, before and after when that is the
change) only when the change alters a flow, data path, state machine, or layout
and prose would take a paragraph. When the PR claims a performance effect, put
it in a table with baseline commit, hardware, and how it was run; report
regressions in the same table; no numbers you did not measure.

Checklist: tick only what is true. The docs box refers to the docs site
repository (`GreptimeTeam/docs`); tick it only when that site would be wrong or
incomplete without a matching change. Backport labels only when asked.

The PR body is public and keeps edit history. Never mention the private review
repository, people's permissions, internal infrastructure, private benchmarks,
or customer data.

### Voice

Write it the way an engineer describes their change to a reviewer who will read
the diff next. Plain means precise and restrained, not casual.

- Short declarative sentences, present tense for what the code does.
- Concrete nouns in code spans: types, functions, config keys.
- Do not restate the title, the diff, or the previous sentence.
- Banned: "This PR aims to", "In order to", "comprehensive", "robust",
  "seamless", "leverage", "significantly", "enhance", "streamline", "ensure",
  "crucial", "effectively", "It's worth noting", "Additionally", "Furthermore",
  "In summary", "Overall".
- No hedges ("should", "may", "potentially", "minimal", "just") and no
  self-assessment ("clean", "simple", "low-risk").
- No closing summary, thanks, emoji, bold for emphasis, or em-dash chains.
- Bullets for parallel items only.

> Bad: This PR aims to significantly improve the performance of bloom filter index creation by leveraging a more efficient deduplication strategy. It's worth noting that this change is fully backward compatible.
>
> Good: The bloom creator copied every element into a `HashSet<Vec<u8>>` to deduplicate. It now deduplicates by the `u64` bloom hash.

Reread the body and delete every sentence a reviewer would skip, that restates
something, that defends or grades the change, or that contains a banned word.

## Step 5: Create or update

1. `git push <fork-remote> <branch>`. Never force-push a branch under review
   unless the user asks.
2. Write the body to a temp file outside the repository, then
   `gh pr create --repo GreptimeTeam/greptimedb --base main --head <fork-owner>:<branch> --title "<title>" --body-file <file> --draft`.
   Omit `--draft` only when the notes say ready. For an existing PR,
   `gh pr edit <n> --repo GreptimeTeam/greptimedb --title ... --body-file <file>`,
   changing only sections the new commits made stale.
3. Note the PR number.

## Step 6: Private review context

```
python3 $S access
```

`none` means the author cannot access the private review repository (for
example an external contributor). Stop here and report "private review context
skipped". This is not an error.

With `full`:

1. Write `rationale.md` to a temp file outside the repository, from this
   session's work:
   - Key design decisions and the reason for each.
   - Options considered and dropped, and why.
   - Parts agents must not change, and why.
   - Where the author is unsure, or what reviewers should check first.

   In the author's language, short and concrete; reviewers read it after their
   own blind review. Do not write credentials, endpoints, IP addresses,
   hostnames, customer names, or personal data; refer to them abstractly ("a
   customer's S3-compatible store"). Show it to the author and apply their
   edits before publishing.

   Updating a PR that already has a published rationale (for example after
   addressing review comments): write only this round's revision instead, and
   publish it with `--revision <file>` in place of `--rationale <file>`. The
   script appends it to the published rationale as `## Revision N`. Cover:
   - Review comments addressed, and how.
   - Review comments not taken, and why.
   - Design changes since the last round, and new doubts.
2. Publish:
   ```
   python3 $S publish --pr <n> --rationale <file> --agent <claude|codex|opencode>
   ```
   `--agent` is the agent running this skill; its current session is included
   (read from `CLAUDE_CODE_SESSION_ID`, `CODEX_THREAD_ID`, or
   `OPENCODE_SESSION_ID`). Add `--session <agent>:<id>` for other sessions the
   author names, such as earlier work in another agent. If the current session
   id is not set, ask the author which session to include rather than guessing.

   Before upload the script drops records a reviewer cannot use (UI state,
   hook output, token counters, duplicated history, signatures, encrypted
   reasoning) and truncates any string over 4000 characters, which cuts long
   tool outputs and file dumps. It scans everything with gitleaks, replaces
   each hit with `[REDACTED:<rule>]`, rescans, and pushes only when the rescan
   is clean.

   Compressed sessions over 20 MB are refused. When the session also covers
   earlier, unrelated work, pass `--since <ISO 8601 time this change started>`,
   taken from the first message about this change; that is the expected fix.
   `--max-string` and `--max-mb` change the limits; raise them only when the
   author asks.

   Sessions are stored by id: a session published before is replaced by its
   current, longer copy, and new sessions are added.
3. If it fails, the PR stays as it is; report the error.

## Step 7: Report

Report in the user's language: the PR URL and title; pre-flight checks that
failed or were skipped; for the private part, either "skipped" or the published path, the sessions included,
the redactions made, and that internal hostnames, customer names, and personal
data are not scanned automatically; only the rationale was reviewed by hand.
