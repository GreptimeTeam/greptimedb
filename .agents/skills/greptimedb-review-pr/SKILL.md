---
name: greptimedb-review-pr
description: Review a GreptimeDB pull request in two phases - a blind review from the PR description, diff, and documented invariants, then a comparison against the author's private rationale. Drafts inline comments in plain English; posts a pending review only when asked. Use when asked to review a GreptimeDB PR.
argument-hint: "<PR number or URL>"
---

# Review a GreptimeDB PR

**Usage**: `/greptimedb-review-pr <PR number or URL>`

Run inside a GreptimeDB checkout. Requires Python 3.9+, `git`, and `gh`
(authenticated). The script below is `python3` on macOS and Linux, `py -3` or
`python` on Windows.

```
S=.agents/review-context/review_context.py
```

Read-only on the PR: do not push to the PR branch, do not switch the current
branch, and post nothing to GitHub unless the user asks.

The order of the phases is the point of this skill. The blind phase must form
its findings before anything about the author's reasoning is read; otherwise
the review follows the author's line of thought and inherits its blind spots.
Until `record-blind` succeeds:

- do not run `open`, `fetch`, or any `gh`/`git` command against the
  private review repository;
- do not read PR comments or review threads, which often carry the author's
  reasoning;
- do not pass the reviewer anything beyond the workdir path.

## Step 1: Prepare

```
python3 $S prepare --pr <n>
```

It prints `workdir`, `head`, and `mode`. The workdir holds `pr.md` (title and
description), `diff.patch`, `src/` (a worktree at the PR head), and
`criteria.md`. `mode` is `full` when you can access the private repository and
`blind-only` otherwise (external contributors). In `blind-only` mode the review
ends after Step 2; nothing is written to the private repository.

## Step 2: Blind review

Run the `greptimedb-blind-reviewer` instructions
(`.claude/agents/greptimedb-blind-reviewer.md`) in an isolated reviewer that
has no GitHub credentials, then record its output.

| Agent | Run | `--isolation` |
|---|---|---|
| Claude Code | Agent tool, `subagent_type: greptimedb-blind-reviewer`, prompt exactly `Workdir: <workdir>`. Write the returned text to `<workdir>/blind.md` unchanged. | `claude-subagent` |
| Codex | `python3 $S blind-run --workdir <workdir> --agent codex` (writes `blind.md`) | `codex-exec` |
| OpenCode | `python3 $S blind-run --workdir <workdir> --agent opencode` (writes `blind.md`; not yet verified) | `opencode-run` |

`blind-run` strips every GitHub credential from the subprocess environment,
does not load the agent's user configuration (and so its MCP servers), and
first checks that the private repository is unreachable through `gh`, git over
HTTPS, and git over SSH. If any of them still connects, it aborts. Network
access stays on, so the reviewer can read public upstream code. This keeps the
reviewer from reading ahead; it is not a filesystem sandbox, so local session
files on the reviewer's machine stay readable.

If none of these can run, do the blind review yourself following the same
instructions, reading only the workdir, and record it with `--isolation none`.
The review file then says the blind phase was not isolated.

Record it:

```
python3 $S record-blind --workdir <workdir> --isolation <mode>
```

It requires non-empty `## Findings` and `## Reconstructed intent` sections and
accepts one blind review per reviewer per head. In `full` mode it commits the
review to the private repository; that commit is the record that the findings
came before the rationale was read. If the workdir is lost after recording,
run `prepare` again and continue at Step 3.

## Step 3: Compare (full mode only)

```
python3 $S open --workdir <workdir>
```

Then read `references/phase2-compare.md` and follow it. If `open` reports no
rationale (for example an external contributor's PR), skip to Step 4. If
`rationale_head` differs from `reviewed_head`, the author republished for a
newer head; say so in the comparison.

## Step 4: Draft inline comments

Now read existing PR feedback so the drafts do not repeat it:

```
gh api repos/GreptimeTeam/greptimedb/pulls/<n>/comments --paginate
gh api repos/GreptimeTeam/greptimedb/pulls/<n>/reviews --paginate
gh api repos/GreptimeTeam/greptimedb/issues/<n>/comments --paginate
```

For each finding worth posting, draft one inline comment. For each existing
comment on the same spot: agree, extend, or skip; do not restate it.

Anchoring:

- Anchor on the head revision (`side: RIGHT`), on a line inside a diff hunk.
  If the real problem is outside the hunks, anchor on the nearest changed line
  that causes it and name the other location in the text.
- Multi-line: give `start_line` and `line`.
- Findings with no good line go into the review body.

Use a `suggestion` block only when the fix is local, complete, and replaces
exactly the anchored lines, with the file's indentation, and compiles in
context.

### Voice

English only. The problem, why it breaks, what to do. Nothing else.

- First sentence is the problem. No setup ("I noticed that", "Looking at this").
- Name the concrete failure: input, path, result. Not "this may cause issues".
- Fix in plain imperative: "Drop the permit before the `?`."
- A question only when you do not know the answer, prefixed `question:`.
- `nit:` for trivia. Blocking issues get no label.
- One point per comment, usually one to three sentences.
- No praise, thanks, or softeners; no "ensure", "robust", "leverage",
  "comprehensive", "potentially", "It's worth noting", "Additionally".
- No headers, bold, bullet lists, or emoji inside a comment.
- Identifiers in code spans; locations as `path:line`.
- Comments are public. Nothing from the private rationale or sessions that the
  author has not made public, nothing about people's permissions, internal
  infrastructure, private benchmarks, or customer data.

> Bad: I noticed that in this function, when the flush is cancelled, the permit might not be properly released, which could potentially lead to resource leaks over time.
>
> Good: A cancelled flush returns at the `?` without releasing `permit`. After a few cancellations writes stall. Hold it in a guard.

## Step 5: Report

Report in English, like every file and draft this skill produces.

1. Conclusion: approve / request changes / comment, and the main reason.
2. Findings by severity, each with `path:line`, the problem, the evidence, and
   its invariant class. Mark anything unverified.
3. Comparison result (full mode): intent mismatches and what they say about the
   code.
4. Proposed invariants: the rule text and target file for each `undocumented`
   finding.
5. Inline comment drafts and, if needed, a two-to-five-line review body draft.
6. Commands you ran beyond this skill's script, and their results.

Then `python3 $S cleanup --workdir <workdir>`.

## Step 6: Post (only when asked)

Create a pending review the user can edit and submit (omit `event`):

```
gh api repos/GreptimeTeam/greptimedb/pulls/<n>/reviews --method POST --input <tmp>/review.json
```

```json
{
  "commit_id": "<head>",
  "body": "<review body, may be empty>",
  "comments": [
    {"path": "src/foo.rs", "line": 42, "side": "RIGHT", "body": "..."},
    {"path": "src/bar.rs", "start_line": 10, "line": 12, "start_side": "RIGHT", "side": "RIGHT", "body": "..."}
  ]
}
```

Write `review.json` outside the repository. If the API rejects a line,
re-anchor it rather than dropping the comment. Submit with an `event` only if
the user says so.

## Re-review

A new head needs a new blind review: run Step 1 again; it removes earlier
workdirs for the PR, including any downloaded rationale. The blind reviewer
sees the full diff at the new head. In Step 3, `open` also lists earlier review
files; fetch your previous one to check which findings were fixed.
