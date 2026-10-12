# Phase 2: compare with the author's rationale

Read this only after `record-blind` succeeded and `open` downloaded the
rationale. The blind review is final: do not edit it. Everything from here is
appended.

## Compare

Read `<workdir>/private/rationale.md` and `<workdir>/blind.md`. Write
`<workdir>/compare.md` in English with these sections:

```
## Intent mismatches

Points where the reconstructed intent differs from the author's: a goal,
design choice, or constraint the blind reviewer missed, got wrong, or marked
as guessed. A mismatch is a finding only when the missing information would
lead a later maintainer to a credible wrong change or misuse (for example a
lock that must cover two writes); then say where the code should carry it (a
name, type, comment, or test). Missing reasons for a choice between
equivalent options are not findings.

## Findings revisited

Each blind finding, in order: still stands / resolved by the rationale /
rationale explains the intent but the code is still wrong. "Resolved" needs
the rationale to show a fact the code depends on, not an argument that the
risk is small. Rationale points that conflict with the PR description, the
invariants, or the code are findings too.

## Author's open questions

The places the rationale marks as uncertain or asks reviewers to check, and
your answer to each.

## New findings

Problems the rationale led you to that the blind review missed. Mark each
"found via rationale".
```

Omit a section that has nothing in it.

## Look up a session (only for a specific question)

When one question cannot be answered from the code and rationale, for example
"why not reuse `X`?", fetch the session that covers it:

```
python3 $S fetch --workdir <workdir> --name sessions/<file from open>
```

Sessions keep the conversation and tool calls, but long tool outputs are
truncated (`...[truncated N chars]...`); rerun a command in `src/` when its
full output matters. They are large and mostly trial and error. Do not read them whole and do
not load them into your own context. Give the question and the file path to a
subagent (Claude Code: Agent tool, `general-purpose`) and ask for the answer
with quoted evidence. Without subagents, search the file for the identifiers in
the question and read only the matching messages. Record the question and the
answer under the relevant section of `compare.md`.

## Record

```
python3 $S record-compare --workdir <workdir>
```

It appends `compare.md` to your review file in the private repository, after
the blind review. Then continue with Step 4 of the skill.
