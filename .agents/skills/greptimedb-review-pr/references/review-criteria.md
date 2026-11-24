# Review criteria

Read every changed file in full, not only the hunks. For each potential issue,
trace data flow through callers and callees, two to three layers deep, before
deciding it is fine.

## Correctness

- Intent: does the code solve the problem stated in the PR description, within
  its non-goals and constraints? "Compiles and passes tests" is not the bar.
- Logic errors, off-by-one, boundary conditions, null handling.
- Concurrency: data races, deadlocks, missing synchronization, cancellation.
- Resource leaks: unclosed handles, missing cleanup, permits or memory not
  released on error paths.
- Error handling: swallowed errors, wrong propagation, missing cases.
- API contracts: wrong types, missing validation at system boundaries.
- Persisted and wire formats: any change must stay readable by older and newer
  versions as `architecture-invariants.md` requires.

## Architecture

- Responsibilities and crate layering as documented in the invariants and
  `AGENTS.md` files.
- Coupling: new tight coupling or dependency cycles.
- Public APIs: minimal, consistent, hard to misuse.
- Deviation from existing patterns without a reason visible in the code.

## Code smells

- Duplicated logic that should be shared.
- Functions with too many branches, deep nesting, or excessive length.
- Misleading names or abstractions.
- Commented-out code, TODO/FIXME without a tracking issue.

## Tests

- New or changed behavior has tests; edge cases, error paths, and boundaries
  are covered where they matter.
- Each new test can fail while all others pass. Flag tests that assert what
  they just built, compare a constant to itself, or duplicate existing coverage
  under a new name.
- Deleted or merged tests: find why each was added (`git log -L`, blame, the
  original PR). Judge redundancy by the state transitions it exercised, not by
  whether its output appears elsewhere.

## Security

Input validation, authentication and authorization checks, injection, and
secrets or sensitive data in logs and error messages.

## How to look

- Changed predicates (pushdown, split, cast, guard, reject): list every input
  class whose outcome flips and follow each into the path it now takes. Grep
  all implementors. "Unsupported" or "unreachable" claims need every entry
  point checked: each protocol, dialect, and API. A guard and the conversion it
  protects must match the same way (by position vs by name).
- Invariants (ordering, concurrency, resource or memory accounting): write the
  invariant in one sentence, then trace the whole path end to end, dependency
  internals included, and list every reader, writer, and allocation that can
  break it. One bug found means the path was not fully traced; look for the
  rest of the class.
- Silent corruption paths: "won't happen in practice" is not an answer. Ask
  for a guard or an error.
- Comments: flag ones that restate code, defend a choice between equivalent
  options, or carry review discussion into the source.
- Constants: a literal that crosses a crate, protocol, or storage boundary
  should be a shared constant. One used within a single function stays a
  literal.
- Performance claims: check that the benchmark exercises the changed path
  (caches, warm data, and small datasets hide read-path changes) and that the
  baseline ran under the same conditions.
- Scope: gaps the PR did not cause and does not need are not its problem
  unless the PR makes them worse.

## Ablation

For each entity the change introduces (abstraction, type, helper, config
option, fallback, comment, test), assume it is deleted and name what breaks:
which requirement it serves, which test fails, which caller breaks. No concrete
answer means it should go. An ablation that changes behavior on any reachable
path is a regression, not an ablation.

## Evidence

Every finding needs the location in the head revision, the triggering input or
state, and the consequence. Without a concrete failure it is a question, not a
bug.

Severity: `critical` (must fix), `warning` (should fix), `suggestion`.
Ablation findings are `suggestion`, or `warning` when the redundant entity is a
hazard itself, such as a fallback that masks an error.
