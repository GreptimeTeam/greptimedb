# Native PromQL expectations

A partial corpus for the native PromQL route: each case here carries the `.result` the native route
produces, so `sqlness-runner bare -c tests/narthex-native-results --enable-native-promql` compares
against native output without touching `tests/cases`.

`tests/cases` is the corpus the mainline sqlness runs read, and those runs never select the native
route, so a native expectation recorded there fails every one of them. That is why this tree exists
separately, and why it holds only the cases whose native output differs from the mainline
expectation: a case that agrees needs no second copy.

## Layout

- `standalone/…` and `distributed/…` mirror the corpus paths. A case that both environments answer
  the same way lives under `standalone/common/…` with a `distributed/common` symlink to it; a case
  whose plan differs per environment is recorded on both sides instead (`standalone/promql/…` and
  `distributed/promql/…`), because one shared `.result` cannot hold both.
- A case's `.sql` is a symlink into `tests/cases` whenever the statements are identical, so a
  statement change on the mainline is picked up here on the next run. The four cases whose
  statements need sqlness directives the mainline copy does not carry (`SORT_RESULT` for a
  row-ordered aggregate, `REPLACE` for a diagnostic) keep a real file.

## Updating a golden

Run the job (or, locally, the same `sqlness-runner` invocation), then read `git diff --
tests/narthex-native-results`: sqlness writes what it produced into the `.result` it compared
against, so the diff is the new expectation. Review it against the plan structure the change
intended, then commit it. A `.sql` edit that adds or changes a `-- SQLNESS …` directive must update
the `.result` too: sqlness echoes the directive line, and a stale echo fails the comparison before
any answer is read.

## Known differences from the legacy route

- A source-free statement (`time()`, `label_replace(vector(1), …)`) is planned natively in one
  environment and keeps the existing plan in the other; those cases are recorded per environment.
- Diagnostic text from the native runtime carries no evaluation timestamp, while the legacy runtime
  appends ` at <ms> ms`. The native expectation records the native wording.
