# GreptimeDB compatibility test

The compatibility test check whether a newer version of GreptimeDB can read from the data written by an old version of
GreptimeDB (the "backward" compatibility), and vice-versa (the "forward" compatibility). It's often ran in the Github
Actions to ensure there are no breaking changes by accident.

The test work like this: reuse the sqlness-runner two times but each for a read or write side. For example, if we are
testing backward compatibility, we use sqlness-runner to run the SQLs with writes against the old GreptimeDB binary, and
use the same sqlness-runner to run the SQLs with reads against the new GreptimeDB binary. If the reads were executed
expectedly, we have achieved backward compatibility.

This compatibility test is inspired by [Databend](https://github.com/datafuselabs/databend/).

## Usage

### Legacy script flow

```shell
tests/compat/test-compat.sh <old_ver>
```

E.g. `tests/compat/test-compat.sh 0.6.0` tests if the data written by GreptimeDB **v0.6.0** can be read by **current**
version of GreptimeDB, and vice-versa. By "current", it's meant the fresh binary built by current codes.

### Sqlness compat command (recommended)

```shell
cargo sqlness compat --from=1.0.0-beta.1 --to=current --preserve-state
```

The compat command runs cases in three phases:

1. `1.feature` on `--from`
2. `2.verify` on `--to`
3. `3.cleanup` on `--to`

## Case markers

Compatibility cases can use sqlness markers:

- `-- SQLNESS SINCE <version>`
- `-- SQLNESS TILL <version>`

For `since`/`till` skips, runner rewrites the statement before execution to avoid running skipped SQL.

## Filter behavior

`--test-filter` still accepts sqlness regex. Compat runner also recognizes optional leading path prefixes:

- phase prefix: `1.feature/`, `2.verify/`, `3.cleanup/`
- mode prefix: `standalone/`, `distributed/`
- group prefix: `common/`, `only/`

Examples:

```shell
# run one standalone case path
cargo sqlness compat --from=1.0.0-beta.1 --to=current --test-filter="standalone/common/show-create-table.sql"

# same target with explicit phase prefix
cargo sqlness compat --from=1.0.0-beta.1 --to=current --test-filter="2.verify/distributed/common/show-create-table.sql"
```

## Prerequisites

Current version of GreptimeDB's binaries must reside in `./bins`:

- `./bins/current/greptime`
- `./bins/current/sqlness-runner`

The steps in Github Action already assure that. When running in local host, you have to `cp` them from target directory
manually.
