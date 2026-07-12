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

```shell
tests/compat/test-compat.sh <old_ver>
```

E.g. `tests/compat/test-compat.sh 0.6.0` tests if the data written by GreptimeDB **v0.6.0** can be read by **current**
version of GreptimeDB, and vice-versa. By "current", it's meant the fresh binary built by current codes.

## Prerequisites

Current version of GreptimeDB's binaries must reside in `./bins`:

- `./bins/current/greptime`
- `./bins/current/sqlness-runner`

The steps in Github Action already assure that. When running in local host, you have to `cp` them from target directory
manually.
