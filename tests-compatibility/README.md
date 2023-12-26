# When compatibility tests failed:

- Start an old version (or built from old source codes) GreptimeDB with its data home and WAL set to directory `tests-compatibility/data_home` and `tests-compatibility/data_home/wal/` respectively.
- Using our CLI tool to [export](https://docs.greptime.com/user-guide/upgrade) the data.
- Clear everything under `tests-compatibility/data_home/`.
- Build a new GreptimeDB with your changes with the same data home and WAL dir config.
- Using our CLI tool again to [import](https://docs.greptime.com/user-guide/upgrade) the data.
- Rerun the compatibility test. If it passes, commit the data files.
