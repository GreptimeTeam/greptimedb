# tests-integration — Agent & Contributor Guide

Integration test modules are registered in `tests/main.rs` (`autotests = false`).
Shared instance and storage helpers live in `src/instance.rs` and
`src/test_util.rs`. See [README.md](README.md) for external-service setup.

## Import/export portability

Import/export and COPY tests must work on Windows as well as Linux/macOS.
[#9227](https://github.com/GreptimeTeam/greptimedb/issues/9227) records export
tests failing because native Windows paths were compared with `/`-separated
COPY locations.

- Keep native filesystem paths (`Path`/`PathBuf`), file URIs, and `/`-separated
  object-store keys distinct. Compare filesystem paths as paths; for COPY
  location assertions, normalize both sides to the defined location format.
  Do not compare native `PathBuf::join().display()` strings directly with
  `/`-separated export locations.
- Build filesystem fixtures with temporary directories and convert absolute
  paths to file URIs with `Url::from_file_path`; do not hard-code `/tmp` or
  concatenate `file://` with native paths. Close file handles before rename or
  cleanup, including import-state writes.
- Linux [Nightly Build](../.github/workflows/nightly-build.yml) runs tests as root in
  `dev-builder`; `chmod`/read-only permissions cannot reliably force I/O failures.
  For storage error propagation tests, reuse `object_store::layers::mock` to inject
  deterministic failures rather than relying on execution-user permissions.

## Validation

Run focused cases from the repository root:

```bash
cargo nextest run -p tests-integration --test main -E 'test(<test-name>)'
```

For import/export changes, run affected tests on Windows. The reference job is
`test-on-windows` in [Nightly CI](../.github/workflows/nightly-ci.yml)
(`cargo nextest run -F dashboard`). Linux/macOS success does not establish
Windows compatibility. If Windows execution is unavailable, explicitly report
that gap and inspect platform-specific paths and file operations.
