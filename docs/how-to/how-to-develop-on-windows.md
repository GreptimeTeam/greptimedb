# Develop on Windows

Regular pull request CI does not run on Windows, and the scheduled Windows jobs
run as an administrator. This page covers what a normal (non-elevated) Windows
account needs and the checks to run yourself.

## Clone

The repository tracks symbolic links, including `CLAUDE.md`, `.claude/skills`,
and cases under `tests/cases/distributed/`. Git for Windows creates symlinks
only when `core.symlinks` is `true` and the account may create them: elevated,
with Developer Mode, or with the "Create symbolic links" privilege
([Symbolic Links](https://gitforwindows.org/symbolic-links)). With
`core.symlinks=false`, which `git clone` sets when it cannot create symlinks,
each link is checked out as a small text file that contains its target:

- `CLAUDE.md` contains only the text `AGENTS.md`, and `.claude/skills` is not a
  directory, so Claude Code gets neither the agent guide nor the skills.
- In the distributed environment, `cargo sqlness bare` silently skips the
  shared cases that `tests/cases/distributed/common` links to. The linked `.sql`
  files run no statements, so their cases fail and their `.result` files are
  emptied.

Turn on [Developer Mode](https://learn.microsoft.com/windows/advanced-settings/developer-mode)
(Windows Settings, **For developers**; this needs administrator rights), then
clone with:

```shell
git clone -c core.symlinks=true -c core.autocrlf=false <repository-url>
```

If the account still cannot create symlinks, this checkout fails with
`unable to create symlink ...: Permission denied`.

`core.autocrlf=false` keeps the LF line endings that the repository uses
(`.editorconfig` sets `end_of_line = lf`, and there is no `.gitattributes`). The
Git for Windows installer defaults to `core.autocrlf=true`, which checks out
every text file with CRLF; the `test-on-windows` and `check-windows` jobs turn
it off before checkout. With it off, Git commits line endings as they are, so
keep your editor on LF.

In an existing clone, `git config core.symlinks true` makes `git status` list
the links as type-changed (`T`); `git checkout -- <path>` then recreates them.

## Toolchain

- The `x86_64-pc-windows-msvc` Rust toolchain needs the MSVC build tools and a
  Windows SDK, for example the Visual Studio "Desktop development with C++"
  workload ([MSVC prerequisites](https://rust-lang.github.io/rustup/installation/windows-msvc.html)).
  They replace the `gcc`/glibc packages listed in the
  [README](../../README.md#build-from-source).
- Put `protoc` (>= 3.15; CI uses 3.23.4) on `PATH`.

## Build and test

`make` is optional; you can run cargo directly:

| Task | Command |
| --- | --- |
| Build (`make build`) | `cargo build --locked -p cmd --bin greptime` |
| One crate's tests | `cargo nextest run -p <package>` |
| SQL tests (`make sqlness-test`) | `cargo sqlness bare` |
| Windows compile check (`check-windows`) | `cargo check --locked --workspace --all-targets` |

- Run tests with `RUST_MIN_STACK=8388608`, as CI does. Without it, at least one
  test overflows the stack on Windows
  ([#9860](https://github.com/GreptimeTeam/greptimedb/pull/9860)).
- `check-windows` builds with `RUSTFLAGS=-D warnings`, so treat warnings from
  the compile check as errors, for example an import used only by
  `#[cfg(unix)]` code.
- With `-F dashboard`, `src/servers/build.rs` downloads the dashboard assets by
  running `bash` with WSL paths (`/mnt/<drive>/...`), so that feature needs a
  WSL distribution.
- Without symlink rights, `test_slow_query_logging` in `common-telemetry` fails
  with `os error 1314`, and so does starting GreptimeDB with
  `logging.max_log_dir_size` set
  ([#10448](https://github.com/GreptimeTeam/greptimedb/issues/10448)).

## Windows in CI

Regular pull request and merge queue CI runs no Windows jobs. Windows runs in:

- [Nightly CI](../../.github/workflows/nightly-ci.yml): `test-on-windows`
  (`cargo nextest run -F dashboard`) and `sqlness-windows`
  (`make sqlness-test`).
- [Rust CI](../../.github/workflows/rust.yml): `check-windows`, on the weekday
  schedule or a manual dispatch.

These runners run as administrators with UAC disabled
([GitHub-hosted runners](https://docs.github.com/en/actions/reference/runners/github-hosted-runners))
and install Git with
[symlinks enabled](https://github.com/actions/runner-images/blob/main/images/windows/scripts/build/Install-Git.ps1),
so the symlink problems above do not show up in CI. A change can break Windows
without failing its pull request checks, so run the affected tests on Windows
when you can. For import, export, and COPY changes, also follow
[`tests-integration/AGENTS.md`](../../tests-integration/AGENTS.md).
