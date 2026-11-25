# SecureFs for OpenDAL

This draft proposes a standalone filesystem service for
[GreptimeDB #8988](https://github.com/GreptimeTeam/greptimedb/issues/8988) and
[OpenDAL #8200](https://github.com/apache/opendal/issues/8200). The code adapts
GreptimeDB's existing Apache-licensed `SecureFsBackend` for OpenDAL's
`Builder` and `Service` interfaces. It uses OpenDAL Core 0.59.4 to match the
GreptimeDB version used as the reference. The permanent crate location is
open for review.

```rust
use cap_std::fs::Dir;
use opendal_core::Operator;
use opendal_secure_fs::SecureFs;

fn operator(root: Dir) -> opendal_core::Result<Operator> {
    Operator::new(SecureFs::from_dir(root))
}
```

## Security boundary

The caller chooses and opens the trusted directory. The service stores the
directory handle and uses capability-relative operations, including when opening
files for Tokio I/O. An OpenDAL object name never selects an ambient filesystem
root. The guarantee is limited to path resolution provided by `cap-std` on
the running platform. Provision the directory for regular files and directories.
Control who can move or mount files into it, create special files, or introduce
hard links to sensitive files.

OpenDAL normalizes paths before calling the service, including removing an
initial slash from object names. A caller that rejects absolute user paths must
check the original input before calling OpenDAL. The service accepts slash
separated names and the OpenDAL root marker `/`. It rejects parent and current
directory components, backslashes, colons, NULs, and components surrounded by
whitespace. On Windows it also rejects components ending in a period to avoid
name aliases. Listing skips names that cannot be represented in that namespace.

## Operations

The service implements directory creation, stat, reads and range reads, writes
(including append, empty writes, and conditional creation), bounded-batch listing,
and descendant deletion, including recursive deletion. Root deletion is refused.
Copy, rename, compose, presign, and writer rollback are outside the draft API.
Writes go directly to the destination. A write error or cancellation can leave
partial output.

Operations require a Tokio runtime. Filesystem calls on the directory capability
run on Tokio's blocking pool. A blocking call already running may finish after
the caller drops its future.

## Local validation

Run these from this crate directory with Rust and a C toolchain if a dependency
requires it. The first Cargo build may fetch dependencies and creates a local
`Cargo.lock` and `target/`.

```powershell
cargo fmt --all -- --check
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
```

Run the tests on Windows and Linux before using this draft as a replacement
for GreptimeDB's backend.
