# Licensing

GreptimeDB is an open-core project with a dual-license layout.

| Scope | License | Where it's declared |
| --- | --- | --- |
| Core (default build) | Apache-2.0 | [`/LICENSE`](../LICENSE); per-file Apache header enforced by [`/licenserc.toml`](../licenserc.toml) |
| Enterprise features | GreptimeDB Enterprise License | [`/LICENSE-ENTERPRISE`](../LICENSE-ENTERPRISE); per-file Enterprise header enforced by [`/licenserc-enterprise.toml`](../licenserc-enterprise.toml) |

Enterprise sources are gated behind the `enterprise` Cargo feature and are **not**
compiled into the default open-source build. Each one carries an explicit
Enterprise License header generated from [`enterprise-header.txt`](enterprise-header.txt).

## How it's enforced

CI (`license-header-check` in `.github/workflows/develop.yml`) runs
[hawkeye](https://github.com/korandoru/hawkeye) twice:

1. Default config — applies the Apache-2.0 header to all sources, **excluding**
   the enterprise files.
2. `licenserc-enterprise.toml` — applies the Enterprise header to **only** the
   enterprise files.

The two file lists are complementary and must stay in sync:

- `licenserc.toml` `excludes` — the `# enterprise` block.
- `licenserc-enterprise.toml` `includes`.

## Adding a new enterprise file

1. Add its path to **both** lists above.
2. Apply the header: `hawkeye format --config licenserc-enterprise.toml`.
3. Verify: `hawkeye check --config licenserc-enterprise.toml`.

Do not put an Apache-2.0 header on an enterprise file, and do not leave an
enterprise file without a header — CI fails in both cases.
