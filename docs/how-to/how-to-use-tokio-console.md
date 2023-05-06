This document introduces how to use the [tokio-console](https://github.com/tokio-rs/console) in GreptimeDB.

First, build GreptimeDB with feature `common-telemetry/console`. Also the `tokio_unstable` cfg must be enabled:

```bash
RUSTFLAGS="--cfg tokio_unstable" cargo build -F cmd/tokio-console
```

Then start GreptimeDB with tokio console binding address config: `--tokio-console-addr`. Remember to run with
the `tokio_unstable` cfg. For example, start Frontend:

```bash
RUSTFLAGS="--cfg tokio_unstable" greptime --tokio-console-addr="127.0.0.1:6669" frontend start
```

Now you can use `tokio-console` to connect to GreptimeDB's tokio console subscriber:

```bash
tokio-consile [TARGET_ADDR]
```

"TARGET_ADDR" defaults to "http://127.0.0.1:6669".
