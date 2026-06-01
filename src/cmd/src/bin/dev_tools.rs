// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![recursion_limit = "256"]

//! Standalone binary hosting developer-only tools (benchmarks, diagnostics).
//! Kept separate from the production `greptime` binary.

use clap::Parser;
use cmd::dev_tools;
use cmd::error::{InitTlsProviderSnafu, Result};
use common_version::{product_name, verbose_version, version};
use servers::install_default_crypto_provider;

#[derive(Parser)]
#[command(name = "dev-tools", author, version, long_version = verbose_version(), about)]
#[command(propagate_version = true)]
struct Command {
    #[clap(subcommand)]
    subcmd: dev_tools::SubCommand,
}

#[cfg(not(windows))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(debug_assertions)]
fn main() -> Result<()> {
    use snafu::ResultExt;
    // Set the stack size to 8MB for the thread so it wouldn't overflow on large stack usage in debug mode
    // see https://github.com/GreptimeTeam/greptimedb/pull/4317
    // and https://github.com/rust-lang/rust/issues/34283
    std::thread::Builder::new()
        .name("main_spawn".to_string())
        .stack_size(8 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .thread_stack_size(8 * 1024 * 1024)
                .enable_all()
                .build()
                .expect("Failed building the Runtime")
                .block_on(main_body())
        })
        .context(cmd::error::SpawnThreadSnafu)?
        .join()
        .expect("Couldn't join on the associated thread")
}

#[cfg(not(debug_assertions))]
#[tokio::main]
async fn main() -> Result<()> {
    main_body().await
}

async fn main_body() -> Result<()> {
    setup_human_panic();
    install_default_crypto_provider().map_err(|msg| InitTlsProviderSnafu { msg }.build())?;
    Command::parse().subcmd.run().await
}

fn setup_human_panic() {
    human_panic::setup_panic!(
        human_panic::Metadata::new(product_name(), version())
            .homepage("https://github.com/GreptimeTeam/greptimedb/discussions")
    );

    common_telemetry::set_panic_hook();
}
