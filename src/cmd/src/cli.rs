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

use clap::Parser;
use cli::Tool;
use common_telemetry::logging::{LoggingOptions, TracingOptions};
use plugins::SubCommand;
use snafu::ResultExt;
use tracing_appender::non_blocking::WorkerGuard;

use crate::options::GlobalOptions;
use crate::{App, Result, error};
pub const APP_NAME: &str = "greptime-cli";
use async_trait::async_trait;

pub struct Instance {
    tool: Box<dyn Tool>,

    // Keep the logging guard to prevent the worker from being dropped.
    _guard: Vec<WorkerGuard>,
}

impl Instance {
    pub fn new(tool: Box<dyn Tool>, guard: Vec<WorkerGuard>) -> Self {
        Self {
            tool,
            _guard: guard,
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        self.tool.do_work().await.context(error::StartCliSnafu)
    }
}

#[async_trait]
impl App for Instance {
    fn name(&self) -> &str {
        APP_NAME
    }

    async fn start(&mut self) -> Result<()> {
        self.start().await
    }

    fn wait_signal(&self) -> bool {
        false
    }

    async fn stop(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Parser)]
pub struct Command {
    #[clap(subcommand)]
    cmd: SubCommand,
}

impl Command {
    pub async fn build(&self, opts: LoggingOptions) -> Result<Instance> {
        let guard = common_telemetry::init_global_logging(
            APP_NAME,
            &opts,
            &TracingOptions::default(),
            None,
            None,
        );

        let tool = self.cmd.build().await.context(error::BuildCliSnafu)?;
        let instance = Instance {
            tool,
            _guard: guard,
        };
        Ok(instance)
    }

    pub fn load_options(&self, global_options: &GlobalOptions) -> Result<LoggingOptions> {
        let mut logging_opts = LoggingOptions::default();

        if let Some(dir) = &global_options.log_dir {
            logging_opts.dir.clone_from(dir);
        }

        logging_opts.level.clone_from(&global_options.log_level);

        Ok(logging_opts)
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use std::ops::RangeInclusive;

    use clap::Parser;
    use client::{Client, Database};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_telemetry::logging::LoggingOptions;
    use rand::Rng;

    use crate::error::Result as CmdResult;
    use crate::options::GlobalOptions;
    use crate::{App, cli, standalone};

    fn random_standalone_addrs() -> (String, String, String, String) {
        let offset = choose_random_unused_port_offset(14000..=24000, 10);

        (
            format!("127.0.0.1:{}", 4000 + offset),
            format!("127.0.0.1:{}", 4001 + offset),
            format!("127.0.0.1:{}", 4002 + offset),
            format!("127.0.0.1:{}", 4003 + offset),
        )
    }

    fn choose_random_unused_port_offset(
        port_range: RangeInclusive<u16>,
        max_attempts: usize,
    ) -> u16 {
        let mut rng = rand::rng();

        for _ in 0..max_attempts {
            let http_port = rng.random_range(port_range.clone());
            let offset = http_port - 4000;
            let ports = [4000 + offset, 4001 + offset, 4002 + offset, 4003 + offset];

            let listeners = ports
                .into_iter()
                .map(|port| TcpListener::bind(("127.0.0.1", port)))
                .collect::<Result<Vec<_>, _>>();

            if listeners.is_ok() {
                return offset;
            }
        }

        panic!("failed to find unused standalone test ports");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_export_create_table_with_quoted_names() -> CmdResult<()> {
        let output_dir = tempfile::tempdir().unwrap();
        let (http_addr, rpc_addr, mysql_addr, postgres_addr) = random_standalone_addrs();

        let standalone = standalone::Command::parse_from([
            "standalone",
            "start",
            "--data-home",
            &*output_dir.path().to_string_lossy(),
            "--http-addr",
            &http_addr,
            "--grpc-bind-addr",
            &rpc_addr,
            "--mysql-addr",
            &mysql_addr,
            "--postgres-addr",
            &postgres_addr,
        ]);

        let standalone_opts = standalone.load_options(&GlobalOptions::default()).unwrap();
        let mut instance = standalone.build(standalone_opts).await?;
        instance.start().await?;

        let client = Client::with_urls([rpc_addr.as_str()]);
        let database = Database::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, client);
        database
            .sql(r#"CREATE DATABASE "cli.export.create_table";"#)
            .await
            .unwrap();
        database
            .sql(
                r#"CREATE TABLE "cli.export.create_table"."a.b.c"(
                        ts TIMESTAMP,
                        TIME INDEX (ts)
                    ) engine=mito;
                "#,
            )
            .await
            .unwrap();

        let output_dir = tempfile::tempdir().unwrap();
        let cli = cli::Command::parse_from([
            "cli",
            "data",
            "export",
            "--addr",
            &http_addr,
            "--output-dir",
            &*output_dir.path().to_string_lossy(),
            "--target",
            "schema",
        ]);
        let mut cli_app = cli.build(LoggingOptions::default()).await?;
        cli_app.start().await?;

        instance.stop().await?;

        let output_file = output_dir
            .path()
            .join("greptime")
            .join("cli.export.create_table")
            .join("create_tables.sql");
        let res = std::fs::read_to_string(output_file).unwrap();
        let expect = r#"CREATE TABLE IF NOT EXISTS "a.b.c" (
  "ts" TIMESTAMP(3) NOT NULL,
  TIME INDEX ("ts")
)

ENGINE=mito
;
"#;
        assert_eq!(res.trim(), expect.trim());

        Ok(())
    }
}
