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

use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use clap::Parser;
use sqlness::interceptor::Registry;
use sqlness::{ConfigBuilder, Runner};

use crate::cmd::SqlnessConfig;
use crate::env::kube::{Env, NaiveResourcesManager};
use crate::{protocol_interceptor, util};

#[derive(Debug, Parser)]
/// Run sqlness tests in kube mode.
pub struct KubeCommand {
    #[clap(flatten)]
    config: SqlnessConfig,

    /// Whether to delete the namespace on stop.
    #[clap(long, default_value = "false")]
    delete_namespace_on_stop: bool,

    /// Address of the grpc server.
    #[clap(short, long)]
    server_addr: String,

    /// Address of the postgres server. Must be set if server_addr is set.
    #[clap(short, long)]
    pg_server_addr: String,

    /// Address of the mysql server. Must be set if server_addr is set.
    #[clap(short, long)]
    mysql_server_addr: String,

    /// The namespace of the GreptimeDB.
    #[clap(short, long)]
    namespace: String,
}

impl KubeCommand {
    pub async fn run(self) -> Result<()> {
        let mut interceptor_registry: Registry = Default::default();
        interceptor_registry.register(
            protocol_interceptor::PREFIX,
            Arc::new(protocol_interceptor::ProtocolInterceptorFactory),
        );

        if let Some(d) = &self.config.case_dir {
            ensure!(d.is_dir(), "{} is not a directory", d.display());
        }

        let config = ConfigBuilder::default()
            .case_dir(util::get_case_dir(self.config.case_dir))
            .fail_fast(self.config.fail_fast)
            .test_filter(self.config.test_filter)
            .follow_links(true)
            .env_config_file(self.config.env_config_file)
            .interceptor_registry(interceptor_registry)
            .build()
            .context("Failed to build sqlness config")?;

        let runner = Runner::new(
            config,
            Env {
                delete_namespace_on_stop: self.delete_namespace_on_stop,
                server_addr: self.server_addr,
                pg_server_addr: self.pg_server_addr,
                mysql_server_addr: self.mysql_server_addr,
                database_manager: Arc::new(()),
                resources_manager: Arc::new(NaiveResourcesManager::new(self.namespace)),
            },
        );
        runner.run().await.context("Sqlness tests failed")?;

        println!("\x1b[32mAll sqlness tests passed!\x1b[0m");
        Ok(())
    }
}
