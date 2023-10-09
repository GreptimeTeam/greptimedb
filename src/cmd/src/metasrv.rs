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

use std::time::Duration;

use clap::Parser;
use common_telemetry::logging;
use meta_srv::bootstrap::MetaSrvInstance;
use meta_srv::metasrv::MetaSrvOptions;
use snafu::ResultExt;

use crate::error::{self, Result, StartMetaServerSnafu};
use crate::options::{Options, TopLevelOptions};

pub struct Instance {
    instance: MetaSrvInstance,
}

impl Instance {
    pub async fn start(&mut self) -> Result<()> {
        plugins::start_meta_srv_plugins(self.instance.plugins())
            .await
            .context(StartMetaServerSnafu)?;
        self.instance.start().await.context(StartMetaServerSnafu)
    }

    pub async fn stop(&self) -> Result<()> {
        self.instance
            .shutdown()
            .await
            .context(error::ShutdownMetaServerSnafu)
    }
}

#[derive(Parser)]
pub struct Command {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

impl Command {
    pub async fn build(self, opts: MetaSrvOptions) -> Result<Instance> {
        self.subcmd.build(opts).await
    }

    pub fn load_options(&self, top_level_opts: TopLevelOptions) -> Result<Options> {
        self.subcmd.load_options(top_level_opts)
    }
}

#[derive(Parser)]
enum SubCommand {
    Start(StartCommand),
}

impl SubCommand {
    async fn build(self, opts: MetaSrvOptions) -> Result<Instance> {
        match self {
            SubCommand::Start(cmd) => cmd.build(opts).await,
        }
    }

    fn load_options(&self, top_level_opts: TopLevelOptions) -> Result<Options> {
        match self {
            SubCommand::Start(cmd) => cmd.load_options(top_level_opts),
        }
    }
}

#[derive(Debug, Default, Parser)]
struct StartCommand {
    #[clap(long)]
    bind_addr: Option<String>,
    #[clap(long)]
    server_addr: Option<String>,
    #[clap(long)]
    store_addr: Option<String>,
    #[clap(short, long)]
    config_file: Option<String>,
    #[clap(short, long)]
    selector: Option<String>,
    #[clap(long)]
    use_memory_store: Option<bool>,
    #[clap(long)]
    enable_region_failover: Option<bool>,
    #[clap(long)]
    http_addr: Option<String>,
    #[clap(long)]
    http_timeout: Option<u64>,
    #[clap(long, default_value = "GREPTIMEDB_METASRV")]
    env_prefix: String,
}

impl StartCommand {
    fn load_options(&self, top_level_opts: TopLevelOptions) -> Result<Options> {
        let mut opts: MetaSrvOptions = Options::load_layered_options(
            self.config_file.as_deref(),
            self.env_prefix.as_ref(),
            None,
        )?;

        if let Some(dir) = top_level_opts.log_dir {
            opts.logging.dir = dir;
        }

        if top_level_opts.log_level.is_some() {
            opts.logging.level = top_level_opts.log_level;
        }

        if let Some(addr) = &self.bind_addr {
            opts.bind_addr = addr.clone();
        }

        if let Some(addr) = &self.server_addr {
            opts.server_addr = addr.clone();
        }

        if let Some(addr) = &self.store_addr {
            opts.store_addr = addr.clone();
        }

        if let Some(selector_type) = &self.selector {
            opts.selector = selector_type[..]
                .try_into()
                .context(error::UnsupportedSelectorTypeSnafu { selector_type })?;
        }

        if let Some(use_memory_store) = self.use_memory_store {
            opts.use_memory_store = use_memory_store;
        }

        if let Some(enable_region_failover) = self.enable_region_failover {
            opts.enable_region_failover = enable_region_failover;
        }

        if let Some(http_addr) = &self.http_addr {
            opts.http.addr = http_addr.clone();
        }

        if let Some(http_timeout) = self.http_timeout {
            opts.http.timeout = Duration::from_secs(http_timeout);
        }

        // Disable dashboard in metasrv.
        opts.http.disable_dashboard = true;

        Ok(Options::Metasrv(Box::new(opts)))
    }

    async fn build(self, mut opts: MetaSrvOptions) -> Result<Instance> {
        let plugins = plugins::setup_meta_srv_plugins(&mut opts)
            .await
            .context(StartMetaServerSnafu)?;

        logging::info!("MetaSrv start command: {:#?}", self);
        logging::info!("MetaSrv options: {:#?}", opts);

        let instance = MetaSrvInstance::new(opts, plugins)
            .await
            .context(error::BuildMetaServerSnafu)?;

        Ok(Instance { instance })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use common_test_util::temp_dir::create_named_temp_file;
    use meta_srv::selector::SelectorType;

    use super::*;
    use crate::options::ENV_VAR_SEP;

    #[test]
    fn test_read_from_cmd() {
        let cmd = StartCommand {
            bind_addr: Some("127.0.0.1:3002".to_string()),
            server_addr: Some("127.0.0.1:3002".to_string()),
            store_addr: Some("127.0.0.1:2380".to_string()),
            selector: Some("LoadBased".to_string()),
            ..Default::default()
        };

        let Options::Metasrv(options) = cmd.load_options(TopLevelOptions::default()).unwrap()
        else {
            unreachable!()
        };
        assert_eq!("127.0.0.1:3002".to_string(), options.bind_addr);
        assert_eq!("127.0.0.1:2380".to_string(), options.store_addr);
        assert_eq!(SelectorType::LoadBased, options.selector);
    }

    #[test]
    fn test_read_from_config_file() {
        let mut file = create_named_temp_file();
        let toml_str = r#"
            bind_addr = "127.0.0.1:3002"
            server_addr = "127.0.0.1:3002"
            store_addr = "127.0.0.1:2379"
            selector = "LeaseBased"
            use_memory_store = false

            [logging]
            level = "debug"
            dir = "/tmp/greptimedb/test/logs"
        "#;
        write!(file, "{}", toml_str).unwrap();

        let cmd = StartCommand {
            config_file: Some(file.path().to_str().unwrap().to_string()),
            ..Default::default()
        };

        let Options::Metasrv(options) = cmd.load_options(TopLevelOptions::default()).unwrap()
        else {
            unreachable!()
        };
        assert_eq!("127.0.0.1:3002".to_string(), options.bind_addr);
        assert_eq!("127.0.0.1:3002".to_string(), options.server_addr);
        assert_eq!("127.0.0.1:2379".to_string(), options.store_addr);
        assert_eq!(SelectorType::LeaseBased, options.selector);
        assert_eq!("debug", options.logging.level.as_ref().unwrap());
        assert_eq!("/tmp/greptimedb/test/logs".to_string(), options.logging.dir);
    }

    #[test]
    fn test_top_level_options() {
        let cmd = StartCommand {
            bind_addr: Some("127.0.0.1:3002".to_string()),
            server_addr: Some("127.0.0.1:3002".to_string()),
            store_addr: Some("127.0.0.1:2380".to_string()),
            selector: Some("LoadBased".to_string()),
            ..Default::default()
        };

        let options = cmd
            .load_options(TopLevelOptions {
                log_dir: Some("/tmp/greptimedb/test/logs".to_string()),
                log_level: Some("debug".to_string()),
            })
            .unwrap();

        let logging_opt = options.logging_options();
        assert_eq!("/tmp/greptimedb/test/logs", logging_opt.dir);
        assert_eq!("debug", logging_opt.level.as_ref().unwrap());
    }

    #[test]
    fn test_config_precedence_order() {
        let mut file = create_named_temp_file();
        let toml_str = r#"
            server_addr = "127.0.0.1:3002"
            datanode_lease_secs = 15
            selector = "LeaseBased"
            use_memory_store = false

            [http]
            addr = "127.0.0.1:4000"

            [logging]
            level = "debug"
            dir = "/tmp/greptimedb/test/logs"
        "#;
        write!(file, "{}", toml_str).unwrap();

        let env_prefix = "METASRV_UT";
        temp_env::with_vars(
            [
                (
                    // bind_addr = 127.0.0.1:14002
                    [env_prefix.to_string(), "bind_addr".to_uppercase()].join(ENV_VAR_SEP),
                    Some("127.0.0.1:14002"),
                ),
                (
                    // server_addr = 127.0.0.1:13002
                    [env_prefix.to_string(), "server_addr".to_uppercase()].join(ENV_VAR_SEP),
                    Some("127.0.0.1:13002"),
                ),
                (
                    // http.addr = 127.0.0.1:24000
                    [
                        env_prefix.to_string(),
                        "http".to_uppercase(),
                        "addr".to_uppercase(),
                    ]
                    .join(ENV_VAR_SEP),
                    Some("127.0.0.1:24000"),
                ),
            ],
            || {
                let command = StartCommand {
                    http_addr: Some("127.0.0.1:14000".to_string()),
                    config_file: Some(file.path().to_str().unwrap().to_string()),
                    env_prefix: env_prefix.to_string(),
                    ..Default::default()
                };

                let Options::Metasrv(opts) =
                    command.load_options(TopLevelOptions::default()).unwrap()
                else {
                    unreachable!()
                };

                // Should be read from env, env > default values.
                assert_eq!(opts.bind_addr, "127.0.0.1:14002");

                // Should be read from config file, config file > env > default values.
                assert_eq!(opts.server_addr, "127.0.0.1:3002");

                // Should be read from cli, cli > config file > env > default values.
                assert_eq!(opts.http.addr, "127.0.0.1:14000");

                // Should be default value.
                assert_eq!(opts.store_addr, "127.0.0.1:2379");
            },
        );
    }
}
