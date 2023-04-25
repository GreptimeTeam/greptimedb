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

use crate::error::Result;
use crate::options::ConfigOptions;
use crate::{error, toml_loader};

pub struct Instance {
    instance: MetaSrvInstance,
}

impl Instance {
    pub async fn run(&mut self) -> Result<()> {
        self.instance
            .start()
            .await
            .context(error::StartMetaServerSnafu)
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

    pub fn load_options(
        &self,
        log_dir: Option<String>,
        log_level: Option<String>,
    ) -> Result<ConfigOptions> {
        self.subcmd.load_options(log_dir, log_level)
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

    fn load_options(
        &self,
        log_dir: Option<String>,
        log_level: Option<String>,
    ) -> Result<ConfigOptions> {
        match self {
            SubCommand::Start(cmd) => cmd.load_options(log_dir, log_level),
        }
    }
}

#[derive(Debug, Parser)]
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
    use_memory_store: bool,
    #[clap(long)]
    http_addr: Option<String>,
    #[clap(long)]
    http_timeout: Option<u64>,
}

impl StartCommand {
    fn load_options(
        &self,
        log_dir: Option<String>,
        log_level: Option<String>,
    ) -> Result<ConfigOptions> {
        let mut opts: MetaSrvOptions = if let Some(path) = self.config_file.clone() {
            toml_loader::from_file!(&path)?
        } else {
            MetaSrvOptions::default()
        };

        if let Some(dir) = log_dir {
            opts.logging.dir = dir;
        }
        if let Some(level) = log_level {
            opts.logging.level = level;
        }

        if let Some(addr) = self.bind_addr.clone() {
            opts.bind_addr = addr;
        }
        if let Some(addr) = self.server_addr.clone() {
            opts.server_addr = addr.clone();
        }
        if let Some(addr) = self.store_addr.clone() {
            opts.store_addr = addr.clone();
        }
        if let Some(selector_type) = &self.selector {
            opts.selector = selector_type[..]
                .try_into()
                .context(error::UnsupportedSelectorTypeSnafu { selector_type })?;
            // info!("Using {} selector", selector_type);
        }

        if self.use_memory_store {
            // warn!("Using memory store for Meta. Make sure you are in running tests.");
            opts.use_memory_store = true;
        }

        if let Some(http_addr) = self.http_addr.clone() {
            opts.http_opts.addr = http_addr;
        }
        if let Some(http_timeout) = self.http_timeout {
            opts.http_opts.timeout = Duration::from_secs(http_timeout);
        }

        // Disable dashboard in metasrv.
        opts.http_opts.disable_dashboard = true;

        Ok(ConfigOptions::Metasrv(opts))
    }

    async fn build(self, opts: MetaSrvOptions) -> Result<Instance> {
        logging::info!("MetaSrv start command: {:#?}", self);

        logging::info!("MetaSrv options: {:#?}", opts);

        let instance = MetaSrvInstance::new(opts)
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

    #[test]
    fn test_read_from_cmd() {
        let cmd = StartCommand {
            bind_addr: Some("127.0.0.1:3002".to_string()),
            server_addr: Some("127.0.0.1:3002".to_string()),
            store_addr: Some("127.0.0.1:2380".to_string()),
            config_file: None,
            selector: Some("LoadBased".to_string()),
            use_memory_store: false,
            http_addr: None,
            http_timeout: None,
        };

        if let ConfigOptions::Metasrv(options) = cmd.load_options(None, None).unwrap() {
            assert_eq!("127.0.0.1:3002".to_string(), options.bind_addr);
            assert_eq!("127.0.0.1:3002".to_string(), options.server_addr);
            assert_eq!("127.0.0.1:2380".to_string(), options.store_addr);
            assert_eq!(SelectorType::LoadBased, options.selector);
        } else {
            unreachable!()
        }
    }

    #[test]
    fn test_read_from_config_file() {
        let mut file = create_named_temp_file();
        let toml_str = r#"
            bind_addr = "127.0.0.1:3002"
            server_addr = "127.0.0.1:3002"
            store_addr = "127.0.0.1:2379"
            datanode_lease_secs = 15
            selector = "LeaseBased"
            use_memory_store = false

            [logging]
            level = "debug"
            dir = "/tmp/greptimedb/test/logs"
        "#;
        write!(file, "{}", toml_str).unwrap();

        let cmd = StartCommand {
            bind_addr: None,
            server_addr: None,
            store_addr: None,
            selector: None,
            config_file: Some(file.path().to_str().unwrap().to_string()),
            use_memory_store: false,
            http_addr: None,
            http_timeout: None,
        };

        if let ConfigOptions::Metasrv(options) = cmd.load_options(None, None).unwrap() {
            assert_eq!("127.0.0.1:3002".to_string(), options.bind_addr);
            assert_eq!("127.0.0.1:3002".to_string(), options.server_addr);
            assert_eq!("127.0.0.1:2379".to_string(), options.store_addr);
            assert_eq!(15, options.datanode_lease_secs);
            assert_eq!(SelectorType::LeaseBased, options.selector);
            assert_eq!("debug".to_string(), options.logging.level);
            assert_eq!("/tmp/greptimedb/test/logs".to_string(), options.logging.dir);
        }
    }
}
