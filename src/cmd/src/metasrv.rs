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
use common_telemetry::{info, logging, warn};
use meta_srv::bootstrap::MetaSrvInstance;
use meta_srv::metasrv::MetaSrvOptions;
use snafu::ResultExt;

use crate::error::{Error, Result};
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
    pub async fn build(self) -> Result<Instance> {
        self.subcmd.build().await
    }
}

#[derive(Parser)]
enum SubCommand {
    Start(StartCommand),
}

impl SubCommand {
    async fn build(self) -> Result<Instance> {
        match self {
            SubCommand::Start(cmd) => cmd.build().await,
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
    metrics_addr: Option<String>,
}

impl StartCommand {
    async fn build(self) -> Result<Instance> {
        logging::info!("MetaSrv start command: {:#?}", self);

        let opts: MetaSrvOptions = self.try_into()?;

        logging::info!("MetaSrv options: {:#?}", opts);
        let instance = MetaSrvInstance::new(opts)
            .await
            .context(error::BuildMetaServerSnafu)?;

        Ok(Instance { instance })
    }
}

impl TryFrom<StartCommand> for MetaSrvOptions {
    type Error = Error;

    fn try_from(cmd: StartCommand) -> Result<Self> {
        let mut opts: MetaSrvOptions = if let Some(path) = cmd.config_file {
            toml_loader::from_file!(&path)?
        } else {
            MetaSrvOptions::default()
        };

        if let Some(addr) = cmd.bind_addr {
            opts.bind_addr = addr;
        }
        if let Some(addr) = cmd.server_addr {
            opts.server_addr = addr;
        }
        if let Some(addr) = cmd.store_addr {
            opts.store_addr = addr;
        }
        if let Some(selector_type) = &cmd.selector {
            opts.selector = selector_type[..]
                .try_into()
                .context(error::UnsupportedSelectorTypeSnafu { selector_type })?;
            info!("Using {} selector", selector_type);
        }

        if cmd.use_memory_store {
            warn!("Using memory store for Meta. Make sure you are in running tests.");
            opts.use_memory_store = true;
        }

        if let Some(metrics_addr) = cmd.metrics_addr {
            opts.metrics_addr = metrics_addr;
        }

        Ok(opts)
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
            metrics_addr: None,
        };
        let options: MetaSrvOptions = cmd.try_into().unwrap();
        assert_eq!("127.0.0.1:3002".to_string(), options.bind_addr);
        assert_eq!("127.0.0.1:3002".to_string(), options.server_addr);
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
            datanode_lease_secs = 15
            selector = "LeaseBased"
            use_memory_store = false
        "#;
        write!(file, "{}", toml_str).unwrap();

        let cmd = StartCommand {
            bind_addr: None,
            server_addr: None,
            store_addr: None,
            selector: None,
            config_file: Some(file.path().to_str().unwrap().to_string()),
            use_memory_store: false,
            metrics_addr: None,
        };
        let options: MetaSrvOptions = cmd.try_into().unwrap();
        assert_eq!("127.0.0.1:3002".to_string(), options.bind_addr);
        assert_eq!("127.0.0.1:3002".to_string(), options.server_addr);
        assert_eq!("127.0.0.1:2379".to_string(), options.store_addr);
        assert_eq!(15, options.datanode_lease_secs);
        assert_eq!(SelectorType::LeaseBased, options.selector);
    }
}
