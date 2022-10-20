use clap::Parser;
use common_telemetry::logging;
use meta_srv::bootstrap;
use meta_srv::metasrv::MetaSrvOptions;
use snafu::ResultExt;

use crate::error;
use crate::error::Error;
use crate::error::Result;
use crate::toml_loader;

#[derive(Parser)]
pub struct Command {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

impl Command {
    pub async fn run(self) -> Result<()> {
        self.subcmd.run().await
    }
}

#[derive(Parser)]
enum SubCommand {
    Start(StartCommand),
}

impl SubCommand {
    async fn run(self) -> Result<()> {
        match self {
            SubCommand::Start(cmd) => cmd.run().await,
        }
    }
}

#[derive(Debug, Parser)]
struct StartCommand {
    #[clap(long)]
    server_addr: Option<String>,
    #[clap(long)]
    store_addr: Option<String>,
    #[clap(short, long)]
    config_file: Option<String>,
}

impl StartCommand {
    async fn run(self) -> Result<()> {
        logging::info!("MetaSrv start command: {:#?}", self);

        let opts: MetaSrvOptions = self.try_into()?;

        logging::info!("MetaSrv options: {:#?}", opts);

        bootstrap::bootstrap_meta_srv(opts)
            .await
            .context(error::StartMetaServerSnafu)
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

        if let Some(addr) = cmd.server_addr {
            opts.server_addr = addr;
        }
        if let Some(addr) = cmd.store_addr {
            opts.store_addr = addr;
        }

        Ok(opts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_from_cmd() {
        let cmd = StartCommand {
            server_addr: Some("0.0.0.0:3002".to_string()),
            store_addr: Some("127.0.0.1:2380".to_string()),
            config_file: None,
        };
        let options: MetaSrvOptions = cmd.try_into().unwrap();
        assert_eq!("0.0.0.0:3002".to_string(), options.server_addr);
        assert_eq!("127.0.0.1:2380".to_string(), options.store_addr);
    }

    #[test]
    fn test_read_from_config_file() {
        let cmd = StartCommand {
            server_addr: None,
            store_addr: None,
            config_file: Some(format!(
                "{}/../../config/metasrv.example.toml",
                std::env::current_dir().unwrap().as_path().to_str().unwrap()
            )),
        };
        let options: MetaSrvOptions = cmd.try_into().unwrap();
        assert_eq!("0.0.0.0:3002".to_string(), options.server_addr);
        assert_eq!("127.0.0.1:2380".to_string(), options.store_addr);
    }
}
