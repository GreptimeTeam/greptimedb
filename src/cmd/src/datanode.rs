use clap::Parser;
use common_telemetry::logging;
use datanode::datanode::{Datanode, DatanodeOptions};
use snafu::ResultExt;

use crate::error::{Error, Result, StartDatanodeSnafu};
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
    http_addr: Option<String>,
    #[clap(long)]
    rpc_addr: Option<String>,
    #[clap(long)]
    mysql_addr: Option<String>,
    #[clap(short, long)]
    config_file: Option<String>,
}

impl StartCommand {
    async fn run(self) -> Result<()> {
        logging::info!("Datanode start command: {:#?}", self);

        let opts: DatanodeOptions = self.try_into()?;

        logging::info!("Datanode options: {:#?}", opts);

        Datanode::new(opts)
            .await
            .context(StartDatanodeSnafu)?
            .start()
            .await
            .context(StartDatanodeSnafu)
    }
}

impl TryFrom<StartCommand> for DatanodeOptions {
    type Error = Error;
    fn try_from(cmd: StartCommand) -> Result<Self> {
        let mut opts: DatanodeOptions = if let Some(path) = cmd.config_file {
            toml_loader::from_file!(&path)?
        } else {
            DatanodeOptions::default()
        };

        if let Some(addr) = cmd.http_addr {
            opts.http_addr = addr;
        }
        if let Some(addr) = cmd.rpc_addr {
            opts.rpc_addr = addr;
        }
        if let Some(addr) = cmd.mysql_addr {
            opts.mysql_addr = addr;
        }

        Ok(opts)
    }
}

#[cfg(test)]
mod tests {
    use datanode::datanode::ObjectStoreConfig;

    use super::*;

    #[test]
    fn test_read_from_config_file() {
        let cmd = StartCommand {
            http_addr: None,
            rpc_addr: None,
            mysql_addr: None,
            config_file: Some(format!(
                "{}/../../config/datanode.example.toml",
                std::env::current_dir().unwrap().as_path().to_str().unwrap()
            )),
        };
        let options: DatanodeOptions = cmd.try_into().unwrap();
        assert_eq!("0.0.0.0:3000".to_string(), options.http_addr);
        assert_eq!("0.0.0.0:3001".to_string(), options.rpc_addr);
        assert_eq!("/tmp/wal".to_string(), options.wal_dir);
        assert_eq!("0.0.0.0:3306".to_string(), options.mysql_addr);
        assert_eq!(4, options.mysql_runtime_size);
        match options.storage {
            ObjectStoreConfig::File { data_dir } => {
                assert_eq!("/tmp/greptimedb/data/".to_string(), data_dir)
            }
        };
    }
}
