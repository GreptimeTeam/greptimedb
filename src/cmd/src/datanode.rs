use clap::Parser;
use common_telemetry::logging;
use datanode::datanode::{Datanode, DatanodeOptions};
use frontend::frontend::Mode;
use snafu::ResultExt;

use crate::error::{Error, MissingConfigSnafu, Result, StartDatanodeSnafu};
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
    node_id: Option<u64>,
    #[clap(long)]
    rpc_addr: Option<String>,
    #[clap(long)]
    mysql_addr: Option<String>,
    #[clap(long)]
    metasrv_addr: Option<String>,
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

        if let Some(addr) = cmd.rpc_addr {
            opts.rpc_addr = addr;
        }
        if let Some(addr) = cmd.mysql_addr {
            opts.mysql_addr = addr;
        }

        match (cmd.metasrv_addr, cmd.node_id) {
            (Some(meta_addr), Some(node_id)) => {
                // Running mode is only set to Distributed when
                // both metasrv addr and node id are set in
                // commandline options
                opts.meta_client_opts.metasrv_addr = meta_addr;
                opts.node_id = node_id;
                opts.mode = Mode::Distributed;
            }
            (None, None) => {
                opts.mode = Mode::Standalone;
            }
            (None, Some(_)) => {
                return MissingConfigSnafu {
                    msg: "Missing metasrv address option",
                }
                .fail();
            }
            (Some(_), None) => {
                return MissingConfigSnafu {
                    msg: "Missing node id option",
                }
                .fail();
            }
        }
        Ok(opts)
    }
}

#[cfg(test)]
mod tests {
    use datanode::datanode::ObjectStoreConfig;
    use frontend::frontend::Mode;

    use super::*;

    #[test]
    fn test_read_from_config_file() {
        let cmd = StartCommand {
            node_id: None,
            rpc_addr: None,
            mysql_addr: None,
            metasrv_addr: None,
            config_file: Some(format!(
                "{}/../../config/datanode.example.toml",
                std::env::current_dir().unwrap().as_path().to_str().unwrap()
            )),
        };
        let options: DatanodeOptions = cmd.try_into().unwrap();
        assert_eq!("0.0.0.0:3001".to_string(), options.rpc_addr);
        assert_eq!("/tmp/greptimedb/wal".to_string(), options.wal_dir);
        assert_eq!("0.0.0.0:3306".to_string(), options.mysql_addr);
        assert_eq!(4, options.mysql_runtime_size);
        assert_eq!(
            "1.1.1.1:3002".to_string(),
            options.meta_client_opts.metasrv_addr
        );
        assert_eq!(5000, options.meta_client_opts.connect_timeout_millis);
        assert_eq!(3000, options.meta_client_opts.timeout_millis);
        assert!(options.meta_client_opts.tcp_nodelay);

        match options.storage {
            ObjectStoreConfig::File { data_dir } => {
                assert_eq!("/tmp/greptimedb/data/".to_string(), data_dir)
            }
        };
    }

    #[test]
    fn test_try_from_cmd() {
        assert_eq!(
            Mode::Standalone,
            DatanodeOptions::try_from(StartCommand {
                node_id: None,
                rpc_addr: None,
                mysql_addr: None,
                metasrv_addr: None,
                config_file: None
            })
            .unwrap()
            .mode
        );

        assert_eq!(
            Mode::Distributed,
            DatanodeOptions::try_from(StartCommand {
                node_id: Some(42),
                rpc_addr: None,
                mysql_addr: None,
                metasrv_addr: Some("127.0.0.1:3002".to_string()),
                config_file: None
            })
            .unwrap()
            .mode
        );

        assert!(DatanodeOptions::try_from(StartCommand {
            node_id: None,
            rpc_addr: None,
            mysql_addr: None,
            metasrv_addr: Some("127.0.0.1:3002".to_string()),
            config_file: None,
        })
        .is_err());
        assert!(DatanodeOptions::try_from(StartCommand {
            node_id: Some(42),
            rpc_addr: None,
            mysql_addr: None,
            metasrv_addr: None,
            config_file: None,
        })
        .is_err());
    }
}
