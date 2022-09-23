use clap::Parser;
use frontend::frontend::{Frontend, FrontendOptions};
#[cfg(feature = "postgres")]
use frontend::postgres::PostgresOptions;
use snafu::ResultExt;

use crate::error::{self, Result};
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
    grpc_addr: Option<String>,
    #[clap(long)]
    mysql_addr: Option<String>,
    #[cfg(feature = "postgres")]
    #[clap(long)]
    postgres_addr: Option<String>,
    #[clap(short, long)]
    config_file: Option<String>,
}

impl StartCommand {
    async fn run(self) -> Result<()> {
        let opts = self.try_into()?;
        let mut frontend = Frontend::new(opts);
        frontend.start().await.context(error::StartFrontendSnafu)
    }
}

impl TryFrom<StartCommand> for FrontendOptions {
    type Error = error::Error;

    fn try_from(cmd: StartCommand) -> Result<Self> {
        let mut opts: FrontendOptions = if let Some(path) = cmd.config_file {
            toml_loader::from_file!(&path)?
        } else {
            FrontendOptions::default()
        };

        if let Some(addr) = cmd.http_addr {
            opts.http_addr = Some(addr);
        }
        if let Some(addr) = cmd.grpc_addr {
            opts.grpc_addr = Some(addr);
        }
        if let Some(addr) = cmd.mysql_addr {
            opts.mysql_addr = Some(addr);
        }
        #[cfg(feature = "postgres")]
        if let Some(addr) = cmd.postgres_addr {
            opts.postgres_options = Some(PostgresOptions {
                addr,
                ..Default::default()
            });
        }
        Ok(opts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_try_from_start_command() {
        let command = StartCommand {
            http_addr: Some("127.0.0.1:1234".to_string()),
            grpc_addr: None,
            mysql_addr: Some("127.0.0.1:5678".to_string()),
            #[cfg(feature = "postgres")]
            postgres_addr: Some("127.0.0.1:5432".to_string()),
            config_file: None,
        };

        let opts: FrontendOptions = command.try_into().unwrap();
        assert_eq!(opts.http_addr, Some("127.0.0.1:1234".to_string()));
        assert_eq!(opts.mysql_addr, Some("127.0.0.1:5678".to_string()));
        #[cfg(feature = "postgres")]
        assert_eq!(
            opts.postgres_options.as_ref().unwrap().addr,
            "127.0.0.1:5432"
        );

        let default_opts = FrontendOptions::default();
        assert_eq!(opts.grpc_addr, default_opts.grpc_addr);
        assert_eq!(opts.mysql_runtime_size, default_opts.mysql_runtime_size);
        #[cfg(feature = "postgres")]
        assert_eq!(
            opts.postgres_options.as_ref().unwrap().runtime_size,
            default_opts.postgres_options.as_ref().unwrap().runtime_size
        );
    }
}
