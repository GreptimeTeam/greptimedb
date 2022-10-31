use clap::Parser;
use common_telemetry::warn;
use frontend::error as frontend_error;
use frontend::frontend::{Frontend, FrontendOptions};
use frontend::influxdb::InfluxdbOptions;
use frontend::mysql::MysqlOptions;
use frontend::opentsdb::OpentsdbOptions;
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
    #[clap(long)]
    postgres_addr: Option<String>,
    #[clap(long)]
    opentsdb_addr: Option<String>,
    #[clap(short, long)]
    config_file: Option<String>,
    #[clap(short, long)]
    influxdb_enable: Option<bool>,
}

impl StartCommand {
    async fn run(self) -> Result<()> {
        let opts: FrontendOptions = self.try_into()?;

        // TODO(zyy17): Put retry args in options.
        let max_retry_times = 10;
        let retry_interval = std::time::Duration::from_secs(5);

        let mut retry_times = 0;

        let handle = tokio::spawn(async move {
            loop {
                let mut frontend = Frontend::new(opts.clone());
                match frontend.start().await {
                    Ok(_) => return Ok(()),
                    Err(err) => match &err {
                        frontend_error::Error::ConnectDatanode { addr, .. } => {
                            retry_times += 1;
                            if retry_times > max_retry_times {
                                return Err(err).context(error::StartFrontendSnafu);
                            }
                            warn!(
                                "connect datanode {} failed,  retry {} times",
                                addr, retry_times
                            );
                            tokio::time::sleep(retry_interval).await;
                        }
                        _ => return Err(err).context(error::StartFrontendSnafu),
                    },
                }
            }
        });

        handle.await.unwrap()
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
            opts.mysql_options = Some(MysqlOptions {
                addr,
                ..Default::default()
            });
        }
        if let Some(addr) = cmd.postgres_addr {
            opts.postgres_options = Some(PostgresOptions {
                addr,
                ..Default::default()
            });
        }
        if let Some(addr) = cmd.opentsdb_addr {
            opts.opentsdb_options = Some(OpentsdbOptions {
                addr,
                ..Default::default()
            });
        }
        if let Some(enable) = cmd.influxdb_enable {
            opts.influxdb_options = Some(InfluxdbOptions { enable });
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
            postgres_addr: Some("127.0.0.1:5432".to_string()),
            opentsdb_addr: Some("127.0.0.1:4321".to_string()),
            influxdb_enable: Some(false),
            config_file: None,
        };

        let opts: FrontendOptions = command.try_into().unwrap();
        assert_eq!(opts.http_addr, Some("127.0.0.1:1234".to_string()));
        assert_eq!(opts.mysql_options.as_ref().unwrap().addr, "127.0.0.1:5678");
        assert_eq!(
            opts.postgres_options.as_ref().unwrap().addr,
            "127.0.0.1:5432"
        );
        assert_eq!(
            opts.opentsdb_options.as_ref().unwrap().addr,
            "127.0.0.1:4321"
        );

        let default_opts = FrontendOptions::default();
        assert_eq!(opts.grpc_addr, default_opts.grpc_addr);
        assert_eq!(
            opts.mysql_options.as_ref().unwrap().runtime_size,
            default_opts.mysql_options.as_ref().unwrap().runtime_size
        );
        assert_eq!(
            opts.postgres_options.as_ref().unwrap().runtime_size,
            default_opts.postgres_options.as_ref().unwrap().runtime_size
        );
        assert_eq!(
            opts.opentsdb_options.as_ref().unwrap().runtime_size,
            default_opts.opentsdb_options.as_ref().unwrap().runtime_size
        );

        assert!(!opts.influxdb_options.unwrap().enable);
    }
}
