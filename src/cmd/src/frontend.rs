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

use clap::Parser;
use common_base::Plugins;
use frontend::frontend::{Frontend, FrontendOptions};
use frontend::grpc::GrpcOptions;
use frontend::influxdb::InfluxdbOptions;
use frontend::instance::Instance;
use frontend::mysql::MysqlOptions;
use frontend::opentsdb::OpentsdbOptions;
use frontend::postgres::PostgresOptions;
use meta_client::MetaClientOptions;
use servers::auth::UserProviderRef;
use servers::http::HttpOptions;
use servers::tls::{TlsMode, TlsOption};
use servers::{auth, Mode};
use snafu::ResultExt;

use crate::error::{self, IllegalAuthConfigSnafu, Result};
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
pub struct StartCommand {
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
    #[clap(long)]
    metasrv_addr: Option<String>,
    #[clap(long)]
    tls_mode: Option<TlsMode>,
    #[clap(long)]
    tls_cert_path: Option<String>,
    #[clap(long)]
    tls_key_path: Option<String>,
    #[clap(long)]
    user_provider: Option<String>,
}

impl StartCommand {
    async fn run(self) -> Result<()> {
        let plugins = Arc::new(load_frontend_plugins(&self.user_provider)?);
        let opts: FrontendOptions = self.try_into()?;

        let instance = Instance::try_new_distributed(&opts, plugins.clone())
            .await
            .context(error::StartFrontendSnafu)?;

        let mut frontend = Frontend::new(opts, instance, plugins);
        frontend.start().await.context(error::StartFrontendSnafu)
    }
}

pub fn load_frontend_plugins(user_provider: &Option<String>) -> Result<Plugins> {
    let mut plugins = Plugins::new();

    if let Some(provider) = user_provider {
        let provider = auth::user_provider_from_option(provider).context(IllegalAuthConfigSnafu)?;
        plugins.insert::<UserProviderRef>(provider);
    }
    Ok(plugins)
}

impl TryFrom<StartCommand> for FrontendOptions {
    type Error = error::Error;

    fn try_from(cmd: StartCommand) -> Result<Self> {
        let mut opts: FrontendOptions = if let Some(path) = cmd.config_file {
            toml_loader::from_file!(&path)?
        } else {
            FrontendOptions::default()
        };

        let tls_option = TlsOption::new(cmd.tls_mode, cmd.tls_cert_path, cmd.tls_key_path);

        if let Some(addr) = cmd.http_addr {
            opts.http_options = Some(HttpOptions {
                addr,
                ..Default::default()
            });
        }
        if let Some(addr) = cmd.grpc_addr {
            opts.grpc_options = Some(GrpcOptions {
                addr,
                ..Default::default()
            });
        }
        if let Some(addr) = cmd.mysql_addr {
            opts.mysql_options = Some(MysqlOptions {
                addr,
                tls: tls_option.clone(),
                ..Default::default()
            });
        }
        if let Some(addr) = cmd.postgres_addr {
            opts.postgres_options = Some(PostgresOptions {
                addr,
                tls: tls_option,
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
        if let Some(metasrv_addr) = cmd.metasrv_addr {
            opts.meta_client_options
                .get_or_insert_with(MetaClientOptions::default)
                .metasrv_addrs = metasrv_addr
                .split(',')
                .map(&str::trim)
                .map(&str::to_string)
                .collect::<Vec<_>>();
            opts.mode = Mode::Distributed;
        }
        Ok(opts)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::time::Duration;

    use common_test_util::temp_dir::create_named_temp_file;
    use servers::auth::{Identity, Password, UserProviderRef};

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
            metasrv_addr: None,
            tls_mode: None,
            tls_cert_path: None,
            tls_key_path: None,
            user_provider: None,
        };

        let opts: FrontendOptions = command.try_into().unwrap();
        assert_eq!(opts.http_options.as_ref().unwrap().addr, "127.0.0.1:1234");
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
        assert_eq!(
            opts.grpc_options.unwrap().addr,
            default_opts.grpc_options.unwrap().addr
        );
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

    #[test]
    fn test_read_from_config_file() {
        let mut file = create_named_temp_file();
        let toml_str = r#"
            mode = "distributed"

            [http_options]
            addr = "127.0.0.1:4000"
            timeout = "30s"
        "#;
        write!(file, "{}", toml_str).unwrap();

        let command = StartCommand {
            http_addr: None,
            grpc_addr: None,
            mysql_addr: None,
            postgres_addr: None,
            opentsdb_addr: None,
            influxdb_enable: None,
            config_file: Some(file.path().to_str().unwrap().to_string()),
            metasrv_addr: None,
            tls_mode: None,
            tls_cert_path: None,
            tls_key_path: None,
            user_provider: None,
        };

        let fe_opts = FrontendOptions::try_from(command).unwrap();
        assert_eq!(Mode::Distributed, fe_opts.mode);
        assert_eq!(
            "127.0.0.1:4000".to_string(),
            fe_opts.http_options.as_ref().unwrap().addr
        );
        assert_eq!(
            Duration::from_secs(30),
            fe_opts.http_options.as_ref().unwrap().timeout
        );
    }

    #[tokio::test]
    async fn test_try_from_start_command_to_anymap() {
        let command = StartCommand {
            http_addr: None,
            grpc_addr: None,
            mysql_addr: None,
            postgres_addr: None,
            opentsdb_addr: None,
            influxdb_enable: None,
            config_file: None,
            metasrv_addr: None,
            tls_mode: None,
            tls_cert_path: None,
            tls_key_path: None,
            user_provider: Some("static_user_provider:cmd:test=test".to_string()),
        };

        let plugins = load_frontend_plugins(&command.user_provider);
        assert!(plugins.is_ok());
        let plugins = plugins.unwrap();
        let provider = plugins.get::<UserProviderRef>();
        assert!(provider.is_some());

        let provider = provider.unwrap();
        let result = provider
            .authenticate(Identity::UserId("test", None), Password::PlainText("test"))
            .await;
        assert!(result.is_ok());
    }
}
