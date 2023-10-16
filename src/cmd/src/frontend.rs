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
use frontend::frontend::FrontendOptions;
use frontend::instance::{FrontendInstance, Instance as FeInstance};
use meta_client::MetaClientOptions;
use servers::tls::{TlsMode, TlsOption};
use servers::Mode;
use snafu::ResultExt;

use crate::error::{self, Result, StartFrontendSnafu};
use crate::options::{Options, TopLevelOptions};

pub struct Instance {
    frontend: FeInstance,
}

impl Instance {
    pub async fn start(&mut self) -> Result<()> {
        plugins::start_frontend_plugins(self.frontend.plugins().clone())
            .await
            .context(StartFrontendSnafu)?;

        self.frontend.start().await.context(StartFrontendSnafu)
    }

    pub async fn stop(&self) -> Result<()> {
        self.frontend
            .shutdown()
            .await
            .context(error::ShutdownFrontendSnafu)
    }
}

#[derive(Parser)]
pub struct Command {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

impl Command {
    pub async fn build(self, opts: FrontendOptions) -> Result<Instance> {
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
    async fn build(self, opts: FrontendOptions) -> Result<Instance> {
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
pub struct StartCommand {
    #[clap(long)]
    http_addr: Option<String>,
    #[clap(long)]
    http_timeout: Option<u64>,
    #[clap(long)]
    rpc_addr: Option<String>,
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
    #[clap(long, multiple = true, value_delimiter = ',')]
    metasrv_addr: Option<Vec<String>>,
    #[clap(long)]
    tls_mode: Option<TlsMode>,
    #[clap(long)]
    tls_cert_path: Option<String>,
    #[clap(long)]
    tls_key_path: Option<String>,
    #[clap(long)]
    user_provider: Option<String>,
    #[clap(long)]
    disable_dashboard: Option<bool>,
    #[clap(long, default_value = "GREPTIMEDB_FRONTEND")]
    env_prefix: String,
}

impl StartCommand {
    fn load_options(&self, top_level_opts: TopLevelOptions) -> Result<Options> {
        let mut opts: FrontendOptions = Options::load_layered_options(
            self.config_file.as_deref(),
            self.env_prefix.as_ref(),
            FrontendOptions::env_list_keys(),
        )?;

        if let Some(dir) = top_level_opts.log_dir {
            opts.logging.dir = dir;
        }

        if top_level_opts.log_level.is_some() {
            opts.logging.level = top_level_opts.log_level;
        }

        let tls_opts = TlsOption::new(
            self.tls_mode.clone(),
            self.tls_cert_path.clone(),
            self.tls_key_path.clone(),
        );

        if let Some(addr) = &self.http_addr {
            opts.http.addr = addr.clone()
        }

        if let Some(http_timeout) = self.http_timeout {
            opts.http.timeout = Duration::from_secs(http_timeout)
        }

        if let Some(disable_dashboard) = self.disable_dashboard {
            opts.http.disable_dashboard = disable_dashboard;
        }

        if let Some(addr) = &self.rpc_addr {
            opts.grpc.addr = addr.clone()
        }

        if let Some(addr) = &self.mysql_addr {
            opts.mysql.enable = true;
            opts.mysql.addr = addr.clone();
            opts.mysql.tls = tls_opts.clone();
        }

        if let Some(addr) = &self.postgres_addr {
            opts.postgres.enable = true;
            opts.postgres.addr = addr.clone();
            opts.postgres.tls = tls_opts;
        }

        if let Some(addr) = &self.opentsdb_addr {
            opts.opentsdb.enable = true;
            opts.opentsdb.addr = addr.clone();
        }

        if let Some(enable) = self.influxdb_enable {
            opts.influxdb.enable = enable;
        }

        if let Some(metasrv_addrs) = &self.metasrv_addr {
            opts.meta_client
                .get_or_insert_with(MetaClientOptions::default)
                .metasrv_addrs = metasrv_addrs.clone();
            opts.mode = Mode::Distributed;
        }

        opts.user_provider = self.user_provider.clone();

        Ok(Options::Frontend(Box::new(opts)))
    }

    async fn build(self, mut opts: FrontendOptions) -> Result<Instance> {
        let plugins = plugins::setup_frontend_plugins(&mut opts)
            .await
            .context(StartFrontendSnafu)?;

        logging::info!("Frontend start command: {:#?}", self);
        logging::info!("Frontend options: {:#?}", opts);

        let mut instance = FeInstance::try_new_distributed(&opts, plugins.clone())
            .await
            .context(StartFrontendSnafu)?;

        instance
            .build_servers(&opts)
            .await
            .context(StartFrontendSnafu)?;

        Ok(Instance { frontend: instance })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::time::Duration;

    use auth::{Identity, Password, UserProviderRef};
    use common_base::readable_size::ReadableSize;
    use common_test_util::temp_dir::create_named_temp_file;
    use frontend::service_config::GrpcOptions;
    use servers::http::HttpOptions;

    use super::*;
    use crate::options::ENV_VAR_SEP;

    #[test]
    fn test_try_from_start_command() {
        let command = StartCommand {
            http_addr: Some("127.0.0.1:1234".to_string()),
            mysql_addr: Some("127.0.0.1:5678".to_string()),
            postgres_addr: Some("127.0.0.1:5432".to_string()),
            opentsdb_addr: Some("127.0.0.1:4321".to_string()),
            influxdb_enable: Some(false),
            disable_dashboard: Some(false),
            ..Default::default()
        };

        let Options::Frontend(opts) = command.load_options(TopLevelOptions::default()).unwrap()
        else {
            unreachable!()
        };

        assert_eq!(opts.http.addr, "127.0.0.1:1234");
        assert_eq!(ReadableSize::mb(64), opts.http.body_limit);
        assert_eq!(opts.mysql.addr, "127.0.0.1:5678");
        assert_eq!(opts.postgres.addr, "127.0.0.1:5432");
        assert_eq!(opts.opentsdb.addr, "127.0.0.1:4321");

        let default_opts = FrontendOptions::default();

        assert_eq!(opts.grpc.addr, default_opts.grpc.addr);
        assert!(opts.mysql.enable);
        assert_eq!(opts.mysql.runtime_size, default_opts.mysql.runtime_size);
        assert!(opts.postgres.enable);
        assert_eq!(
            opts.postgres.runtime_size,
            default_opts.postgres.runtime_size
        );
        assert!(opts.opentsdb.enable);
        assert_eq!(
            opts.opentsdb.runtime_size,
            default_opts.opentsdb.runtime_size
        );

        assert!(!opts.influxdb.enable);
    }

    #[test]
    fn test_read_from_config_file() {
        let mut file = create_named_temp_file();
        let toml_str = r#"
            mode = "distributed"

            [http]
            addr = "127.0.0.1:4000"
            timeout = "30s"
            body_limit = "2GB"

            [logging]
            level = "debug"
            dir = "/tmp/greptimedb/test/logs"
        "#;
        write!(file, "{}", toml_str).unwrap();

        let command = StartCommand {
            config_file: Some(file.path().to_str().unwrap().to_string()),
            disable_dashboard: Some(false),
            ..Default::default()
        };

        let Options::Frontend(fe_opts) = command.load_options(TopLevelOptions::default()).unwrap()
        else {
            unreachable!()
        };
        assert_eq!(Mode::Distributed, fe_opts.mode);
        assert_eq!("127.0.0.1:4000".to_string(), fe_opts.http.addr);
        assert_eq!(Duration::from_secs(30), fe_opts.http.timeout);

        assert_eq!(ReadableSize::gb(2), fe_opts.http.body_limit);

        assert_eq!("debug", fe_opts.logging.level.as_ref().unwrap());
        assert_eq!("/tmp/greptimedb/test/logs".to_string(), fe_opts.logging.dir);
    }

    #[tokio::test]
    async fn test_try_from_start_command_to_anymap() {
        let mut fe_opts = FrontendOptions {
            http: HttpOptions {
                disable_dashboard: false,
                ..Default::default()
            },
            user_provider: Some("static_user_provider:cmd:test=test".to_string()),
            ..Default::default()
        };

        let plugins = plugins::setup_frontend_plugins(&mut fe_opts).await.unwrap();

        let provider = plugins.get::<UserProviderRef>().unwrap();
        let result = provider
            .authenticate(
                Identity::UserId("test", None),
                Password::PlainText("test".to_string().into()),
            )
            .await;
        let _ = result.unwrap();
    }

    #[test]
    fn test_top_level_options() {
        let cmd = StartCommand {
            disable_dashboard: Some(false),
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
            mode = "distributed"

            [http]
            addr = "127.0.0.1:4000"

            [meta_client]
            timeout = "3s"
            connect_timeout = "5s"
            tcp_nodelay = true

            [mysql]
            addr = "127.0.0.1:4002"
        "#;
        write!(file, "{}", toml_str).unwrap();

        let env_prefix = "FRONTEND_UT";
        temp_env::with_vars(
            [
                (
                    // mysql.addr = 127.0.0.1:14002
                    [
                        env_prefix.to_string(),
                        "mysql".to_uppercase(),
                        "addr".to_uppercase(),
                    ]
                    .join(ENV_VAR_SEP),
                    Some("127.0.0.1:14002"),
                ),
                (
                    // mysql.runtime_size = 11
                    [
                        env_prefix.to_string(),
                        "mysql".to_uppercase(),
                        "runtime_size".to_uppercase(),
                    ]
                    .join(ENV_VAR_SEP),
                    Some("11"),
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
                (
                    // meta_client.metasrv_addrs = 127.0.0.1:3001,127.0.0.1:3002,127.0.0.1:3003
                    [
                        env_prefix.to_string(),
                        "meta_client".to_uppercase(),
                        "metasrv_addrs".to_uppercase(),
                    ]
                    .join(ENV_VAR_SEP),
                    Some("127.0.0.1:3001,127.0.0.1:3002,127.0.0.1:3003"),
                ),
            ],
            || {
                let command = StartCommand {
                    config_file: Some(file.path().to_str().unwrap().to_string()),
                    http_addr: Some("127.0.0.1:14000".to_string()),
                    env_prefix: env_prefix.to_string(),
                    ..Default::default()
                };

                let top_level_opts = TopLevelOptions {
                    log_dir: None,
                    log_level: Some("error".to_string()),
                };
                let Options::Frontend(fe_opts) = command.load_options(top_level_opts).unwrap()
                else {
                    unreachable!()
                };

                // Should be read from env, env > default values.
                assert_eq!(fe_opts.mysql.runtime_size, 11);
                assert_eq!(
                    fe_opts.meta_client.unwrap().metasrv_addrs,
                    vec![
                        "127.0.0.1:3001".to_string(),
                        "127.0.0.1:3002".to_string(),
                        "127.0.0.1:3003".to_string()
                    ]
                );

                // Should be read from config file, config file > env > default values.
                assert_eq!(fe_opts.mysql.addr, "127.0.0.1:4002");

                // Should be read from cli, cli > config file > env > default values.
                assert_eq!(fe_opts.http.addr, "127.0.0.1:14000");

                // Should be default value.
                assert_eq!(fe_opts.grpc.addr, GrpcOptions::default().addr);
            },
        );
    }
}
