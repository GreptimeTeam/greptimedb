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

use std::fmt;
use std::time::Duration;

use async_trait::async_trait;
use clap::Parser;
use common_base::Plugins;
use common_config::Configurable;
use common_telemetry::info;
use common_telemetry::logging::TracingOptions;
use common_version::{short_version, version};
use meta_srv::bootstrap::MetasrvInstance;
use meta_srv::metasrv::BackendImpl;
use snafu::ResultExt;
use tracing_appender::non_blocking::WorkerGuard;

use crate::error::{self, LoadLayeredConfigSnafu, Result, StartMetaServerSnafu};
use crate::options::{GlobalOptions, GreptimeOptions};
use crate::{log_versions, App};

type MetasrvOptions = GreptimeOptions<meta_srv::metasrv::MetasrvOptions>;

pub const APP_NAME: &str = "greptime-metasrv";

pub struct Instance {
    instance: MetasrvInstance,

    // Keep the logging guard to prevent the worker from being dropped.
    _guard: Vec<WorkerGuard>,
}

impl Instance {
    pub fn new(instance: MetasrvInstance, guard: Vec<WorkerGuard>) -> Self {
        Self {
            instance,
            _guard: guard,
        }
    }

    pub fn get_inner(&self) -> &MetasrvInstance {
        &self.instance
    }
}

#[async_trait]
impl App for Instance {
    fn name(&self) -> &str {
        APP_NAME
    }

    async fn start(&mut self) -> Result<()> {
        plugins::start_metasrv_plugins(self.instance.plugins())
            .await
            .context(StartMetaServerSnafu)?;

        self.instance.start().await.context(StartMetaServerSnafu)
    }

    async fn stop(&mut self) -> Result<()> {
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
    pub async fn build(&self, opts: MetasrvOptions) -> Result<Instance> {
        self.subcmd.build(opts).await
    }

    pub fn load_options(&self, global_options: &GlobalOptions) -> Result<MetasrvOptions> {
        self.subcmd.load_options(global_options)
    }

    pub fn config_file(&self) -> &Option<String> {
        self.subcmd.config_file()
    }

    pub fn env_prefix(&self) -> &String {
        self.subcmd.env_prefix()
    }
}

#[derive(Parser)]
enum SubCommand {
    Start(StartCommand),
}

impl SubCommand {
    async fn build(&self, opts: MetasrvOptions) -> Result<Instance> {
        match self {
            SubCommand::Start(cmd) => cmd.build(opts).await,
        }
    }

    fn load_options(&self, global_options: &GlobalOptions) -> Result<MetasrvOptions> {
        match self {
            SubCommand::Start(cmd) => cmd.load_options(global_options),
        }
    }

    fn config_file(&self) -> &Option<String> {
        match self {
            SubCommand::Start(cmd) => &cmd.config_file,
        }
    }

    fn env_prefix(&self) -> &String {
        match self {
            SubCommand::Start(cmd) => &cmd.env_prefix,
        }
    }
}

#[derive(Default, Parser)]
pub struct StartCommand {
    /// The address to bind the gRPC server.
    #[clap(long, alias = "bind-addr")]
    rpc_bind_addr: Option<String>,
    /// The communication server address for the frontend and datanode to connect to metasrv.
    /// If left empty or unset, the server will automatically use the IP address of the first network interface
    /// on the host, with the same port number as the one specified in `rpc_bind_addr`.
    #[clap(long, alias = "server-addr")]
    rpc_server_addr: Option<String>,
    #[clap(long, alias = "store-addr", value_delimiter = ',', num_args = 1..)]
    store_addrs: Option<Vec<String>>,
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
    /// The working home directory of this metasrv instance.
    #[clap(long)]
    data_home: Option<String>,
    /// If it's not empty, the metasrv will store all data with this key prefix.
    #[clap(long, default_value = "")]
    store_key_prefix: String,
    /// The max operations per txn
    #[clap(long)]
    max_txn_ops: Option<usize>,
    /// The database backend.
    #[clap(long, value_enum)]
    backend: Option<BackendImpl>,
}

impl fmt::Debug for StartCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StartCommand")
            .field("rpc_bind_addr", &self.rpc_bind_addr)
            .field("rpc_server_addr", &self.rpc_server_addr)
            .field("store_addrs", &self.sanitize_store_addrs())
            .field("config_file", &self.config_file)
            .field("selector", &self.selector)
            .field("use_memory_store", &self.use_memory_store)
            .field("enable_region_failover", &self.enable_region_failover)
            .field("http_addr", &self.http_addr)
            .field("http_timeout", &self.http_timeout)
            .field("env_prefix", &self.env_prefix)
            .field("data_home", &self.data_home)
            .field("store_key_prefix", &self.store_key_prefix)
            .field("max_txn_ops", &self.max_txn_ops)
            .field("backend", &self.backend)
            .finish()
    }
}

impl StartCommand {
    pub fn load_options(&self, global_options: &GlobalOptions) -> Result<MetasrvOptions> {
        let mut opts = MetasrvOptions::load_layered_options(
            self.config_file.as_deref(),
            self.env_prefix.as_ref(),
        )
        .context(LoadLayeredConfigSnafu)?;

        self.merge_with_cli_options(global_options, &mut opts)?;

        Ok(opts)
    }

    fn sanitize_store_addrs(&self) -> Option<Vec<String>> {
        self.store_addrs.as_ref().map(|addrs| {
            addrs
                .iter()
                .map(|addr| common_meta::kv_backend::util::sanitize_connection_string(addr))
                .collect()
        })
    }

    // The precedence order is: cli > config file > environment variables > default values.
    fn merge_with_cli_options(
        &self,
        global_options: &GlobalOptions,
        opts: &mut MetasrvOptions,
    ) -> Result<()> {
        let opts = &mut opts.component;

        if let Some(dir) = &global_options.log_dir {
            opts.logging.dir.clone_from(dir);
        }

        if global_options.log_level.is_some() {
            opts.logging.level.clone_from(&global_options.log_level);
        }

        opts.tracing = TracingOptions {
            #[cfg(feature = "tokio-console")]
            tokio_console_addr: global_options.tokio_console_addr.clone(),
        };

        if let Some(addr) = &self.rpc_bind_addr {
            opts.bind_addr.clone_from(addr);
        }

        if let Some(addr) = &self.rpc_server_addr {
            opts.server_addr.clone_from(addr);
        }

        if let Some(addrs) = &self.store_addrs {
            opts.store_addrs.clone_from(addrs);
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
            opts.http.addr.clone_from(http_addr);
        }

        if let Some(http_timeout) = self.http_timeout {
            opts.http.timeout = Duration::from_secs(http_timeout);
        }

        if let Some(data_home) = &self.data_home {
            opts.data_home.clone_from(data_home);
        }

        if !self.store_key_prefix.is_empty() {
            opts.store_key_prefix.clone_from(&self.store_key_prefix)
        }

        if let Some(max_txn_ops) = self.max_txn_ops {
            opts.max_txn_ops = max_txn_ops;
        }

        if let Some(backend) = &self.backend {
            opts.backend.clone_from(backend);
        }

        // Disable dashboard in metasrv.
        opts.http.disable_dashboard = true;

        Ok(())
    }

    pub async fn build(&self, opts: MetasrvOptions) -> Result<Instance> {
        common_runtime::init_global_runtimes(&opts.runtime);

        let guard = common_telemetry::init_global_logging(
            APP_NAME,
            &opts.component.logging,
            &opts.component.tracing,
            None,
            None,
        );
        log_versions(version(), short_version(), APP_NAME);

        info!("Metasrv start command: {:#?}", self);

        let plugin_opts = opts.plugins;
        let mut opts = opts.component;
        opts.detect_server_addr();

        info!("Metasrv options: {:#?}", opts);

        let mut plugins = Plugins::new();
        plugins::setup_metasrv_plugins(&mut plugins, &plugin_opts, &opts)
            .await
            .context(StartMetaServerSnafu)?;

        let builder = meta_srv::bootstrap::metasrv_builder(&opts, plugins.clone(), None)
            .await
            .context(error::BuildMetaServerSnafu)?;
        let metasrv = builder.build().await.context(error::BuildMetaServerSnafu)?;

        let instance = MetasrvInstance::new(opts, plugins, metasrv)
            .await
            .context(error::BuildMetaServerSnafu)?;

        Ok(Instance::new(instance, guard))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use common_base::readable_size::ReadableSize;
    use common_config::ENV_VAR_SEP;
    use common_test_util::temp_dir::create_named_temp_file;
    use meta_srv::selector::SelectorType;

    use super::*;

    #[test]
    fn test_read_from_cmd() {
        let cmd = StartCommand {
            rpc_bind_addr: Some("127.0.0.1:3002".to_string()),
            rpc_server_addr: Some("127.0.0.1:3002".to_string()),
            store_addrs: Some(vec!["127.0.0.1:2380".to_string()]),
            selector: Some("LoadBased".to_string()),
            ..Default::default()
        };

        let options = cmd.load_options(&Default::default()).unwrap().component;
        assert_eq!("127.0.0.1:3002".to_string(), options.bind_addr);
        assert_eq!(vec!["127.0.0.1:2380".to_string()], options.store_addrs);
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
            dir = "./greptimedb_data/test/logs"

            [failure_detector]
            threshold = 8.0
            min_std_deviation = "100ms"
            acceptable_heartbeat_pause = "3000ms"
            first_heartbeat_estimate = "1000ms"
        "#;
        write!(file, "{}", toml_str).unwrap();

        let cmd = StartCommand {
            config_file: Some(file.path().to_str().unwrap().to_string()),
            ..Default::default()
        };

        let options = cmd.load_options(&Default::default()).unwrap().component;
        assert_eq!("127.0.0.1:3002".to_string(), options.bind_addr);
        assert_eq!("127.0.0.1:3002".to_string(), options.server_addr);
        assert_eq!(vec!["127.0.0.1:2379".to_string()], options.store_addrs);
        assert_eq!(SelectorType::LeaseBased, options.selector);
        assert_eq!("debug", options.logging.level.as_ref().unwrap());
        assert_eq!(
            "./greptimedb_data/test/logs".to_string(),
            options.logging.dir
        );
        assert_eq!(8.0, options.failure_detector.threshold);
        assert_eq!(
            100.0,
            options.failure_detector.min_std_deviation.as_millis() as f32
        );
        assert_eq!(
            3000,
            options
                .failure_detector
                .acceptable_heartbeat_pause
                .as_millis()
        );
        assert_eq!(
            1000,
            options
                .failure_detector
                .first_heartbeat_estimate
                .as_millis()
        );
        assert_eq!(
            options.procedure.max_metadata_value_size,
            Some(ReadableSize::kb(1500))
        );
    }

    #[test]
    fn test_load_log_options_from_cli() {
        let cmd = StartCommand {
            rpc_bind_addr: Some("127.0.0.1:3002".to_string()),
            rpc_server_addr: Some("127.0.0.1:3002".to_string()),
            store_addrs: Some(vec!["127.0.0.1:2380".to_string()]),
            selector: Some("LoadBased".to_string()),
            ..Default::default()
        };

        let options = cmd
            .load_options(&GlobalOptions {
                log_dir: Some("./greptimedb_data/test/logs".to_string()),
                log_level: Some("debug".to_string()),

                #[cfg(feature = "tokio-console")]
                tokio_console_addr: None,
            })
            .unwrap()
            .component;

        let logging_opt = options.logging;
        assert_eq!("./greptimedb_data/test/logs", logging_opt.dir);
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
            dir = "./greptimedb_data/test/logs"
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

                let opts = command.load_options(&Default::default()).unwrap().component;

                // Should be read from env, env > default values.
                assert_eq!(opts.bind_addr, "127.0.0.1:14002");

                // Should be read from config file, config file > env > default values.
                assert_eq!(opts.server_addr, "127.0.0.1:3002");

                // Should be read from cli, cli > config file > env > default values.
                assert_eq!(opts.http.addr, "127.0.0.1:14000");

                // Should be default value.
                assert_eq!(opts.store_addrs, vec!["127.0.0.1:2379".to_string()]);
            },
        );
    }
}
