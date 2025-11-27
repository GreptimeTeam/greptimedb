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

use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cache::{build_fundamental_cache_registry, with_default_composite_cache_registry};
use catalog::information_extension::DistributedInformationExtension;
use catalog::kvbackend::{
    CachedKvBackendBuilder, CatalogManagerConfiguratorRef, KvBackendCatalogManagerBuilder,
    MetaKvBackend,
};
use catalog::process_manager::ProcessManager;
use clap::Parser;
use client::client_manager::NodeClients;
use common_base::Plugins;
use common_config::{Configurable, DEFAULT_DATA_HOME};
use common_error::ext::BoxedError;
use common_grpc::channel_manager::ChannelConfig;
use common_meta::cache::{CacheRegistryBuilder, LayeredCacheRegistryBuilder};
use common_meta::heartbeat::handler::HandlerGroupExecutor;
use common_meta::heartbeat::handler::invalidate_table_cache::InvalidateCacheHandler;
use common_meta::heartbeat::handler::parse_mailbox_message::ParseMailboxMessageHandler;
use common_query::prelude::set_default_prefix;
use common_stat::ResourceStatImpl;
use common_telemetry::info;
use common_telemetry::logging::{DEFAULT_LOGGING_DIR, TracingOptions};
use common_time::timezone::set_default_timezone;
use common_version::{short_version, verbose_version};
use frontend::frontend::Frontend;
use frontend::heartbeat::HeartbeatTask;
use frontend::instance::builder::FrontendBuilder;
use frontend::server::Services;
use meta_client::{MetaClientOptions, MetaClientRef, MetaClientType};
use servers::addrs;
use servers::grpc::GrpcOptions;
use servers::tls::{TlsMode, TlsOption};
use snafu::{OptionExt, ResultExt};
use tracing_appender::non_blocking::WorkerGuard;

use crate::error::{self, OtherSnafu, Result};
use crate::options::{GlobalOptions, GreptimeOptions};
use crate::{App, create_resource_limit_metrics, log_versions, maybe_activate_heap_profile};

type FrontendOptions = GreptimeOptions<frontend::frontend::FrontendOptions>;

pub struct Instance {
    frontend: Frontend,
    // Keep the logging guard to prevent the worker from being dropped.
    _guard: Vec<WorkerGuard>,
}

pub const APP_NAME: &str = "greptime-frontend";

impl Instance {
    pub fn new(frontend: Frontend, _guard: Vec<WorkerGuard>) -> Self {
        Self { frontend, _guard }
    }

    pub fn inner(&self) -> &Frontend {
        &self.frontend
    }

    pub fn mut_inner(&mut self) -> &mut Frontend {
        &mut self.frontend
    }
}

#[async_trait]
impl App for Instance {
    fn name(&self) -> &str {
        APP_NAME
    }

    async fn start(&mut self) -> Result<()> {
        let plugins = self.frontend.instance.plugins().clone();
        plugins::start_frontend_plugins(plugins)
            .await
            .context(error::StartFrontendSnafu)?;

        self.frontend
            .start()
            .await
            .context(error::StartFrontendSnafu)
    }

    async fn stop(&mut self) -> Result<()> {
        self.frontend
            .shutdown()
            .await
            .context(error::ShutdownFrontendSnafu)
    }
}

#[derive(Parser)]
pub struct Command {
    #[clap(subcommand)]
    pub subcmd: SubCommand,
}

impl Command {
    pub async fn build(&self, opts: FrontendOptions) -> Result<Instance> {
        self.subcmd.build(opts).await
    }

    pub fn load_options(&self, global_options: &GlobalOptions) -> Result<FrontendOptions> {
        self.subcmd.load_options(global_options)
    }
}

#[derive(Parser)]
pub enum SubCommand {
    Start(StartCommand),
}

impl SubCommand {
    async fn build(&self, opts: FrontendOptions) -> Result<Instance> {
        match self {
            SubCommand::Start(cmd) => cmd.build(opts).await,
        }
    }

    fn load_options(&self, global_options: &GlobalOptions) -> Result<FrontendOptions> {
        match self {
            SubCommand::Start(cmd) => cmd.load_options(global_options),
        }
    }
}

#[derive(Debug, Default, Parser)]
pub struct StartCommand {
    /// The address to bind the gRPC server.
    #[clap(long, alias = "rpc-addr")]
    rpc_bind_addr: Option<String>,
    /// The address advertised to the metasrv, and used for connections from outside the host.
    /// If left empty or unset, the server will automatically use the IP address of the first network interface
    /// on the host, with the same port number as the one specified in `rpc_bind_addr`.
    #[clap(long, alias = "rpc-hostname")]
    rpc_server_addr: Option<String>,
    /// The address to bind the internal gRPC server.
    #[clap(long, alias = "internal-rpc-addr")]
    internal_rpc_bind_addr: Option<String>,
    /// The address advertised to the metasrv, and used for connections from outside the host.
    /// If left empty or unset, the server will automatically use the IP address of the first network interface
    /// on the host, with the same port number as the one specified in `internal_rpc_bind_addr`.
    #[clap(long, alias = "internal-rpc-hostname")]
    internal_rpc_server_addr: Option<String>,
    #[clap(long)]
    http_addr: Option<String>,
    #[clap(long)]
    http_timeout: Option<u64>,
    #[clap(long)]
    mysql_addr: Option<String>,
    #[clap(long)]
    postgres_addr: Option<String>,
    #[clap(short, long)]
    pub config_file: Option<String>,
    #[clap(short, long)]
    influxdb_enable: Option<bool>,
    #[clap(long, value_delimiter = ',', num_args = 1..)]
    metasrv_addrs: Option<Vec<String>>,
    #[clap(long)]
    tls_mode: Option<TlsMode>,
    #[clap(long)]
    tls_cert_path: Option<String>,
    #[clap(long)]
    tls_key_path: Option<String>,
    #[clap(long)]
    tls_watch: bool,
    #[clap(long)]
    user_provider: Option<String>,
    #[clap(long)]
    disable_dashboard: Option<bool>,
    #[clap(long, default_value = "GREPTIMEDB_FRONTEND")]
    pub env_prefix: String,
}

impl StartCommand {
    fn load_options(&self, global_options: &GlobalOptions) -> Result<FrontendOptions> {
        let mut opts = FrontendOptions::load_layered_options(
            self.config_file.as_deref(),
            self.env_prefix.as_ref(),
        )
        .context(error::LoadLayeredConfigSnafu)?;

        self.merge_with_cli_options(global_options, &mut opts)?;

        Ok(opts)
    }

    // The precedence order is: cli > config file > environment variables > default values.
    fn merge_with_cli_options(
        &self,
        global_options: &GlobalOptions,
        opts: &mut FrontendOptions,
    ) -> Result<()> {
        let opts = &mut opts.component;

        if let Some(dir) = &global_options.log_dir {
            opts.logging.dir.clone_from(dir);
        }

        // If the logging dir is not set, use the default logs dir in the data home.
        if opts.logging.dir.is_empty() {
            opts.logging.dir = Path::new(DEFAULT_DATA_HOME)
                .join(DEFAULT_LOGGING_DIR)
                .to_string_lossy()
                .to_string();
        }

        if global_options.log_level.is_some() {
            opts.logging.level.clone_from(&global_options.log_level);
        }

        opts.tracing = TracingOptions {
            #[cfg(feature = "tokio-console")]
            tokio_console_addr: global_options.tokio_console_addr.clone(),
        };

        let tls_opts = TlsOption::new(
            self.tls_mode.clone(),
            self.tls_cert_path.clone(),
            self.tls_key_path.clone(),
            self.tls_watch,
        );

        if let Some(addr) = &self.http_addr {
            opts.http.addr.clone_from(addr);
        }

        if let Some(http_timeout) = self.http_timeout {
            opts.http.timeout = Duration::from_secs(http_timeout)
        }

        if let Some(disable_dashboard) = self.disable_dashboard {
            opts.http.disable_dashboard = disable_dashboard;
        }

        if let Some(addr) = &self.rpc_bind_addr {
            opts.grpc.bind_addr.clone_from(addr);
            opts.grpc.tls = tls_opts.clone();
        }

        if let Some(addr) = &self.rpc_server_addr {
            opts.grpc.server_addr.clone_from(addr);
        }

        if let Some(addr) = &self.internal_rpc_bind_addr {
            if let Some(internal_grpc) = &mut opts.internal_grpc {
                internal_grpc.bind_addr = addr.clone();
            } else {
                let grpc_options = GrpcOptions {
                    bind_addr: addr.clone(),
                    ..Default::default()
                };

                opts.internal_grpc = Some(grpc_options);
            }
        }

        if let Some(addr) = &self.internal_rpc_server_addr {
            if let Some(internal_grpc) = &mut opts.internal_grpc {
                internal_grpc.server_addr = addr.clone();
            } else {
                let grpc_options = GrpcOptions {
                    server_addr: addr.clone(),
                    ..Default::default()
                };
                opts.internal_grpc = Some(grpc_options);
            }
        }

        if let Some(addr) = &self.mysql_addr {
            opts.mysql.enable = true;
            opts.mysql.addr.clone_from(addr);
            opts.mysql.tls = tls_opts.clone();
        }

        if let Some(addr) = &self.postgres_addr {
            opts.postgres.enable = true;
            opts.postgres.addr.clone_from(addr);
            opts.postgres.tls = tls_opts;
        }

        if let Some(enable) = self.influxdb_enable {
            opts.influxdb.enable = enable;
        }

        if let Some(metasrv_addrs) = &self.metasrv_addrs {
            opts.meta_client
                .get_or_insert_with(MetaClientOptions::default)
                .metasrv_addrs
                .clone_from(metasrv_addrs);
        }

        if let Some(user_provider) = &self.user_provider {
            opts.user_provider = Some(user_provider.clone());
        }

        Ok(())
    }

    async fn build(&self, opts: FrontendOptions) -> Result<Instance> {
        common_runtime::init_global_runtimes(&opts.runtime);

        let guard = common_telemetry::init_global_logging(
            APP_NAME,
            &opts.component.logging,
            &opts.component.tracing,
            opts.component.node_id.clone(),
            Some(&opts.component.slow_query),
        );

        log_versions(verbose_version(), short_version(), APP_NAME);
        maybe_activate_heap_profile(&opts.component.memory);
        create_resource_limit_metrics(APP_NAME);

        info!("Frontend start command: {:#?}", self);
        info!("Frontend options: {:#?}", opts);

        let plugin_opts = opts.plugins;
        let mut opts = opts.component;
        opts.grpc.detect_server_addr();
        let mut plugins = Plugins::new();
        plugins::setup_frontend_plugins(&mut plugins, &plugin_opts, &opts)
            .await
            .context(error::StartFrontendSnafu)?;

        set_default_timezone(opts.default_timezone.as_deref()).context(error::InitTimezoneSnafu)?;
        set_default_prefix(opts.default_column_prefix.as_deref())
            .map_err(BoxedError::new)
            .context(error::BuildCliSnafu)?;

        let meta_client_options = opts
            .meta_client
            .as_ref()
            .context(error::MissingConfigSnafu {
                msg: "'meta_client'",
            })?;

        let cache_max_capacity = meta_client_options.metadata_cache_max_capacity;
        let cache_ttl = meta_client_options.metadata_cache_ttl;
        let cache_tti = meta_client_options.metadata_cache_tti;

        let meta_client = meta_client::create_meta_client(
            MetaClientType::Frontend,
            meta_client_options,
            Some(&plugins),
            None,
        )
        .await
        .context(error::MetaClientInitSnafu)?;

        // TODO(discord9): add helper function to ease the creation of cache registry&such
        let cached_meta_backend =
            CachedKvBackendBuilder::new(Arc::new(MetaKvBackend::new(meta_client.clone())))
                .cache_max_capacity(cache_max_capacity)
                .cache_ttl(cache_ttl)
                .cache_tti(cache_tti)
                .build();
        let cached_meta_backend = Arc::new(cached_meta_backend);

        // Builds cache registry
        let layered_cache_builder = LayeredCacheRegistryBuilder::default().add_cache_registry(
            CacheRegistryBuilder::default()
                .add_cache(cached_meta_backend.clone())
                .build(),
        );
        let fundamental_cache_registry =
            build_fundamental_cache_registry(Arc::new(MetaKvBackend::new(meta_client.clone())));
        let layered_cache_registry = Arc::new(
            with_default_composite_cache_registry(
                layered_cache_builder.add_cache_registry(fundamental_cache_registry),
            )
            .context(error::BuildCacheRegistrySnafu)?
            .build(),
        );

        // frontend to datanode need not timeout.
        // Some queries are expected to take long time.
        let mut channel_config = ChannelConfig {
            timeout: None,
            tcp_nodelay: opts.datanode.client.tcp_nodelay,
            connect_timeout: Some(opts.datanode.client.connect_timeout),
            ..Default::default()
        };
        if opts.grpc.flight_compression.transport_compression() {
            channel_config.accept_compression = true;
            channel_config.send_compression = true;
        }
        let client = Arc::new(NodeClients::new(channel_config));

        let information_extension = Arc::new(DistributedInformationExtension::new(
            meta_client.clone(),
            client.clone(),
        ));

        let process_manager = Arc::new(ProcessManager::new(
            addrs::resolve_addr(&opts.grpc.bind_addr, Some(&opts.grpc.server_addr)),
            Some(meta_client.clone()),
        ));

        let builder = KvBackendCatalogManagerBuilder::new(
            information_extension,
            cached_meta_backend.clone(),
            layered_cache_registry.clone(),
        )
        .with_process_manager(process_manager.clone());
        let builder = if let Some(configurator) =
            plugins.get::<CatalogManagerConfiguratorRef<CatalogManagerConfigureContext>>()
        {
            let ctx = CatalogManagerConfigureContext {
                meta_client: meta_client.clone(),
            };
            configurator
                .configure(builder, ctx)
                .await
                .context(OtherSnafu)?
        } else {
            builder
        };
        let catalog_manager = builder.build();

        let executor = HandlerGroupExecutor::new(vec![
            Arc::new(ParseMailboxMessageHandler),
            Arc::new(InvalidateCacheHandler::new(layered_cache_registry.clone())),
        ]);

        let mut resource_stat = ResourceStatImpl::default();
        resource_stat.start_collect_cpu_usage();

        let heartbeat_task = HeartbeatTask::new(
            &opts,
            meta_client.clone(),
            opts.heartbeat.clone(),
            Arc::new(executor),
            Arc::new(resource_stat),
        );
        let heartbeat_task = Some(heartbeat_task);

        let instance = FrontendBuilder::new(
            opts.clone(),
            cached_meta_backend.clone(),
            layered_cache_registry.clone(),
            catalog_manager,
            client,
            meta_client,
            process_manager,
        )
        .with_plugin(plugins.clone())
        .with_local_cache_invalidator(layered_cache_registry)
        .try_build()
        .await
        .context(error::StartFrontendSnafu)?;
        let instance = Arc::new(instance);

        let servers = Services::new(opts, instance.clone(), plugins)
            .build()
            .context(error::StartFrontendSnafu)?;

        let frontend = Frontend {
            instance,
            servers,
            heartbeat_task,
        };

        Ok(Instance::new(frontend, guard))
    }
}

/// The context for [`CatalogManagerConfigratorRef`] in frontend.
pub struct CatalogManagerConfigureContext {
    pub meta_client: MetaClientRef,
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::time::Duration;

    use auth::{Identity, Password, UserProviderRef};
    use common_base::readable_size::ReadableSize;
    use common_config::ENV_VAR_SEP;
    use common_test_util::temp_dir::create_named_temp_file;
    use servers::grpc::GrpcOptions;
    use servers::http::HttpOptions;

    use super::*;
    use crate::options::GlobalOptions;

    #[test]
    fn test_try_from_start_command() {
        let command = StartCommand {
            http_addr: Some("127.0.0.1:1234".to_string()),
            mysql_addr: Some("127.0.0.1:5678".to_string()),
            postgres_addr: Some("127.0.0.1:5432".to_string()),
            internal_rpc_bind_addr: Some("127.0.0.1:4010".to_string()),
            internal_rpc_server_addr: Some("10.0.0.24:4010".to_string()),
            influxdb_enable: Some(false),
            disable_dashboard: Some(false),
            ..Default::default()
        };

        let opts = command.load_options(&Default::default()).unwrap().component;

        assert_eq!(opts.http.addr, "127.0.0.1:1234");
        assert_eq!(ReadableSize::mb(64), opts.http.body_limit);
        assert_eq!(opts.mysql.addr, "127.0.0.1:5678");
        assert_eq!(opts.postgres.addr, "127.0.0.1:5432");

        let internal_grpc = opts.internal_grpc.as_ref().unwrap();
        assert_eq!(internal_grpc.bind_addr, "127.0.0.1:4010");
        assert_eq!(internal_grpc.server_addr, "10.0.0.24:4010");

        let default_opts = FrontendOptions::default().component;

        assert_eq!(opts.grpc.bind_addr, default_opts.grpc.bind_addr);
        assert!(opts.mysql.enable);
        assert_eq!(opts.mysql.runtime_size, default_opts.mysql.runtime_size);
        assert!(opts.postgres.enable);
        assert_eq!(
            opts.postgres.runtime_size,
            default_opts.postgres.runtime_size
        );
        assert!(opts.opentsdb.enable);

        assert!(!opts.influxdb.enable);
    }

    #[test]
    fn test_read_from_config_file() {
        let mut file = create_named_temp_file();
        let toml_str = r#"
            [http]
            addr = "127.0.0.1:4000"
            timeout = "0s"
            body_limit = "2GB"

            [opentsdb]
            enable = false

            [logging]
            level = "debug"
            dir = "./greptimedb_data/test/logs"
        "#;
        write!(file, "{}", toml_str).unwrap();

        let command = StartCommand {
            config_file: Some(file.path().to_str().unwrap().to_string()),
            disable_dashboard: Some(false),
            ..Default::default()
        };

        let fe_opts = command.load_options(&Default::default()).unwrap().component;

        assert_eq!("127.0.0.1:4000".to_string(), fe_opts.http.addr);
        assert_eq!(Duration::from_secs(0), fe_opts.http.timeout);

        assert_eq!(ReadableSize::gb(2), fe_opts.http.body_limit);

        assert_eq!("debug", fe_opts.logging.level.as_ref().unwrap());
        assert_eq!(
            "./greptimedb_data/test/logs".to_string(),
            fe_opts.logging.dir
        );
        assert!(!fe_opts.opentsdb.enable);
    }

    #[tokio::test]
    async fn test_try_from_start_command_to_anymap() {
        let fe_opts = frontend::frontend::FrontendOptions {
            http: HttpOptions {
                disable_dashboard: false,
                ..Default::default()
            },
            user_provider: Some("static_user_provider:cmd:test=test".to_string()),
            ..Default::default()
        };

        let mut plugins = Plugins::new();
        plugins::setup_frontend_plugins(&mut plugins, &[], &fe_opts)
            .await
            .unwrap();

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
    fn test_load_log_options_from_cli() {
        let cmd = StartCommand {
            disable_dashboard: Some(false),
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

                let fe_opts = command.load_options(&Default::default()).unwrap().component;

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
                assert_eq!(fe_opts.grpc.bind_addr, GrpcOptions::default().bind_addr);
            },
        );
    }
}
