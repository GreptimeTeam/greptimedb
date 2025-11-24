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

use cache::{build_fundamental_cache_registry, with_default_composite_cache_registry};
use catalog::information_extension::DistributedInformationExtension;
use catalog::kvbackend::{CachedKvBackendBuilder, KvBackendCatalogManagerBuilder, MetaKvBackend};
use clap::Parser;
use client::client_manager::NodeClients;
use common_base::Plugins;
use common_config::{Configurable, DEFAULT_DATA_HOME};
use common_grpc::channel_manager::ChannelConfig;
use common_meta::cache::{CacheRegistryBuilder, LayeredCacheRegistryBuilder};
use common_meta::heartbeat::handler::HandlerGroupExecutor;
use common_meta::heartbeat::handler::invalidate_table_cache::InvalidateCacheHandler;
use common_meta::heartbeat::handler::parse_mailbox_message::ParseMailboxMessageHandler;
use common_meta::key::TableMetadataManager;
use common_meta::key::flow::FlowMetadataManager;
use common_stat::ResourceStatImpl;
use common_telemetry::info;
use common_telemetry::logging::{DEFAULT_LOGGING_DIR, TracingOptions};
use common_version::{short_version, verbose_version};
use flow::{
    FlownodeBuilder, FlownodeInstance, FlownodeServiceBuilder, FrontendClient, FrontendInvoker,
    get_flow_auth_options,
};
use meta_client::{MetaClientOptions, MetaClientType};
use snafu::{OptionExt, ResultExt, ensure};
use tracing_appender::non_blocking::WorkerGuard;

use crate::error::{
    BuildCacheRegistrySnafu, InitMetadataSnafu, LoadLayeredConfigSnafu, MetaClientInitSnafu,
    MissingConfigSnafu, OtherSnafu, Result, ShutdownFlownodeSnafu, StartFlownodeSnafu,
};
use crate::extension::flownode::{ExtensionContext, ExtensionFactory};
use crate::options::{GlobalOptions, GreptimeOptions};
use crate::{App, create_resource_limit_metrics, log_versions, maybe_activate_heap_profile};

pub const APP_NAME: &str = "greptime-flownode";

type FlownodeOptions<E> = GreptimeOptions<flow::FlownodeOptions, E>;

pub struct Instance {
    flownode: FlownodeInstance,
    // Keep the logging guard to prevent the worker from being dropped.
    _guard: Vec<WorkerGuard>,
}

impl Instance {
    pub fn new(flownode: FlownodeInstance, guard: Vec<WorkerGuard>) -> Self {
        Self {
            flownode,
            _guard: guard,
        }
    }

    pub fn flownode(&self) -> &FlownodeInstance {
        &self.flownode
    }

    /// allow customizing flownode for downstream projects
    pub fn flownode_mut(&mut self) -> &mut FlownodeInstance {
        &mut self.flownode
    }
}

#[async_trait::async_trait]
impl App for Instance {
    fn name(&self) -> &str {
        APP_NAME
    }

    async fn start(&mut self) -> Result<()> {
        plugins::start_flownode_plugins(self.flownode.flow_engine().plugins().clone())
            .await
            .context(StartFlownodeSnafu)?;

        self.flownode.start().await.context(StartFlownodeSnafu)
    }

    async fn stop(&mut self) -> Result<()> {
        self.flownode
            .shutdown()
            .await
            .context(ShutdownFlownodeSnafu)
    }
}

#[derive(Parser)]
pub struct Command {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

impl Command {
    pub async fn build<E: Debug, F: ExtensionFactory>(
        &self,
        opts: FlownodeOptions<E>,
        extension_factory: F,
    ) -> Result<Instance> {
        self.subcmd.build(opts, extension_factory).await
    }

    pub fn load_options<E: Configurable>(
        &self,
        global_options: &GlobalOptions,
    ) -> Result<FlownodeOptions<E>> {
        match &self.subcmd {
            SubCommand::Start(cmd) => cmd.load_options(global_options),
        }
    }
}

#[derive(Parser)]
enum SubCommand {
    Start(StartCommand),
}

impl SubCommand {
    async fn build<E: Debug, F: ExtensionFactory>(
        &self,
        opts: FlownodeOptions<E>,
        extension_factory: F,
    ) -> Result<Instance> {
        match self {
            SubCommand::Start(cmd) => cmd.build(opts, extension_factory).await,
        }
    }
}

#[derive(Debug, Parser, Default)]
struct StartCommand {
    /// Flownode's id
    #[clap(long)]
    node_id: Option<u64>,
    /// Bind address for the gRPC server.
    #[clap(long, alias = "rpc-addr")]
    rpc_bind_addr: Option<String>,
    /// The address advertised to the metasrv, and used for connections from outside the host.
    /// If left empty or unset, the server will automatically use the IP address of the first network interface
    /// on the host, with the same port number as the one specified in `rpc_bind_addr`.
    #[clap(long, alias = "rpc-hostname")]
    rpc_server_addr: Option<String>,
    /// Metasrv address list;
    #[clap(long, value_delimiter = ',', num_args = 1..)]
    metasrv_addrs: Option<Vec<String>>,
    /// The configuration file for flownode
    #[clap(short, long)]
    config_file: Option<String>,
    /// The prefix of environment variables, default is `GREPTIMEDB_FLOWNODE`;
    #[clap(long, default_value = "GREPTIMEDB_FLOWNODE")]
    env_prefix: String,
    #[clap(long)]
    http_addr: Option<String>,
    /// HTTP request timeout in seconds.
    #[clap(long)]
    http_timeout: Option<u64>,
    /// User Provider cfg, for auth, currently only support static user provider
    #[clap(long)]
    user_provider: Option<String>,
}

impl StartCommand {
    fn load_options<E: Configurable>(
        &self,
        global_options: &GlobalOptions,
    ) -> Result<FlownodeOptions<E>> {
        let mut opts = FlownodeOptions::load_layered_options(
            self.config_file.as_deref(),
            self.env_prefix.as_ref(),
        )
        .context(LoadLayeredConfigSnafu)?;

        self.merge_with_cli_options(global_options, &mut opts)?;

        Ok(opts)
    }

    // The precedence order is: cli > config file > environment variables > default values.
    fn merge_with_cli_options<E>(
        &self,
        global_options: &GlobalOptions,
        opts: &mut FlownodeOptions<E>,
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

        if let Some(addr) = &self.rpc_bind_addr {
            opts.grpc.bind_addr.clone_from(addr);
        }

        if let Some(server_addr) = &self.rpc_server_addr {
            opts.grpc.server_addr.clone_from(server_addr);
        }

        if let Some(node_id) = self.node_id {
            opts.node_id = Some(node_id);
        }

        if let Some(metasrv_addrs) = &self.metasrv_addrs {
            opts.meta_client
                .get_or_insert_with(MetaClientOptions::default)
                .metasrv_addrs
                .clone_from(metasrv_addrs);
        }

        if let Some(http_addr) = &self.http_addr {
            opts.http.addr.clone_from(http_addr);
        }

        if let Some(http_timeout) = self.http_timeout {
            opts.http.timeout = Duration::from_secs(http_timeout);
        }

        if let Some(user_provider) = &self.user_provider {
            opts.user_provider = Some(user_provider.clone());
        }

        ensure!(
            opts.node_id.is_some(),
            MissingConfigSnafu {
                msg: "Missing node id option"
            }
        );

        Ok(())
    }

    async fn build<E: Debug, F: ExtensionFactory>(
        &self,
        opts: FlownodeOptions<E>,
        extension_factory: F,
    ) -> Result<Instance> {
        common_runtime::init_global_runtimes(&opts.runtime);

        let guard = common_telemetry::init_global_logging(
            APP_NAME,
            &opts.component.logging,
            &opts.component.tracing,
            opts.component.node_id.map(|x| x.to_string()),
            None,
        );

        log_versions(verbose_version(), short_version(), APP_NAME);
        maybe_activate_heap_profile(&opts.component.memory);
        create_resource_limit_metrics(APP_NAME);

        info!("Flownode start command: {:#?}", self);
        info!("Flownode options: {:#?}", opts);

        let plugin_opts = opts.plugins;
        let mut opts = opts.component;
        opts.grpc.detect_server_addr();

        let mut plugins = Plugins::new();
        plugins::setup_flownode_plugins(&mut plugins, &plugin_opts, &opts)
            .await
            .context(StartFlownodeSnafu)?;

        let member_id = opts
            .node_id
            .context(MissingConfigSnafu { msg: "'node_id'" })?;

        let meta_config = opts.meta_client.as_ref().context(MissingConfigSnafu {
            msg: "'meta_client_options'",
        })?;

        let meta_client = meta_client::create_meta_client(
            MetaClientType::Flownode { member_id },
            meta_config,
            None,
            None,
        )
        .await
        .context(MetaClientInitSnafu)?;

        let cache_max_capacity = meta_config.metadata_cache_max_capacity;
        let cache_ttl = meta_config.metadata_cache_ttl;
        let cache_tti = meta_config.metadata_cache_tti;

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
            .context(BuildCacheRegistrySnafu)?
            .build(),
        );

        // flownode's frontend to datanode need not timeout.
        // Some queries are expected to take long time.
        let channel_config = ChannelConfig {
            timeout: None,
            ..Default::default()
        };
        let client = Arc::new(NodeClients::new(channel_config));

        let information_extension = Arc::new(DistributedInformationExtension::new(
            meta_client.clone(),
            client.clone(),
        ));
        let catalog_manager = KvBackendCatalogManagerBuilder::new(
            information_extension,
            cached_meta_backend.clone(),
            layered_cache_registry.clone(),
        )
        .build();

        let table_metadata_manager =
            Arc::new(TableMetadataManager::new(cached_meta_backend.clone()));
        table_metadata_manager
            .init()
            .await
            .context(InitMetadataSnafu)?;

        let executor = HandlerGroupExecutor::new(vec![
            Arc::new(ParseMailboxMessageHandler),
            Arc::new(InvalidateCacheHandler::new(layered_cache_registry.clone())),
        ]);

        let mut resource_stat = ResourceStatImpl::default();
        resource_stat.start_collect_cpu_usage();

        let heartbeat_task = flow::heartbeat::HeartbeatTask::new(
            &opts,
            meta_client.clone(),
            opts.heartbeat.clone(),
            Arc::new(executor),
            Arc::new(resource_stat),
        );

        let flow_metadata_manager = Arc::new(FlowMetadataManager::new(cached_meta_backend.clone()));
        let flow_auth_header = get_flow_auth_options(&opts).context(StartFlownodeSnafu)?;
        let frontend_client = FrontendClient::from_meta_client(
            meta_client.clone(),
            flow_auth_header,
            opts.query.clone(),
            opts.flow.batching_mode.clone(),
        )
        .context(StartFlownodeSnafu)?;
        let frontend_client = Arc::new(frontend_client);
        let flownode_builder = FlownodeBuilder::new(
            opts.clone(),
            plugins,
            table_metadata_manager,
            catalog_manager.clone(),
            flow_metadata_manager,
            frontend_client.clone(),
        )
        .with_heartbeat_task(heartbeat_task);

        let mut flownode = flownode_builder.build().await.context(StartFlownodeSnafu)?;

        let context = ExtensionContext {
            kv_backend: cached_meta_backend.clone(),
            fe_client: frontend_client.clone(),
            flownode_id: member_id,
            catalog_manager: catalog_manager.clone(),
        };
        let extension = extension_factory
            .create(context)
            .await
            .context(OtherSnafu)?;

        let mut builder =
            FlownodeServiceBuilder::grpc_server_builder(&opts, flownode.flownode_server());
        if let Some(grpc_extension) = extension.grpc {
            grpc_extension
                .extend_grpc_services(&mut builder)
                .await
                .context(OtherSnafu)?
        }
        let grpc_server = builder.build();

        let services = FlownodeServiceBuilder::new(&opts)
            .with_grpc_server(grpc_server)
            .enable_http_service()
            .build()
            .context(StartFlownodeSnafu)?;
        flownode.setup_services(services);
        let flownode = flownode;

        let invoker = FrontendInvoker::build_from(
            flownode.flow_engine().streaming_engine(),
            catalog_manager.clone(),
            cached_meta_backend.clone(),
            layered_cache_registry.clone(),
            meta_client.clone(),
            client,
        )
        .await
        .context(StartFlownodeSnafu)?;
        flownode
            .flow_engine()
            .streaming_engine()
            // TODO(discord9): refactor and avoid circular reference
            .set_frontend_invoker(invoker)
            .await;

        Ok(Instance::new(flownode, guard))
    }
}
