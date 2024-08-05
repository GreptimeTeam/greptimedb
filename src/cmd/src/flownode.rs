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

use cache::{build_fundamental_cache_registry, with_default_composite_cache_registry};
use catalog::kvbackend::{CachedMetaKvBackendBuilder, KvBackendCatalogManager, MetaKvBackend};
use clap::Parser;
use client::client_manager::NodeClients;
use common_base::Plugins;
use common_config::Configurable;
use common_grpc::channel_manager::ChannelConfig;
use common_meta::cache::{CacheRegistryBuilder, LayeredCacheRegistryBuilder};
use common_meta::heartbeat::handler::parse_mailbox_message::ParseMailboxMessageHandler;
use common_meta::heartbeat::handler::HandlerGroupExecutor;
use common_meta::key::flow::FlowMetadataManager;
use common_meta::key::TableMetadataManager;
use common_telemetry::info;
use common_telemetry::logging::TracingOptions;
use common_version::{short_version, version};
use flow::{FlownodeBuilder, FlownodeInstance, FrontendInvoker};
use frontend::heartbeat::handler::invalidate_table_cache::InvalidateTableCacheHandler;
use meta_client::{MetaClientOptions, MetaClientType};
use servers::Mode;
use snafu::{OptionExt, ResultExt};
use tracing_appender::non_blocking::WorkerGuard;

use crate::error::{
    BuildCacheRegistrySnafu, InitMetadataSnafu, LoadLayeredConfigSnafu, MetaClientInitSnafu,
    MissingConfigSnafu, Result, ShutdownFlownodeSnafu, StartFlownodeSnafu,
};
use crate::options::{GlobalOptions, GreptimeOptions};
use crate::{log_versions, App};

pub const APP_NAME: &str = "greptime-flownode";

type FlownodeOptions = GreptimeOptions<flow::FlownodeOptions>;

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

    pub fn flownode_mut(&mut self) -> &mut FlownodeInstance {
        &mut self.flownode
    }

    pub fn flownode(&self) -> &FlownodeInstance {
        &self.flownode
    }
}

#[async_trait::async_trait]
impl App for Instance {
    fn name(&self) -> &str {
        APP_NAME
    }

    async fn start(&mut self) -> Result<()> {
        self.flownode.start().await.context(StartFlownodeSnafu)
    }

    async fn stop(&self) -> Result<()> {
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
    pub async fn build(&self, opts: FlownodeOptions) -> Result<Instance> {
        self.subcmd.build(opts).await
    }

    pub fn load_options(&self, global_options: &GlobalOptions) -> Result<FlownodeOptions> {
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
    async fn build(&self, opts: FlownodeOptions) -> Result<Instance> {
        match self {
            SubCommand::Start(cmd) => cmd.build(opts).await,
        }
    }
}

#[derive(Debug, Parser, Default)]
struct StartCommand {
    /// Flownode's id
    #[clap(long)]
    node_id: Option<u64>,
    /// Bind address for the gRPC server.
    #[clap(long)]
    rpc_addr: Option<String>,
    /// Hostname for the gRPC server.
    #[clap(long)]
    rpc_hostname: Option<String>,
    /// Metasrv address list;
    #[clap(long, value_delimiter = ',', num_args = 1..)]
    metasrv_addrs: Option<Vec<String>>,
    /// The configuration file for flownode
    #[clap(short, long)]
    config_file: Option<String>,
    /// The prefix of environment variables, default is `GREPTIMEDB_FLOWNODE`;
    #[clap(long, default_value = "GREPTIMEDB_FLOWNODE")]
    env_prefix: String,
}

impl StartCommand {
    fn load_options(&self, global_options: &GlobalOptions) -> Result<FlownodeOptions> {
        let mut opts = FlownodeOptions::load_layered_options(
            self.config_file.as_deref(),
            self.env_prefix.as_ref(),
        )
        .context(LoadLayeredConfigSnafu)?;

        self.merge_with_cli_options(global_options, &mut opts)?;

        Ok(opts)
    }

    // The precedence order is: cli > config file > environment variables > default values.
    fn merge_with_cli_options(
        &self,
        global_options: &GlobalOptions,
        opts: &mut FlownodeOptions,
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

        if let Some(addr) = &self.rpc_addr {
            opts.grpc.addr.clone_from(addr);
        }

        if let Some(hostname) = &self.rpc_hostname {
            opts.grpc.hostname.clone_from(hostname);
        }

        if let Some(node_id) = self.node_id {
            opts.node_id = Some(node_id);
        }

        if let Some(metasrv_addrs) = &self.metasrv_addrs {
            opts.meta_client
                .get_or_insert_with(MetaClientOptions::default)
                .metasrv_addrs
                .clone_from(metasrv_addrs);
            opts.mode = Mode::Distributed;
        }

        if let (Mode::Distributed, None) = (&opts.mode, &opts.node_id) {
            return MissingConfigSnafu {
                msg: "Missing node id option",
            }
            .fail();
        }

        Ok(())
    }

    async fn build(&self, opts: FlownodeOptions) -> Result<Instance> {
        common_runtime::init_global_runtimes(&opts.runtime);

        let guard = common_telemetry::init_global_logging(
            APP_NAME,
            &opts.component.logging,
            &opts.component.tracing,
            opts.component.node_id.map(|x| x.to_string()),
        );
        log_versions(version(), short_version());

        info!("Flownode start command: {:#?}", self);
        info!("Flownode options: {:#?}", opts);

        let opts = opts.component;

        // TODO(discord9): make it not optionale after cluster id is required
        let cluster_id = opts.cluster_id.unwrap_or(0);

        let member_id = opts
            .node_id
            .context(MissingConfigSnafu { msg: "'node_id'" })?;

        let meta_config = opts.meta_client.as_ref().context(MissingConfigSnafu {
            msg: "'meta_client_options'",
        })?;

        let meta_client = meta_client::create_meta_client(
            cluster_id,
            MetaClientType::Flownode { member_id },
            meta_config,
        )
        .await
        .context(MetaClientInitSnafu)?;

        let cache_max_capacity = meta_config.metadata_cache_max_capacity;
        let cache_ttl = meta_config.metadata_cache_ttl;
        let cache_tti = meta_config.metadata_cache_tti;

        // TODO(discord9): add helper function to ease the creation of cache registry&such
        let cached_meta_backend = CachedMetaKvBackendBuilder::new(meta_client.clone())
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

        let catalog_manager = KvBackendCatalogManager::new(
            opts.mode,
            Some(meta_client.clone()),
            cached_meta_backend.clone(),
            layered_cache_registry.clone(),
        );

        let table_metadata_manager =
            Arc::new(TableMetadataManager::new(cached_meta_backend.clone()));
        table_metadata_manager
            .init()
            .await
            .context(InitMetadataSnafu)?;

        let executor = HandlerGroupExecutor::new(vec![
            Arc::new(ParseMailboxMessageHandler),
            Arc::new(InvalidateTableCacheHandler::new(
                layered_cache_registry.clone(),
            )),
        ]);

        let heartbeat_task = flow::heartbeat::HeartbeatTask::new(
            &opts,
            meta_client.clone(),
            opts.heartbeat.clone(),
            Arc::new(executor),
        );

        let flow_metadata_manager = Arc::new(FlowMetadataManager::new(cached_meta_backend.clone()));
        let flownode_builder = FlownodeBuilder::new(
            opts,
            Plugins::new(),
            table_metadata_manager,
            catalog_manager.clone(),
            flow_metadata_manager,
        )
        .with_heartbeat_task(heartbeat_task);

        let flownode = flownode_builder.build().await.context(StartFlownodeSnafu)?;

        // flownode's frontend to datanode need not timeout.
        // Some queries are expected to take long time.
        let channel_config = ChannelConfig {
            timeout: None,
            ..Default::default()
        };
        let client = Arc::new(NodeClients::new(channel_config));

        let invoker = FrontendInvoker::build_from(
            flownode.flow_worker_manager().clone(),
            catalog_manager.clone(),
            cached_meta_backend.clone(),
            layered_cache_registry.clone(),
            meta_client.clone(),
            client,
        )
        .await
        .context(StartFlownodeSnafu)?;
        flownode
            .flow_worker_manager()
            .set_frontend_invoker(invoker)
            .await;

        Ok(Instance::new(flownode, guard))
    }
}
