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

use cache::build_datanode_cache_registry;
use catalog::kvbackend::MetaKvBackend;
use common_base::Plugins;
use common_meta::cache::LayeredCacheRegistryBuilder;
use common_telemetry::info;
use common_version::{short_version, version};
use datanode::datanode::DatanodeBuilder;
use datanode::service::DatanodeServiceBuilder;
use meta_client::MetaClientType;
use snafu::{OptionExt, ResultExt};
use tracing_appender::non_blocking::WorkerGuard;

use crate::datanode::{DatanodeOptions, Instance, APP_NAME};
use crate::error::{MetaClientInitSnafu, MissingConfigSnafu, Result, StartDatanodeSnafu};
use crate::log_versions;

/// Builder for Datanode instance.
pub struct InstanceBuilder {
    guard: Vec<WorkerGuard>,
    opts: DatanodeOptions,
    datanode_builder: DatanodeBuilder,
}

impl InstanceBuilder {
    /// Try to create a new [InstanceBuilder], and do some initialization work like allocating
    /// runtime resources, setting up global logging and plugins, etc.
    pub async fn try_new_with_init(
        mut opts: DatanodeOptions,
        mut plugins: Plugins,
    ) -> Result<Self> {
        let guard = Self::init(&mut opts, &mut plugins).await?;

        let datanode_builder = Self::datanode_builder(&opts, plugins).await?;

        Ok(Self {
            guard,
            opts,
            datanode_builder,
        })
    }

    async fn init(opts: &mut DatanodeOptions, plugins: &mut Plugins) -> Result<Vec<WorkerGuard>> {
        common_runtime::init_global_runtimes(&opts.runtime);

        let dn_opts = &mut opts.component;
        let guard = common_telemetry::init_global_logging(
            APP_NAME,
            &dn_opts.logging,
            &dn_opts.tracing,
            dn_opts.node_id.map(|x| x.to_string()),
            None,
        );

        log_versions(version(), short_version(), APP_NAME);

        plugins::setup_datanode_plugins(plugins, &opts.plugins, dn_opts)
            .await
            .context(StartDatanodeSnafu)?;

        dn_opts.grpc.detect_server_addr();

        info!("Initialized Datanode instance with {:#?}", opts);
        Ok(guard)
    }

    async fn datanode_builder(opts: &DatanodeOptions, plugins: Plugins) -> Result<DatanodeBuilder> {
        let dn_opts = &opts.component;

        let member_id = dn_opts
            .node_id
            .context(MissingConfigSnafu { msg: "'node_id'" })?;
        let meta_client_options = dn_opts.meta_client.as_ref().context(MissingConfigSnafu {
            msg: "meta client options",
        })?;
        let client = meta_client::create_meta_client(
            MetaClientType::Datanode { member_id },
            meta_client_options,
            Some(&plugins),
        )
        .await
        .context(MetaClientInitSnafu)?;

        let backend = Arc::new(MetaKvBackend {
            client: client.clone(),
        });
        let mut builder = DatanodeBuilder::new(dn_opts.clone(), plugins.clone(), backend.clone());

        let registry = Arc::new(
            LayeredCacheRegistryBuilder::default()
                .add_cache_registry(build_datanode_cache_registry(backend))
                .build(),
        );
        builder
            .with_cache_registry(registry)
            .with_meta_client(client.clone());
        Ok(builder)
    }

    /// Get the mutable builder for Datanode, in case you want to change some fields before the
    /// final construction.
    pub fn mut_datanode_builder(&mut self) -> &mut DatanodeBuilder {
        &mut self.datanode_builder
    }

    /// Try to build the Datanode instance.
    pub async fn build(self) -> Result<Instance> {
        let mut datanode = self
            .datanode_builder
            .build()
            .await
            .context(StartDatanodeSnafu)?;

        let services = DatanodeServiceBuilder::new(&self.opts.component)
            .with_default_grpc_server(&datanode.region_server())
            .enable_http_service()
            .build()
            .context(StartDatanodeSnafu)?;
        datanode.setup_services(services);

        Ok(Instance::new(datanode, self.guard))
    }
}
