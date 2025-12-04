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
use std::time::Duration;

use client::RegionFollowerClientRef;
use common_base::Plugins;
use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
use common_meta::distributed_time_constants::META_KEEP_ALIVE_INTERVAL_SECS;
use common_telemetry::{debug, info};
use serde::{Deserialize, Serialize};

use crate::client::{LeaderProviderRef, MetaClientBuilder};

pub mod client;
pub mod error;

// Options for meta client in datanode instance.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct MetaClientOptions {
    pub metasrv_addrs: Vec<String>,
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub ddl_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub connect_timeout: Duration,
    pub tcp_nodelay: bool,
    pub metadata_cache_max_capacity: u64,
    #[serde(with = "humantime_serde")]
    pub metadata_cache_ttl: Duration,
    #[serde(with = "humantime_serde")]
    pub metadata_cache_tti: Duration,
}

impl Default for MetaClientOptions {
    fn default() -> Self {
        Self {
            metasrv_addrs: vec!["127.0.0.1:3002".to_string()],
            timeout: Duration::from_millis(3_000u64),
            ddl_timeout: Duration::from_millis(10_000u64),
            connect_timeout: Duration::from_millis(1_000u64),
            tcp_nodelay: true,
            metadata_cache_max_capacity: 100_000u64,
            metadata_cache_ttl: Duration::from_secs(600u64),
            metadata_cache_tti: Duration::from_secs(300u64),
        }
    }
}

#[derive(Debug)]
pub enum MetaClientType {
    Datanode { member_id: u64 },
    Flownode { member_id: u64 },
    Frontend,
}

pub type MetaClientRef = Arc<client::MetaClient>;

pub async fn create_meta_client(
    client_type: MetaClientType,
    meta_client_options: &MetaClientOptions,
    plugins: Option<&Plugins>,
    leader_provider: Option<LeaderProviderRef>,
) -> error::Result<MetaClientRef> {
    info!(
        "Creating {:?} instance with Metasrv addrs {:?}",
        client_type, meta_client_options.metasrv_addrs
    );

    let mut builder = match client_type {
        MetaClientType::Datanode { member_id } => {
            MetaClientBuilder::datanode_default_options(member_id)
        }
        MetaClientType::Flownode { member_id } => {
            MetaClientBuilder::flownode_default_options(member_id)
        }
        MetaClientType::Frontend => MetaClientBuilder::frontend_default_options(),
    };

    let base_config = ChannelConfig::new()
        .timeout(meta_client_options.timeout)
        .connect_timeout(meta_client_options.connect_timeout)
        .tcp_nodelay(meta_client_options.tcp_nodelay);
    let heartbeat_config = base_config
        .clone()
        .timeout(Duration::from_secs(META_KEEP_ALIVE_INTERVAL_SECS + 1))
        .http2_keep_alive_interval(Duration::from_secs(META_KEEP_ALIVE_INTERVAL_SECS + 1))
        .http2_keep_alive_timeout(Duration::from_secs(META_KEEP_ALIVE_INTERVAL_SECS + 1));

    if let MetaClientType::Frontend = client_type {
        let ddl_config = base_config.clone().timeout(meta_client_options.ddl_timeout);
        builder = builder.ddl_channel_manager(ChannelManager::with_config(ddl_config, None));
        if let Some(plugins) = plugins {
            let region_follower = plugins.get::<RegionFollowerClientRef>();
            if let Some(region_follower) = region_follower {
                debug!("Region follower client found in plugins");
                builder = builder.with_region_follower(region_follower);
            }
        }
    }

    builder = builder
        .channel_manager(ChannelManager::with_config(base_config, None))
        .heartbeat_channel_manager(ChannelManager::with_config(heartbeat_config, None));

    let mut meta_client = builder.build();

    if let Some(leader_provider) = leader_provider {
        meta_client
            .start_with(leader_provider, &meta_client_options.metasrv_addrs)
            .await?;
    } else {
        meta_client
            .start(&meta_client_options.metasrv_addrs)
            .await?;
    }

    meta_client.ask_leader().await?;

    Ok(Arc::new(meta_client))
}

#[cfg(test)]
mod mocks;
