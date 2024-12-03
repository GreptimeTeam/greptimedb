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
use common_meta::error::Result as CommentMetaResult;
use common_meta::kv_backend::chroot::ChrootKvBackend;
use common_meta::kv_backend::etcd::EtcdStore;
use common_meta::kv_backend::KvBackendRef;
use common_meta::range_stream::PaginationStream;
use common_meta::rpc::store::RangeRequest;
use common_meta::rpc::KeyValue;
use common_meta::utils::{
    KvBackendMetadataApplier, MetadataApplier, MetadataProcessor, NoopMetadataApplier,
    RellocateRegionWalOptions,
};
use common_meta::wal_options_allocator::WalOptionsAllocator;
use common_telemetry::warn;
use common_wal::config::kafka::common::KafkaTopicConfig;
use common_wal::config::kafka::MetasrvKafkaConfig;
use common_wal::config::MetasrvWalConfig;
use etcd_client::Client;
use futures::TryStreamExt;
use meta_srv::error::ConnectEtcdSnafu;
use meta_srv::Result as MetaResult;
use snafu::ResultExt;
use tonic::async_trait;
use tracing_appender::non_blocking::WorkerGuard;

use crate::cli::{Instance, Tool};
use crate::error::{self, Result};

#[derive(Debug, Default, Parser)]
pub struct SwitchToRemoteWalCommand {
    /// Store server address default to etcd store.
    #[clap(long, value_delimiter = ',', default_value="127.0.0.1:2379", num_args=1..)]
    store_addr: Vec<String>,

    /// If it's not empty, the metasrv will store all data with this key prefix.
    #[clap(long, default_value = "")]
    store_key_prefix: String,

    // A Kafka topic is constructed by concatenating `topic_name_prefix` and `topic_id`.
    // i.g., greptimedb_wal_topic_0, greptimedb_wal_topic_1.
    #[clap(long, default_value = "greptimedb_wal_topic")]
    topic_name_prefix: String,

    // Number of topics.
    #[clap(long, default_value = "64")]
    num_topics: usize,

    /// Maximum number of operations permitted in a transaction.
    #[clap(long, default_value = "128")]
    max_txn_ops: usize,

    /// Running in dry mode, no metadata changes will be applied.
    #[clap(long)]
    dry: bool,
}

impl SwitchToRemoteWalCommand {
    pub async fn build(&self, guard: Vec<WorkerGuard>) -> Result<Instance> {
        Ok(Instance::new(
            Box::new(RegionWalOptionProcessor {
                store_addr: self
                    .store_addr
                    .iter()
                    .map(|x| x.trim().to_string())
                    .filter(|x| !x.is_empty())
                    .collect::<Vec<_>>(),
                store_key_prefix: self.store_key_prefix.to_string(),
                max_txn_ops: self.max_txn_ops,
                dry: self.dry,
                wal_config: MetasrvWalConfig::Kafka(MetasrvKafkaConfig {
                    kafka_topic: KafkaTopicConfig {
                        num_topics: self.num_topics,
                        topic_name_prefix: self.topic_name_prefix.to_string(),
                        ..Default::default()
                    },
                    auto_create_topics: false,
                    ..Default::default()
                }),
            }),
            guard,
        ))
    }
}

#[derive(Debug, Default, Parser)]
pub struct SwitchToLocalWalCommand {
    /// Store server address default to etcd store.
    #[clap(long, value_delimiter = ',', default_value="127.0.0.1:2379", num_args=1..)]
    store_addr: Vec<String>,

    /// If it's not empty, the metasrv will store all data with this key prefix.
    #[clap(long, default_value = "")]
    store_key_prefix: String,

    /// Maximum number of operations permitted in a transaction.
    #[clap(long, default_value = "128")]
    max_txn_ops: usize,

    /// Running in dry mode, no metadata changes will be applied.
    #[clap(long)]
    dry: bool,
}

impl SwitchToLocalWalCommand {
    pub async fn build(&self, guard: Vec<WorkerGuard>) -> Result<Instance> {
        Ok(Instance::new(
            Box::new(RegionWalOptionProcessor {
                store_addr: self
                    .store_addr
                    .iter()
                    .map(|x| x.trim().to_string())
                    .filter(|x| !x.is_empty())
                    .collect::<Vec<_>>(),
                store_key_prefix: self.store_key_prefix.to_string(),
                max_txn_ops: self.max_txn_ops,
                dry: self.dry,
                wal_config: MetasrvWalConfig::RaftEngine,
            }),
            guard,
        ))
    }
}

pub struct RegionWalOptionProcessor {
    store_addr: Vec<String>,
    store_key_prefix: String,
    max_txn_ops: usize,
    dry: bool,
    wal_config: MetasrvWalConfig,
}

impl RegionWalOptionProcessor {
    async fn create_etcd_client(&self) -> MetaResult<KvBackendRef> {
        let etcd_client = Client::connect(&self.store_addr, None)
            .await
            .context(ConnectEtcdSnafu)?;
        let kv_backend = {
            let etcd_backend = EtcdStore::with_etcd_client(etcd_client, self.max_txn_ops);
            if !self.store_key_prefix.is_empty() {
                Arc::new(ChrootKvBackend::new(
                    self.store_key_prefix.clone().into_bytes(),
                    etcd_backend,
                ))
            } else {
                etcd_backend
            }
        };

        Ok(kv_backend)
    }
}

fn decoder(kv: KeyValue) -> CommentMetaResult<(Vec<u8>, Vec<u8>)> {
    Ok((kv.key, kv.value))
}

#[async_trait]
impl Tool for RegionWalOptionProcessor {
    async fn do_work(&self) -> Result<()> {
        let kv_backend = self
            .create_etcd_client()
            .await
            .context(error::BuildKvBackendSnafu)?;
        let wal_options_allocator = Arc::new(WalOptionsAllocator::new(
            self.wal_config.clone(),
            kv_backend.clone(),
        ));
        let req = RangeRequest::new().with_range("\0", "\0");
        let mut stream = Box::pin(
            PaginationStream::new(kv_backend.clone(), req, 1024, Arc::new(decoder)).into_stream(),
        );
        let transformers =
            vec![Box::new(RellocateRegionWalOptions::new(wal_options_allocator)) as _];
        let applier = if self.dry {
            warn!("Running in dry mode: no metadata changes will be applied");
            Box::new(NoopMetadataApplier) as Box<dyn MetadataApplier>
        } else {
            Box::new(KvBackendMetadataApplier::new(kv_backend)) as Box<dyn MetadataApplier>
        };
        let processor = MetadataProcessor::new(transformers, applier);
        while let Some((key, value)) = stream.try_next().await.context(error::DecodeValueSnafu)? {
            processor
                .handle(key, value)
                .await
                .context(error::MetadataProcessorSnafu)?;
        }

        Ok(())
    }
}
