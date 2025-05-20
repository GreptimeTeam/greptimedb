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

use common_meta::key::{TableMetadataManager, TableMetadataManagerRef};
use common_meta::kv_backend::memory::MemoryKvBackend;
use common_meta::region_registry::{LeaderRegionRegistry, LeaderRegionRegistryRef};
use common_meta::sequence::SequenceBuilder;
use common_meta::state_store::KvStateStore;
use common_meta::wal_options_allocator::build_kafka_client;
use common_procedure::local::{LocalManager, ManagerConfig};
use common_procedure::test_util::InMemoryPoisonStore;
use common_procedure::ProcedureManagerRef;
use common_wal::config::kafka::common::{KafkaConnectionConfig, KafkaTopicConfig};
use common_wal::config::kafka::MetasrvKafkaConfig;
use rskafka::client::Client;

use crate::procedure::test_util::MailboxContext;
use crate::procedure::wal_prune::Context as WalPruneContext;

pub struct TestEnv {
    pub table_metadata_manager: TableMetadataManagerRef,
    pub leader_region_registry: LeaderRegionRegistryRef,
    pub procedure_manager: ProcedureManagerRef,
    pub mailbox: MailboxContext,
    pub server_addr: String,
}

impl TestEnv {
    pub fn new() -> Self {
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let table_metadata_manager = Arc::new(TableMetadataManager::new(kv_backend.clone()));
        let leader_region_registry = Arc::new(LeaderRegionRegistry::new());
        let mailbox_sequence =
            SequenceBuilder::new("test_heartbeat_mailbox", kv_backend.clone()).build();

        let state_store = Arc::new(KvStateStore::new(kv_backend.clone()));
        let poison_manager = Arc::new(InMemoryPoisonStore::default());
        let procedure_manager = Arc::new(LocalManager::new(
            ManagerConfig::default(),
            state_store,
            poison_manager,
        ));

        let mailbox_ctx = MailboxContext::new(mailbox_sequence);

        Self {
            table_metadata_manager,
            leader_region_registry,
            procedure_manager,
            mailbox: mailbox_ctx,
            server_addr: "localhost".to_string(),
        }
    }

    async fn build_kafka_client(broker_endpoints: Vec<String>) -> Arc<Client> {
        let kafka_topic = KafkaTopicConfig {
            replication_factor: broker_endpoints.len() as i16,
            ..Default::default()
        };
        let config = MetasrvKafkaConfig {
            connection: KafkaConnectionConfig {
                broker_endpoints,
                ..Default::default()
            },
            kafka_topic,
            ..Default::default()
        };
        Arc::new(build_kafka_client(&config.connection).await.unwrap())
    }

    pub async fn build_wal_prune_context(&self, broker_endpoints: Vec<String>) -> WalPruneContext {
        let client = Self::build_kafka_client(broker_endpoints).await;
        WalPruneContext {
            client,
            table_metadata_manager: self.table_metadata_manager.clone(),
            leader_region_registry: self.leader_region_registry.clone(),
            server_addr: self.server_addr.to_string(),
            mailbox: self.mailbox.mailbox().clone(),
        }
    }

    pub async fn prepare_topic(client: &Arc<Client>, topic_name: &str) {
        let controller_client = client.controller_client().unwrap();
        controller_client
            .create_topic(topic_name.to_string(), 1, 1, 5000)
            .await
            .unwrap();
    }
}
