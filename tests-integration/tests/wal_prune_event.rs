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

use common_meta::key::TableMetadataManager;
use common_meta::key::topic_name::TopicNameKey;
use common_meta::region_registry::LeaderRegionRegistry;
use common_meta::wal_provider::build_kafka_client;
use common_procedure::{ProcedureWithId, watcher};
use common_wal::config::kafka::common::KafkaConnectionConfig;
use common_wal::maybe_skip_kafka_integration_test;
use common_wal::test_util::get_kafka_endpoints;
use meta_srv::procedure::wal_prune::{Context as WalPruneContext, WalPruneProcedure};
use rskafka::client::partition::{Compression, UnknownTopicHandling};
use rskafka::record::Record;
use tests_integration::standalone::GreptimeDbStandaloneBuilder;

use crate::event_recorder_test_util::assert_single_event;

#[tokio::test(flavor = "multi_thread")]
async fn test_standalone_wal_prune_event() {
    maybe_skip_kafka_integration_test!();
    common_telemetry::init_default_ut_logging();

    let standalone = GreptimeDbStandaloneBuilder::new("test_standalone_wal_prune_event")
        .build()
        .await;
    let topic_name = format!("test_standalone_wal_prune_event-{}", uuid::Uuid::new_v4());
    let connection = KafkaConnectionConfig {
        broker_endpoints: get_kafka_endpoints(),
        ..Default::default()
    };
    let client = Arc::new(build_kafka_client(&connection).await.unwrap());
    let controller_client = client.controller_client().unwrap();
    controller_client
        .create_topic(&topic_name, 1, 1, 5_000)
        .await
        .unwrap();
    let partition_client = client
        .partition_client(&topic_name, 0, UnknownTopicHandling::Retry)
        .await
        .unwrap();
    let records = (0..3)
        .map(|i| Record {
            key: Some(format!("key_{i}").into()),
            value: Some(format!("value_{i}").into()),
            timestamp: chrono::Utc::now(),
            headers: Default::default(),
        })
        .collect();
    let offsets = partition_client
        .produce(records, Compression::NoCompression)
        .await
        .unwrap()
        .offsets;
    let pruned_entry_id = offsets[2] as u64;

    let table_metadata_manager = Arc::new(TableMetadataManager::new(standalone.kv_backend.clone()));
    table_metadata_manager
        .topic_name_manager()
        .batch_put(vec![TopicNameKey::new(&topic_name)])
        .await
        .unwrap();
    let procedure = WalPruneProcedure::new(
        WalPruneContext {
            client: client.clone(),
            table_metadata_manager,
            leader_region_registry: Arc::new(LeaderRegionRegistry::default()),
        },
        None,
        topic_name.clone(),
        pruned_entry_id,
        false,
    );
    let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));
    let procedure_id = procedure_with_id.id;
    let mut watcher = standalone
        .procedure_manager
        .submit(procedure_with_id)
        .await
        .unwrap();
    watcher::wait(&mut watcher).await.unwrap();

    let query = format!(
        r#"SELECT count(*) AS event_count
FROM greptime_private.events
WHERE type = 'wal_prune'
  AND procedure_id = '{procedure_id}'
  AND procedure_state = 'Done'
  AND procedure_trigger = 'Succeeded'
  AND topic_name = '{topic_name}'
  AND previous_pruned_entry_id = 0
  AND pruned_entry_id = {pruned_entry_id}
  AND json_path_match(payload, '$.version == 1')
  AND json_path_match(payload, '$.logical_delete == false')"#
    );
    assert_single_event(standalone.fe_instance(), &query).await;

    controller_client
        .delete_topic(&topic_name, 5_000)
        .await
        .unwrap();
}
