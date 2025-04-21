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

use chrono::{TimeZone, Utc};
use common_wal::config::kafka::common::KafkaConnectionConfig;
use common_wal::config::kafka::DatanodeKafkaConfig;
use dashmap::DashMap;
use rskafka::client::ClientBuilder;
use rskafka::record::Record;

use crate::kafka::client_manager::ClientManager;

/// Creates `num_topics` number of topics each will be decorated by the given decorator.
pub(crate) async fn create_topics<F>(
    num_topics: usize,
    decorator: F,
    broker_endpoints: &[String],
) -> Vec<String>
where
    F: Fn(usize) -> String,
{
    assert!(!broker_endpoints.is_empty());
    let client = ClientBuilder::new(broker_endpoints.to_vec())
        .build()
        .await
        .unwrap();
    let ctrl_client = client.controller_client().unwrap();
    let (topics, tasks): (Vec<_>, Vec<_>) = (0..num_topics)
        .map(|i| {
            let topic = decorator(i);
            let task = ctrl_client.create_topic(topic.clone(), 1, 1, 500);
            (topic, task)
        })
        .unzip();
    futures::future::try_join_all(tasks).await.unwrap();
    topics
}

/// Prepares for a test in that a collection of topics and a client manager are created.
pub(crate) async fn prepare(
    test_name: &str,
    num_topics: usize,
    broker_endpoints: Vec<String>,
) -> (ClientManager, Vec<String>) {
    let topics = create_topics(
        num_topics,
        |i| format!("{test_name}_{}_{}", i, uuid::Uuid::new_v4()),
        &broker_endpoints,
    )
    .await;

    let config = DatanodeKafkaConfig {
        connection: KafkaConnectionConfig {
            broker_endpoints,
            ..Default::default()
        },
        ..Default::default()
    };
    let high_watermark = Arc::new(DashMap::new());
    let manager = ClientManager::try_new(&config, None, high_watermark)
        .await
        .unwrap();

    (manager, topics)
}

/// Generate a record to produce.
pub fn record() -> Record {
    Record {
        key: Some(vec![0; 4]),
        value: Some(vec![0; 6]),
        headers: Default::default(),
        timestamp: Utc.timestamp_millis_opt(320).unwrap(),
    }
}
