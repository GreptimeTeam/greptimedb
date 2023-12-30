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

pub mod entry_builder;
pub mod topic_builder;

use common_config::wal::KafkaWalTopic as Topic;
use rskafka::client::ClientBuilder;

pub use crate::test_util::kafka::entry_builder::EntryBuilder;
pub use crate::test_util::kafka::topic_builder::TopicBuilder;

/// Gets broker endpoints from environment variables with the given key.
#[macro_export]
macro_rules! get_broker_endpoints_from_env {
    ($key:expr) => {{
        let broker_endpoints = std::env::var($key)
            .unwrap_or("localhost:9092".to_string())
            .split(',')
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        assert!(!broker_endpoints.is_empty());
        broker_endpoints
    }};
}

/// Creates `num_topiocs` number of topics with the given TopicBuilder.
/// Requests for creating these topics on the Kafka cluster specified with the `broker_endpoints`.
pub async fn create_topics(
    num_topics: usize,
    mut builder: TopicBuilder,
    broker_endpoints: &[String],
) -> Vec<Topic> {
    assert!(!broker_endpoints.is_empty());

    let client = ClientBuilder::new(broker_endpoints.to_vec())
        .build()
        .await
        .unwrap();
    let ctrl_client = client.controller_client().unwrap();

    let (topics, tasks): (Vec<_>, Vec<_>) = (0..num_topics)
        .map(|i| {
            let topic = builder.build(&format!("topic_{i}"));
            let task = ctrl_client.create_topic(topic.clone(), 1, 1, 500);
            (topic, task)
        })
        .unzip();
    futures::future::try_join_all(tasks).await.unwrap();

    topics
}
