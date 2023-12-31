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
pub mod topic_decorator;

use common_config::wal::KafkaWalTopic as Topic;
use rskafka::client::ClientBuilder;

use crate::kafka::EntryImpl;
pub use crate::test_util::kafka::entry_builder::EntryBuilder;
pub use crate::test_util::kafka::topic_decorator::{Affix, TopicDecorator};

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

/// Creates `num_topiocs` number of topics from the seed topic which are going to be decorated with the given TopicDecorator.
pub async fn create_topics(
    num_topics: usize,
    mut decorator: TopicDecorator,
    broker_endpoints: &[String],
    seed: Option<&str>,
) -> Vec<Topic> {
    assert!(!broker_endpoints.is_empty());

    let client = ClientBuilder::new(broker_endpoints.to_vec())
        .build()
        .await
        .unwrap();
    let ctrl_client = client.controller_client().unwrap();

    let seed = seed.unwrap_or("topic");
    let (topics, tasks): (Vec<_>, Vec<_>) = (0..num_topics)
        .map(|i| {
            let topic = decorator.decorate(&format!("{seed}_{i}"));
            let task = ctrl_client.create_topic(topic.clone(), 1, 1, 500);
            (topic, task)
        })
        .unzip();
    futures::future::try_join_all(tasks).await.unwrap();

    topics
}

/// Builds a batch of entries each with random data.
pub fn entries_with_random_data(batch_size: usize, builder: &EntryBuilder) -> Vec<EntryImpl> {
    (0..batch_size)
        .map(|_| builder.with_random_data())
        .collect()
}
