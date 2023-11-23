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

use std::collections::HashSet;

use rand::seq::SliceRandom;
use rskafka::client::ClientBuilder;
use snafu::{ensure, OptionExt, ResultExt};

use crate::error::{
    BuildKafkaClientSnafu, BuildKafkaCtrlClientSnafu, CreateKafkaTopicSnafu, DeserKafkaTopicsSnafu,
    InvalidNumTopicsSnafu, MissingKafkaOptsSnafu, Result, SerKafkaTopicsSnafu,
    TooManyCreatedKafkaTopicsSnafu,
};
use crate::kv_backend::KvBackendRef;
use crate::rpc::store::PutRequest;
use crate::wal::kafka::topic_selector::{build_topic_selector, TopicSelectorRef};
use crate::wal::kafka::{KafkaOptions, TopicSelectorType};

pub type Topic = String;

const METASRV_CREATED_TOPICS_KEY: &str = "metasrv_created_topics";
const CREATE_TOPIC_TIMEOUT: i32 = 5_000; // 5,000 ms.

pub struct TopicManager {
    topic_pool: Vec<Topic>,
    topic_selector: TopicSelectorRef,
}

impl TopicManager {
    pub async fn try_new(
        kafka_opts: Option<&KafkaOptions>,
        kv_backend: &KvBackendRef,
    ) -> Result<Self> {
        let opts = kafka_opts.context(MissingKafkaOptsSnafu)?;
        let topic_pool = build_topic_pool(opts, kv_backend).await?;
        let topic_selector = build_topic_selector(&opts.selector_type);

        // The cursor in the round-robin selector is not persisted which may break the round-robin strategy cross crashes.
        // Introduces a shuffling may help mitigate this issue.
        let topic_pool = match opts.selector_type {
            TopicSelectorType::RoundRobin => shuffle_topic_pool(topic_pool),
        };

        Ok(Self {
            topic_pool,
            topic_selector,
        })
    }

    pub fn select_topics(&self, num_regions: usize) -> Vec<Topic> {
        (0..num_regions)
            .map(|_| self.topic_selector.select(&self.topic_pool))
            .collect()
    }
}

async fn build_topic_pool(opts: &KafkaOptions, kv_backend: &KvBackendRef) -> Result<Vec<Topic>> {
    let num_topics = opts.num_topics;
    ensure!(num_topics > 0, InvalidNumTopicsSnafu { num_topics });

    let broker_endpoints = opts.broker_endpoints.clone();
    let kafka_client = ClientBuilder::new(broker_endpoints.clone())
        .build()
        .await
        .context(BuildKafkaClientSnafu { broker_endpoints })?;

    let kafka_ctrl_client = kafka_client
        .controller_client()
        .context(BuildKafkaCtrlClientSnafu)?;

    let topics = (0..num_topics)
        .map(|topic_id| format!("{}_{topic_id}", opts.topic_name_prefix))
        .collect::<Vec<_>>();

    let created_topics = restore_created_topics(kv_backend)
        .await?
        .into_iter()
        .collect::<HashSet<Topic>>();

    let num_created_topics = created_topics.len();
    ensure!(
        num_created_topics <= num_topics,
        TooManyCreatedKafkaTopicsSnafu { num_created_topics }
    );

    let create_topic_tasks = topics
        .iter()
        .filter_map(|topic| {
            if created_topics.contains(topic) {
                return None;
            }

            // TODO(niebayes): Determine how rskafka handles an already-exist topic. Check if an error would be raised.
            Some(kafka_ctrl_client.create_topic(
                topic,
                opts.num_partitions,
                opts.replication_factor,
                CREATE_TOPIC_TIMEOUT,
            ))
        })
        .collect::<Vec<_>>();

    futures::future::try_join_all(create_topic_tasks)
        .await
        .context(CreateKafkaTopicSnafu)?;

    persist_created_topics(&topics, kv_backend).await?;

    Ok(topics)
}

fn shuffle_topic_pool(mut topic_pool: Vec<Topic>) -> Vec<Topic> {
    topic_pool.shuffle(&mut rand::thread_rng());
    topic_pool
}

async fn restore_created_topics(kv_backend: &KvBackendRef) -> Result<Vec<Topic>> {
    kv_backend
        .get(METASRV_CREATED_TOPICS_KEY.as_bytes())
        .await?
        .map(|key_value| serde_json::from_slice(&key_value.value).context(DeserKafkaTopicsSnafu))
        .unwrap_or_else(|| Ok(vec![]))
}

async fn persist_created_topics(topics: &[Topic], kv_backend: &KvBackendRef) -> Result<()> {
    let raw_topics = serde_json::to_string(topics).context(SerKafkaTopicsSnafu)?;
    kv_backend
        .put(PutRequest {
            key: METASRV_CREATED_TOPICS_KEY.as_bytes().to_vec(),
            value: raw_topics.into_bytes(),
            prev_kv: false,
        })
        .await
        .map(|_| ())
}
