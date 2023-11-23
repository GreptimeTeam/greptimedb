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

use rskafka::client::ClientBuilder;
use snafu::{ensure, OptionExt, ResultExt};

use crate::error::{
    BuildKafkaClientSnafu, BuildKafkaCtrlClientSnafu, CreateKafkaTopicSnafu, DeserKafkaTopicsSnafu,
    InvalidNumTopicsSnafu, MissingKafkaOptsSnafu, PersistKafkaTopicsSnafu, Result,
    SerKafkaTopicsSnafu, TooManyCreatedKafkaTopicsSnafu,
};
use crate::kv_backend::KvBackendRef;
use crate::wal::kafka::topic_selector::{build_topic_selector, TopicSelectorRef};
use crate::wal::kafka::KafkaOptions;

pub type Topic = String;

const TOPICS_KEY: &str = "gt_kafka_topics";
const CREATE_TOPIC_TIMEOUT: i32 = 5_000; // 5,000 ms.

pub struct TopicManager {
    topic_pool: Vec<Topic>,
    topic_selector: TopicSelectorRef,
}

impl TopicManager {
    pub async fn try_new(
        kafka_opts: &Option<KafkaOptions>,
        kv_backend: &KvBackendRef,
    ) -> Result<Self> {
        let opts = kafka_opts.as_ref().context(MissingKafkaOptsSnafu)?;

        Ok(Self {
            topic_pool: build_topic_pool(opts, kv_backend).await?,
            topic_selector: build_topic_selector(&opts.selector_type),
        })
    }

    pub fn select_topics(&self, num_regions: usize) -> Vec<Topic> {
        (0..num_regions)
            .map(|_| self.topic_selector.select(&self.topic_pool))
            .collect()
    }
}

async fn build_topic_pool(opts: &KafkaOptions, kv_backend: &KvBackendRef) -> Result<Vec<Topic>> {
    let KafkaOptions {
        broker_endpoints,
        num_topics,
        topic_name_prefix,
        num_partitions,
        replication_factor,
        ..
    } = opts.clone();

    ensure!(num_topics > 0, InvalidNumTopicsSnafu { num_topics });

    let kafka_client = ClientBuilder::new(broker_endpoints.clone())
        .build()
        .await
        .context(BuildKafkaClientSnafu { broker_endpoints })?;

    let kafka_ctrl_client = kafka_client
        .controller_client()
        .context(BuildKafkaCtrlClientSnafu)?;

    let topics = (0..num_topics)
        .map(|topic_id| format!("{topic_name_prefix}_{topic_id}"))
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

            Some(kafka_ctrl_client.create_topic(
                topic,
                num_partitions,
                replication_factor,
                CREATE_TOPIC_TIMEOUT,
            ))
        })
        .collect::<Vec<_>>();

    let _ = futures::future::try_join_all(create_topic_tasks)
        .await
        .context(CreateKafkaTopicSnafu)?;

    // FIXME(niebayes): current persistence strategy is all-or-none. Maybe we should increase the granularity.
    persist_created_topics(&topics, kv_backend).await?;

    Ok(topics)
}

async fn restore_created_topics(kv_backend: &KvBackendRef) -> Result<Vec<Topic>> {
    let raw_topics = kv_backend
        .get(TOPICS_KEY.as_bytes())
        .await?
        .map(|key_value| key_value.value)
        .unwrap_or_default();

    serde_json::from_slice(&raw_topics).context(DeserKafkaTopicsSnafu)
}

async fn persist_created_topics(topics: &[Topic], kv_backend: &KvBackendRef) -> Result<()> {
    let raw_topics = serde_json::to_string(topics).context(SerKafkaTopicsSnafu)?;
    kv_backend
        .put_conditionally(
            TOPICS_KEY.as_bytes().to_vec(),
            raw_topics.into_bytes(),
            false,
        )
        .await
        .and_then(|persisted| {
            if !persisted {
                PersistKafkaTopicsSnafu.fail()
            } else {
                Ok(())
            }
        })
}
