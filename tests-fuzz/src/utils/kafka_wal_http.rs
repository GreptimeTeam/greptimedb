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

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::error::{self, Result};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TopicOffset {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RecordOffsetsRequest {
    pub broker_endpoints: Vec<String>,
    pub topic_prefix: String,
    pub num_topics: usize,
    #[serde(default)]
    pub partition: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteRecordsRequest {
    pub broker_endpoints: Vec<String>,
    pub offsets: Vec<TopicOffset>,
    pub timeout_ms: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RecordOffsetsResponse {
    pub offsets: Vec<TopicOffset>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteRecordsResponse {
    pub deleted: Vec<TopicOffset>,
}

#[async_trait]
pub trait WalAdmin: Send + Sync {
    async fn record_topic_latest_offsets(&self) -> Result<Vec<TopicOffset>>;

    async fn delete_records_to_offsets(&self, offsets: &[TopicOffset]) -> Result<()>;
}

pub struct HttpWalAdmin {
    client: reqwest::Client,
    base_url: String,
    broker_endpoints: Vec<String>,
    topic_prefix: String,
    num_topics: usize,
    partition: i32,
    delete_records_timeout_ms: i32,
}

pub struct HttpWalAdminConfig {
    pub base_url: String,
    pub broker_endpoints: Vec<String>,
    pub topic_prefix: String,
    pub num_topics: usize,
    pub partition: i32,
    pub delete_records_timeout_ms: i32,
}

impl HttpWalAdmin {
    pub fn new(config: HttpWalAdminConfig) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: config.base_url,
            broker_endpoints: config.broker_endpoints,
            topic_prefix: config.topic_prefix,
            num_topics: config.num_topics,
            partition: config.partition,
            delete_records_timeout_ms: config.delete_records_timeout_ms,
        }
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}{}", self.base_url.trim_end_matches('/'), path)
    }
}

#[async_trait]
impl WalAdmin for HttpWalAdmin {
    async fn record_topic_latest_offsets(&self) -> Result<Vec<TopicOffset>> {
        let request = RecordOffsetsRequest {
            broker_endpoints: self.broker_endpoints.clone(),
            topic_prefix: self.topic_prefix.clone(),
            num_topics: self.num_topics,
            partition: self.partition,
        };
        let response = self
            .client
            .post(self.endpoint("/record-offsets"))
            .json(&request)
            .send()
            .await
            .context(error::KafkaWalHelperRequestSnafu {
                operation: "record-offsets".to_string(),
            })?;
        ensure_success(response, "record-offsets")
            .await?
            .json::<RecordOffsetsResponse>()
            .await
            .context(error::KafkaWalHelperRequestSnafu {
                operation: "record-offsets".to_string(),
            })
            .map(|response| response.offsets)
    }

    async fn delete_records_to_offsets(&self, offsets: &[TopicOffset]) -> Result<()> {
        let request = DeleteRecordsRequest {
            broker_endpoints: self.broker_endpoints.clone(),
            offsets: offsets.to_vec(),
            timeout_ms: Some(self.delete_records_timeout_ms),
        };
        let response = self
            .client
            .post(self.endpoint("/delete-records"))
            .json(&request)
            .send()
            .await
            .context(error::KafkaWalHelperRequestSnafu {
                operation: "delete-records".to_string(),
            })?;
        ensure_success(response, "delete-records")
            .await?
            .json::<DeleteRecordsResponse>()
            .await
            .context(error::KafkaWalHelperRequestSnafu {
                operation: "delete-records".to_string(),
            })
            .map(|_| ())
    }
}

async fn ensure_success(response: reqwest::Response, operation: &str) -> Result<reqwest::Response> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }

    let body = response.text().await.unwrap_or_default();
    error::KafkaWalHelperStatusSnafu {
        operation: operation.to_string(),
        status,
        body,
    }
    .fail()
}
