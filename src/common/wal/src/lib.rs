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

use error::{EndpointIpNotFoundSnafu, ResolveEndpointSnafu, Result};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};

pub mod config;
pub mod error;
pub mod options;
#[cfg(any(test, feature = "testing"))]
pub mod test_util;

pub const BROKER_ENDPOINT: &str = "127.0.0.1:9092";
pub const TOPIC_NAME_PREFIX: &str = "greptimedb_wal_topic";

/// The type of the topic selector, i.e. with which strategy to select a topic.
// The enum is defined here to work around cyclic dependency issues.
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TopicSelectorType {
    #[default]
    RoundRobin,
}

pub async fn resolve_broker_endpoint(broker_endpoint: &str) -> Result<String> {
    let ip = tokio::net::lookup_host(broker_endpoint)
        .await
        .with_context(|_| ResolveEndpointSnafu {
            broker_endpoint: broker_endpoint.to_string(),
        })?
        // only IPv4 addresses are valid
        .find(|addr| addr.is_ipv4())
        .with_context(|| EndpointIpNotFoundSnafu {
            broker_endpoint: broker_endpoint.to_string(),
        })?;
    Ok(ip.to_string())
}
