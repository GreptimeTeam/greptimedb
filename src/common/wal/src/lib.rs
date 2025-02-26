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

#![feature(assert_matches)]

use std::net::SocketAddr;

use error::{EndpointIPV4NotFoundSnafu, ResolveEndpointSnafu, Result};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use tokio::net;

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

pub async fn resolve_to_ipv4<T: AsRef<str>>(endpoints: &[T]) -> Result<Vec<String>> {
    futures_util::future::try_join_all(endpoints.iter().map(resolve_to_ipv4_one)).await
}

async fn resolve_to_ipv4_one<T: AsRef<str>>(endpoint: T) -> Result<String> {
    let endpoint = endpoint.as_ref();
    net::lookup_host(endpoint)
        .await
        .context(ResolveEndpointSnafu {
            broker_endpoint: endpoint,
        })?
        .find(SocketAddr::is_ipv4)
        .map(|addr| addr.to_string())
        .context(EndpointIPV4NotFoundSnafu {
            broker_endpoint: endpoint,
        })
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use common_telemetry::warn;
    use rskafka::client::{Credentials, SaslConfig};

    use super::*;
    use crate::error::Error;

    // test for resolve_broker_endpoint
    #[tokio::test]
    async fn test_valid_host() {
        let host = "localhost:9092";
        let got = resolve_to_ipv4_one(host).await;
        assert_eq!(got.unwrap(), "127.0.0.1:9092");
    }

    #[tokio::test]
    async fn test_valid_host_ipv6() {
        // the host is valid, it is an IPv6 address, but we only accept IPv4 addresses
        let host = "::1:9092";
        let got = resolve_to_ipv4_one(host).await;
        assert_matches!(got.unwrap_err(), Error::EndpointIPV4NotFound { .. });
    }

    #[tokio::test]
    async fn test_invalid_host() {
        let host = "non-exist-host:9092";
        let got = resolve_to_ipv4_one(host).await;
        assert_matches!(got.unwrap_err(), Error::ResolveEndpoint { .. });
    }

    #[tokio::test]
    async fn test_sasl() {
        common_telemetry::init_default_ut_logging();
        let Ok(broker_endpoints) = std::env::var("GT_KAFKA_SASL_ENDPOINTS") else {
            warn!("The endpoints is empty, skipping the test 'test_sasl'");
            return;
        };
        let broker_endpoints = broker_endpoints
            .split(',')
            .map(|s| s.trim().to_string())
            .collect::<Vec<_>>();

        let username = "user_kafka";
        let password = "secret";
        let _ = rskafka::client::ClientBuilder::new(broker_endpoints.clone())
            .sasl_config(SaslConfig::Plain(Credentials::new(
                username.to_string(),
                password.to_string(),
            )))
            .build()
            .await
            .unwrap();
        let _ = rskafka::client::ClientBuilder::new(broker_endpoints.clone())
            .sasl_config(SaslConfig::ScramSha256(Credentials::new(
                username.to_string(),
                password.to_string(),
            )))
            .build()
            .await
            .unwrap();
        let _ = rskafka::client::ClientBuilder::new(broker_endpoints)
            .sasl_config(SaslConfig::ScramSha512(Credentials::new(
                username.to_string(),
                password.to_string(),
            )))
            .build()
            .await
            .unwrap();
    }
}
