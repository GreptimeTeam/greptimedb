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

use std::io;
use std::net::SocketAddr;

use async_trait::async_trait;
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

#[mockall::automock]
#[async_trait]
trait DNSResolver {
    async fn resolve_broker_endpoint(&self, broker_endpoint: &str) -> io::Result<Vec<SocketAddr>>;
}
struct TokioDnsResolver;
#[async_trait]
impl DNSResolver for TokioDnsResolver {
    async fn resolve_broker_endpoint(&self, broker_endpoint: &str) -> io::Result<Vec<SocketAddr>> {
        net::lookup_host(broker_endpoint)
            .await
            .map(|iter| iter.collect())
    }
}
async fn resolve_broker_endpoint_inner<R: DNSResolver>(
    broker_endpoint: &str,
    resolver: R,
) -> Result<String> {
    resolver
        .resolve_broker_endpoint(broker_endpoint)
        .await
        .with_context(|_| ResolveEndpointSnafu {
            broker_endpoint: broker_endpoint.to_string(),
        })?
        .into_iter()
        // only IPv4 addresses are valid
        .find(|addr| addr.is_ipv4())
        .map(|addr| addr.to_string())
        .with_context(|| EndpointIPV4NotFoundSnafu {
            broker_endpoint: broker_endpoint.to_string(),
        })
}
pub async fn resolve_broker_endpoint(broker_endpoint: &str) -> Result<String> {
    resolve_broker_endpoint_inner(broker_endpoint, TokioDnsResolver).await
}

#[cfg(test)]
mod tests {
    use mockall::predicate::eq;

    use super::*;
    use crate::error::Error;
    // test for resolve_broker_endpoint
    #[tokio::test]
    async fn test_resolve_error_occur() {
        let endpoint = "example.com:9092";
        let mut mock_resolver = MockDNSResolver::new();
        mock_resolver
            .expect_resolve_broker_endpoint()
            .with(eq("example.com:9092"))
            .times(1)
            .returning(|_| {
                Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "mocked error",
                ))
            });
        let ip = resolve_broker_endpoint_inner(endpoint, mock_resolver).await;
        assert!(ip.is_err_and(|err| { matches!(err, Error::ResolveEndpoint { .. }) }))
    }
    #[tokio::test]
    async fn test_resolve_only_ipv6() {
        let endpoint = "example.com:9092";
        let mut mock_resolver = MockDNSResolver::new();
        mock_resolver
            .expect_resolve_broker_endpoint()
            .with(eq("example.com:9092"))
            .times(1)
            .returning(|_| {
                Ok(vec![SocketAddr::new(
                    std::net::IpAddr::V6(std::net::Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)),
                    9092,
                )])
            });
        let ip = resolve_broker_endpoint_inner(endpoint, mock_resolver).await;
        assert!(ip.is_err_and(|err| { matches!(err, Error::EndpointIPV4NotFound { .. }) }))
    }
    #[tokio::test]
    async fn test_resolve_normal() {
        let mut mock_resolver = MockDNSResolver::new();
        mock_resolver
            .expect_resolve_broker_endpoint()
            .with(eq("example.com:9092"))
            .times(1)
            .returning(|_| Ok(vec!["127.0.0.1:9092".parse().unwrap()]));
        let ip = resolve_broker_endpoint_inner("example.com:9092", mock_resolver)
            .await
            .unwrap();
        assert_eq!(ip, "127.0.0.1:9092")
    }
}
