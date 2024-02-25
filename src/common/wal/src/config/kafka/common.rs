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

use std::time::Duration;

use serde::{de, Deserialize, Serialize};
use serde_with::with_prefix;

with_prefix!(pub backoff_prefix "backoff_");

/// Backoff configurations for kafka clients.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct BackoffConfig {
    /// The initial backoff delay.
    #[serde(with = "humantime_serde")]
    pub init: Duration,
    /// The maximum backoff delay.
    #[serde(with = "humantime_serde")]
    pub max: Duration,
    /// The exponential backoff rate, i.e. next backoff = base * current backoff.
    pub base: u32,
    /// The deadline of retries. `None` stands for no deadline.
    #[serde(with = "humantime_serde")]
    pub deadline: Option<Duration>,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            init: Duration::from_millis(500),
            max: Duration::from_secs(10),
            base: 2,
            deadline: Some(Duration::from_secs(60 * 5)), // 5 mins
        }
    }
}
fn lookup_endpoint<'de, D>(endpoint: &str) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let mut iter = endpoint.split(':');
    let ip_or_domain: &str = iter.next().unwrap();
    let port: &str = iter.next().unwrap();
    if ip_or_domain.parse::<std::net::IpAddr>().is_ok() {
        return Ok(endpoint.to_string());
    }
    let ips: Vec<_> = dns_lookup::lookup_host(ip_or_domain)
        .map_err(de::Error::custom)?
        .into_iter()
        .filter(|addr| addr.is_ipv4())
        .collect();
    if ips.is_empty() {
        return Err(de::Error::custom(format!(
            "failed to resolve the domain name: {}",
            ip_or_domain
        )));
    }
    Ok(format!("{}:{}", ips[0], port))
}
pub fn deserialize_broker_endpoints<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let mut broker_endpoints: Vec<String> = Vec::deserialize(deserializer)?;
    for endpoint in &mut broker_endpoints {
        *endpoint = lookup_endpoint::<D>(endpoint)?;
    }
    Ok(broker_endpoints)
}
