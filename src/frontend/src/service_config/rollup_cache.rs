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

use common_base::readable_size::ReadableSize;
use serde::{Deserialize, Serialize};

use crate::query_cache::{ConfigError, RollupCacheConfig};

/// Controls the frontend-local cache of PromQL range-query chunks.
///
/// The cache is disabled by default. When enabled, a range query whose expression is
/// in the window allowlist is cut into canonical chunks, and a chunk a request has
/// read is reused until it reaches `ttl`.
///
/// Nothing probes the source: a chunk is served without the frontend asking whether
/// the data behind it moved, and `ttl` is the whole freshness rule. The cache is
/// therefore a bounded-staleness one — a write reaches a served chunk within `ttl`,
/// and a late write can stay invisible for that long — which is the trade it makes
/// for never reading the source on the request path.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct RollupCacheOptions {
    /// Whether range queries may be served from the cache.
    pub enable: bool,
    /// The total memory budget for cached chunks and their annotations.
    pub max_bytes: ReadableSize,
    /// The target chunk duration. It is global while the step belongs to the query,
    /// so planning aligns it down to a whole number of the query's steps, at least
    /// one: any step is cacheable, and the actual chunk stays fixed for that step.
    #[serde(with = "humantime_serde")]
    pub chunk: Duration,
    /// Output points this close to wall-clock now may still be receiving data,
    /// so they are executed directly and never cached.
    #[serde(with = "humantime_serde")]
    pub tail: Duration,
    /// How long a cached chunk is served, counted from the read that produced it.
    ///
    /// It is the cache's whole freshness rule: no probe decides that a chunk is still
    /// current, so this age alone bounds how stale a served answer may be. An entry
    /// reaching it is dropped, and the next request for that chunk reads storage.
    #[serde(with = "humantime_serde")]
    pub ttl: Duration,
    /// Optional admin token for `POST /internal/resetRollupResultCache`.
    ///
    /// `None` disables the endpoint. Resetting only clears this frontend's
    /// process-local cache; it does not touch source data or other frontends.
    pub reset_token: Option<String>,
}

impl Default for RollupCacheOptions {
    fn default() -> Self {
        Self {
            enable: false,
            max_bytes: ReadableSize::mb(256),
            chunk: Duration::from_secs(300),
            tail: Duration::from_secs(600),
            ttl: Duration::from_secs(3_600),
            reset_token: None,
        }
    }
}

impl RollupCacheOptions {
    /// Validates the options and builds the cache configuration.
    pub fn config(&self) -> Result<RollupCacheConfig, ConfigError> {
        RollupCacheConfig::new(self.max_bytes.0, self.tail, self.chunk, self.ttl)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_disabled_and_valid() {
        let options = RollupCacheOptions::default();
        assert!(!options.enable);
        assert_eq!(ReadableSize::mb(256), options.max_bytes);
        assert_eq!(Duration::from_secs(300), options.chunk);
        assert_eq!(Duration::from_secs(600), options.tail);
        assert_eq!(Duration::from_secs(3_600), options.ttl);
        assert!(options.config().is_ok());
    }

    #[test]
    fn the_ttl_is_the_whole_freshness_bound() {
        let options = RollupCacheOptions::default();
        // An hour, and nothing else bounds staleness: no probe shortens it, so the
        // configured age is the age a served chunk may reach.
        assert_eq!(options.ttl, options.config().unwrap().ttl());
        assert!(
            options.ttl > options.tail,
            "a chunk has to outlive the tail it was read behind"
        );
    }

    #[test]
    fn zero_bounds_are_rejected() {
        let options = RollupCacheOptions {
            max_bytes: ReadableSize(0),
            ..Default::default()
        };
        assert_eq!(options.config(), Err(ConfigError::Bytes));

        let options = RollupCacheOptions {
            chunk: Duration::ZERO,
            ..Default::default()
        };
        assert_eq!(options.config(), Err(ConfigError::Chunk));

        let options = RollupCacheOptions {
            ttl: Duration::ZERO,
            ..Default::default()
        };
        assert_eq!(options.config(), Err(ConfigError::Ttl));
    }
}
