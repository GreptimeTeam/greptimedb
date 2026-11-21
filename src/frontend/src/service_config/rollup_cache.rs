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
/// The cache is disabled by default. When enabled, a range query whose expression is in
/// the window allowlist is cut into canonical chunks, and a chunk is reused while the
/// source table it reads has been proved by the background refresher.
///
/// The refresher probes the sequence of every source table a request asked for once per
/// `refresh_interval` and invalidates the chunks whose input the new rows reach. That
/// probe, which runs every second or so, is what keeps a served chunk fresh: `max_age` is
/// not the freshness mechanism, only the last-resort backstop behind it. The request path
/// never probes: it reads the table's readiness, and a table no round has proved yet
/// answers from storage.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct RollupCacheOptions {
    /// Whether range queries may be served from the cache.
    pub enable: bool,
    /// The total memory budget for cached chunks and their annotations.
    pub max_bytes: ReadableSize,
    /// The canonical chunk duration. A query whose step does not divide it
    /// bypasses the cache.
    #[serde(with = "humantime_serde")]
    pub chunk: Duration,
    /// Output points this close to wall-clock now may still be receiving data,
    /// so they are executed directly and never cached.
    #[serde(with = "humantime_serde")]
    pub tail: Duration,
    /// How often the background refresher probes an active source table.
    ///
    /// It bounds how stale a served chunk can be, and it is the only thing it
    /// controls: no request waits for a round, and no request measures one.
    #[serde(with = "humantime_serde")]
    pub refresh_interval: Duration,
    /// The last-resort age backstop, not a freshness bound.
    ///
    /// Freshness is the background refresher's job: it proves a source table's sequence
    /// once per `refresh_interval` — seconds — and drops every chunk whose input the new
    /// rows reach. This bound only covers what that probe cannot describe: a truncate, a
    /// TTL or compaction removal, a DELETE mixed with a distant PUT, a physical table
    /// renamed or reused, and a refresher that stalled without failing. Nothing else would
    /// ever take such an entry out, so this is the backstop of last resort. It does not
    /// have to be long: the cache is in memory and every entry dies with the process, so
    /// an hour is enough for that role, and shortening it only adds misses.
    #[serde(with = "humantime_serde")]
    pub max_age: Duration,
}

impl Default for RollupCacheOptions {
    fn default() -> Self {
        Self {
            enable: false,
            max_bytes: ReadableSize::mb(256),
            chunk: Duration::from_secs(300),
            tail: Duration::from_secs(600),
            refresh_interval: Duration::from_secs(1),
            max_age: Duration::from_secs(3_600),
        }
    }
}

impl RollupCacheOptions {
    /// Validates the options and builds the cache configuration.
    pub fn config(&self) -> Result<RollupCacheConfig, ConfigError> {
        RollupCacheConfig::new(
            self.max_bytes.0,
            self.tail,
            self.chunk,
            self.refresh_interval,
            self.max_age,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_disabled_and_valid() {
        let options = RollupCacheOptions::default();
        assert!(!options.enable);
        assert_eq!(Duration::from_secs(1), options.refresh_interval);
        assert!(options.config().is_ok());
    }

    #[test]
    fn the_default_max_age_is_a_last_resort_backstop() {
        let options = RollupCacheOptions::default();
        // One hour: long enough that the refresher, never this bound, decides freshness.
        assert_eq!(Duration::from_secs(3_600), options.max_age);
        assert!(
            options.max_age > options.refresh_interval * 1_000,
            "the age backstop stays orders of magnitude longer than one refresh round"
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
            refresh_interval: Duration::ZERO,
            ..Default::default()
        };
        assert_eq!(options.config(), Err(ConfigError::RefreshInterval));

        let options = RollupCacheOptions {
            max_age: Duration::ZERO,
            ..Default::default()
        };
        assert_eq!(options.config(), Err(ConfigError::MaxAge));
    }
}
