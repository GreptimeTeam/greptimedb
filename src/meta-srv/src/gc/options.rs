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

use serde::{Deserialize, Serialize};
use snafu::ensure;

use crate::error::{self, Result};

/// The interval of the gc ticker.
#[allow(unused)]
pub(crate) const TICKER_INTERVAL: Duration = Duration::from_secs(60 * 5);

/// Configuration for GC operations.
///
/// TODO(discord9): not expose most config to users for now, until GC scheduler is fully stable.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct GcSchedulerOptions {
    /// Whether GC is enabled. Default to false.
    /// If set to false, no GC will be performed, and potentially some
    /// files from datanodes will never be deleted.
    pub enable: bool,
    /// Maximum number of tables to process concurrently.
    pub max_concurrent_tables: usize,
    /// Maximum number of retries per region when GC fails.
    pub max_retries_per_region: usize,
    /// Concurrency for region GC within a table.
    pub region_gc_concurrency: usize,
    /// Backoff duration between retries.
    pub retry_backoff_duration: Duration,
    /// Minimum region size threshold for GC (in bytes).
    pub min_region_size_threshold: u64,
    /// Weight for SST file count in GC scoring.
    pub sst_count_weight: f64,
    /// Weight for file removal rate in GC scoring.
    pub file_removed_count_weight: f64,
    /// Cooldown period between GC operations on the same region.
    pub gc_cooldown_period: Duration,
    /// Maximum number of regions to select for GC per table.
    pub regions_per_table_threshold: usize,
    /// Timeout duration for mailbox communication with datanodes.
    pub mailbox_timeout: Duration,
    /// Interval for performing full file listing during GC to find orphan files.
    /// Full file listing is expensive but necessary to clean up orphan files.
    /// Set to a larger value (e.g., 24 hours) to balance performance and cleanup.
    /// Every Nth GC cycle will use full file listing, where N = full_file_listing_interval / TICKER_INTERVAL.
    pub full_file_listing_interval: Duration,
    /// Interval for cleaning up stale region entries from the GC tracker.
    /// This removes entries for regions that no longer exist (e.g., after table drops).
    /// Set to a larger value (e.g., 6 hours) since this is just for memory cleanup.
    pub tracker_cleanup_interval: Duration,
}

impl Default for GcSchedulerOptions {
    fn default() -> Self {
        Self {
            enable: false,
            max_concurrent_tables: 10,
            max_retries_per_region: 3,
            retry_backoff_duration: Duration::from_secs(5),
            region_gc_concurrency: 16,
            min_region_size_threshold: 100 * 1024 * 1024, // 100MB
            sst_count_weight: 1.0,
            file_removed_count_weight: 0.5,
            gc_cooldown_period: Duration::from_secs(60 * 5), // 5 minutes
            regions_per_table_threshold: 20,                 // Select top 20 regions per table
            mailbox_timeout: Duration::from_secs(60),        // 60 seconds
            // Perform full file listing every 24 hours to find orphan files
            full_file_listing_interval: Duration::from_secs(60 * 60 * 24),
            // Clean up stale tracker entries every 6 hours
            tracker_cleanup_interval: Duration::from_secs(60 * 60 * 6),
        }
    }
}

impl GcSchedulerOptions {
    /// Validates the configuration options.
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.max_concurrent_tables > 0,
            error::InvalidArgumentsSnafu {
                err_msg: "max_concurrent_tables must be greater than 0",
            }
        );

        ensure!(
            self.max_retries_per_region > 0,
            error::InvalidArgumentsSnafu {
                err_msg: "max_retries_per_region must be greater than 0",
            }
        );

        ensure!(
            self.region_gc_concurrency > 0,
            error::InvalidArgumentsSnafu {
                err_msg: "region_gc_concurrency must be greater than 0",
            }
        );

        ensure!(
            !self.retry_backoff_duration.is_zero(),
            error::InvalidArgumentsSnafu {
                err_msg: "retry_backoff_duration must be greater than 0",
            }
        );

        ensure!(
            self.sst_count_weight >= 0.0,
            error::InvalidArgumentsSnafu {
                err_msg: "sst_count_weight must be non-negative",
            }
        );

        ensure!(
            self.file_removed_count_weight >= 0.0,
            error::InvalidArgumentsSnafu {
                err_msg: "file_removal_rate_weight must be non-negative",
            }
        );

        ensure!(
            !self.gc_cooldown_period.is_zero(),
            error::InvalidArgumentsSnafu {
                err_msg: "gc_cooldown_period must be greater than 0",
            }
        );

        ensure!(
            self.regions_per_table_threshold > 0,
            error::InvalidArgumentsSnafu {
                err_msg: "regions_per_table_threshold must be greater than 0",
            }
        );

        ensure!(
            !self.mailbox_timeout.is_zero(),
            error::InvalidArgumentsSnafu {
                err_msg: "mailbox_timeout must be greater than 0",
            }
        );

        ensure!(
            !self.full_file_listing_interval.is_zero(),
            error::InvalidArgumentsSnafu {
                err_msg: "full_file_listing_interval must be greater than 0",
            }
        );

        ensure!(
            !self.tracker_cleanup_interval.is_zero(),
            error::InvalidArgumentsSnafu {
                err_msg: "tracker_cleanup_interval must be greater than 0",
            }
        );

        Ok(())
    }
}
