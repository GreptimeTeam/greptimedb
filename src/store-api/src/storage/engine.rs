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

//! Storage Engine traits.
//!
//! [`StorageEngine`] is the abstraction over a multi-regions, schematized data storage system,
//! a [`StorageEngine`] instance manages a bunch of storage unit called [`Region`], which holds
//! chunks of rows, support operations like PUT/DELETE/SCAN.

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use common_error::ext::ErrorExt;

use crate::storage::descriptors::RegionDescriptor;
use crate::storage::region::Region;

const COMPACTION_STRATEGY_KEY: &str = "compaction";
const COMPACTION_STRATEGY_TWCS_VALUE: &str = "TWCS";
const TWCS_MAX_ACTIVE_WINDOW_FILES_KEY: &str = "compaction.twcs.max_active_window_files";
const TWCS_TIME_WINDOW_SECONDS_KEY: &str = "compaction.twcs.time_window_seconds";
const TWCS_MAX_INACTIVE_WINDOW_FILES_KEY: &str = "compaction.twcs.max_inactive_window_files";

/// Storage engine provides primitive operations to store and access data.
#[async_trait]
pub trait StorageEngine: Send + Sync + Clone + 'static {
    type Error: ErrorExt + Send + Sync;
    type Region: Region;

    /// Opens an existing region. Returns `Ok(None)` if region does not exists.
    async fn open_region(
        &self,
        ctx: &EngineContext,
        name: &str,
        opts: &OpenOptions,
    ) -> Result<Option<Self::Region>, Self::Error>;

    /// Closes given region.
    async fn close_region(
        &self,
        ctx: &EngineContext,
        name: &str,
        opts: &CloseOptions,
    ) -> Result<(), Self::Error>;

    /// Creates and returns the created region.
    ///
    /// Returns existing region if region with same name already exists. The region will
    /// be opened before returning.
    async fn create_region(
        &self,
        ctx: &EngineContext,
        descriptor: RegionDescriptor,
        opts: &CreateOptions,
    ) -> Result<Self::Region, Self::Error>;

    /// Drops given region.
    ///
    /// The region will be closed before dropping.
    async fn drop_region(
        &self,
        ctx: &EngineContext,
        region: Self::Region,
    ) -> Result<(), Self::Error>;

    /// Returns the opened region with given name.
    fn get_region(
        &self,
        ctx: &EngineContext,
        name: &str,
    ) -> Result<Option<Self::Region>, Self::Error>;

    /// Close the engine.
    async fn close(&self, ctx: &EngineContext) -> Result<(), Self::Error>;
}

/// Storage engine context.
#[derive(Debug, Clone, Default)]
pub struct EngineContext {}

/// Options to create a region.
#[derive(Debug, Clone, Default)]
pub struct CreateOptions {
    /// Region parent directory
    pub parent_dir: String,
    /// Region memtable max size in bytes
    pub write_buffer_size: Option<usize>,
    /// Region SST files TTL
    pub ttl: Option<Duration>,
    /// Compaction strategy
    pub compaction_strategy: CompactionStrategy,
}

/// Options to open a region.
#[derive(Debug, Clone, Default)]
pub struct OpenOptions {
    /// Region parent directory
    pub parent_dir: String,
    /// Region memtable max size in bytes
    pub write_buffer_size: Option<usize>,
    /// Region SST files TTL
    pub ttl: Option<Duration>,
    /// Compaction strategy
    pub compaction_strategy: CompactionStrategy,
}

/// Options to close a region.
#[derive(Debug, Clone, Default)]
pub struct CloseOptions {
    /// Flush region
    pub flush: bool,
}

/// Options for compactions
#[derive(Debug, Clone)]
pub enum CompactionStrategy {
    /// TWCS
    Twcs(TwcsOptions),
}

impl Default for CompactionStrategy {
    fn default() -> Self {
        Self::Twcs(TwcsOptions::default())
    }
}

/// TWCS compaction options.
#[derive(Debug, Clone)]
pub struct TwcsOptions {
    /// Max num of files that can be kept in active writing time window.
    pub max_active_window_files: usize,
    /// Max num of files that can be kept in inactive time window.
    pub max_inactive_window_files: usize,
    /// Compaction time window defined when creating tables.
    pub time_window_seconds: Option<i64>,
}

impl Default for TwcsOptions {
    fn default() -> Self {
        Self {
            max_active_window_files: 4,
            max_inactive_window_files: 1,
            time_window_seconds: None,
        }
    }
}

impl From<&HashMap<String, String>> for CompactionStrategy {
    fn from(opts: &HashMap<String, String>) -> Self {
        let Some(strategy_name) = opts.get(COMPACTION_STRATEGY_KEY) else {
            return CompactionStrategy::default();
        };
        if strategy_name.eq_ignore_ascii_case(COMPACTION_STRATEGY_TWCS_VALUE) {
            let mut twcs_opts = TwcsOptions::default();
            if let Some(max_active_window_files) = opts
                .get(TWCS_MAX_ACTIVE_WINDOW_FILES_KEY)
                .and_then(|num| num.parse::<usize>().ok())
            {
                twcs_opts.max_active_window_files = max_active_window_files;
            }

            if let Some(max_inactive_window_files) = opts
                .get(TWCS_MAX_INACTIVE_WINDOW_FILES_KEY)
                .and_then(|num| num.parse::<usize>().ok())
            {
                twcs_opts.max_inactive_window_files = max_inactive_window_files;
            }

            if let Some(time_window) = opts
                .get(TWCS_TIME_WINDOW_SECONDS_KEY)
                .and_then(|num| num.parse::<i64>().ok()) && time_window > 0
            {
                twcs_opts.time_window_seconds = Some(time_window);
            }

            CompactionStrategy::Twcs(twcs_opts)
        } else {
            // unrecognized compaction strategy
            CompactionStrategy::default()
        }
    }
}
