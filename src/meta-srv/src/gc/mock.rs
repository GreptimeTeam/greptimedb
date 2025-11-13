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

mod basic;
mod candidate_select;
mod con;
mod config;
mod err_handle;
mod full_list;
mod integration;
mod misc;
mod retry;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common_meta::datanode::{RegionManifestInfo, RegionStat};
use common_meta::key::table_route::PhysicalTableRouteValue;
use common_meta::peer::Peer;
use common_meta::rpc::router::{Region, RegionRoute};
use ordered_float::OrderedFloat;
use snafu::OptionExt;
use store_api::region_engine::RegionRole;
use store_api::storage::{FileRefsManifest, GcReport, RegionId};
use table::metadata::TableId;
use tokio::sync::mpsc::Sender;

use crate::error::{Result, UnexpectedSnafu};
use crate::gc::candidate::GcCandidate;
use crate::gc::ctx::SchedulerCtx;
use crate::gc::options::GcSchedulerOptions;
use crate::gc::scheduler::{Event, GcScheduler};

#[allow(clippy::type_complexity)]
#[derive(Debug, Default)]
pub struct MockSchedulerCtx {
    pub table_to_region_stats: Arc<Mutex<Option<HashMap<TableId, Vec<RegionStat>>>>>,
    pub table_routes: Arc<Mutex<HashMap<TableId, (TableId, PhysicalTableRouteValue)>>>,
    pub file_refs: Arc<Mutex<Option<FileRefsManifest>>>,
    pub gc_reports: Arc<Mutex<HashMap<RegionId, GcReport>>>,
    pub candidates: Arc<Mutex<Option<HashMap<TableId, Vec<GcCandidate>>>>>,
    pub get_table_to_region_stats_calls: Arc<Mutex<usize>>,
    pub get_file_references_calls: Arc<Mutex<usize>>,
    pub gc_regions_calls: Arc<Mutex<usize>>,
    // Error injection fields for testing
    pub get_table_to_region_stats_error: Arc<Mutex<Option<crate::error::Error>>>,
    pub get_table_route_error: Arc<Mutex<Option<crate::error::Error>>>,
    pub get_file_references_error: Arc<Mutex<Option<crate::error::Error>>>,
    pub gc_regions_error: Arc<Mutex<Option<crate::error::Error>>>,
    // Retry testing fields
    pub gc_regions_retry_count: Arc<Mutex<HashMap<RegionId, usize>>>,
    pub gc_regions_error_sequence: Arc<Mutex<Vec<crate::error::Error>>>,
    pub gc_regions_success_after_retries: Arc<Mutex<HashMap<RegionId, usize>>>,
    // Per-region error injection
    pub gc_regions_per_region_errors: Arc<Mutex<HashMap<RegionId, crate::error::Error>>>,
}

impl MockSchedulerCtx {
    pub fn with_table_routes(
        self,
        table_routes: HashMap<TableId, (TableId, Vec<(RegionId, Peer)>)>,
    ) -> Self {
        *self.table_routes.lock().unwrap() = table_routes
            .into_iter()
            .map(|(k, (phy_id, region2peer))| {
                let phy = PhysicalTableRouteValue::new(
                    region2peer
                        .into_iter()
                        .map(|(region_id, peer)| RegionRoute {
                            region: Region::new_test(region_id),
                            leader_peer: Some(peer),
                            ..Default::default()
                        })
                        .collect(),
                );

                (k, (phy_id, phy))
            })
            .collect();
        self
    }

    /// Set an error to be returned by `get_table_to_region_stats`
    #[allow(dead_code)]
    pub fn with_get_table_to_region_stats_error(self, error: crate::error::Error) -> Self {
        *self.get_table_to_region_stats_error.lock().unwrap() = Some(error);
        self
    }

    /// Set an error to be returned by `get_table_route`
    pub fn with_get_table_route_error(self, error: crate::error::Error) -> Self {
        *self.get_table_route_error.lock().unwrap() = Some(error);
        self
    }

    /// Set an error to be returned by `get_file_references`
    #[allow(dead_code)]
    pub fn with_get_file_references_error(self, error: crate::error::Error) -> Self {
        *self.get_file_references_error.lock().unwrap() = Some(error);
        self
    }

    /// Set an error to be returned by `gc_regions`
    pub fn with_gc_regions_error(self, error: crate::error::Error) -> Self {
        *self.gc_regions_error.lock().unwrap() = Some(error);
        self
    }

    /// Set a sequence of errors to be returned by `gc_regions` for retry testing
    pub fn set_gc_regions_error_sequence(&self, errors: Vec<crate::error::Error>) {
        *self.gc_regions_error_sequence.lock().unwrap() = errors;
    }

    /// Set success after a specific number of retries for a region
    pub fn set_gc_regions_success_after_retries(&self, region_id: RegionId, retries: usize) {
        *self.gc_regions_success_after_retries.lock().unwrap() =
            HashMap::from([(region_id, retries)]);
    }

    /// Get the retry count for a specific region
    pub fn get_retry_count(&self, region_id: RegionId) -> usize {
        self.gc_regions_retry_count
            .lock()
            .unwrap()
            .get(&region_id)
            .copied()
            .unwrap_or(0)
    }

    /// Reset all retry tracking
    pub fn reset_retry_tracking(&self) {
        *self.gc_regions_retry_count.lock().unwrap() = HashMap::new();
        *self.gc_regions_error_sequence.lock().unwrap() = Vec::new();
        *self.gc_regions_success_after_retries.lock().unwrap() = HashMap::new();
    }

    /// Set an error to be returned for a specific region
    pub fn set_gc_regions_error_for_region(&self, region_id: RegionId, error: crate::error::Error) {
        self.gc_regions_per_region_errors
            .lock()
            .unwrap()
            .insert(region_id, error);
    }

    /// Clear per-region errors
    #[allow(unused)]
    pub fn clear_gc_regions_per_region_errors(&self) {
        self.gc_regions_per_region_errors.lock().unwrap().clear();
    }
}

#[async_trait::async_trait]
impl SchedulerCtx for MockSchedulerCtx {
    async fn get_table_to_region_stats(&self) -> Result<HashMap<TableId, Vec<RegionStat>>> {
        *self.get_table_to_region_stats_calls.lock().unwrap() += 1;

        // Check if we should return an injected error
        if let Some(error) = self.get_table_to_region_stats_error.lock().unwrap().take() {
            return Err(error);
        }

        Ok(self
            .table_to_region_stats
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_default())
    }

    async fn get_table_route(
        &self,
        table_id: TableId,
    ) -> Result<(TableId, PhysicalTableRouteValue)> {
        // Check if we should return an injected error
        if let Some(error) = self.get_table_route_error.lock().unwrap().take() {
            return Err(error);
        }

        Ok(self
            .table_routes
            .lock()
            .unwrap()
            .get(&table_id)
            .cloned()
            .unwrap_or_else(|| (table_id, PhysicalTableRouteValue::default())))
    }

    async fn get_file_references(
        &self,
        _region_ids: &[RegionId],
        _region_to_peer: &HashMap<RegionId, Peer>,
        _timeout: Duration,
    ) -> Result<FileRefsManifest> {
        *self.get_file_references_calls.lock().unwrap() += 1;

        // Check if we should return an injected error
        if let Some(error) = self.get_file_references_error.lock().unwrap().take() {
            return Err(error);
        }

        Ok(self.file_refs.lock().unwrap().clone().unwrap_or_default())
    }

    async fn gc_regions(
        &self,
        _peer: Peer,
        region_id: RegionId,
        _file_refs_manifest: &FileRefsManifest,
        _full_file_listing: bool,
        _timeout: Duration,
    ) -> Result<GcReport> {
        *self.gc_regions_calls.lock().unwrap() += 1;

        // Track retry count for this region
        {
            let mut retry_count = self.gc_regions_retry_count.lock().unwrap();
            *retry_count.entry(region_id).or_insert(0) += 1;
        }

        // Check per-region error injection first
        if let Some(error) = self
            .gc_regions_per_region_errors
            .lock()
            .unwrap()
            .remove(&region_id)
        {
            return Err(error);
        }

        // Check if we should return an injected error
        if let Some(error) = self.gc_regions_error.lock().unwrap().take() {
            return Err(error);
        }

        // Handle error sequence for retry testing
        {
            let mut error_sequence = self.gc_regions_error_sequence.lock().unwrap();
            if !error_sequence.is_empty() {
                let error = error_sequence.remove(0);
                return Err(error);
            }
        }

        // Handle success after specific number of retries
        {
            let retry_count = self
                .gc_regions_retry_count
                .lock()
                .unwrap()
                .get(&region_id)
                .copied()
                .unwrap_or(0);
            let success_after_retries = self.gc_regions_success_after_retries.lock().unwrap();
            if let Some(&required_retries) = success_after_retries.get(&region_id)
                && retry_count <= required_retries
            {
                // Return retryable error until we reach the required retry count
                return Err(crate::error::RetryLaterSnafu {
                    reason: format!(
                        "Mock retryable error for region {} (attempt {}/{})",
                        region_id, retry_count, required_retries
                    ),
                }
                .build());
            }
        }

        self.gc_reports
            .lock()
            .unwrap()
            .get(&region_id)
            .cloned()
            .with_context(|| UnexpectedSnafu {
                violated: format!("No corresponding gc report for {}", region_id),
            })
    }
}

pub struct TestEnv {
    pub scheduler: GcScheduler,
    pub ctx: Arc<MockSchedulerCtx>,
    #[allow(dead_code)]
    tx: Sender<Event>,
}

impl TestEnv {
    pub fn new() -> Self {
        let ctx = Arc::new(MockSchedulerCtx::default());
        let (tx, rx) = GcScheduler::channel();
        let config = GcSchedulerOptions::default();

        let scheduler = GcScheduler {
            ctx: ctx.clone(),
            receiver: rx,
            config,
            region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
        };

        Self { scheduler, ctx, tx }
    }

    pub fn with_candidates(self, candidates: HashMap<TableId, Vec<GcCandidate>>) -> Self {
        *self.ctx.candidates.lock().unwrap() = Some(candidates);
        self
    }

    #[allow(dead_code)]
    pub async fn run_scheduler(mut self) {
        self.scheduler.run().await;
    }

    #[allow(dead_code)]
    pub async fn tick(&self) {
        self.tx.send(Event::Tick).await.unwrap();
    }
}

fn new_candidate(region_id: RegionId, score: f64) -> GcCandidate {
    // well pass threshold for gc
    let region_stat = mock_region_stat(region_id, RegionRole::Leader, 10_000, 10);

    GcCandidate {
        region_id,
        score: OrderedFloat(score),
        region_stat,
    }
}

// Helper function to create a mock GC candidate
fn mock_candidate(region_id: RegionId) -> GcCandidate {
    let region_stat = mock_region_stat(region_id, RegionRole::Leader, 200_000_000, 10);
    GcCandidate {
        region_id,
        score: ordered_float::OrderedFloat(1.0),
        region_stat,
    }
}

fn mock_region_stat(
    id: RegionId,
    role: RegionRole,
    approximate_bytes: u64,
    sst_num: u64,
) -> RegionStat {
    RegionStat {
        id,
        role,
        approximate_bytes,
        sst_num,
        region_manifest: RegionManifestInfo::Mito {
            manifest_version: 0,
            flushed_entry_id: 0,
            file_removed_cnt: 0,
        },
        rcus: 0,
        wcus: 0,
        engine: "mito".to_string(),
        num_rows: 0,
        memtable_size: 0,
        manifest_size: 0,
        sst_size: 0,
        index_size: 0,
        data_topic_latest_entry_id: 0,
        metadata_topic_latest_entry_id: 0,
        written_bytes: 0,
    }
}
