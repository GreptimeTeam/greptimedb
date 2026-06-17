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

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common_meta::datanode::RegionManifestInfo;
use common_meta::key::table_repart::TableRepartValue;
use common_meta::peer::Peer;
use common_telemetry::init_default_ut_logging;
use store_api::region_engine::RegionRole;
use store_api::storage::{FileId, FileRefsManifest, GcReport, RegionId};

use crate::gc::mock::{MockSchedulerCtx, TEST_REGION_SIZE_200MB, mock_region_stat};
use crate::gc::{GcScheduler, GcSchedulerOptions, Region2Peers};

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct ScenarioOptions {
    dropped_region: bool,
    route_override: bool,
    retry_once: bool,
    multi_region: bool,
}

pub(super) struct ScenarioFixture {
    pub(super) scheduler: GcScheduler,
    pub(super) ctx: Arc<MockSchedulerCtx>,
    pub(super) table_id: u32,
    pub(super) active_region_id: RegionId,
    pub(super) extra_active_region_ids: Vec<RegionId>,
    pub(super) dropped_region_id: RegionId,
    pub(super) route_override: Region2Peers,
    pub(super) reachable_files: HashSet<FileId>,
    pub(super) protected_files: HashSet<FileId>,
}

pub(super) struct ScenarioFixtureBuilder {
    seed: u64,
    options: ScenarioOptions,
}

impl ScenarioFixtureBuilder {
    pub(super) fn new(seed: u64) -> Self {
        Self {
            seed,
            options: ScenarioOptions::default(),
        }
    }

    pub(super) fn with_dropped_region(mut self) -> Self {
        self.options.dropped_region = true;
        self
    }

    pub(super) fn with_route_override(mut self) -> Self {
        self.options.route_override = true;
        self
    }

    pub(super) fn with_retry_once(mut self) -> Self {
        self.options.retry_once = true;
        self
    }

    pub(super) fn with_multi_region(mut self) -> Self {
        self.options.multi_region = true;
        self
    }

    pub(super) fn build(self) -> ScenarioFixture {
        let table_id = self.seed as u32 + 100;
        let active_region_id = RegionId::new(table_id, 1);
        let dropped_region_id = RegionId::new(table_id, 2);
        let extra_active_region_ids = if self.options.multi_region {
            vec![RegionId::new(table_id, 3), RegionId::new(table_id, 4)]
        } else {
            Vec::new()
        };
        let active_peer = Peer::new(10 + self.seed, "");
        let mut region_stats = Vec::new();
        for region_id in
            std::iter::once(active_region_id).chain(extra_active_region_ids.iter().copied())
        {
            let mut region_stat =
                mock_region_stat(region_id, RegionRole::Leader, TEST_REGION_SIZE_200MB, 10);
            if let RegionManifestInfo::Mito {
                file_removed_cnt, ..
            } = &mut region_stat.region_manifest
            {
                *file_removed_cnt = 4;
            }
            region_stats.push(region_stat);
        }

        let file_refs = FileRefsManifest {
            manifest_version: std::iter::once((active_region_id, 1))
                .chain(std::iter::once((dropped_region_id, 1)))
                .chain(
                    extra_active_region_ids
                        .iter()
                        .copied()
                        .map(|region_id| (region_id, 1)),
                )
                .collect(),
            ..Default::default()
        };

        let active_deleted = deterministic_file_id(self.seed, 1);
        let dropped_deleted = deterministic_file_id(self.seed, 2);
        let active_reachable = deterministic_file_id(self.seed, 11);
        let active_protected = deterministic_file_id(self.seed, 12);
        let dropped_reachable = deterministic_file_id(self.seed, 21);
        let dropped_protected = deterministic_file_id(self.seed, 22);
        let mut gc_reports = HashMap::new();
        gc_reports.insert(
            active_region_id,
            GcReport {
                deleted_files: HashMap::from([(active_region_id, vec![active_deleted])]),
                processed_regions: [active_region_id].into(),
                ..Default::default()
            },
        );
        gc_reports.insert(
            dropped_region_id,
            GcReport {
                deleted_files: HashMap::from([(dropped_region_id, vec![dropped_deleted])]),
                processed_regions: [dropped_region_id].into(),
                ..Default::default()
            },
        );
        let mut reachable_files = HashSet::from([
            active_reachable,
            active_protected,
            dropped_reachable,
            dropped_protected,
        ]);
        let mut protected_files = HashSet::from([active_protected, dropped_protected]);
        let mut region_live_files = HashMap::from([
            (
                active_region_id,
                HashSet::from([active_reachable, active_protected]),
            ),
            (
                dropped_region_id,
                HashSet::from([dropped_reachable, dropped_protected]),
            ),
        ]);
        let mut regions_require_full_listing = HashSet::new();
        let mut dropped_regions = HashSet::new();

        if self.options.dropped_region || self.options.route_override {
            regions_require_full_listing.insert(dropped_region_id);
            dropped_regions.insert(dropped_region_id);
        }

        for (idx, region_id) in extra_active_region_ids.iter().copied().enumerate() {
            let deleted = deterministic_file_id(self.seed, 100 + idx as u64);
            let reachable = deterministic_file_id(self.seed, 110 + idx as u64);
            let protected = deterministic_file_id(self.seed, 120 + idx as u64);
            gc_reports.insert(
                region_id,
                GcReport {
                    deleted_files: HashMap::from([(region_id, vec![deleted])]),
                    processed_regions: [region_id].into(),
                    ..Default::default()
                },
            );
            reachable_files.extend([reachable, protected]);
            protected_files.insert(protected);
            region_live_files.insert(region_id, HashSet::from([reachable, protected]));
            if idx == 0 {
                regions_require_full_listing.insert(region_id);
            }
        }

        let route_override = if self.options.route_override {
            HashMap::from([(dropped_region_id, (active_peer.clone(), Vec::new()))])
        } else {
            HashMap::new()
        };

        let retry_targets = if self.options.retry_once {
            if self.options.dropped_region || self.options.route_override {
                HashMap::from([(dropped_region_id, 1)])
            } else {
                HashMap::from([(active_region_id, 1)])
            }
        } else {
            HashMap::new()
        };

        let mut table_repart = TableRepartValue::new();
        if self.options.dropped_region {
            table_repart.update_mappings(dropped_region_id, &[active_region_id]);
        }

        let ctx = MockSchedulerCtx {
            table_to_region_stats: Arc::new(Mutex::new(Some(HashMap::from([(
                table_id,
                region_stats,
            )])))),
            table_reparts: Arc::new(Mutex::new(if self.options.dropped_region {
                HashMap::from([(table_id, table_repart)])
            } else {
                HashMap::new()
            })),
            file_refs: Arc::new(Mutex::new(Some(file_refs))),
            gc_reports: Arc::new(Mutex::new(gc_reports)),
            gc_regions_success_after_retries: Arc::new(Mutex::new(retry_targets)),
            region_live_files: Arc::new(Mutex::new(region_live_files)),
            regions_require_full_listing: Arc::new(Mutex::new(regions_require_full_listing)),
            dropped_regions: Arc::new(Mutex::new(dropped_regions)),
            ..Default::default()
        }
        .with_table_routes(HashMap::from([(
            table_id,
            (
                table_id,
                std::iter::once((active_region_id, active_peer.clone()))
                    .chain(
                        extra_active_region_ids
                            .iter()
                            .copied()
                            .map(|region_id| (region_id, active_peer.clone())),
                    )
                    .collect(),
            ),
        )]));

        let ctx = Arc::new(ctx);
        let scheduler = GcScheduler {
            ctx: ctx.clone(),
            receiver: GcScheduler::channel().1,
            config: GcSchedulerOptions {
                retry_backoff_duration: Duration::from_millis(1),
                ..Default::default()
            },
            region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
        };

        ScenarioFixture {
            scheduler,
            ctx,
            table_id,
            active_region_id,
            extra_active_region_ids,
            dropped_region_id,
            route_override,
            reachable_files,
            protected_files,
        }
    }
}

fn deterministic_file_id(seed: u64, slot: u64) -> FileId {
    FileId::parse_str(&format!(
        "00000000-0000-0000-{seed_part:04x}-{slot_part:012x}",
        seed_part = (seed & 0xffff) as u16,
        slot_part = ((seed << 8) | slot) & 0x0000_ffff_ffff_ffff,
    ))
    .unwrap()
}

#[tokio::test]
async fn test_phase2_fixture_dropped_only() {
    init_default_ut_logging();

    let fixture = ScenarioFixtureBuilder::new(1).with_dropped_region().build();
    let report = fixture
        .scheduler
        .handle_manual_gc(
            Some(vec![fixture.dropped_region_id]),
            Some(false),
            Some(Duration::from_secs(1)),
        )
        .await
        .unwrap()
        .merge_to_report();

    assert!(
        report
            .deleted_files
            .contains_key(&fixture.dropped_region_id)
    );
    let full_listing_calls = fixture
        .ctx
        .last_gc_regions_full_file_listing
        .lock()
        .unwrap();
    assert_eq!(full_listing_calls.last().copied(), Some(true));

    let route_overrides = fixture.ctx.last_gc_regions_route_overrides.lock().unwrap();
    let last_override = route_overrides.last().unwrap();
    assert!(last_override.contains_key(&fixture.dropped_region_id));
}

#[tokio::test]
async fn test_phase2_fixture_route_override_only() {
    init_default_ut_logging();

    let fixture = ScenarioFixtureBuilder::new(2)
        .with_dropped_region()
        .with_route_override()
        .build();
    let report = fixture
        .scheduler
        .handle_manual_gc(
            Some(vec![fixture.dropped_region_id]),
            Some(false),
            Some(Duration::from_secs(1)),
        )
        .await
        .unwrap()
        .merge_to_report();

    assert!(
        report
            .deleted_files
            .contains_key(&fixture.dropped_region_id)
    );
    let route_overrides = fixture.ctx.last_gc_regions_route_overrides.lock().unwrap();
    let last_override = route_overrides.last().unwrap();
    assert_eq!(last_override, &fixture.route_override);
    assert!(last_override.contains_key(&fixture.dropped_region_id));
    assert_eq!(fixture.table_id, fixture.active_region_id.table_id());
}

#[tokio::test]
async fn test_phase2_fixture_retry_only() {
    init_default_ut_logging();

    let fixture = ScenarioFixtureBuilder::new(3).with_retry_once().build();
    let first = fixture
        .scheduler
        .handle_manual_gc(
            Some(vec![fixture.active_region_id]),
            Some(false),
            Some(Duration::from_secs(1)),
        )
        .await
        .unwrap()
        .merge_to_report();
    assert!(first.need_retry_regions.contains(&fixture.active_region_id));

    let second = fixture
        .scheduler
        .handle_manual_gc(
            Some(vec![fixture.active_region_id]),
            Some(false),
            Some(Duration::from_secs(1)),
        )
        .await
        .unwrap()
        .merge_to_report();
    assert!(second.deleted_files.contains_key(&fixture.active_region_id));
}

#[tokio::test]
async fn test_phase2_fixture_multi_region_batching() {
    init_default_ut_logging();

    let fixture = ScenarioFixtureBuilder::new(4).with_multi_region().build();
    let requested = vec![fixture.active_region_id, fixture.extra_active_region_ids[0]];
    let report = fixture
        .scheduler
        .handle_manual_gc(
            Some(requested.clone()),
            Some(true),
            Some(Duration::from_secs(1)),
        )
        .await
        .unwrap()
        .merge_to_report();

    for region_id in requested {
        assert!(report.deleted_files.contains_key(&region_id));
    }
}

#[tokio::test]
async fn test_phase2_fixture_mixed_mode_active_and_dropped() {
    init_default_ut_logging();

    let fixture = ScenarioFixtureBuilder::new(5)
        .with_dropped_region()
        .with_multi_region()
        .build();
    let report = fixture
        .scheduler
        .handle_manual_gc(
            Some(vec![fixture.active_region_id, fixture.dropped_region_id]),
            Some(false),
            Some(Duration::from_secs(1)),
        )
        .await
        .unwrap()
        .merge_to_report();

    assert!(report.deleted_files.contains_key(&fixture.active_region_id));
    assert!(
        report
            .deleted_files
            .contains_key(&fixture.dropped_region_id)
    );
    let full_listing_calls = fixture
        .ctx
        .last_gc_regions_full_file_listing
        .lock()
        .unwrap();
    assert_eq!(&*full_listing_calls, &[false, true]);
    let route_overrides = fixture.ctx.last_gc_regions_route_overrides.lock().unwrap();
    assert!(
        route_overrides
            .last()
            .unwrap()
            .contains_key(&fixture.dropped_region_id)
    );
}
