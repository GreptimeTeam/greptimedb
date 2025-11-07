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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use common_meta::peer::Peer;
use common_telemetry::init_default_ut_logging;
use store_api::region_engine::RegionRole;
use store_api::storage::{FileId, FileRefsManifest, GcReport, RegionId};

use crate::gc::mock::{MockSchedulerCtx, TestEnv, mock_region_stat, new_candidate};
use crate::gc::{GcScheduler, GcSchedulerOptions};

#[tokio::test]
async fn test_process_tables_concurrently_empty() {
    let env = TestEnv::new();
    let report = env
        .scheduler
        .process_tables_concurrently(HashMap::new())
        .await;

    assert_eq!(report.processed_tables, 0);
    assert_eq!(report.table_reports.len(), 0);
    assert!(report.table_reports.is_empty());
}

#[tokio::test]
async fn test_process_tables_concurrently_with_candidates() {
    init_default_ut_logging();

    let table_id = 1;
    let region_id = RegionId::new(table_id, 1);
    let peer = Peer::new(1, "");
    let candidates = HashMap::from([(table_id, vec![new_candidate(region_id, 1.0)])]);

    let mut gc_reports = HashMap::new();
    let deleted_files = vec![FileId::random()];
    gc_reports.insert(
        region_id,
        GcReport {
            deleted_files: HashMap::from([(region_id, deleted_files.clone())]),
            ..Default::default()
        },
    );
    let file_refs = FileRefsManifest {
        manifest_version: HashMap::from([(region_id, 1)]),
        ..Default::default()
    };
    let ctx = MockSchedulerCtx {
        gc_reports: Arc::new(Mutex::new(gc_reports)),
        file_refs: Arc::new(Mutex::new(Some(file_refs))),
        ..Default::default()
    }
    .with_table_routes(HashMap::from([(
        table_id,
        (table_id, vec![(region_id, peer)]),
    )]));

    let env = TestEnv::new().with_candidates(candidates);
    // We need to replace the ctx with the one with gc_reports
    let mut scheduler = env.scheduler;
    scheduler.ctx = Arc::new(ctx);

    let candidates = env
        .ctx
        .candidates
        .lock()
        .unwrap()
        .clone()
        .unwrap_or_default();

    let report = scheduler.process_tables_concurrently(candidates).await;

    assert_eq!(report.processed_tables, 1);
    assert_eq!(report.table_reports.len(), 1);
    assert_eq!(
        report.table_reports[0].success_regions[0].deleted_files[&region_id],
        deleted_files
    );
    assert!(report.table_reports[0].failed_regions.is_empty());
}

#[tokio::test]
async fn test_handle_tick() {
    init_default_ut_logging();

    let table_id = 1;
    let region_id = RegionId::new(table_id, 1);
    let peer = Peer::new(1, "");
    let candidates = HashMap::from([(table_id, vec![new_candidate(region_id, 1.0)])]);

    let mut gc_reports = HashMap::new();
    gc_reports.insert(region_id, GcReport::default());
    let file_refs = FileRefsManifest {
        manifest_version: HashMap::from([(region_id, 1)]),
        ..Default::default()
    };
    let ctx = Arc::new(
        MockSchedulerCtx {
            table_to_region_stats: Arc::new(Mutex::new(Some(HashMap::from([(
                table_id,
                vec![mock_region_stat(
                    region_id,
                    RegionRole::Leader,
                    200_000_000,
                    10,
                )],
            )])))),
            gc_reports: Arc::new(Mutex::new(gc_reports)),
            candidates: Arc::new(Mutex::new(Some(candidates))),
            file_refs: Arc::new(Mutex::new(Some(file_refs))),
            ..Default::default()
        }
        .with_table_routes(HashMap::from([(
            table_id,
            (table_id, vec![(region_id, peer)]),
        )])),
    );

    let scheduler = GcScheduler {
        ctx: ctx.clone(),
        receiver: GcScheduler::channel().1,
        config: GcSchedulerOptions::default(),
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };

    let report = scheduler.handle_tick().await.unwrap();

    // Validate the returned GcJobReport
    assert_eq!(report.processed_tables, 1, "Should process 1 table");
    assert_eq!(report.table_reports.len(), 1, "Should have 1 table report");

    let table_report = &report.table_reports[0];
    assert_eq!(table_report.table_id, table_id, "Table ID should match");
    assert_eq!(
        table_report.success_regions.len(),
        1,
        "Should have 1 successful region"
    );
    assert!(
        table_report.failed_regions.is_empty(),
        "Should have no failed regions"
    );

    assert_eq!(*ctx.get_table_to_region_stats_calls.lock().unwrap(), 1);
    assert_eq!(*ctx.get_file_references_calls.lock().unwrap(), 1);
    assert_eq!(*ctx.gc_regions_calls.lock().unwrap(), 1);

    let tracker = scheduler.region_gc_tracker.lock().await;
    assert!(
        tracker.contains_key(&region_id),
        "Tracker should have one region: {:?}",
        tracker
    );
}
