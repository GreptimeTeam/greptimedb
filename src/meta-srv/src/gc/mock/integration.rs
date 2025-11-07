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
use std::time::{Duration, Instant};

use common_meta::datanode::RegionManifestInfo;
use common_meta::peer::Peer;
use common_telemetry::init_default_ut_logging;
use store_api::region_engine::RegionRole;
use store_api::storage::{FileId, FileRefsManifest, GcReport, RegionId};

use crate::gc::mock::{MockSchedulerCtx, mock_region_stat};
use crate::gc::{GcScheduler, GcSchedulerOptions};

// Integration Flow Tests

#[tokio::test]
async fn test_full_gc_workflow() {
    init_default_ut_logging();

    let table_id = 1;
    let region_id = RegionId::new(table_id, 1);
    let peer = Peer::new(1, "");

    let mut region_stat = mock_region_stat(region_id, RegionRole::Leader, 200_000_000, 10); // 200MB

    if let RegionManifestInfo::Mito {
        file_removal_rate, ..
    } = &mut region_stat.region_manifest
    {
        *file_removal_rate = 5;
    }

    let table_stats = HashMap::from([(table_id, vec![region_stat])]);

    let mut gc_reports = HashMap::new();
    gc_reports.insert(
        region_id,
        GcReport {
            deleted_files: HashMap::from([(region_id, vec![FileId::random(), FileId::random()])]),
            ..Default::default()
        },
    );

    let file_refs = FileRefsManifest {
        manifest_version: HashMap::from([(region_id, 1)]),
        ..Default::default()
    };

    let ctx = Arc::new(
        MockSchedulerCtx {
            table_to_region_stats: Arc::new(Mutex::new(Some(table_stats))),
            gc_reports: Arc::new(Mutex::new(gc_reports)),
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

    // Run the full workflow
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

    // Verify all steps were executed
    assert_eq!(
        *ctx.get_table_to_region_stats_calls.lock().unwrap(),
        1,
        "Expected 1 call to get_table_to_region_stats"
    );
    assert_eq!(
        *ctx.get_file_references_calls.lock().unwrap(),
        1,
        "Expected 1 call to get_file_references"
    );
    assert_eq!(
        *ctx.gc_regions_calls.lock().unwrap(),
        1,
        "Expected 1 call to gc_regions"
    );
}

#[tokio::test]
async fn test_tracker_cleanup() {
    init_default_ut_logging();

    let table_id = 1;
    let region_id = RegionId::new(table_id, 1);
    let peer = Peer::new(1, "");

    // Create region stat with proper file_removal_rate to ensure it gets selected as candidate
    let mut region_stat = mock_region_stat(region_id, RegionRole::Leader, 200_000_000, 10); // 200MB
    if let RegionManifestInfo::Mito {
        file_removal_rate, ..
    } = &mut region_stat.region_manifest
    {
        *file_removal_rate = 5;
    }

    let table_stats = HashMap::from([(table_id, vec![region_stat])]);

    let mut gc_reports = HashMap::new();
    gc_reports.insert(region_id, GcReport::default());

    let file_refs = FileRefsManifest {
        manifest_version: HashMap::from([(region_id, 1)]),
        ..Default::default()
    };

    let ctx = Arc::new(
        MockSchedulerCtx {
            table_to_region_stats: Arc::new(Mutex::new(Some(table_stats))),
            gc_reports: Arc::new(Mutex::new(gc_reports)),
            file_refs: Arc::new(Mutex::new(Some(file_refs))),
            ..Default::default()
        }
        .with_table_routes(HashMap::from([(
            table_id,
            (table_id, vec![(region_id, peer)]),
        )])),
    );

    let old_region_gc_tracker = {
        let mut tracker = HashMap::new();
        tracker.insert(
            region_id,
            crate::gc::tracker::RegionGcInfo {
                last_full_listing_time: Some(Instant::now() - Duration::from_secs(7200)), // 2 hours ago
                last_gc_time: Instant::now() - Duration::from_secs(7200), // 2 hours ago
            },
        );
        // also insert a different table that should also be cleaned up
        tracker.insert(
            RegionId::new(2, 1),
            crate::gc::tracker::RegionGcInfo {
                last_full_listing_time: Some(Instant::now() - Duration::from_secs(7200)), // 2 hours ago
                last_gc_time: Instant::now() - Duration::from_secs(7200), // 2 hours ago
            },
        );
        tracker
    };

    // Use a custom config with shorter cleanup interval to trigger cleanup
    let config = GcSchedulerOptions {
        // 30 minutes
        tracker_cleanup_interval: Duration::from_secs(1800),
        ..Default::default()
    };

    let scheduler = GcScheduler {
        ctx: ctx.clone(),
        receiver: GcScheduler::channel().1,
        config,
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(old_region_gc_tracker)),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(
            Instant::now() - Duration::from_secs(3600), // Old cleanup time (1 hour ago)
        )),
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

    // Verify tracker was updated
    let tracker = scheduler.region_gc_tracker.lock().await;
    assert!(
        tracker.contains_key(&region_id),
        "Tracker should contain region {}",
        region_id
    );
    // only one entry
    assert_eq!(tracker.len(), 1, "Tracker should only have 1 entry");
}
