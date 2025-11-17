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

use common_meta::peer::Peer;
use common_telemetry::init_default_ut_logging;
use store_api::region_engine::RegionRole;
use store_api::storage::{FileId, FileRefsManifest, GcReport, RegionId};

use crate::gc::mock::{MockSchedulerCtx, mock_candidate, mock_region_stat};
use crate::gc::{GcScheduler, GcSchedulerOptions};

// Full File Listing Tests

#[tokio::test]
async fn test_full_file_listing_first_time_gc() {
    init_default_ut_logging();

    let table_id = 1;
    let region_id = RegionId::new(table_id, 1);
    let peer = Peer::new(1, "");

    let region_stat = mock_region_stat(region_id, RegionRole::Leader, 200_000_000, 10); // 200MB
    let table_stats = HashMap::from([(table_id, vec![region_stat])]);

    let gc_report = GcReport {
        deleted_files: HashMap::from([(region_id, vec![FileId::random(), FileId::random()])]),
        ..Default::default()
    };

    let file_refs = FileRefsManifest {
        manifest_version: HashMap::from([(region_id, 1)]),
        ..Default::default()
    };

    let ctx = Arc::new(
        MockSchedulerCtx {
            table_to_region_stats: Arc::new(Mutex::new(Some(table_stats))),
            gc_reports: Arc::new(Mutex::new(HashMap::from([(region_id, gc_report)]))),
            file_refs: Arc::new(Mutex::new(Some(file_refs))),
            ..Default::default()
        }
        .with_table_routes(HashMap::from([(
            table_id,
            (table_id, vec![(region_id, peer.clone())]),
        )])),
    );

    // Configure short full file listing interval for testing
    let config = GcSchedulerOptions {
        full_file_listing_interval: Duration::from_secs(3600), // 1 hour
        ..Default::default()
    };

    let scheduler = GcScheduler {
        ctx: ctx.clone(),
        receiver: GcScheduler::channel().1,
        config,
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };

    // First GC - should use full listing since region has never been GC'd
    let reports = scheduler
        .process_datanode_gc(peer.clone(), vec![(table_id, mock_candidate(region_id))])
        .await
        .unwrap();

    assert_eq!(reports.deleted_files.len(), 1);

    // Verify that full listing was used by checking the tracker
    let tracker = scheduler.region_gc_tracker.lock().await;
    let gc_info = tracker
        .get(&region_id)
        .expect("Region should be in tracker");
    assert!(
        gc_info.last_full_listing_time.is_some(),
        "First GC should use full listing"
    );
}

#[tokio::test]
async fn test_full_file_listing_interval_enforcement() {
    init_default_ut_logging();

    let table_id = 1;
    let region_id = RegionId::new(table_id, 1);
    let peer = Peer::new(1, "");

    let region_stat = mock_region_stat(region_id, RegionRole::Leader, 200_000_000, 10); // 200MB
    let table_stats = HashMap::from([(table_id, vec![region_stat])]);

    let gc_report = GcReport {
        deleted_files: HashMap::from([(region_id, vec![FileId::random(), FileId::random()])]),
        ..Default::default()
    };

    let file_refs = FileRefsManifest {
        manifest_version: HashMap::from([(region_id, 1)]),
        ..Default::default()
    };

    let ctx = Arc::new(
        MockSchedulerCtx {
            table_to_region_stats: Arc::new(Mutex::new(Some(table_stats))),
            gc_reports: Arc::new(Mutex::new(HashMap::from([(region_id, gc_report)]))),
            file_refs: Arc::new(Mutex::new(Some(file_refs))),
            ..Default::default()
        }
        .with_table_routes(HashMap::from([(
            table_id,
            (table_id, vec![(region_id, peer.clone())]),
        )])),
    );

    // Configure very short full file listing interval for testing
    let config = GcSchedulerOptions {
        full_file_listing_interval: Duration::from_millis(100), // 100ms
        ..Default::default()
    };

    let scheduler = GcScheduler {
        ctx: ctx.clone(),
        receiver: GcScheduler::channel().1,
        config,
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };

    // First GC - should use full listing
    let reports1 = scheduler
        .process_datanode_gc(peer.clone(), vec![(table_id, mock_candidate(region_id))])
        .await
        .unwrap();
    assert_eq!(reports1.deleted_files.len(), 1);

    // Get the first full listing time
    let first_full_listing_time = {
        let tracker = scheduler.region_gc_tracker.lock().await;
        let gc_info = tracker
            .get(&region_id)
            .expect("Region should be in tracker");
        gc_info
            .last_full_listing_time
            .expect("Should have full listing time")
    };

    // Wait for interval to pass
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Second GC - should use full listing again since interval has passed
    let _reports2 = scheduler
        .process_datanode_gc(peer.clone(), vec![(table_id, mock_candidate(region_id))])
        .await
        .unwrap();

    // Verify that full listing was used again
    let tracker = scheduler.region_gc_tracker.lock().await;
    let gc_info = tracker
        .get(&region_id)
        .expect("Region should be in tracker");
    let second_full_listing_time = gc_info
        .last_full_listing_time
        .expect("Should have full listing time");

    assert!(
        second_full_listing_time > first_full_listing_time,
        "Second GC should update full listing time"
    );
}

#[tokio::test]
async fn test_full_file_listing_no_interval_passed() {
    init_default_ut_logging();

    let table_id = 1;
    let region_id = RegionId::new(table_id, 1);
    let peer = Peer::new(1, "");

    let region_stat = mock_region_stat(region_id, RegionRole::Leader, 200_000_000, 10); // 200MB
    let table_stats = HashMap::from([(table_id, vec![region_stat])]);

    let gc_report = GcReport {
        deleted_files: HashMap::from([(region_id, vec![FileId::random(), FileId::random()])]),
        ..Default::default()
    };

    let file_refs = FileRefsManifest {
        manifest_version: HashMap::from([(region_id, 1)]),
        ..Default::default()
    };

    let ctx = Arc::new(
        MockSchedulerCtx {
            table_to_region_stats: Arc::new(Mutex::new(Some(table_stats))),
            gc_reports: Arc::new(Mutex::new(HashMap::from([(region_id, gc_report)]))),
            file_refs: Arc::new(Mutex::new(Some(file_refs))),
            ..Default::default()
        }
        .with_table_routes(HashMap::from([(
            table_id,
            (table_id, vec![(region_id, peer.clone())]),
        )])),
    );

    // Configure long full file listing interval
    let config = GcSchedulerOptions {
        full_file_listing_interval: Duration::from_secs(3600), // 1 hour
        ..Default::default()
    };

    let scheduler = GcScheduler {
        ctx: ctx.clone(),
        receiver: GcScheduler::channel().1,
        config,
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };

    // First GC - should use full listing
    let reports1 = scheduler
        .process_datanode_gc(peer.clone(), vec![(table_id, mock_candidate(region_id))])
        .await
        .unwrap();
    assert_eq!(reports1.deleted_files.len(), 1);

    // Get the first full listing time
    let first_full_listing_time = {
        let tracker = scheduler.region_gc_tracker.lock().await;
        let gc_info = tracker
            .get(&region_id)
            .expect("Region should be in tracker");
        gc_info
            .last_full_listing_time
            .expect("Should have full listing time")
    };

    // Second GC immediately - should NOT use full listing since interval hasn't passed
    let reports2 = scheduler
        .process_datanode_gc(peer.clone(), vec![(table_id, mock_candidate(region_id))])
        .await
        .unwrap();
    assert_eq!(reports2.deleted_files.len(), 1);

    // Verify that full listing time was NOT updated
    let tracker = scheduler.region_gc_tracker.lock().await;
    let gc_info = tracker
        .get(&region_id)
        .expect("Region should be in tracker");
    let second_full_listing_time = gc_info
        .last_full_listing_time
        .expect("Should have full listing time");

    assert_eq!(
        second_full_listing_time, first_full_listing_time,
        "Second GC should not update full listing time when interval hasn't passed"
    );
}
