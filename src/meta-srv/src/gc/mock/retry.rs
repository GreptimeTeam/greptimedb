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

/// Retry Logic Tests
#[tokio::test]
async fn test_retry_logic_with_retryable_errors() {
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
            (table_id, vec![(region_id, peer)]),
        )])),
    );

    // Configure retry settings
    let config = GcSchedulerOptions {
        max_retries_per_region: 3,
        retry_backoff_duration: Duration::from_millis(100), // Short backoff for testing
        ..Default::default()
    };

    let scheduler = GcScheduler {
        ctx: ctx.clone(),
        receiver: GcScheduler::channel().1,
        config,
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };

    // Test 1: Success after 2 retryable errors
    ctx.reset_retry_tracking();
    ctx.set_gc_regions_success_after_retries(region_id, 2);

    let start_time = Instant::now();
    let report = scheduler
        .process_table_gc(table_id, vec![mock_candidate(region_id)])
        .await
        .unwrap();
    let duration = start_time.elapsed();

    assert_eq!(report.success_regions.len(), 1);
    assert_eq!(report.failed_regions.len(), 0);
    assert_eq!(ctx.get_retry_count(region_id), 3); // 2 retries + 1 success

    // Verify backoff was applied (should take at least 200ms for 2 retries with 100ms backoff)
    assert!(
        duration >= Duration::from_millis(200),
        "Expected backoff duration not met: {:?}",
        duration
    );

    // Test 2: Exceed max retries
    ctx.reset_retry_tracking();
    ctx.set_gc_regions_success_after_retries(region_id, 5); // More than max_retries_per_region (3)

    let report = scheduler
        .process_table_gc(table_id, vec![mock_candidate(region_id)])
        .await
        .unwrap();

    assert_eq!(report.success_regions.len(), 0);
    assert_eq!(report.failed_regions.len(), 1);
    assert_eq!(ctx.get_retry_count(region_id), 3); // Should stop at max_retries_per_region
}

#[tokio::test]
async fn test_retry_logic_with_error_sequence() {
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
            (table_id, vec![(region_id, peer)]),
        )])),
    );

    let config = GcSchedulerOptions {
        max_retries_per_region: 5,
        retry_backoff_duration: Duration::from_millis(50),
        ..Default::default()
    };

    let scheduler = GcScheduler {
        ctx: ctx.clone(),
        receiver: GcScheduler::channel().1,
        config,
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };

    // Test with sequence: retryable error -> retryable error -> success
    let retryable_error1 = crate::error::RetryLaterSnafu {
        reason: "Test retryable error 1".to_string(),
    }
    .build();

    let retryable_error2 = crate::error::MailboxTimeoutSnafu { id: 123u64 }.build();

    ctx.reset_retry_tracking();
    ctx.set_gc_regions_error_sequence(vec![retryable_error1, retryable_error2]);

    let report = scheduler
        .process_table_gc(table_id, vec![mock_candidate(region_id)])
        .await
        .unwrap();

    assert_eq!(report.success_regions.len(), 1);
    assert_eq!(report.failed_regions.len(), 0);
    assert_eq!(ctx.get_retry_count(region_id), 3); // 2 errors + 1 success
}

#[tokio::test]
async fn test_retry_logic_non_retryable_error() {
    init_default_ut_logging();

    let table_id = 1;
    let region_id = RegionId::new(table_id, 1);
    let peer = Peer::new(1, "");

    let region_stat = mock_region_stat(region_id, RegionRole::Leader, 200_000_000, 10); // 200MB
    let table_stats = HashMap::from([(table_id, vec![region_stat])]);

    let file_refs = FileRefsManifest {
        manifest_version: HashMap::from([(region_id, 1)]),
        ..Default::default()
    };

    let ctx = Arc::new(
        MockSchedulerCtx {
            table_to_region_stats: Arc::new(Mutex::new(Some(table_stats))),
            file_refs: Arc::new(Mutex::new(Some(file_refs))),
            ..Default::default()
        }
        .with_table_routes(HashMap::from([(
            table_id,
            (table_id, vec![(region_id, peer)]),
        )])),
    );

    let config = GcSchedulerOptions {
        max_retries_per_region: 3,
        ..Default::default()
    };

    let scheduler = GcScheduler {
        ctx: ctx.clone(),
        receiver: GcScheduler::channel().1,
        config,
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };

    // Test with non-retryable error (should fail immediately)
    let non_retryable_error = crate::error::RegionRouteNotFoundSnafu { region_id }.build();

    ctx.reset_retry_tracking();
    ctx.gc_regions_error
        .lock()
        .unwrap()
        .replace(non_retryable_error);

    let report = scheduler
        .process_table_gc(table_id, vec![mock_candidate(region_id)])
        .await
        .unwrap();

    assert_eq!(report.success_regions.len(), 0);
    assert_eq!(report.failed_regions.len(), 1);
    assert_eq!(ctx.get_retry_count(region_id), 1); // Only 1 attempt, no retries
}
