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

use common_meta::datanode::RegionManifestInfo;
use common_telemetry::init_default_ut_logging;
use store_api::region_engine::RegionRole;
use store_api::storage::RegionId;

use crate::gc::mock::{MockSchedulerCtx, TEST_REGION_SIZE_200MB, mock_region_stat};
use crate::gc::{GcScheduler, GcSchedulerOptions};

/// Configuration Tests
#[tokio::test]
async fn test_different_gc_weights() {
    init_default_ut_logging();

    let table_id = 1;
    let region_id = RegionId::new(table_id, 1);

    let mut region_stat =
        mock_region_stat(region_id, RegionRole::Leader, TEST_REGION_SIZE_200MB, 10); // 200MB to pass size threshold

    if let RegionManifestInfo::Mito {
        file_removed_cnt, ..
    } = &mut region_stat.region_manifest
    {
        *file_removed_cnt = 5;
    }

    let table_stats = HashMap::from([(table_id, vec![region_stat])]);

    let ctx = Arc::new(MockSchedulerCtx {
        table_to_region_stats: Arc::new(Mutex::new(Some(table_stats))),
        ..Default::default()
    });

    // Test with different weights
    let config1 = GcSchedulerOptions {
        sst_count_weight: 2.0,
        file_removed_count_weight: 0.5,
        min_region_size_threshold: 100 * 1024 * 1024, // 100MB (default)
        ..Default::default()
    };

    let scheduler1 = GcScheduler {
        ctx: ctx.clone(),
        receiver: GcScheduler::channel().1,
        config: config1,
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };

    let stats = ctx
        .table_to_region_stats
        .lock()
        .unwrap()
        .clone()
        .unwrap_or_default();

    let candidates1 = scheduler1.select_gc_candidates(&stats).await.unwrap();

    let config2 = GcSchedulerOptions {
        sst_count_weight: 0.5,
        file_removed_count_weight: 2.0,
        min_region_size_threshold: 100 * 1024 * 1024, // 100MB (default)
        ..Default::default()
    };

    let scheduler2 = GcScheduler {
        ctx: ctx.clone(),
        receiver: GcScheduler::channel().1,
        config: config2,
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };

    let stats = &ctx
        .table_to_region_stats
        .lock()
        .unwrap()
        .clone()
        .unwrap_or_default();
    let candidates2 = scheduler2.select_gc_candidates(stats).await.unwrap();

    // Both should select the region but with different scores
    assert_eq!(
        candidates1.len(),
        1,
        "Expected 1 table with candidates for config1, got {}",
        candidates1.len()
    );
    assert_eq!(
        candidates2.len(),
        1,
        "Expected 1 table with candidates for config2, got {}",
        candidates2.len()
    );

    // Verify the region is actually selected
    assert!(
        candidates1.contains_key(&table_id),
        "Config1 should contain table_id {}",
        table_id
    );
    assert!(
        candidates2.contains_key(&table_id),
        "Config2 should contain table_id {}",
        table_id
    );
}

#[tokio::test]
async fn test_regions_per_table_threshold() {
    init_default_ut_logging();

    let table_id = 1;
    let mut region_stats = Vec::new();

    // Create many regions
    for i in 1..=10 {
        let region_id = RegionId::new(table_id, i as u32);
        let mut stat = mock_region_stat(region_id, RegionRole::Leader, TEST_REGION_SIZE_200MB, 10); // 200MB

        if let RegionManifestInfo::Mito {
            file_removed_cnt, ..
        } = &mut stat.region_manifest
        {
            *file_removed_cnt = 5;
        }

        region_stats.push(stat);
    }

    let table_stats = HashMap::from([(table_id, region_stats)]);

    let ctx = Arc::new(MockSchedulerCtx {
        table_to_region_stats: Arc::new(Mutex::new(Some(table_stats))),
        ..Default::default()
    });

    let config = GcSchedulerOptions {
        regions_per_table_threshold: 3, // Limit to 3 regions per table
        min_region_size_threshold: 100 * 1024 * 1024, // 100MB (default)
        ..Default::default()
    };

    let scheduler = GcScheduler {
        ctx: ctx.clone(),
        receiver: GcScheduler::channel().1,
        config,
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };

    let stats = ctx
        .table_to_region_stats
        .lock()
        .unwrap()
        .clone()
        .unwrap_or_default();

    let candidates = scheduler.select_gc_candidates(&stats).await.unwrap();

    assert_eq!(
        candidates.len(),
        1,
        "Expected 1 table with candidates, got {}",
        candidates.len()
    );
    if let Some(table_candidates) = candidates.get(&table_id) {
        // Should be limited to 3 regions
        assert_eq!(
            table_candidates.len(),
            3,
            "Expected 3 candidates for table {}, got {}",
            table_id,
            table_candidates.len()
        );
    } else {
        panic!("Expected table {} to have candidates", table_id);
    }
}

#[tokio::test]
async fn test_max_regions_per_batch() {
    init_default_ut_logging();

    use common_meta::peer::Peer;
    use store_api::storage::GcReport;

    use crate::gc::mock::{MockSchedulerCtx, new_empty_report_with};

    let table_id = 1;
    let peer = Peer::new(1, "");

    // Create 100 candidate regions
    let mut candidates = Vec::new();
    let mut region_ids = Vec::new();
    for i in 1..=100 {
        let region_id = RegionId::new(table_id, i);
        region_ids.push(region_id);
        candidates.push((table_id, crate::gc::mock::mock_candidate(region_id)));
    }

    // Setup mock GC reports for all regions
    let mut gc_reports = HashMap::new();
    for region_id in &region_ids {
        gc_reports.insert(*region_id, new_empty_report_with([*region_id]));
    }

    let ctx = Arc::new(
        MockSchedulerCtx {
            gc_reports: Arc::new(Mutex::new(gc_reports)),
            ..Default::default()
        }
        .with_table_routes(HashMap::from([(
            table_id,
            (
                table_id,
                region_ids.iter().map(|r| (*r, peer.clone())).collect(),
            ),
        )])),
    );

    // Set max_regions_per_batch to 25 (expecting 4 batches for 100 regions)
    let config = GcSchedulerOptions {
        max_regions_per_batch: 25,
        min_region_size_threshold: 100 * 1024 * 1024, // 100MB (default)
        ..Default::default()
    };

    let scheduler = GcScheduler {
        ctx: ctx.clone(),
        receiver: GcScheduler::channel().1,
        config,
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };

    // Process all candidates for the datanode
    let report = scheduler
        .process_datanode_gc(peer, candidates)
        .await
        .unwrap();

    // Verify gc_regions was called multiple times (at least 4 batches)
    // Since each batch may split into fast_list and full_list, we expect >= 4 calls
    let gc_calls = *ctx.gc_regions_calls.lock().unwrap();
    assert!(
        gc_calls >= 4,
        "Expected at least 4 gc_regions calls for 100 regions with batch size 25, got {}",
        gc_calls
    );

    // Verify all regions were processed in the final report
    // The tracker should have entries for all regions
    let tracker = scheduler.region_gc_tracker.lock().await;
    assert_eq!(
        tracker.len(),
        100,
        "Expected 100 regions in tracker, got {}",
        tracker.len()
    );
}
