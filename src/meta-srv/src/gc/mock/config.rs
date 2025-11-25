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

use crate::gc::mock::{MockSchedulerCtx, mock_region_stat};
use crate::gc::{GcScheduler, GcSchedulerOptions};

/// Configuration Tests
#[tokio::test]
async fn test_different_gc_weights() {
    init_default_ut_logging();

    let table_id = 1;
    let region_id = RegionId::new(table_id, 1);

    let mut region_stat = mock_region_stat(region_id, RegionRole::Leader, 200_000_000, 10); // 200MB to pass size threshold

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
        let mut stat = mock_region_stat(region_id, RegionRole::Leader, 200_000_000, 10); // 200MB

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
