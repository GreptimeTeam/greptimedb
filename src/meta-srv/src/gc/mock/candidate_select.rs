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

/// Candidate Selection Tests
#[tokio::test]
async fn test_gc_candidate_filtering_by_role() {
    init_default_ut_logging();

    let table_id = 1;
    let leader_region = RegionId::new(table_id, 1);
    let follower_region = RegionId::new(table_id, 2);

    let mut leader_stat = mock_region_stat(leader_region, RegionRole::Leader, 200_000_000, 10); // 200MB

    let mut follower_stat =
        mock_region_stat(follower_region, RegionRole::Follower, 200_000_000, 10); // 200MB

    // Set up manifest info for scoring
    if let RegionManifestInfo::Mito {
        file_removed_cnt, ..
    } = &mut leader_stat.region_manifest
    {
        *file_removed_cnt = 5;
    }
    if let RegionManifestInfo::Mito {
        file_removed_cnt, ..
    } = &mut follower_stat.region_manifest
    {
        *file_removed_cnt = 5;
    }

    let table_stats = HashMap::from([(table_id, vec![leader_stat.clone(), follower_stat.clone()])]);

    let ctx = Arc::new(MockSchedulerCtx {
        table_to_region_stats: Arc::new(Mutex::new(Some(table_stats))),
        ..Default::default()
    });

    let scheduler = GcScheduler {
        ctx: ctx.clone(),
        receiver: GcScheduler::channel().1,
        config: GcSchedulerOptions::default(),
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

    // Should only select leader regions
    assert_eq!(
        candidates.len(),
        1,
        "Expected 1 table with candidates, got {}",
        candidates.len()
    );
    if let Some(table_candidates) = candidates.get(&table_id) {
        assert_eq!(
            table_candidates.len(),
            1,
            "Expected 1 candidate for table {}, got {}",
            table_id,
            table_candidates.len()
        );
        assert_eq!(
            table_candidates[0].region_id, leader_region,
            "Expected leader region {}, got {}",
            leader_region, table_candidates[0].region_id
        );
    } else {
        panic!("Expected table {} to have candidates", table_id);
    }
}

#[tokio::test]
async fn test_gc_candidate_size_threshold() {
    init_default_ut_logging();

    let table_id = 1;
    let small_region = RegionId::new(table_id, 1);
    let large_region = RegionId::new(table_id, 2);

    let mut small_stat = mock_region_stat(small_region, RegionRole::Leader, 50_000_000, 5); // 50MB
    if let RegionManifestInfo::Mito {
        file_removed_cnt, ..
    } = &mut small_stat.region_manifest
    {
        *file_removed_cnt = 3;
    }

    let mut large_stat = mock_region_stat(large_region, RegionRole::Leader, 200_000_000, 20); // 200MB
    if let RegionManifestInfo::Mito {
        file_removed_cnt, ..
    } = &mut large_stat.region_manifest
    {
        *file_removed_cnt = 5;
    }

    let table_stats = HashMap::from([(table_id, vec![small_stat, large_stat])]);

    let ctx = Arc::new(MockSchedulerCtx {
        table_to_region_stats: Arc::new(Mutex::new(Some(table_stats))),
        ..Default::default()
    });

    let config = GcSchedulerOptions {
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

    // Should only select large region
    assert_eq!(
        candidates.len(),
        1,
        "Expected 1 table with candidates, got {}",
        candidates.len()
    );
    if let Some(table_candidates) = candidates.get(&table_id) {
        assert_eq!(
            table_candidates.len(),
            1,
            "Expected 1 candidate for table {}, got {}",
            table_id,
            table_candidates.len()
        );
        assert_eq!(
            table_candidates[0].region_id, large_region,
            "Expected large region {}, got {}",
            large_region, table_candidates[0].region_id
        );
    } else {
        panic!("Expected table {} to have candidates", table_id);
    }
}

#[tokio::test]
async fn test_gc_candidate_scoring() {
    init_default_ut_logging();

    let table_id = 1;
    let low_score_region = RegionId::new(table_id, 1);
    let high_score_region = RegionId::new(table_id, 2);

    let mut low_stat = mock_region_stat(low_score_region, RegionRole::Leader, 200_000_000, 5); // 200MB
    // Set low file removal rate for low_score_region
    if let RegionManifestInfo::Mito {
        file_removed_cnt, ..
    } = &mut low_stat.region_manifest
    {
        *file_removed_cnt = 2;
    }

    let mut high_stat = mock_region_stat(high_score_region, RegionRole::Leader, 200_000_000, 50); // 200MB
    // Set high file removal rate for high_score_region
    if let RegionManifestInfo::Mito {
        file_removed_cnt, ..
    } = &mut high_stat.region_manifest
    {
        *file_removed_cnt = 20;
    }

    let table_stats = HashMap::from([(table_id, vec![low_stat, high_stat])]);

    let ctx = Arc::new(MockSchedulerCtx {
        table_to_region_stats: Arc::new(Mutex::new(Some(table_stats))),
        ..Default::default()
    });

    let config = GcSchedulerOptions {
        sst_count_weight: 1.0,
        file_removed_count_weight: 0.5,
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

    // Should select both regions but high score region should be first
    assert_eq!(
        candidates.len(),
        1,
        "Expected 1 table with candidates, got {}",
        candidates.len()
    );
    if let Some(table_candidates) = candidates.get(&table_id) {
        assert_eq!(
            table_candidates.len(),
            2,
            "Expected 2 candidates for table {}, got {}",
            table_id,
            table_candidates.len()
        );
        // Higher score region should come first (sorted by score descending)
        assert_eq!(
            table_candidates[0].region_id, high_score_region,
            "High score region should be first"
        );
        assert!(
            table_candidates[0].score > table_candidates[1].score,
            "High score region should have higher score: {} > {}",
            table_candidates[0].score,
            table_candidates[1].score
        );
    } else {
        panic!("Expected table {} to have candidates", table_id);
    }
}

#[tokio::test]
async fn test_gc_candidate_regions_per_table_threshold() {
    init_default_ut_logging();

    let table_id = 1;
    // Create 10 regions for the same table
    let mut region_stats = Vec::new();

    for i in 0..10 {
        let region_id = RegionId::new(table_id, i + 1);
        let mut stat = mock_region_stat(region_id, RegionRole::Leader, 200_000_000, 20); // 200MB

        // Set different file removal rates to create different scores
        // Higher region IDs get higher scores (better GC candidates)
        if let RegionManifestInfo::Mito {
            file_removed_cnt, ..
        } = &mut stat.region_manifest
        {
            *file_removed_cnt = (i as u64 + 1) * 2; // Region 1: 2, Region 2: 4, ..., Region 10: 20
        }

        region_stats.push(stat);
    }

    let table_stats = HashMap::from([(table_id, region_stats)]);

    let ctx = Arc::new(MockSchedulerCtx {
        table_to_region_stats: Arc::new(Mutex::new(Some(table_stats))),
        ..Default::default()
    });

    // Set regions_per_table_threshold to 3
    let config = GcSchedulerOptions {
        regions_per_table_threshold: 3,
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

    // Should have 1 table with candidates
    assert_eq!(
        candidates.len(),
        1,
        "Expected 1 table with candidates, got {}",
        candidates.len()
    );

    if let Some(table_candidates) = candidates.get(&table_id) {
        // Should only have 3 candidates due to regions_per_table_threshold
        assert_eq!(
            table_candidates.len(),
            3,
            "Expected 3 candidates for table {} due to regions_per_table_threshold, got {}",
            table_id,
            table_candidates.len()
        );

        // Verify that the top 3 scoring regions are selected
        // Regions 8, 9, 10 should have the highest scores (file_removed_cnt: 16, 18, 20)
        // They should be returned in descending order by score
        let expected_regions = vec![10, 9, 8];
        let actual_regions: Vec<u32> = table_candidates
            .iter()
            .map(|c| c.region_id.region_number())
            .collect();

        assert_eq!(
            actual_regions, expected_regions,
            "Expected regions {:?} to be selected, got {:?}",
            expected_regions, actual_regions
        );

        // Verify they are sorted by score in descending order
        for i in 0..table_candidates.len() - 1 {
            assert!(
                table_candidates[i].score >= table_candidates[i + 1].score,
                "Candidates should be sorted by score descending: {} >= {}",
                table_candidates[i].score,
                table_candidates[i + 1].score
            );
        }
    } else {
        panic!("Expected table {} to have candidates", table_id);
    }
}
