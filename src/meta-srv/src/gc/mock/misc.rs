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
use store_api::storage::{FileRefsManifest, GcReport, RegionId};

use crate::gc::mock::{MockSchedulerCtx, new_candidate};
use crate::gc::{GcScheduler, GcSchedulerOptions};

/// Edge Case Tests

#[tokio::test]
async fn test_empty_file_refs_manifest() {
    init_default_ut_logging();

    let table_id = 1;
    let region_id = RegionId::new(table_id, 1);
    let peer = Peer::new(1, "");
    let candidates = HashMap::from([(table_id, vec![new_candidate(region_id, 1.0)])]);

    // Empty file refs manifest
    let file_refs = FileRefsManifest::default();

    let ctx = Arc::new(
        MockSchedulerCtx {
            file_refs: Arc::new(Mutex::new(Some(file_refs))),
            candidates: Arc::new(Mutex::new(Some(candidates))),
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

    let candidates = ctx.candidates.lock().unwrap().clone().unwrap_or_default();

    // Convert table-based candidates to datanode-based candidates
    let peer = Peer::new(1, "");
    let datanode_to_candidates = HashMap::from([(
        peer,
        candidates
            .into_iter()
            .flat_map(|(table_id, candidates)| candidates.into_iter().map(move |c| (table_id, c)))
            .collect(),
    )]);

    let report = scheduler
        .parallel_process_datanodes(datanode_to_candidates, HashMap::new(), HashMap::new())
        .await;

    match report {
        crate::gc::scheduler::GcJobReport::PerDatanode {
            per_datanode_reports,
            failed_datanodes,
        } => {
            assert_eq!(per_datanode_reports.len(), 1);
            assert_eq!(failed_datanodes.len(), 0);
        }
        crate::gc::scheduler::GcJobReport::Combined { .. } => {
            panic!("expected per-datanode report");
        }
    }
    // Should handle empty file refs gracefully
}

#[tokio::test]
async fn test_multiple_regions_per_table() {
    init_default_ut_logging();

    let table_id = 1;
    let region1 = RegionId::new(table_id, 1);
    let region2 = RegionId::new(table_id, 2);
    let region3 = RegionId::new(table_id, 3);
    let peer = Peer::new(1, "");

    let candidates = HashMap::from([(
        table_id,
        vec![
            new_candidate(region1, 1.0),
            new_candidate(region2, 2.0),
            new_candidate(region3, 3.0),
        ],
    )]);

    let mut gc_reports = HashMap::new();
    gc_reports.insert(region1, GcReport::default());
    gc_reports.insert(region2, GcReport::default());
    gc_reports.insert(region3, GcReport::default());

    let file_refs = FileRefsManifest {
        manifest_version: HashMap::from([(region1, 1), (region2, 1), (region3, 1)]),
        ..Default::default()
    };

    let ctx = Arc::new(
        MockSchedulerCtx {
            gc_reports: Arc::new(Mutex::new(gc_reports)),
            file_refs: Arc::new(Mutex::new(Some(file_refs))),
            candidates: Arc::new(Mutex::new(Some(candidates))),
            ..Default::default()
        }
        .with_table_routes(HashMap::from([(
            table_id,
            (
                table_id,
                vec![
                    (region1, peer.clone()),
                    (region2, peer.clone()),
                    (region3, peer.clone()),
                ],
            ),
        )])),
    );

    let scheduler = GcScheduler {
        ctx: ctx.clone(),
        receiver: GcScheduler::channel().1,
        config: GcSchedulerOptions::default(),
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };

    let candidates = ctx.candidates.lock().unwrap().clone().unwrap_or_default();

    // Convert table-based candidates to datanode-based candidates
    let datanode_to_candidates = HashMap::from([(
        peer.clone(),
        candidates
            .into_iter()
            .flat_map(|(table_id, candidates)| candidates.into_iter().map(move |c| (table_id, c)))
            .collect(),
    )]);

    let report = scheduler
        .parallel_process_datanodes(datanode_to_candidates, HashMap::new(), HashMap::new())
        .await;

    match report {
        crate::gc::scheduler::GcJobReport::PerDatanode {
            per_datanode_reports,
            failed_datanodes,
        } => {
            assert_eq!(per_datanode_reports.len(), 1);
            assert_eq!(failed_datanodes.len(), 0);
        }
        crate::gc::scheduler::GcJobReport::Combined { .. } => {
            panic!("expected per-datanode report");
        }
    }
}
