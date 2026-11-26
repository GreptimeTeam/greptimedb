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

//! Tests for classic-table primary-key interval sampling.
//!
//! `ScanRequest::pk_sample` maps a requested fraction onto one interval of the
//! encoded primary key, which is what lets a *range* predicate prune files and
//! row groups instead of only filtering rows after they are read.

use std::collections::BTreeMap;

use api::v1::Rows;
use api::v1::helper::row;
use api::v1::value::ValueData;
use common_recordbatch::RecordBatches;
use datatypes::arrow::array::{StringArray, TimestampMillisecondArray};
use store_api::region_engine::RegionEngine;
use store_api::region_request::RegionRequest;
use store_api::storage::{PkSample, RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::metrics::PK_SAMPLE_STATS;
use crate::test_util::{CreateRequestBuilder, TestEnv, flush_region, put_rows, rows_schema};

/// Rows per row group, so a flushed file is split into many row groups and the
/// sample has something to prune below the file level.
const ROW_GROUP_ROWS: usize = 20;

/// Counter kinds recorded by the primary-key sampler.
const STAT_KINDS: [&str; 6] = [
    "files_in_range",
    "files_without_stats",
    "files_pruned",
    "row_groups_checked",
    "row_groups_pruned",
    "rows_filtered",
];

type Counters = BTreeMap<&'static str, u64>;

fn counters() -> Counters {
    STAT_KINDS
        .into_iter()
        .map(|kind| (kind, PK_SAMPLE_STATS.with_label_values(&[kind]).get()))
        .collect()
}

fn delta(before: &Counters, after: &Counters, kind: &str) -> u64 {
    after[kind] - before[kind]
}

/// Reads `(tag -> sorted timestamps)` of a scan so a test can compare keys
/// instead of row order.
async fn read_keys(
    engine: &MitoEngine,
    region_id: RegionId,
    request: ScanRequest,
) -> BTreeMap<String, Vec<i64>> {
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let mut keys: BTreeMap<String, Vec<i64>> = BTreeMap::new();
    for batch in batches.iter() {
        let batch = batch.df_record_batch();
        let tags = batch
            .column_by_name("tag_0")
            .expect("tag_0 must exist")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let timestamps = batch
            .column_by_name("ts")
            .expect("ts must exist")
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        for (tag, ts) in tags.iter().zip(timestamps.iter()) {
            keys.entry(tag.unwrap().to_string())
                .or_default()
                .push(ts.unwrap());
        }
    }
    for timestamps in keys.values_mut() {
        timestamps.sort_unstable();
    }
    keys
}

fn key_rows(keys: impl Iterator<Item = usize>, ts: i64) -> Vec<api::v1::Row> {
    keys.map(|i| {
        row(vec![
            ValueData::StringValue(format!("t{i:04}")),
            ValueData::F64Value(i as f64),
            ValueData::TimestampMillisecondValue(ts),
        ])
    })
    .collect()
}

/// A dense (classic) table keeps its rows in primary-key order, so one encoded-key
/// interval covers about the requested fraction of them. The sample prunes whole
/// files and row groups and never splits a key across the memtable and the SSTs.
#[tokio::test]
#[allow(clippy::print_stdout)] // The read counts are this test's evidence; report them.
async fn test_pk_sample_prunes_and_never_splits_a_key() {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            // Keep the five files this test builds; compaction would merge them and
            // remove the overlap the sample has to straddle.
            min_compaction_interval: std::time::Duration::from_secs(60 * 60),
            schedule_compaction_after_edit: false,
            ..Default::default()
        })
        .await;
    let region_id = RegionId::new(1024, 0);
    let request = CreateRequestBuilder::new().build();
    let schema = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let write = |keys: Vec<api::v1::Row>| {
        put_rows(
            &engine,
            region_id,
            Rows {
                schema: schema.clone(),
                rows: keys,
            },
        )
    };

    // `wide` sorts first and covers keys beyond the interval, so the sample keeps the
    // file but prunes its upper row groups: it straddles the interval end.
    write(key_rows(10..400, 1000)).await;
    flush_region(&engine, region_id, Some(ROW_GROUP_ROWS)).await;
    // A file below the interval is scanned whole.
    write(key_rows(0..60, 2000)).await;
    flush_region(&engine, region_id, Some(ROW_GROUP_ROWS)).await;
    // A file entirely above the interval is pruned by its manifest statistics.
    write(key_rows(400..460, 3000)).await;
    flush_region(&engine, region_id, Some(ROW_GROUP_ROWS)).await;
    // A memtable version of every tenth key: each of these keys now has rows in both
    // an SST and the memtable.
    write(key_rows((0..460).step_by(10), 4000)).await;

    let full_request = ScanRequest::default();
    let before = counters();
    let full = read_keys(&engine, region_id, full_request.clone()).await;
    let full_counters = counters();
    let full_rows: usize = full.values().map(|timestamps| timestamps.len()).sum();

    let sample = PkSample { num: 1, den: 4 };
    let sampled_request = ScanRequest {
        pk_sample: Some(sample),
        ..full_request
    };
    let before_sampled = counters();
    let sampled = read_keys(&engine, region_id, sampled_request).await;
    let sampled_counters = counters();
    let sampled_rows: usize = sampled.values().map(|ts| ts.len()).sum();

    let files_pruned = delta(&before_sampled, &sampled_counters, "files_pruned");
    let row_groups_pruned = delta(&before_sampled, &sampled_counters, "row_groups_pruned");
    let row_groups_checked = delta(&before_sampled, &sampled_counters, "row_groups_checked");
    let rows_filtered = delta(&before_sampled, &sampled_counters, "rows_filtered");
    let ratio = sampled_rows as f64 / full_rows as f64;
    println!(
        "pk sample {sample}: rows {sampled_rows}/{full_rows} (ratio {ratio:.3}), \
         files pruned {files_pruned}, row groups pruned {row_groups_pruned}/{row_groups_checked}, \
         rows filtered {rows_filtered}"
    );

    // The full scan has nothing to prune.
    assert_eq!(0, delta(&before, &full_counters, "files_pruned"));
    assert_eq!(0, delta(&before, &full_counters, "row_groups_pruned"));
    // A range on the encoded key really reads less instead of only filtering rows:
    // both whole files and whole row groups are skipped.
    assert!(
        files_pruned > 0,
        "the sample must prune whole files: {files_pruned}"
    );
    assert!(
        row_groups_pruned > 0,
        "the sample must prune whole row groups of a straddling file: {row_groups_pruned}"
    );
    assert!(
        row_groups_pruned < row_groups_checked,
        "some row groups must still be read: {row_groups_pruned}/{row_groups_checked}"
    );

    // The sample is a subset of the full scan and roughly the requested fraction.
    assert!(
        sampled.keys().all(|key| full.contains_key(key)),
        "a sampled scan must return a subset of the full scan"
    );
    assert!(
        (0.15..=0.38).contains(&ratio),
        "sampled rows must be about a quarter of the full scan: {sampled_rows}/{full_rows} = {ratio}"
    );

    // A key is kept entirely or dropped entirely: a key with both a memtable and an
    // SST version never returns only one of its versions.
    for (key, timestamps) in &full {
        if let Some(sampled_timestamps) = sampled.get(key) {
            assert_eq!(
                timestamps, sampled_timestamps,
                "key {key} is half-in the sample: {sampled_timestamps:?} != {timestamps:?}"
            );
        }
    }
    // Both halves of the range are exercised: keys with a memtable version are kept
    // below the interval and dropped above it.
    assert!(sampled.keys().any(|key| key == "t0000"));
    assert!(!sampled.contains_key("t0450"));
    assert!(sampled.keys().any(|key| key == "t0040"));
    assert!(!sampled.contains_key("t0440"));
}

/// A request the sampler cannot honor is rejected instead of silently returning
/// every row.
#[tokio::test]
async fn test_pk_sample_rejects_invalid_fraction() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1024, 1);
    let request = CreateRequestBuilder::new().build();
    let schema = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    put_rows(
        &engine,
        region_id,
        Rows {
            schema,
            rows: key_rows(0..10, 1000),
        },
    )
    .await;
    flush_region(&engine, region_id, Some(ROW_GROUP_ROWS)).await;

    for sample in [
        PkSample { num: 0, den: 4 },
        PkSample { num: 4, den: 4 },
        PkSample { num: 5, den: 4 },
        PkSample { num: 1, den: 0 },
    ] {
        let request = ScanRequest {
            pk_sample: Some(sample),
            ..Default::default()
        };
        let err = engine.scan_to_stream(region_id, request).await;
        assert!(err.is_err(), "invalid sample {sample} must be rejected");
    }

    // The valid fraction still works.
    let request = ScanRequest {
        pk_sample: Some(PkSample { num: 1, den: 2 }),
        ..Default::default()
    };
    let keys = read_keys(&engine, region_id, request).await;
    assert!(!keys.is_empty());
}
