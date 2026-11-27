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

use std::collections::{BTreeMap, BTreeSet};

use api::v1::Rows;
use api::v1::helper::row;
use api::v1::value::ValueData;
use common_recordbatch::RecordBatches;
use datafusion_expr::{Expr, col, lit};
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
const STAT_KINDS: [&str; 7] = [
    "files_in_range",
    "files_outside_key_predicate",
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

/// Files per fixture region, each holding [`KEYS_PER_FILE`] key-disjoint keys.
const FILES: usize = 4;
/// Keys per fixture file, so the key axis has `FILES * KEYS_PER_FILE` keys.
const KEYS_PER_FILE: usize = 100;
/// Row groups the whole fixture region holds at [`ROW_GROUP_ROWS`] rows per group.
const FIXTURE_ROW_GROUPS: u64 = (FILES * KEYS_PER_FILE / ROW_GROUP_ROWS) as u64;

/// Returns an engine config that keeps one file per flush.
///
/// Compaction would merge the key-disjoint files of a fixture into one file whose
/// range covers the whole key axis, which is exactly the shape these tests exclude.
fn one_file_per_flush_config() -> MitoConfig {
    MitoConfig {
        min_compaction_interval: std::time::Duration::from_secs(60 * 60),
        schedule_compaction_after_edit: false,
        ..Default::default()
    }
}

/// Creates a classic region whose files have key-disjoint manifest ranges: file
/// `i` holds the keys `t{i}000` .. `t{i}099` at timestamp `(i + 1) * 1000`, one row
/// per key. A key filter therefore selects whole files by their statistics.
async fn create_key_disjoint_files(engine: &MitoEngine, region_id: RegionId) {
    let request = CreateRequestBuilder::new().build();
    let schema = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    for file in 0..FILES {
        let start = file * KEYS_PER_FILE;
        let ts = ((file + 1) * 1000) as i64;
        put_rows(
            engine,
            region_id,
            Rows {
                schema: schema.clone(),
                rows: key_rows(start..start + KEYS_PER_FILE, ts),
            },
        )
        .await;
        flush_region(engine, region_id, Some(ROW_GROUP_ROWS)).await;
    }
}

/// The fixture keys `range` as strings, which is how a scan returns them.
fn fixture_keys(range: std::ops::Range<usize>) -> BTreeSet<String> {
    range.map(|i| format!("t{i:04}")).collect()
}

/// A quarter sample with the given filters.
fn quarter_sample(filters: Vec<Expr>) -> ScanRequest {
    ScanRequest {
        filters,
        pk_sample: Some(PkSample { num: 1, den: 4 }),
        ..Default::default()
    }
}

/// An equality predicate restricts the step function to the file it reads, so the
/// sample can no longer pick an interval the predicate never reads and return no
/// row at all.
#[tokio::test]
#[allow(clippy::print_stdout)] // The read counts are this test's evidence; report them.
async fn test_pk_sample_with_equality_predicate_keeps_the_key_it_reads() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(one_file_per_flush_config()).await;
    let region_id = RegionId::new(1024, 2);
    create_key_disjoint_files(&engine, region_id).await;

    // `t0200` is the first key of the third file: a sample over the whole key axis
    // would keep the first quarter of the axis, which holds no row of it.
    let before = counters();
    let sampled = read_keys(
        &engine,
        region_id,
        quarter_sample(vec![col("tag_0").eq(lit("t0200"))]),
    )
    .await;
    let after = counters();

    let files_outside = delta(&before, &after, "files_outside_key_predicate");
    let files_pruned = delta(&before, &after, "files_pruned");
    let row_groups_checked = delta(&before, &after, "row_groups_checked");
    println!(
        "key equality with a quarter sample: {} rows, files outside the predicate \
         {files_outside}, files pruned by the sample {files_pruned}, row groups checked \
         {row_groups_checked}/{FIXTURE_ROW_GROUPS}",
        sampled.values().map(|ts| ts.len()).sum::<usize>()
    );

    // The predicate reads the third file only: the other three never enter the
    // step function, and the interval of the third file prunes them as well.
    assert_eq!(
        BTreeSet::from(["t0200".to_string()]),
        sampled.keys().cloned().collect::<BTreeSet<_>>(),
        "the sampled scan must return the rows of the key predicate"
    );
    assert_eq!(vec![3000i64], sampled["t0200"], "with all of its columns");
    assert_eq!(FILES - 1, files_outside as usize);
    assert_eq!(FILES - 1, files_pruned as usize);
    assert!(
        row_groups_checked < FIXTURE_ROW_GROUPS,
        "the sample must read less than the whole table: {row_groups_checked}/{FIXTURE_ROW_GROUPS}"
    );
    // The sample still covers at most the requested fraction of the table.
    assert!(sampled.len() <= FILES * KEYS_PER_FILE / 4);
}

/// A range predicate that spans two files keeps pruning: only the files it reads
/// enter the step function, and the interval it picks still drops whole files.
#[tokio::test]
#[allow(clippy::print_stdout)] // The read counts are this test's evidence; report them.
async fn test_pk_sample_with_range_predicate_prunes_files_it_does_not_read() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(one_file_per_flush_config()).await;
    let region_id = RegionId::new(1024, 3);
    create_key_disjoint_files(&engine, region_id).await;

    // Keys `t0050`..=`t0250` live in the first three files.
    let filters = vec![
        col("tag_0")
            .gt_eq(lit("t0050"))
            .and(col("tag_0").lt_eq(lit("t0250"))),
    ];
    let before = counters();
    let sampled = read_keys(&engine, region_id, quarter_sample(filters)).await;
    let after = counters();

    let files_outside = delta(&before, &after, "files_outside_key_predicate");
    let files_pruned = delta(&before, &after, "files_pruned");
    let row_groups_checked = delta(&before, &after, "row_groups_checked");
    let query_rows = 250 - 50 + 1;
    let ratio = sampled.len() as f64 / query_rows as f64;
    println!(
        "key range with a quarter sample: {} rows of {query_rows} the predicate reads \
         (ratio {ratio:.3}), files outside the predicate {files_outside}, files pruned by the \
         sample {files_pruned}, row groups checked {row_groups_checked}/{FIXTURE_ROW_GROUPS}",
        sampled.len()
    );

    // The three files the predicate reads hold 300 rows, a quarter of them is 75,
    // so the interval is the first of those files: keys `t0000`..`t0099`.
    assert_eq!(
        fixture_keys(50..100),
        sampled.keys().cloned().collect::<BTreeSet<_>>(),
        "the interval starts at the first file the predicate reads"
    );
    // The fourth file is excluded twice over: by the predicate and by the interval.
    // Only the file the interval starts in survives, so every other file is pruned.
    assert_eq!(1, files_outside);
    assert_eq!(FILES - 1, files_pruned as usize);
    assert!(
        row_groups_checked < FIXTURE_ROW_GROUPS,
        "the sample must read less than the whole table: {row_groups_checked}/{FIXTURE_ROW_GROUPS}"
    );
    // The fraction is measured against the rows the query reads, not against the
    // whole table.
    assert!(
        (0.15..=0.38).contains(&ratio),
        "the sample must cover about a quarter of the rows the predicate reads: {ratio}"
    );
}

/// A predicate that matches no file returns no row instead of an error.
#[tokio::test]
#[allow(clippy::print_stdout)] // The read counts are this test's evidence; report them.
async fn test_pk_sample_with_key_predicate_outside_every_file_returns_no_rows() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(one_file_per_flush_config()).await;
    let region_id = RegionId::new(1024, 4);
    create_key_disjoint_files(&engine, region_id).await;

    let before = counters();
    let sampled = read_keys(
        &engine,
        region_id,
        quarter_sample(vec![col("tag_0").eq(lit("zzz"))]),
    )
    .await;
    let after = counters();

    let files_outside = delta(&before, &after, "files_outside_key_predicate");
    let files_pruned = delta(&before, &after, "files_pruned");
    println!(
        "empty key predicate with a quarter sample: {} rows, files outside the predicate \
         {files_outside}, files pruned by the sample {files_pruned}",
        sampled.len()
    );

    // No row matches, so no row is returned and no error is raised.
    assert!(
        sampled.is_empty(),
        "a predicate without a matching key reads nothing"
    );
    // Every file is excluded by the predicate, so the sampler plans no interval and
    // leaves the pruning to the predicate itself.
    assert_eq!(FILES, files_outside as usize);
    assert_eq!(0, files_pruned);
}

/// Without a key predicate the step function is the whole key axis, exactly as
/// before the key predicate was taken into account.
#[tokio::test]
#[allow(clippy::print_stdout)] // The read counts are this test's evidence; report them.
async fn test_pk_sample_without_key_predicate_is_unchanged() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(one_file_per_flush_config()).await;
    let region_id = RegionId::new(1024, 5);
    create_key_disjoint_files(&engine, region_id).await;

    let before = counters();
    let sampled = read_keys(&engine, region_id, quarter_sample(vec![])).await;
    let after = counters();

    let files_outside = delta(&before, &after, "files_outside_key_predicate");
    let row_groups_checked = delta(&before, &after, "row_groups_checked");
    let ratio = sampled.len() as f64 / (FILES * KEYS_PER_FILE) as f64;
    println!(
        "no key predicate with a quarter sample: {} rows of {} (ratio {ratio:.3}), files outside \
         the predicate {files_outside}, row groups checked {row_groups_checked}/{FIXTURE_ROW_GROUPS}",
        sampled.len(),
        FILES * KEYS_PER_FILE
    );

    // The four files hold 400 rows in key order, so a quarter sample (100 rows)
    // keeps the first file and nothing else.
    let expected = fixture_keys(0..KEYS_PER_FILE);
    assert_eq!(
        expected,
        sampled.keys().cloned().collect::<BTreeSet<_>>(),
        "the interval is the first quarter of the key axis, as before"
    );
    assert_eq!(0, files_outside, "no key filter excludes a file");
    assert!(
        (0.15..=0.38).contains(&ratio),
        "about a quarter of the table: {ratio}"
    );

    // An empty filter list and a key filter that keeps every file plan the same
    // interval, so both are equivalent to no predicate at all.
    for filters in [
        vec![col("tag_0").gt_eq(lit("t0000"))],
        vec![col("field_0").gt_eq(lit(0.0))],
    ] {
        let before = counters();
        let filtered = read_keys(&engine, region_id, quarter_sample(filters)).await;
        let after = counters();
        assert_eq!(
            0,
            delta(&before, &after, "files_outside_key_predicate"),
            "a filter that keeps every file must not narrow the step function"
        );
        assert_eq!(
            expected,
            filtered.keys().cloned().collect::<BTreeSet<_>>(),
            "the interval must not depend on a filter that keeps every file"
        );
    }
}
