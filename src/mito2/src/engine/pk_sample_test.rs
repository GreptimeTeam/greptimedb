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
use std::sync::Arc;

use api::v1::Rows;
use api::v1::helper::row;
use api::v1::value::ValueData;
use common_recordbatch::RecordBatches;
use datafusion_expr::{Expr, col, lit};
use datatypes::arrow::array::{ArrayRef, Float64Array, StringArray, TimestampMillisecondArray};
use datatypes::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datatypes::arrow::record_batch::RecordBatch as DfRecordBatch;
use mito_codec::row_converter::build_primary_key_codec;
use store_api::region_engine::RegionEngine;
use store_api::region_request::RegionRequest;
use store_api::storage::{PkSample, RegionId, ScanRequest};

use crate::access_layer::{Metrics, OperationType, SstWriteRequest, WriteType};
use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::manifest::action::RegionEdit;
use crate::memtable::bulk::part::{BulkPart, convert_bulk_part};
use crate::metrics::PK_SAMPLE_STATS;
use crate::sst::file::FileMeta;
use crate::sst::parquet::WriteOptions;
use crate::sst::{FlatSchemaOptions, FormatType, to_flat_sst_arrow_schema};
use crate::test_util::sst_util::new_flat_source_from_record_batches;
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

/// Serializes the tests of this module that sample, and therefore touch
/// [`PK_SAMPLE_STATS`].
///
/// The sampler records its work in that one process-wide counter vector: it has no
/// per-scan handle a test could read instead (the interval travels on the scan
/// input, but the counters are incremented through `pk_sample::inc`). A
/// `before`/`after` difference is only meaningful while the only scans of the
/// process are this module's own, so every test that samples -- the ones that read
/// a difference *and* the ones that only produce increments -- holds this lock.
///
/// The lock costs nothing under a runner that gives each test its own process
/// (nextest), and it keeps these tests honest under a runner that shares one
/// process across threads (`cargo test`), where the differences would otherwise
/// count the reads of a concurrent test.
static PK_SAMPLE_STATS_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn lock_pk_sample_stats() -> tokio::sync::MutexGuard<'static, ()> {
    PK_SAMPLE_STATS_TEST_LOCK.lock().await
}

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
    let _guard = lock_pk_sample_stats().await;
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
    let _guard = lock_pk_sample_stats().await;
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
    let _guard = lock_pk_sample_stats().await;
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
    let _guard = lock_pk_sample_stats().await;
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
    let _guard = lock_pk_sample_stats().await;
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
    let _guard = lock_pk_sample_stats().await;
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

// ---------------------------------------------------------------------------
// Failing closed: a fraction the region cannot order is an error, not a full read.
// ---------------------------------------------------------------------------

/// Creates an empty classic region and returns the schema of its rows.
async fn create_empty_region(
    engine: &MitoEngine,
    region_id: RegionId,
) -> Vec<api::v1::ColumnSchema> {
    let request = CreateRequestBuilder::new().build();
    let schema = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    schema
}

/// Writes one SST holding `keys` at `ts` into `region_id` and publishes it **without
/// encoded-key statistics**.
///
/// The SST data is written by the production writer, so the rows are readable and
/// their row groups carry column statistics; only the manifest entry leaves
/// `primary_key_min`/`primary_key_max` unset, which is what a file whose key range
/// was never recorded (a legacy or externally written SST) looks like. The write
/// path itself is untouched: this test composes a manifest entry, it does not change
/// how mito2 records statistics.
async fn publish_file_without_key_stats(
    engine: &MitoEngine,
    region_id: RegionId,
    keys: std::ops::Range<usize>,
    ts: u64,
) {
    let region = engine.get_region(region_id).unwrap();
    let metadata = region.version().metadata.clone();
    let num_rows = keys.len();
    let ts = ts as i64;

    // The rows as a writer receives them, in the region's own schema.
    let tag = Arc::new(StringArray::from_iter_values(
        keys.clone().map(|i| format!("t{i:04}")),
    )) as ArrayRef;
    let field = Arc::new(Float64Array::from_iter_values(keys.map(|i| i as f64))) as ArrayRef;
    let timestamps = Arc::new(TimestampMillisecondArray::from_iter_values(
        std::iter::repeat_n(ts, num_rows),
    )) as ArrayRef;
    let rows = DfRecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("tag_0", DataType::Utf8, true),
            Field::new("field_0", DataType::Float64, true),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ])),
        vec![tag, field, timestamps],
    )
    .unwrap();

    // Encodes the primary key of every row, exactly like the bulk write path does.
    let part = convert_bulk_part(
        BulkPart {
            batch: rows,
            max_timestamp: ts,
            min_timestamp: ts,
            sequence: 1,
            min_sequence: 1,
            // `ts` is the third column of the batch above.
            timestamp_index: 2,
            raw_data: None,
        },
        &metadata,
        build_primary_key_codec(&metadata),
        to_flat_sst_arrow_schema(
            &metadata,
            &FlatSchemaOptions::from_encoding(metadata.primary_key_encoding),
        ),
        true,
    )
    .unwrap()
    .expect("the part holds rows");

    let info = region
        .access_layer
        .write_sst(
            SstWriteRequest {
                index_build_inputs: None,
                op_type: OperationType::Flush,
                metadata,
                source: new_flat_source_from_record_batches(vec![part.batch]),
                cache_manager: Default::default(),
                max_sequence: Some(1),
                // The layout the region's own flush writes by default.
                sst_write_format: FormatType::Flat,
                preserve_row_sequence: false,
                index_options: Default::default(),
                index_config: Default::default(),
                inverted_index_config: Default::default(),
                fulltext_index_config: Default::default(),
                bloom_filter_index_config: Default::default(),
            },
            &WriteOptions::default(),
            &mut Metrics::new(WriteType::Flush),
        )
        .await
        .unwrap()
        .remove(0);

    let file = FileMeta {
        region_id,
        file_id: info.file_id,
        time_range: info.time_range,
        level: 0,
        file_size: info.file_size,
        max_row_group_uncompressed_size: info.max_row_group_uncompressed_size,
        max_row_group_decoded_size: info.max_row_group_decoded_size,
        num_rows: info.num_rows as u64,
        num_row_groups: info.num_row_groups,
        num_series: info.num_series,
        // `primary_key_min`/`primary_key_max` stay `None`: no key statistics.
        ..Default::default()
    };
    // Guards the fixture: `FileHandle::primary_key_range` reports exactly this as
    // unknown bounds, so the sampler cannot place the file on the key axis.
    assert_eq!(
        None,
        file.primary_key_range(),
        "the published manifest entry must carry no encoded-key statistics"
    );
    engine
        .edit_region(
            region_id,
            RegionEdit {
                files_to_add: vec![file],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Guards the fixture: the handle the next scan sees reports unknown key bounds,
    // which is exactly the state the sampler must count as "no statistics".
    let region = engine.get_region(region_id).unwrap();
    let version = region.version();
    let handle = version
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files.values())
        .find(|handle| handle.meta_ref().file_id == info.file_id)
        .expect("the published file must be visible in the region");
    assert_eq!(
        None,
        handle.raw_primary_key_range(),
        "the region must not know the key range of the published file"
    );
}

/// A region whose candidate files all lack encoded-key statistics cannot honor a
/// sample fraction: the scan fails instead of returning every row.
#[tokio::test]
#[allow(clippy::print_stdout)] // The rejection message is this test's evidence; report it.
async fn test_pk_sample_without_key_statistics_is_rejected() {
    let _guard = lock_pk_sample_stats().await;
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(one_file_per_flush_config()).await;
    let region_id = RegionId::new(1024, 6);
    create_empty_region(&engine, region_id).await;
    publish_file_without_key_stats(&engine, region_id, 0..KEYS_PER_FILE, 1000).await;
    publish_file_without_key_stats(&engine, region_id, KEYS_PER_FILE..2 * KEYS_PER_FILE, 2000)
        .await;

    let before = counters();
    // `scanner` builds the scan input, which is where the sample is planned: the
    // rejection happens before any SST reader exists, so no row can be read.
    let err = engine
        .scanner(region_id, quarter_sample(vec![]))
        .await
        .err()
        .expect("a sample over files without key statistics must be rejected");
    let after = counters();

    // Failing closed means failing before the read: planning the interval rejects the
    // scan, so no SST reader is built and no row is scanned, filtered or returned.
    assert_eq!(
        0,
        delta(&before, &after, "row_groups_checked"),
        "the rejected scan must not read any row group"
    );
    assert_eq!(0, delta(&before, &after, "rows_filtered"));
    assert_eq!(0, delta(&before, &after, "files_pruned"));
    // The sampler reached its decision over the two files, both without statistics.
    assert_eq!(2, delta(&before, &after, "files_without_stats"));

    assert!(
        matches!(err, crate::error::Error::InvalidRequest { .. }),
        "the sample must be rejected as an invalid request, got: {err}"
    );
    let message = err.to_string();
    println!("rejected sample: {message}");
    assert!(
        message.contains(&region_id.to_string()),
        "the error must name the region: {message}"
    );
    assert!(
        message.contains("1/4"),
        "the error must name the requested fraction: {message}"
    );
    assert!(
        message.contains("2 candidate files"),
        "the error must name the candidate files: {message}"
    );
    assert!(
        message.contains("2 of them without encoded primary-key statistics"),
        "the error must name how many files lack statistics: {message}"
    );
    assert!(
        message.contains("0 rows in the key order"),
        "the error must report how many rows entered the key order: {message}"
    );
}

/// A file without key statistics is still scanned: the sample keeps its rows while it
/// samples the files it can order, and only counts the unknown file.
#[tokio::test]
#[allow(clippy::print_stdout)] // The read counts are this test's evidence; report them.
async fn test_pk_sample_keeps_rows_of_files_without_key_statistics() {
    let _guard = lock_pk_sample_stats().await;
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(one_file_per_flush_config()).await;
    let region_id = RegionId::new(1024, 7);
    let schema = create_empty_region(&engine, region_id).await;

    // Two files with statistics: `t0000`..`t0099` and `t0100`..`t0199`.
    for file in 0..2 {
        let start = file * KEYS_PER_FILE;
        put_rows(
            &engine,
            region_id,
            Rows {
                schema: schema.clone(),
                rows: key_rows(start..start + KEYS_PER_FILE, ((file + 1) * 1000) as i64),
            },
        )
        .await;
        flush_region(&engine, region_id, Some(ROW_GROUP_ROWS)).await;
    }
    // A third file with keys inside the interval the two known files plan but no
    // statistics of its own: `t0020`..`t0029` at timestamp 3000.
    publish_file_without_key_stats(&engine, region_id, 20..30, 3000).await;

    let full = read_keys(&engine, region_id, ScanRequest::default()).await;
    let full_rows: usize = full.values().map(|timestamps| timestamps.len()).sum();
    // The published SST is readable and its rows are part of the full scan, so a
    // missing row below can only come from the sample, not from the fixture.
    assert_eq!(
        2 * KEYS_PER_FILE + 10,
        full_rows,
        "the full scan reads both flushed files and the file without statistics"
    );

    let before = counters();
    let sampled = read_keys(&engine, region_id, quarter_sample(vec![])).await;
    let after = counters();

    let files_without_stats = delta(&before, &after, "files_without_stats");
    let files_pruned = delta(&before, &after, "files_pruned");
    let sampled_rows: usize = sampled.values().map(|ts| ts.len()).sum();
    println!(
        "mixed statistics with a quarter sample: {sampled_rows}/{full_rows} rows, \
         files without statistics {files_without_stats}, files pruned by the sample {files_pruned}"
    );

    // A quarter of the 200 rows with statistics is 50, so the interval is the first
    // file: keys `t0000`..=`t0099`. The unknown file cannot be ordered, so it is never
    // pruned and all of its rows are kept.
    assert_eq!(
        fixture_keys(0..KEYS_PER_FILE),
        sampled.keys().cloned().collect::<BTreeSet<_>>(),
        "the interval covers the first known file and the whole unknown file"
    );
    assert_eq!(
        vec![1000i64, 3000],
        sampled["t0020"],
        "the row of the file without statistics must reach the result"
    );
    assert_eq!(KEYS_PER_FILE + 10, sampled_rows, "no row is lost");
    assert_eq!(1, files_without_stats, "the unknown file is only counted");
    // Only the known file above the interval is pruned; the unknown file cannot be.
    assert_eq!(1, files_pruned);
    assert!(
        sampled.keys().all(|key| full.contains_key(key)),
        "a sampled scan must return a subset of the full scan"
    );
}

/// An empty region, and a key predicate that excludes every file, read no row and are
/// not errors: there is nothing to sample in either case.
#[tokio::test]
async fn test_pk_sample_of_nothing_is_not_an_error() {
    let _guard = lock_pk_sample_stats().await;
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(one_file_per_flush_config()).await;

    // A region without any candidate file.
    let empty_region = RegionId::new(1024, 8);
    create_empty_region(&engine, empty_region).await;
    let before = counters();
    let rows = read_keys(&engine, empty_region, quarter_sample(vec![])).await;
    let after = counters();
    assert!(rows.is_empty(), "an empty region has no row to sample");
    assert_eq!(0, delta(&before, &after, "files_in_range"));
    assert_eq!(0, delta(&before, &after, "files_without_stats"));

    // Candidates exist but the key predicate excludes every one of them.
    let filtered_region = RegionId::new(1024, 9);
    create_key_disjoint_files(&engine, filtered_region).await;
    let before = counters();
    let rows = read_keys(
        &engine,
        filtered_region,
        quarter_sample(vec![col("tag_0").eq(lit("zzz"))]),
    )
    .await;
    let after = counters();
    assert!(
        rows.is_empty(),
        "a predicate without a matching key reads nothing"
    );
    assert_eq!(
        FILES,
        delta(&before, &after, "files_outside_key_predicate") as usize
    );
    assert_eq!(
        0,
        delta(&before, &after, "files_without_stats"),
        "the excluded files carry statistics, they are simply out of range"
    );
}
