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

//! Answers aggregates of a scan from manifest and SST statistics instead of
//! reading SST data. The preparation result, an exact answer or a split into
//! exact and residual sources, does not depend on the aggregate; scalar COUNT
//! is the only aggregate served so far.

pub mod overlap;

use std::sync::Arc;

use api::v1::OpType;
use common_telemetry::debug;
use common_time::range::TimestampRange;
use datafusion_common::ScalarValue;
use datafusion_expr::{Expr, Operator};
use datatypes::data_type::DataType;
use datatypes::value::Value;
use futures::StreamExt;
use futures::future::BoxFuture;
use mito_codec::row_converter::{PrimaryKeyCodec, build_primary_key_codec};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::statistics::Statistics;
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{ColumnMetadata, RegionMetadata};
use store_api::storage::ScanRequest;
use store_api::storage::consts::OP_TYPE_COLUMN_NAME;

use crate::read::scan_region::ScanInput;
use crate::read::scan_region::aggr::overlap::{Extent, find_overlaps};
use crate::sst::file::FileHandle;

/// Returns the exact number of rows the scan yields, or `None` if it cannot
/// be proven. Reads no SST data: candidates come from the manifest, and in a
/// deduplicating region their Parquet metadata is read through the SST
/// metadata caches to rule out deletions.
pub(crate) async fn prepare(input: &ScanInput, request: &ScanRequest) -> Option<usize> {
    let classification = classify(input, request)?;
    if !classification.remaining.is_empty() || !input.memtables.is_empty() {
        return None;
    }
    let no_deletes = read_no_deletes(input, &classification).await;
    resolve(input, classification, &no_deletes)
}

/// Applies the deletion evidence of the exact candidates.
fn resolve(
    input: &ScanInput,
    mut classification: Classification,
    no_deletes: &[bool],
) -> Option<usize> {
    classification.retain_exact(input, no_deletes);
    (classification.remaining.is_empty() && input.memtables.is_empty())
        .then_some(classification.exact_rows)
}

/// Returns, for each exact candidate, whether it holds no deletion markers.
/// Append-mode files need no evidence. In a deduplicating region every SST is
/// deduplicated within itself, so a file without deletions yields each of its
/// rows. A file whose metadata cannot be read counts as having deletions.
async fn read_no_deletes(input: &ScanInput, classification: &Classification) -> Vec<bool> {
    if input.append_mode {
        return vec![true; classification.exact.len()];
    }
    // Each future owns its reader, so the stream does not borrow `input`.
    // Boxing erases the metadata loader's deep future type for callers.
    let reads: Vec<BoxFuture<'static, bool>> = classification
        .exact
        .iter()
        .map(|&index| {
            let file = input.files[index].clone();
            let reader = input
                .access_layer
                .read_sst(file.clone())
                .cache(input.cache_strategy.clone());
            Box::pin(async move {
                match reader.read_statistics().await {
                    Ok(metadata) => has_no_deletes(&metadata),
                    Err(e) => {
                        debug!(
                            "Metadata count falls back to scanning file {}: {e:?}",
                            file.file_id()
                        );
                        false
                    }
                }
            }) as BoxFuture<'static, bool>
        })
        .collect();
    futures::stream::iter(reads)
        .buffered(input.max_concurrent_scan_files.max(1))
        .collect()
        .await
}

/// Whether every row group's `__op_type` statistics prove it holds only puts.
fn has_no_deletes(metadata: &ParquetMetaData) -> bool {
    let Some(column) = metadata
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .position(|column| column.name() == OP_TYPE_COLUMN_NAME)
    else {
        return false;
    };
    metadata.row_groups().iter().all(|row_group| {
        matches!(
            row_group.column(column).statistics(),
            Some(Statistics::Int32(stats)) if stats.min_opt() == Some(&(OpType::Put as i32))
        )
    })
}

/// Files of one scan classified against its pinned read view.
struct Classification {
    /// Indexes into `ScanInput::files` whose rows are all yielded exactly once.
    exact: Vec<usize>,
    /// Indexes of the other files intersecting the query time range.
    remaining: Vec<usize>,
    /// Sum of the manifest row counts of `exact`.
    exact_rows: usize,
}

impl Classification {
    /// Moves the exact files without proof to `remaining`.
    fn retain_exact(&mut self, input: &ScanInput, proven: &[bool]) {
        debug_assert_eq!(self.exact.len(), proven.len());
        let mut exact = Vec::with_capacity(self.exact.len());
        for (&index, &proven) in self.exact.iter().zip(proven) {
            if proven {
                exact.push(index);
            } else {
                self.exact_rows -= input.files[index].num_rows();
                self.remaining.push(index);
            }
        }
        self.exact = exact;
        self.remaining.sort_unstable();
    }
}

/// A file is an exact candidate if all of the following hold:
/// - The query time range covers it and every other static filter holds for
///   all of its rows (see [`AggrFilters`]).
/// - Its manifest row count is known.
/// - In a deduplicating region, no other file or memtable may hold a row with
///   the same primary key and timestamp. The candidate must still be proven
///   free of deletions (see [`read_no_deletes`]).
///
/// Returns `None` for scans whose visible rows can differ from the sources'
/// rows for reasons other than the files themselves.
fn classify(input: &ScanInput, request: &ScanRequest) -> Option<Classification> {
    if input.compaction
        || input.series_row_selector.is_some()
        || input.sequence_range.is_some()
        || request.limit.is_some()
        || request.memtable_min_sequence.is_some()
        || request.memtable_max_sequence.is_some()
        || request.sst_min_sequence.is_some()
        || request.snapshot_on_scan
        || request.exact_sequence_range
        || request.skip_sst_files
    {
        return None;
    }
    #[cfg(feature = "enterprise")]
    if !input.extension_ranges.is_empty() {
        return None;
    }
    let filters = AggrFilters::new(input.region_metadata(), &request.filters)?;
    let range = input.time_range.as_ref()?;

    let mut intersecting = Vec::new();
    for (index, file) in input.files.iter().enumerate() {
        let (start, end) = file.time_range();
        if start > end {
            return None;
        }
        if range.intersects(&TimestampRange::new_inclusive(Some(start), Some(end))) {
            intersecting.push(index);
        }
    }
    // Append mode never merges rows, so sources cannot hide each other's rows.
    let overlaps = if input.append_mode {
        vec![false; intersecting.len()]
    } else {
        overlapping_files(input, &intersecting)
    };

    let mut classification = Classification {
        exact: Vec::new(),
        remaining: Vec::new(),
        exact_rows: 0,
    };
    for (index, overlaps) in intersecting.into_iter().zip(overlaps) {
        let file = &input.files[index];
        let (start, end) = file.time_range();
        let exact = range.contains(&start)
            && range.contains(&end)
            // Zero is unknown in legacy manifests.
            && file.num_rows() > 0
            && (input.region_partition_expr.is_none() || input.should_skip_region_partition(file))
            && filters.covers(file)
            && (input.append_mode || !overlaps);
        if exact {
            classification.exact_rows = classification.exact_rows.checked_add(file.num_rows())?;
            classification.exact.push(index);
        } else {
            classification.remaining.push(index);
        }
    }
    Some(classification)
}

/// Returns, for each file in `files`, whether another file or a memtable may
/// hold a row with the same primary key and timestamp.
fn overlapping_files(input: &ScanInput, files: &[usize]) -> Vec<bool> {
    let mapper = input.primary_key_mapper();
    let mut extents: Vec<_> = files
        .iter()
        .map(|&index| {
            let file = &input.files[index];
            Extent {
                time: file.time_range(),
                key: file
                    .raw_primary_key_range()
                    .and_then(|_| file.primary_key_range(mapper)),
            }
        })
        .collect();
    // Memtable key bounds are unknown.
    for memtable in &input.memtables {
        let stats = memtable.stats();
        match stats.time_range {
            Some(time) => extents.push(Extent { time, key: None }),
            None if stats.num_rows == 0 => {}
            None => return vec![true; files.len()],
        }
    }
    let mut overlaps = find_overlaps(&extents);
    overlaps.truncate(files.len());
    overlaps
}

/// Static scan predicates a metadata count can absorb.
struct AggrFilters {
    /// Values the leftmost primary key column must equal.
    leftmost_values: Vec<Value>,
    codec: Option<Arc<dyn PrimaryKeyCodec>>,
}

impl AggrFilters {
    /// Returns `None` if any conjunct is neither a time-index comparison nor an
    /// equality between the leftmost primary key column and a literal.
    fn new(metadata: &RegionMetadata, filters: &[Expr]) -> Option<Self> {
        let time_index = &metadata.time_index_column().column_schema;
        let time_type = time_index.data_type.as_arrow_type();
        let mut conjuncts = Vec::new();
        for expr in filters {
            split_and(expr, &mut conjuncts);
        }
        let mut leftmost_values = Vec::new();
        for expr in conjuncts {
            if time_filter(expr, &time_index.name, &time_type) {
                continue;
            }
            let column = &leftmost_primary_key(metadata)?.column_schema;
            let value = eq_literal(expr, &column.name, &column.data_type.as_arrow_type())?;
            leftmost_values.push(Value::try_from(value.clone()).ok()?);
        }
        let codec = (!leftmost_values.is_empty()).then(|| build_primary_key_codec(metadata));
        Some(Self {
            leftmost_values,
            codec,
        })
    }

    /// Whether the leftmost key equalities hold for every row of the file.
    /// Keys are sorted memcomparable bytes, so when both bounds decode to the
    /// same leftmost value, every key between them shares it.
    fn covers(&self, file: &FileHandle) -> bool {
        let Some(codec) = &self.codec else {
            return true;
        };
        let Some((min, max)) = file.raw_primary_key_range() else {
            return false;
        };
        match (codec.decode_leftmost(&min), codec.decode_leftmost(&max)) {
            (Ok(Some(low)), Ok(Some(high))) => {
                low == high && self.leftmost_values.iter().all(|value| *value == low)
            }
            _ => false,
        }
    }
}

/// The leftmost primary key column. Sparse keys are not supported yet.
fn leftmost_primary_key(metadata: &RegionMetadata) -> Option<&ColumnMetadata> {
    match metadata.primary_key_encoding {
        PrimaryKeyEncoding::Sparse => None,
        PrimaryKeyEncoding::Dense => metadata.primary_key_columns().next(),
    }
}

fn split_and<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            split_and(&binary.left, out);
            split_and(&binary.right, out);
        }
        _ => out.push(expr),
    }
}

/// Returns the literal of a non-null, same-type `column = literal` equality.
fn eq_literal<'a>(
    expr: &'a Expr,
    name: &str,
    data_type: &datatypes::arrow::datatypes::DataType,
) -> Option<&'a ScalarValue> {
    let Expr::BinaryExpr(binary) = expr else {
        return None;
    };
    if binary.op != Operator::Eq {
        return None;
    }
    let (column, value) = match (binary.left.as_ref(), binary.right.as_ref()) {
        (Expr::Column(column), Expr::Literal(value, _))
        | (Expr::Literal(value, _), Expr::Column(column)) => (column, value),
        _ => return None,
    };
    (column.name == name && !value.is_null() && &value.data_type() == data_type).then_some(value)
}

fn time_filter(expr: &Expr, name: &str, data_type: &datatypes::arrow::datatypes::DataType) -> bool {
    let Expr::BinaryExpr(binary) = expr else {
        return false;
    };
    if binary.op == Operator::And {
        return time_filter(&binary.left, name, data_type)
            && time_filter(&binary.right, name, data_type);
    }
    if !matches!(
        binary.op,
        Operator::Eq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
    ) {
        return false;
    }
    let (column, value) = match (binary.left.as_ref(), binary.right.as_ref()) {
        (Expr::Column(column), Expr::Literal(value, _))
        | (Expr::Literal(value, _), Expr::Column(column)) => (column, value),
        _ => return false,
    };
    column.name == name && !value.is_null() && &value.data_type() == data_type
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::{Arc, LazyLock, Mutex};

    use bytes::Bytes;
    use common_time::Timestamp;
    use common_time::timestamp::TimeUnit;
    use datafusion_expr::{col, lit};
    use datatypes::value::ValueRef;
    use mito_codec::row_converter::{DensePrimaryKeyCodec, PrimaryKeyCodecExt};
    use store_api::storage::{FileId, RegionId};
    use table::predicate::build_time_range_predicate;

    use super::*;
    use crate::read::flat_projection::FlatProjectionMapper;
    use crate::read::scan_region::PredicateGroup;
    use crate::sst::file::FileMeta;
    use crate::test_util::memtable_util::metadata_with_primary_key;
    use crate::test_util::new_noop_file_purger;
    use crate::test_util::scheduler_util::SchedulerEnv;

    /// Files whose `__op_type` statistics would show deletions. Unit tests
    /// have no Parquet files; engine tests cover reading the statistics.
    static WITH_DELETES: LazyLock<Mutex<HashSet<FileId>>> = LazyLock::new(Default::default);

    fn file(start: i64, end: i64, rows: u64, no_deletes: bool) -> FileHandle {
        let file_id = FileId::random();
        if !no_deletes {
            WITH_DELETES.lock().unwrap().insert(file_id);
        }
        FileHandle::new(
            FileMeta {
                file_id,
                num_rows: rows,
                time_range: (
                    Timestamp::new_millisecond(start),
                    Timestamp::new_millisecond(end),
                ),
                ..Default::default()
            },
            new_noop_file_purger(),
        )
    }

    /// A file without deletions whose primary keys span `min..=max` over `(k0, k1)`.
    fn keyed_file(
        start: i64,
        end: i64,
        rows: u64,
        min: (&str, u32),
        max: (&str, u32),
    ) -> FileHandle {
        let codec = DensePrimaryKeyCodec::new(&metadata_with_primary_key(vec![0, 1], false));
        let encode = |(k0, k1): (&str, u32)| {
            Bytes::from(
                codec
                    .encode([ValueRef::String(k0), ValueRef::UInt32(k1)].into_iter())
                    .unwrap(),
            )
        };
        FileHandle::new(
            FileMeta {
                region_id: RegionId::new(123, 456),
                file_id: FileId::random(),
                num_rows: rows,
                time_range: (
                    Timestamp::new_millisecond(start),
                    Timestamp::new_millisecond(end),
                ),
                primary_key_min: Some(encode(min)),
                primary_key_max: Some(encode(max)),
                ..Default::default()
            },
            new_noop_file_purger(),
        )
    }

    /// The deletion evidence [`read_no_deletes`] would read for the candidates.
    fn evidence(input: &ScanInput, classification: &Classification) -> Vec<bool> {
        let with_deletes = WITH_DELETES.lock().unwrap();
        classification
            .exact
            .iter()
            .map(|&index| {
                input.append_mode || !with_deletes.contains(&input.files[index].meta_ref().file_id)
            })
            .collect()
    }

    fn exact_count(input: &ScanInput, request: &ScanRequest) -> Option<usize> {
        let classification = classify(input, request)?;
        let no_deletes = evidence(input, &classification);
        resolve(input, classification, &no_deletes)
    }

    fn classified(input: &ScanInput, request: &ScanRequest) -> Classification {
        let mut classification = classify(input, request).unwrap();
        let no_deletes = evidence(input, &classification);
        classification.retain_exact(input, &no_deletes);
        classification
    }

    async fn input(files: Vec<FileHandle>, filters: &[Expr]) -> ScanInput {
        let env = SchedulerEnv::new().await;
        let metadata = Arc::new(metadata_with_primary_key(vec![0, 1], false));
        let mapper = FlatProjectionMapper::new(&metadata, [0, 2, 3]).unwrap();
        ScanInput::builder(env.access_layer.clone(), mapper)
            .with_files(files)
            .with_predicate(PredicateGroup::new(&metadata, filters).unwrap())
            .with_time_range(Some(build_time_range_predicate(
                "ts",
                TimeUnit::Millisecond,
                filters,
            )))
            .build()
    }

    #[tokio::test]
    async fn test_metadata_aggr_coverage_and_overlap() {
        let ts = |v| lit(ScalarValue::TimestampMillisecond(Some(v), None));
        for (filters, expected) in [
            (vec![], Some(5)),
            (vec![col("ts").lt(ts(20))], Some(3)),
            (vec![col("ts").gt_eq(ts(20))], Some(2)),
            (vec![col("ts").lt(ts(5))], None),
            (vec![col("ts").gt(ts(30))], Some(0)),
            (vec![col("ts").eq(ts(0)).or(col("ts").eq(ts(30)))], None),
            (vec![col("ts").not_eq(ts(5))], None),
            (vec![col("k0").eq(lit("x"))], None),
        ] {
            let input = input(vec![file(0, 10, 3, true), file(20, 30, 2, true)], &filters).await;
            let request = ScanRequest {
                filters,
                metadata_aggr_hint: true,
                ..Default::default()
            };
            // Deletion evidence comes from the test, see `WITH_DELETES`.
            assert_eq!(exact_count(&input, &request), expected, "{request:?}");
        }
        for files in [
            vec![file(0, 10, 3, true), file(10, 20, 2, true)],
            vec![file(0, 10, 3, true), file(5, 8, 2, true)],
            vec![file(0, 10, 3, false)],
            vec![file(0, 10, 0, false)],
        ] {
            assert_eq!(
                exact_count(&input(files, &[]).await, &ScanRequest::default()),
                None
            );
        }
    }

    #[tokio::test]
    async fn test_metadata_aggr_leftmost_key_filter() {
        let ts = |v| lit(ScalarValue::TimestampMillisecond(Some(v), None));
        let x = |start, end, rows| keyed_file(start, end, rows, ("x", 1), ("x", 9));
        let mixed = || keyed_file(20, 30, 2, ("x", 1), ("y", 1));
        let k0 = |v: &str| col("k0").eq(lit(v));
        for (files, filters, expected) in [
            // Both bounds prove the equality, so it holds for every row.
            (
                vec![x(0, 10, 3), x(20, 30, 2)],
                vec![col("ts").gt_eq(ts(0)).and(col("ts").lt(ts(40))), k0("x")],
                Some(5),
            ),
            (
                vec![x(0, 10, 3), x(20, 30, 2)],
                vec![lit("x").eq(col("k0"))],
                Some(5),
            ),
            // A file spanning several leftmost values may hold other rows.
            (vec![x(0, 10, 3), mixed()], vec![k0("x")], None),
            // A disproved equality is not used to exclude files.
            (vec![x(0, 10, 3)], vec![k0("y")], None),
            // Only the leftmost column is decodable from the bounds.
            (vec![x(0, 10, 3)], vec![col("k1").eq(lit(1u32))], None),
            (vec![x(0, 10, 3)], vec![col("k0").not_eq(lit("y"))], None),
            // Missing key statistics cannot prove anything.
            (vec![file(0, 10, 3, true)], vec![k0("x")], None),
        ] {
            let input = input(files, &filters).await;
            let request = ScanRequest {
                filters,
                metadata_aggr_hint: true,
                ..Default::default()
            };
            assert_eq!(exact_count(&input, &request), expected, "{request:?}");
        }

        let filters = vec![k0("x")];
        let input = input(vec![x(0, 10, 3), mixed()], &filters).await;
        let request = ScanRequest {
            filters,
            ..Default::default()
        };
        let classification = classified(&input, &request);
        assert_eq!(
            (classification.exact_rows, classification.remaining.len()),
            (3, 1)
        );
    }

    #[tokio::test]
    async fn test_metadata_aggr_fences_and_limits() {
        let input = input(vec![file(0, 10, 3, true)], &[]).await;
        for request in [
            ScanRequest {
                limit: Some(1),
                ..Default::default()
            },
            ScanRequest {
                memtable_max_sequence: Some(10),
                ..Default::default()
            },
            ScanRequest {
                memtable_min_sequence: Some(10),
                ..Default::default()
            },
            ScanRequest {
                snapshot_on_scan: true,
                ..Default::default()
            },
            ScanRequest {
                sst_min_sequence: Some(10),
                ..Default::default()
            },
            ScanRequest {
                exact_sequence_range: true,
                ..Default::default()
            },
            ScanRequest {
                skip_sst_files: true,
                ..Default::default()
            },
        ] {
            assert_eq!(exact_count(&input, &request), None);
        }
        let mut input = input;
        input.files.clear();
        assert_eq!(exact_count(&input, &ScanRequest::default()), Some(0));
    }

    #[tokio::test]
    async fn test_metadata_aggr_unknown_manifest_count_falls_back() {
        let mut input = input(vec![file(0, 10, 0, true)], &[]).await;
        assert_eq!(exact_count(&input, &ScanRequest::default()), None);
        input.append_mode = true;
        assert_eq!(exact_count(&input, &ScanRequest::default()), None);
    }
    #[tokio::test]
    async fn test_metadata_aggr_classifies_files() {
        for (files, exact_rows, exact, remaining) in [
            (
                vec![file(0, 10, 3, true), file(20, 30, 2, false)],
                3,
                vec![0],
                vec![1],
            ),
            // Without key bounds, the wide file overlaps both others in time.
            (
                vec![
                    file(0, 100, 3, true),
                    file(5, 8, 2, false),
                    file(20, 30, 4, true),
                ],
                0,
                vec![],
                vec![0, 1, 2],
            ),
            (
                vec![file(0, 10, 3, true), file(20, 30, 0, true)],
                3,
                vec![0],
                vec![1],
            ),
            // Disjoint keys keep time-overlapping files independent.
            (
                vec![
                    keyed_file(0, 10, 3, ("a", 0), ("a", 9)),
                    keyed_file(5, 15, 4, ("b", 0), ("b", 9)),
                    keyed_file(8, 9, 1, ("a", 5), ("a", 5)),
                ],
                4,
                vec![1],
                vec![0, 2],
            ),
        ] {
            let input = input(files, &[]).await;
            let classification = classified(&input, &ScanRequest::default());
            assert_eq!(classification.exact_rows, exact_rows);
            assert_eq!(classification.exact, exact);
            assert_eq!(classification.remaining, remaining);
        }
    }

    #[test]
    fn test_metadata_aggr_reads_op_type_statistics() {
        use datatypes::arrow::array::{ArrayRef, Int64Array, UInt8Array};
        use datatypes::arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use parquet::file::properties::WriterProperties;

        let metadata = |name: &str, ops: Vec<u8>| {
            let len = ops.len() as i64;
            let batch = RecordBatch::try_from_iter([
                (
                    "v",
                    Arc::new(Int64Array::from_iter_values(0..len)) as ArrayRef,
                ),
                (name, Arc::new(UInt8Array::from(ops)) as ArrayRef),
            ])
            .unwrap();
            let mut buffer = Vec::new();
            // Two rows per row group, so a delete in a later row group counts.
            let props = WriterProperties::builder()
                .set_max_row_group_row_count(Some(2))
                .build();
            let mut writer =
                ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            ParquetRecordBatchReaderBuilder::try_new(Bytes::from(buffer))
                .unwrap()
                .metadata()
                .clone()
        };
        let put = OpType::Put as u8;
        let delete = OpType::Delete as u8;
        assert!(has_no_deletes(&metadata(OP_TYPE_COLUMN_NAME, vec![put; 4])));
        assert!(!has_no_deletes(&metadata(
            OP_TYPE_COLUMN_NAME,
            vec![put, put, put, delete]
        )));
        assert!(!has_no_deletes(&metadata("other", vec![put; 4])));
    }
}
