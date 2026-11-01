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

//! Optional exact-count preparation. All I/O is awaited in the caller's future,
//! so cancellation drops outstanding reads and cannot leave a background scan.

use std::sync::Arc;
use std::time::Duration;

use common_telemetry::debug;
use common_time::range::TimestampRange;
use datafusion_expr::{Expr, Operator};
use datatypes::data_type::DataType;
use futures::FutureExt;
use futures::future::BoxFuture;
use store_api::storage::ScanRequest;

use crate::read::scan_region::ScanInput;
use crate::region::options::MergeMode;
use crate::sst::file::FileHandle;
use crate::sst::parquet::metadata::MetadataLoader;
use crate::sst::parquet::reader::MetadataCacheMetrics;

/// These conservative limits apply to one region scan. Remote regions prepare
/// independently; the prototype does not claim a cluster-wide query I/O budget.
const MAX_FILES: usize = 64;
const MAX_FOOTERS: usize = 8;
const MAX_FOOTER_BYTES: u64 = 1024 * 1024;
const PREPARE_TIMEOUT: Duration = Duration::from_millis(20);

/// Proves the complete scan row count or returns unknown. Preparation timeout
/// falls back; dropping the enclosing query future cancels preparation itself.
pub(crate) fn prepare<'a>(
    input: &'a ScanInput,
    request: &'a ScanRequest,
) -> BoxFuture<'a, Option<usize>> {
    // Erase the optional preparation future at its boundary so the nested
    // Parquet reader types do not inflate callers' ingest/heartbeat futures.
    async move {
        let files = candidates(input, request)?;
        let result = tokio::time::timeout(PREPARE_TIMEOUT, count_files(input, &files)).await;
        match result {
            Ok(rows) => rows,
            Err(_) => {
                debug!("Metadata count preparation deadline exceeded");
                None
            }
        }
    }
    .boxed()
}

/// Checks all visibility conditions before any footer I/O. Only time-index
/// comparisons combined with AND are allowed, and every selected file must be
/// wholly covered. Inclusive file time ranges must be strictly disjoint.
fn candidates<'a>(input: &'a ScanInput, request: &ScanRequest) -> Option<Vec<&'a FileHandle>> {
    if input.compaction
        || input.files.len() > MAX_FILES
        || !input.memtables.is_empty()
        || input.series_row_selector.is_some()
        || input.sequence_range.is_some()
        || request.limit.is_some()
        || request.memtable_min_sequence.is_some()
        || request.memtable_max_sequence.is_some()
        || request.sst_min_sequence.is_some()
        || request.snapshot_on_scan
        || request.exact_sequence_range
        || request.skip_sst_files
        || (!input.append_mode && input.merge_mode != MergeMode::LastRow)
    {
        return None;
    }
    #[cfg(feature = "enterprise")]
    if !input.extension_ranges.is_empty() {
        return None;
    }
    let metadata = input.region_metadata();
    let time_index = &metadata.time_index_column().column_schema;
    if !request.filters.iter().all(|expr| {
        time_filter(
            expr,
            &time_index.name,
            &time_index.data_type.as_arrow_type(),
        )
    }) {
        return None;
    }
    let range = input.time_range.as_ref()?;
    let mut files = Vec::new();
    for file in &input.files {
        let (start, end) = file.time_range();
        if start > end {
            return None;
        }
        let file_range = TimestampRange::new_inclusive(Some(start), Some(end));
        if !range.intersects(&file_range) {
            continue;
        }
        if !range.contains(&start)
            || !range.contains(&end)
            || (input.region_partition_expr.is_some() && !input.should_skip_region_partition(file))
            || (!input.append_mode && !file.meta_ref().num_rows_is_logical)
        {
            return None;
        }
        files.push(file);
    }
    if !input.append_mode {
        files.sort_unstable_by_key(|file| file.time_range().0);
        if files
            .windows(2)
            .any(|pair| pair[0].time_range().1 >= pair[1].time_range().0)
        {
            return None;
        }
    }
    Some(files)
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

async fn count_files(input: &ScanInput, files: &[&FileHandle]) -> Option<usize> {
    let mut total = 0usize;
    let mut footer_reads = 0;
    let mut bytes_left = MAX_FOOTER_BYTES;
    for file in files {
        let rows = if file.meta_ref().num_rows != 0 {
            usize::try_from(file.meta_ref().num_rows).ok()?
        } else if let Some(metadata) = input
            .cache_strategy
            .get_parquet_meta_data_from_mem_cache(file.file_id())
        {
            usize::try_from(metadata.file_metadata().num_rows()).ok()?
        } else {
            if footer_reads == MAX_FOOTERS || bytes_left < 8 {
                return None;
            }
            footer_reads += 1;
            let layer = &input.access_layer;
            let path = file.file_path(layer.table_dir(), layer.path_type());
            let mut loader = MetadataLoader::new(
                layer.object_store().clone(),
                &path,
                file.meta_ref().file_size,
            );
            loader.with_read_budget(bytes_left);
            let mut metrics = MetadataCacheMetrics::default();
            let metadata = match loader.load(&mut metrics).await {
                Ok(metadata) => Arc::new(metadata),
                Err(error) => {
                    debug!(error = %error, "Metadata count preparation unavailable; retaining scan");
                    return None;
                }
            };
            bytes_left = bytes_left.checked_sub(metrics.bytes_read)?;
            let rows = usize::try_from(metadata.file_metadata().num_rows()).ok()?;
            input
                .cache_strategy
                .put_parquet_meta_data(file.file_id(), metadata, None);
            rows
        };
        total = total.checked_add(rows)?;
    }
    debug!(
        rows = total,
        footer_reads,
        footer_bytes = MAX_FOOTER_BYTES - bytes_left,
        "Prepared exact metadata count"
    );
    Some(total)
}

#[cfg(test)]
mod tests {
    use common_time::Timestamp;
    use common_time::timestamp::TimeUnit;
    use datafusion_common::ScalarValue;
    use datafusion_expr::{col, lit};
    use table::predicate::build_time_range_predicate;

    use super::*;
    use crate::read::flat_projection::FlatProjectionMapper;
    use crate::read::scan_region::PredicateGroup;
    use crate::sst::file::FileMeta;
    use crate::test_util::memtable_util::metadata_with_primary_key;
    use crate::test_util::new_noop_file_purger;
    use crate::test_util::scheduler_util::SchedulerEnv;

    fn file(start: i64, end: i64, rows: u64, logical: bool) -> FileHandle {
        FileHandle::new(
            FileMeta {
                num_rows: rows,
                num_rows_is_logical: logical,
                time_range: (
                    Timestamp::new_millisecond(start),
                    Timestamp::new_millisecond(end),
                ),
                ..Default::default()
            },
            new_noop_file_purger(),
        )
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
    async fn test_metadata_count_coverage_and_overlap() {
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
                metadata_count_hint: true,
                ..Default::default()
            };
            // There are no physical files. Success proves that eligible manifest
            // counts do not open SSTs, even when their footer is not cached.
            assert_eq!(prepare(&input, &request).await, expected, "{request:?}");
        }
        for files in [
            vec![file(0, 10, 3, true), file(10, 20, 2, true)],
            vec![file(0, 10, 3, true), file(5, 8, 2, true)],
            vec![file(0, 10, 3, false)],
            vec![file(0, 10, 0, false)],
        ] {
            assert_eq!(
                prepare(&input(files, &[]).await, &ScanRequest::default()).await,
                None
            );
        }
    }

    #[tokio::test]
    async fn test_metadata_count_fences_and_limits() {
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
            assert_eq!(prepare(&input, &request).await, None);
        }
        let mut input = input;
        input.files = (0..=MAX_FILES)
            .map(|i| file(i as i64, i as i64, 1, true))
            .collect();
        assert_eq!(prepare(&input, &ScanRequest::default()).await, None);
        input.files.clear();
        assert_eq!(prepare(&input, &ScanRequest::default()).await, Some(0));
    }
    #[tokio::test]
    async fn test_metadata_count_legacy_footer_cache_and_file_budget() {
        use crate::cache::{CacheManager, CacheStrategy};
        use crate::sst::parquet::PARQUET_METADATA_KEY;
        use datatypes::arrow::array::{ArrayRef, Int64Array};
        use datatypes::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
        use datatypes::arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::metadata::KeyValue;
        use parquet::file::properties::WriterProperties;
        use store_api::storage::FileId;

        let env = SchedulerEnv::new().await;
        let metadata = Arc::new(metadata_with_primary_key(vec![0, 1], false));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            ArrowDataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![KeyValue::new(
                PARQUET_METADATA_KEY.to_string(),
                metadata.to_json().unwrap(),
            )]))
            .build();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let file_size = bytes.len() as u64;
        let mut files = Vec::new();
        for i in 0..=MAX_FOOTERS {
            let file = FileHandle::new(
                FileMeta {
                    file_id: FileId::random(),
                    num_rows: 0, // Legacy manifest: the footer is authoritative.
                    file_size,
                    time_range: (
                        Timestamp::new_millisecond(i as i64),
                        Timestamp::new_millisecond(i as i64),
                    ),
                    ..Default::default()
                },
                new_noop_file_purger(),
            );
            let path = file.file_path(env.access_layer.table_dir(), env.access_layer.path_type());
            env.access_layer
                .object_store()
                .write(&path, bytes.clone())
                .await
                .unwrap();
            files.push(file);
        }
        let mapper = FlatProjectionMapper::new(&metadata, [0, 2, 3]).unwrap();
        let mut input = ScanInput::builder(env.access_layer.clone(), mapper)
            .with_append_mode(true)
            .with_files(files)
            .with_time_range(Some(TimestampRange::min_to_max()))
            .build();
        // Nine uncached footers exceed the eight-file budget.
        let selected = candidates(&input, &ScanRequest::default()).unwrap();
        assert_eq!(count_files(&input, &selected).await, None);
        input.files.truncate(1);
        input.cache_strategy = CacheStrategy::EnableAll(Arc::new(
            CacheManager::builder()
                .sst_meta_cache_size(1024 * 1024)
                .build(),
        ));
        let selected = candidates(&input, &ScanRequest::default()).unwrap();
        assert_eq!(count_files(&input, &selected).await, Some(3));
        let file = &input.files[0];
        assert!(
            input
                .cache_strategy
                .get_parquet_meta_data_from_mem_cache(file.file_id())
                .is_some()
        );
        let path = file.file_path(env.access_layer.table_dir(), env.access_layer.path_type());
        env.access_layer.object_store().delete(&path).await.unwrap();
        assert_eq!(
            count_files(&input, &selected).await,
            Some(3),
            "cached footer must avoid I/O"
        );
        // The same legacy footer cannot certify non-append deduplication.
        input.append_mode = false;
        assert!(candidates(&input, &ScanRequest::default()).is_none());
    }
}
