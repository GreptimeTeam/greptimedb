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

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_schema::extension::EXTENSION_TYPE_METADATA_KEY;
use common_time::Timestamp;
use common_time::range::TimestampRange;
use common_time::timestamp::TimeUnit;
use datafusion_common::ScalarValue;
use datafusion_expr::Expr;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData};
use snafu::OptionExt;
use store_api::metadata::RegionMetadataRef;

use crate::access_layer::AccessLayerRef;
use crate::cache::{CacheManagerRef, CacheStrategy};
use crate::compaction::json2::{
    Json2RewritePlans, collect_json2_rewrite_plans_from_parquet, rewrite_json2_schema,
};
use crate::error::{InvalidRecordBatchSnafu, Result, TimeRangePredicateOverflowSnafu};
use crate::read::FlatSource;
use crate::read::flat_projection::FlatProjectionMapper;
use crate::read::read_columns::ReadColumns;
use crate::read::scan_region::{PredicateGroup, ScanInput};
use crate::read::seq_scan::SeqScan;
use crate::read::stream::spawn_compact_stream;
use crate::region::options::MergeMode;
use crate::sst::file::FileHandle;
use crate::sst::parquet::reader::MetadataCacheMetrics;
use crate::sst::parquet::{Json2RewriteTargets, Json2TargetLayout};

/// Builders to create [BoxedRecordBatchStream] for compaction.
pub(crate) struct CompactionSstReaderBuilder<'a> {
    pub(crate) metadata: RegionMetadataRef,
    pub(crate) sst_layer: AccessLayerRef,
    pub(crate) cache: CacheManagerRef,
    pub(crate) inputs: &'a [FileHandle],
    pub(crate) append_mode: bool,
    pub(crate) filter_deleted: bool,
    pub(crate) time_range: Option<TimestampRange>,
    pub(crate) merge_mode: MergeMode,
}

impl CompactionSstReaderBuilder<'_> {
    /// Build a [FlatSource] that yields Arrow `RecordBatch`s from reading all the input SST files,
    /// for compaction. The schema of the [FlatSource] is unified.
    pub(crate) async fn build_flat_sst_reader(self) -> Result<FlatSource> {
        let parquet_metadata = self.collect_parquet_metadata().await?;
        let plans = collect_json2_rewrite_plans_from_parquet(&self.metadata, &parquet_metadata)?;
        let scan_input = self.build_scan_input(&parquet_metadata, &plans)?;

        let schema = scan_input.mapper.output_schema();
        let schema = rewrite_json2_schema(schema.arrow_schema(), &plans);

        let stream = SeqScan::new(scan_input)
            .build_flat_reader_for_compaction()
            .await?;
        Ok(FlatSource::new_stream(schema, stream))
    }

    fn build_scan_input(
        self,
        parquet_metadata: &[Arc<ParquetMetaData>],
        plans: &Json2RewritePlans,
    ) -> Result<ScanInput> {
        let batch_size = crate::batch_size::estimate_batch_size(
            parquet_metadata
                .iter()
                .flat_map(|metadata| metadata.row_groups())
                .map(|row_group| {
                    let uncompressed_bytes = row_group
                        .columns()
                        .iter()
                        .map(|column| column.uncompressed_size() as u64)
                        .sum();
                    (row_group.num_rows() as u64, uncompressed_bytes)
                }),
        );

        let projection = (0..self.metadata.column_metadatas.len()).collect();
        let read_column_ids = self
            .metadata
            .column_metadatas
            .iter()
            .map(|x| x.column_id)
            .collect::<Vec<_>>();

        let mut json2_target_layouts = BTreeMap::new();
        for (name, plan) in plans {
            let Some(column) = self.metadata.column_by_name(name) else {
                continue;
            };
            let extension_metadata = column
                .column_schema
                .metadata()
                .get(EXTENSION_TYPE_METADATA_KEY)
                .cloned()
                .with_context(|| InvalidRecordBatchSnafu {
                    reason: format!("JSON2 target column '{name}' has no extension metadata"),
                })?;
            json2_target_layouts.insert(
                column.column_id,
                Json2TargetLayout {
                    extension_metadata,
                    target_layout: plan.target_layout.clone(),
                },
            );
        }
        let read_columns = ReadColumns::new(read_column_ids);
        let targets: Json2RewriteTargets = Arc::new(json2_target_layouts);
        let mapper = FlatProjectionMapper::new_with_json2_rewrite_targets(
            &self.metadata,
            projection,
            read_columns,
            &targets,
        )?;

        let mut scan_input = ScanInput::builder(self.sst_layer, mapper)
            .with_json2_rewrite_targets(targets)
            .with_files(self.inputs.to_vec())
            .with_compaction(true)
            .with_batch_size(batch_size)
            .with_append_mode(self.append_mode)
            // We use special cache strategy for compaction.
            .with_cache(CacheStrategy::Compaction(self.cache))
            .with_filter_deleted(self.filter_deleted)
            // We ignore file not found error during compaction.
            .with_ignore_file_not_found(true)
            .with_merge_mode(self.merge_mode);

        // This serves as a workaround of https://github.com/GreptimeTeam/greptimedb/issues/3944
        // by converting time ranges into predicate.
        if let Some(time_range) = self.time_range {
            scan_input =
                scan_input.with_predicate(time_range_to_predicate(time_range, &self.metadata)?);
        }

        Ok(scan_input.build())
    }

    async fn collect_parquet_metadata(&self) -> Result<Vec<Arc<ParquetMetaData>>> {
        let mut metadata = Vec::with_capacity(self.inputs.len());

        for file_handle in self.inputs {
            let file_path =
                file_handle.file_path(self.sst_layer.table_dir(), self.sst_layer.path_type());
            let file_size = file_handle.meta_ref().file_size;
            let parquet_metadata = match self
                .sst_layer
                .read_sst(file_handle.clone())
                .cache(CacheStrategy::Compaction(self.cache.clone()))
                .read_parquet_metadata(
                    &file_path,
                    file_size,
                    &mut MetadataCacheMetrics::default(),
                    PageIndexPolicy::default(),
                )
                .await
                .map(|x| x.0.parquet_metadata())
            {
                Ok(x) => x,
                Err(e) if e.is_object_not_found() => continue,
                Err(e) => return Err(e),
            };
            metadata.push(parquet_metadata);
        }
        Ok(metadata)
    }
}

/// Number of merged batches buffered between the merge task and the SST writer.
const MERGED_BATCH_CHANNEL_SIZE: usize = 2;

/// Reads and merges `source` on a separate task, so the SST writer can encode
/// merged batches while the next ones are being read and merged.
pub(crate) fn spawn_merge(source: FlatSource) -> FlatSource {
    let schema = source.schema().clone();
    let stream = spawn_compact_stream(source.into_stream(), MERGED_BATCH_CHANNEL_SIZE, None);
    FlatSource::new_stream(schema, stream)
}

/// Converts time range to predicates so that rows outside the range will be filtered.
fn time_range_to_predicate(
    range: TimestampRange,
    metadata: &RegionMetadataRef,
) -> Result<PredicateGroup> {
    let ts_col = metadata.time_index_column();

    // safety: time index column's type must be a valid timestamp type.
    let ts_col_unit = ts_col
        .column_schema
        .data_type
        .as_timestamp()
        .unwrap()
        .unit();

    let exprs = match (range.start(), range.end()) {
        (Some(start), Some(end)) => {
            vec![
                datafusion_expr::col(ts_col.column_schema.name.clone())
                    .gt_eq(ts_to_lit(*start, ts_col_unit)?),
                datafusion_expr::col(ts_col.column_schema.name.clone())
                    .lt(ts_to_lit(*end, ts_col_unit)?),
            ]
        }
        (Some(start), None) => {
            vec![
                datafusion_expr::col(ts_col.column_schema.name.clone())
                    .gt_eq(ts_to_lit(*start, ts_col_unit)?),
            ]
        }

        (None, Some(end)) => {
            vec![
                datafusion_expr::col(ts_col.column_schema.name.clone())
                    .lt(ts_to_lit(*end, ts_col_unit)?),
            ]
        }
        (None, None) => {
            return Ok(PredicateGroup::default());
        }
    };

    let predicate = PredicateGroup::new(metadata, &exprs)?;
    Ok(predicate)
}

fn ts_to_lit(ts: Timestamp, ts_col_unit: TimeUnit) -> Result<Expr> {
    let ts = ts
        .convert_to(ts_col_unit)
        .context(TimeRangePredicateOverflowSnafu {
            timestamp: ts,
            unit: ts_col_unit,
        })?;
    let val = ts.value();
    let scalar_value = match ts_col_unit {
        TimeUnit::Second => ScalarValue::TimestampSecond(Some(val), None),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(val), None),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(val), None),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(val), None),
    };
    Ok(datafusion_expr::lit(scalar_value))
}

#[cfg(test)]
mod tests {
    use api::v1::Rows;
    use store_api::region_engine::RegionEngine;
    use store_api::region_request::{RegionFlushRequest, RegionRequest};
    use store_api::storage::RegionId;

    use super::*;
    use crate::test_util::{
        CreateRequestBuilder, TestEnv, build_rows_for_key, put_rows, rows_schema,
    };

    #[tokio::test]
    async fn test_compaction_reader_keeps_short_series_batches_whole() {
        let mut env = TestEnv::new().await;
        let engine = env.create_engine(Default::default()).await;
        let region_id = RegionId::new(1, 1);
        let request = CreateRequestBuilder::new().build();
        let schema = rows_schema(&request);
        engine
            .handle_request(region_id, RegionRequest::Create(request))
            .await
            .unwrap();
        // Each file has 100 short series, which queries split for merging.
        for file in 0..2 {
            let rows = (0..100)
                .flat_map(|series| {
                    build_rows_for_key(&format!("key-{series:03}"), file * 10, file * 10 + 10, 0)
                })
                .collect();
            put_rows(
                &engine,
                region_id,
                Rows {
                    schema: schema.clone(),
                    rows,
                },
            )
            .await;
            engine
                .handle_request(
                    region_id,
                    RegionRequest::Flush(RegionFlushRequest::default()),
                )
                .await
                .unwrap();
        }
        let region = engine.get_region(region_id).unwrap();
        let version = region.version();
        let inputs: Vec<_> = version.ssts.levels()[0].files().cloned().collect();
        assert_eq!(2, inputs.len());

        let mut source = CompactionSstReaderBuilder {
            metadata: version.metadata.clone(),
            sst_layer: region.access_layer.clone(),
            cache: engine.cache_manager(),
            inputs: &inputs,
            append_mode: false,
            filter_deleted: true,
            time_range: None,
            merge_mode: MergeMode::LastRow,
        }
        .build_flat_sst_reader()
        .await
        .unwrap();
        let mut num_batches = 0;
        let mut num_rows = 0;
        while let Some(batch) = source.next_batch().await.unwrap() {
            num_batches += 1;
            num_rows += batch.num_rows();
        }

        // Splitting by series would yield a batch per series and file, 200 in total.
        assert_eq!(2000, num_rows);
        assert!(num_batches < 20, "got {num_batches} batches");
    }
}
