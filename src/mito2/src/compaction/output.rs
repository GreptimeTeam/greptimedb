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

use common_base::readable_size::ReadableSize;
use common_query::logical_plan::DfExpr;
use common_query::prelude::Expr;
use common_telemetry::error;
use datafusion_expr::Operator;
use datatypes::value::timestamp_to_scalar_value;
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::RegionId;
use table::predicate::Predicate;

use crate::access_layer::AccessLayerRef;
use crate::error;
use crate::error::BuildCompactionPredicateSnafu;
use crate::read::projection::ProjectionMapper;
use crate::read::seq_scan::SeqScan;
use crate::read::{BoxedBatchReader, Source};
use crate::sst::file::{FileHandle, FileId, FileMeta, Level};
use crate::sst::parquet::{SstInfo, WriteOptions};

pub(crate) struct CompactionOutput {
    pub output_file_id: FileId,
    /// Compaction output file level.
    pub output_level: Level,
    /// The left bound of time window.
    pub time_window_bound: i64,
    /// Time window size in seconds.
    pub time_window_sec: i64,
    /// Compaction input files.
    pub inputs: Vec<FileHandle>,
    /// If the compaction output is strictly windowed.
    pub strict_window: bool,
}

impl CompactionOutput {
    pub(crate) async fn build(
        &self,
        region_id: RegionId,
        schema: RegionMetadataRef,
        sst_layer: AccessLayerRef,
        sst_write_buffer_size: ReadableSize,
    ) -> error::Result<Option<FileMeta>> {
        let time_range = if self.strict_window {
            (
                Some(self.time_window_bound),
                Some(self.time_window_bound + self.time_window_sec),
            )
        } else {
            (None, None)
        };

        let reader = build_sst_reader(
            region_id,
            schema.clone(),
            sst_layer.clone(),
            &self.inputs,
            time_range,
        )
        .await?;

        let opts = WriteOptions {
            write_buffer_size: sst_write_buffer_size,
            ..Default::default()
        };

        // TODO(hl): measure merge elapsed time.

        let mut writer = sst_layer.write_sst(self.output_file_id, schema, Source::Reader(reader));
        let meta = writer.write_all(&opts).await?.map(
            |SstInfo {
                 time_range,
                 file_size,
                 ..
             }| {
                FileMeta {
                    region_id,
                    file_id: self.output_file_id,
                    time_range,
                    level: self.output_level,
                    file_size,
                }
            },
        );

        Ok(meta)
    }
}

/// Builds [BoxedBatchReader] that reads all SST files and yields batches in primary key order.
async fn build_sst_reader(
    region_id: RegionId,
    schema: RegionMetadataRef,
    sst_layer: AccessLayerRef,
    inputs: &[FileHandle],
    time_range: (Option<i64>, Option<i64>),
) -> error::Result<BoxedBatchReader> {
    let predicate = build_time_range_filter(&schema, time_range)
        .map_err(|e| {
            error!(e; "Failed to build compaction time range predicate for region: {}", region_id);
            e
        })
        .unwrap_or_default();

    SeqScan::new(sst_layer, ProjectionMapper::all(&schema)?)
        .with_files(inputs.to_vec())
        .with_predicate(predicate)
        .build_reader()
        .await
}

/// Builds time range filter expr from lower (inclusive) and upper bound(exclusive).
/// Returns `None` if time range overflows.
fn build_time_range_filter(
    schema: &RegionMetadataRef,
    time_range: (Option<i64>, Option<i64>),
) -> error::Result<Option<Predicate>> {
    let ts_col = schema.time_index_column();
    // safety: Region schema's timestamp columns must be valid.
    let ts_col_unit = ts_col
        .column_schema
        .data_type
        .as_timestamp()
        .unwrap()
        .unit();
    let ts_col_name = ts_col.column_schema.name.clone();

    let (low_ts_inclusive, high_ts_exclusive) = time_range;
    let ts_col = DfExpr::Column(datafusion_common::Column::from_name(ts_col_name));

    // Converting seconds to whatever unit won't lose precision.
    // Here only handles overflow.
    let low_ts = low_ts_inclusive
        .map(common_time::Timestamp::new_second)
        .and_then(|ts| ts.convert_to(ts_col_unit))
        .map(|ts| ts.value());
    let high_ts = high_ts_exclusive
        .map(common_time::Timestamp::new_second)
        .and_then(|ts| ts.convert_to(ts_col_unit))
        .map(|ts| ts.value());

    let expr = match (low_ts, high_ts) {
        (Some(low), Some(high)) => {
            let lower_bound_expr =
                DfExpr::Literal(timestamp_to_scalar_value(ts_col_unit, Some(low)));
            let upper_bound_expr =
                DfExpr::Literal(timestamp_to_scalar_value(ts_col_unit, Some(high)));
            Some(datafusion_expr::and(
                datafusion_expr::binary_expr(ts_col.clone(), Operator::GtEq, lower_bound_expr),
                datafusion_expr::binary_expr(ts_col, Operator::Lt, upper_bound_expr),
            ))
        }

        (Some(low), None) => {
            let lower_bound_expr =
                datafusion_expr::lit(timestamp_to_scalar_value(ts_col_unit, Some(low)));
            Some(datafusion_expr::binary_expr(
                ts_col,
                Operator::GtEq,
                lower_bound_expr,
            ))
        }

        (None, Some(high)) => {
            let upper_bound_expr =
                datafusion_expr::lit(timestamp_to_scalar_value(ts_col_unit, Some(high)));
            Some(datafusion_expr::binary_expr(
                ts_col,
                Operator::Lt,
                upper_bound_expr,
            ))
        }

        (None, None) => None,
    };

    expr.map(Expr::from)
        .map(|e| Predicate::try_new(vec![e], schema.schema.clone()))
        .transpose()
        .context(BuildCompactionPredicateSnafu)
}

#[cfg(test)]
mod tests {
    // TODO(hl): migrate tests for `build_time_range_filter`
}
