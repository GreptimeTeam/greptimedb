// Copyright 2023-2026 GrepTime Inc.
//
// This file is part of the GreptimeDB Enterprise Edition and is licensed under
// the GreptimeDB Enterprise License. You may not use this file except in
// compliance with that license. A copy of the license is available at the root
// of this repository in the file LICENSE-ENTERPRISE.
//
// Unless required by applicable law or agreed to in writing, this software is
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.

use std::sync::Arc;

use common_base::readable_size::ReadableSize;
use snafu::{ResultExt, ensure};
use store_api::codec::PrimaryKeyEncoding;

use crate::access_layer::{Metrics, OperationType, SstWriteRequest, WriteType};
use crate::compaction::multi_region::{
    MultiRegionCompactionContext, MultiRegionCompactionPlan, MultiRegionTarget,
};
use crate::error::{ComputeArrowSnafu, Result};
use crate::read::FlatSource;
use crate::sst::FormatType;
use crate::sst::file::FileMeta;
use crate::sst::parquet::metadata::extract_primary_key_range;
use crate::sst::parquet::{SstInfo, WriteOptions};

pub(crate) async fn write_target_group(
    target: &MultiRegionTarget,
    source: FlatSource,
    plan: &MultiRegionCompactionPlan,
    context: &MultiRegionCompactionContext,
) -> Result<(Vec<FileMeta>, Vec<SstInfo>)> {
    if context.cancellation.is_cancelled() {
        return crate::error::CompactionCancelledSnafu.fail();
    }
    let minimum_chunk = context
        .access_layer
        .object_store()
        .info()
        .capability()
        .write_multi_min_size
        .unwrap_or(0);
    ensure!(
        minimum_chunk <= plan.memory_limit / 8,
        crate::error::MultiRegionCompactionResourceSnafu {
            reason: "Storage multipart minimum exceeds the table writer budget",
        }
    );
    let source = output_projection(target, source, context.cancellation.clone())?;
    let trusted = target.options.preserve_row_sequence
        && target.inputs.values().all(|file| {
            if file.region_id == target.metadata.region_id {
                file.preserve_row_sequence
            } else {
                file.sequence.is_some()
            }
        });
    let request = SstWriteRequest {
        op_type: OperationType::Compact,
        metadata: target.metadata.clone(),
        source,
        cache_manager: context.cache.clone(),
        max_sequence: None,
        sst_write_format: target.options.sst_format.unwrap_or(
            if context.config.default_flat_format {
                FormatType::Flat
            } else {
                FormatType::PrimaryKey
            },
        ),
        preserve_row_sequence: trusted,
        index_options: target.options.index_options.clone(),
        index_config: context.config.index.clone(),
        inverted_index_config: context.config.inverted_index.clone(),
        fulltext_index_config: context.config.fulltext_index.clone(),
        bloom_filter_index_config: context.config.bloom_filter_index.clone(),
    };
    let options = WriteOptions {
        write_buffer_size: ReadableSize((plan.memory_limit / 8) as u64),
        max_file_size: Some(plan.max_output_file_size),
        row_group_size: target.options.row_group_size().min(1024),
        float_field_encoding: target.options.float_field_encoding,
    };
    let mut metrics = Metrics::new(WriteType::Compaction);
    let infos = context
        .access_layer
        .write_sst_with_guard(request, &options, &mut metrics, context.protection.clone())
        .await?;
    metrics.observe();
    let sequence = target
        .inputs
        .values()
        .map(|file| file.sequence)
        .collect::<Option<Vec<_>>>()
        .and_then(|values| values.into_iter().max());
    let partition_expr =
        crate::region::parse_partition_expr(target.metadata.partition_expr.as_deref())?;
    let files = infos
        .iter()
        .map(|info| {
            let range = info
                .file_metadata
                .as_ref()
                .and_then(|footer| extract_primary_key_range(footer, &target.metadata));
            FileMeta {
                region_id: target.metadata.region_id,
                file_id: info.file_id,
                time_range: info.time_range,
                level: 1,
                file_size: info.file_size,
                max_row_group_uncompressed_size: info.max_row_group_uncompressed_size,
                available_indexes: info.index_metadata.build_available_indexes(),
                indexes: info.index_metadata.build_indexes(),
                index_file_size: info.index_metadata.file_size,
                index_version: 0,
                num_rows: info.num_rows as u64,
                num_row_groups: info.num_row_groups,
                sequence,
                partition_expr: partition_expr.clone(),
                num_series: info.num_series,
                primary_key_min: range.as_ref().map(|(min, _)| min.clone()),
                primary_key_max: range.map(|(_, max)| max),
                preserve_row_sequence: trusted,
            }
        })
        .collect();
    Ok((files, infos.into_iter().collect()))
}

fn output_projection(
    target: &MultiRegionTarget,
    mut source: FlatSource,
    cancellation: Arc<common_base::cancellation::CancellationHandle>,
) -> Result<FlatSource> {
    let projection: Vec<_> = source
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| {
            target.metadata.primary_key_encoding != PrimaryKeyEncoding::Sparse
                || !target.metadata.primary_key.iter().any(|id| {
                    target
                        .metadata
                        .column_by_id(*id)
                        .is_some_and(|column| column.column_schema.name == *field.name())
                })
        })
        .map(|(index, _)| index)
        .collect();
    let schema = Arc::new(
        source
            .schema()
            .project(&projection)
            .context(ComputeArrowSnafu)?,
    );
    let stream = async_stream::try_stream! {
        while let Some(batch) = source.next_batch().await? {
            if cancellation.is_cancelled() { Err(crate::error::CompactionCancelledSnafu.build())?; }
            yield batch.project(&projection).context(ComputeArrowSnafu)?;
        }
    };
    Ok(FlatSource::new_stream(schema, Box::pin(stream)))
}
