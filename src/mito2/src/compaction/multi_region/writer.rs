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

use std::num::NonZeroU64;
use std::sync::Arc;

use common_base::readable_size::ReadableSize;
use partition::expr::PartitionExpr;
use snafu::ensure;
use tokio_util::sync::CancellationToken;

use crate::access_layer::{Metrics, OperationType, SstWriteRequest, WriteType};
use crate::compaction::multi_region::memory::{self, MemoryLimits};
use crate::compaction::multi_region::{MultiRegionCompactionContext, MultiRegionTarget};
use crate::error::Result;
use crate::read::FlatSource;
use crate::sst::FormatType;
use crate::sst::file::FileMeta;
use crate::sst::index::build::IndexBuildInputs;
use crate::sst::index::registry::IndexBuildPlan;
use crate::sst::parquet::metadata::extract_primary_key_range;
use crate::sst::parquet::parallel_writer::EncodeRuntime;
use crate::sst::parquet::writer::SstWriteContext;
use crate::sst::parquet::{SstInfo, WriteOptions, row_group_size_for};

pub(crate) struct TargetWriter<'a> {
    target: &'a MultiRegionTarget,
    context: &'a MultiRegionCompactionContext,
    limits: &'a MemoryLimits,
    index_plan: Arc<IndexBuildPlan>,
    max_file_size: usize,
    /// Rows and decoded bytes of the shared sources.
    input_size: (u64, u64),
    sequence: Option<NonZeroU64>,
    preserve_row_sequence: bool,
}

impl<'a> TargetWriter<'a> {
    pub(crate) fn new(
        target: &'a MultiRegionTarget,
        context: &'a MultiRegionCompactionContext,
        limits: &'a MemoryLimits,
        max_file_size: usize,
        input_size: (u64, u64),
        index_plan: Arc<IndexBuildPlan>,
    ) -> Self {
        let sequence = crate::sst::file::known_max_input_sequence(target.inputs.values());
        let preserve_row_sequence = target.options.preserve_row_sequence
            && target
                .inputs
                .values()
                .all(|file| file.is_effective_target_sequence_trusted(target.metadata.region_id));
        Self {
            target,
            context,
            limits,
            index_plan,
            max_file_size,
            input_size,
            sequence,
            preserve_row_sequence,
        }
    }

    pub(crate) async fn write(&self, source: FlatSource) -> Result<(Vec<FileMeta>, Vec<SstInfo>)> {
        ensure!(
            !self.context.cancellation.is_cancelled(),
            crate::error::CompactionCancelledSnafu
        );
        let request = self.request(source)?;
        let mut metrics = Metrics::new(WriteType::Compaction);
        let infos = self
            .context
            .access_layer
            .write_sst_with_context(
                request,
                &self.options(),
                &mut metrics,
                SstWriteContext {
                    guard: Some(self.context.protection.clone()),
                    limits: Some(self.limits.writer),
                    ..Default::default()
                },
            )
            .await?;
        metrics.observe();
        let expression =
            crate::region::parse_partition_expr(self.target.metadata.partition_expr.as_deref())?;
        let files = infos
            .iter()
            .map(|info| {
                self.validate_sequence(info)?;
                self.file_metadata(info, expression.clone())
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((files, infos.into_iter().collect()))
    }

    fn request(&self, source: FlatSource) -> Result<SstWriteRequest> {
        let config = &self.context.config;
        // The shared source plan already applies the admitted index thresholds.
        let source = cancellable_source(source, self.context.cancellation.clone());
        let index_build_inputs = Some(IndexBuildInputs {
            output_rows: None,
            plan: self.index_plan.clone(),
            input_puffin_manager: self.context.access_layer.build_puffin_manager(),
            // Routing and file rollover change coverage. Consume only actual output rows.
            inputs: Arc::from([]),
            can_merge_inputs: false,
            memory_limit: self.limits.extension_index_memory_limit,
        });
        Ok(SstWriteRequest {
            index_build_inputs,
            op_type: OperationType::Compact,
            metadata: self.target.metadata.clone(),
            source,
            cache_manager: self.context.cache.clone(),
            max_sequence: None,
            sst_write_format: self.target.options.sst_format.unwrap_or(
                if config.default_flat_format {
                    FormatType::Flat
                } else {
                    FormatType::PrimaryKey
                },
            ),
            preserve_row_sequence: self.preserve_row_sequence,
            index_options: self.target.options.index_options.clone(),
            index_config: config.index.clone(),
            inverted_index_config: config.inverted_index.clone(),
            fulltext_index_config: config.fulltext_index.clone(),
            bloom_filter_index_config: config.bloom_filter_index.clone(),
        })
    }

    pub(super) fn options(&self) -> WriteOptions {
        WriteOptions {
            write_buffer_size: ReadableSize(self.limits.batch as u64),
            max_file_size: Some(self.max_file_size),
            // Capped so each admitted encoder stays within the task budget; see
            // [memory::MAX_ROW_GROUP_SIZE].
            row_group_size: row_group_size_for(
                self.input_size.0,
                self.input_size.1,
                self.target
                    .options
                    .row_group_size()
                    .min(memory::MAX_ROW_GROUP_SIZE),
            ),
            float_field_encoding: self.target.options.float_field_encoding,
            // Every resident encoder and its finish workspace are admitted together.
            encode_parallelism: 1,
            encode_runtime: EncodeRuntime::Compact,
        }
    }

    fn validate_sequence(&self, info: &SstInfo) -> Result<()> {
        // Routing nonzero versions elsewhere can turn a mixed native input into
        // an all-zero output. A legacy reader must not replace those zeros with
        // the positive file bound; neither trust nor the bound may be fabricated.
        ensure!(
            self.preserve_row_sequence
                || self.sequence.is_none()
                || !info.file_metadata.as_ref().is_some_and(|footer| {
                    crate::sst::parquet::format::need_override_sequence(footer)
                }),
            crate::error::InvalidMultiRegionCompactionSnafu {
                reason: "Legacy sequence override would change zero output versions"
            }
        );
        Ok(())
    }

    fn file_metadata(
        &self,
        info: &SstInfo,
        partition_expr: Option<PartitionExpr>,
    ) -> Result<FileMeta> {
        let range = info
            .file_metadata
            .as_ref()
            .and_then(|footer| extract_primary_key_range(footer, &self.target.metadata));
        Ok(FileMeta {
            region_id: self.target.metadata.region_id,
            file_id: info.file_id,
            time_range: info.time_range,
            // Region-local L0: window merges and semantic folding stay with
            // ordinary region compaction.
            level: 0,
            file_size: info.file_size,
            max_row_group_uncompressed_size: info.max_row_group_uncompressed_size,
            max_row_group_decoded_size: info.max_row_group_decoded_size,
            available_indexes: info.index_metadata.build_available_indexes(),
            indexes: info.index_metadata.build_indexes()?,
            index_file_size: info.index_metadata.file_size,
            index_version: 0,
            num_rows: info.num_rows as u64,
            num_row_groups: info.num_row_groups,
            sequence: self.sequence,
            partition_expr: partition_expr.map(Into::into),
            num_series: info.num_series,
            primary_key_min: range.as_ref().map(|(min, _)| min.clone()),
            primary_key_max: range.map(|(_, max)| max),
            preserve_row_sequence: self.preserve_row_sequence,
            format: info.format.clone(),
        })
    }
}

fn cancellable_source(mut source: FlatSource, cancellation: CancellationToken) -> FlatSource {
    let schema = source.schema().clone();
    let stream = async_stream::try_stream! {
        while let Some(batch) = source.next_batch().await? {
            if cancellation.is_cancelled() { Err(crate::error::CompactionCancelledSnafu.build())?; }
            yield batch;
        }
    };
    FlatSource::new_stream(schema, Box::pin(stream))
}
