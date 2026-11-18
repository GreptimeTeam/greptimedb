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

use api::v1::SemanticType;
use datatypes::arrow::datatypes::SchemaRef;
use datatypes::schema::{ColumnEncoding, FulltextBackend};
use index::fulltext_index::create::TantivyFulltextIndexCreator;
use parquet::arrow::ArrowSchemaConverter;
use parquet::file::metadata::ParquetMetaData;
use snafu::{ResultExt, ensure};

use crate::compaction::multi_region::{
    MultiRegionCompactionContext, MultiRegionCompactionPlan, MultiRegionTarget,
};
use crate::config::{FulltextIndexConfig, MitoConfig};
use crate::error::{IndexOptionsSnafu, MultiRegionCompactionResourceSnafu, Result};
use crate::sst::parquet::writer::SstWriteLimits;

pub(crate) const BATCH_SIZE: usize = 64;
/// Cap on the rows of one output row group.
///
/// Every resident target admits an encoder sized by this count, while readers
/// and index row ids only require uniform groups except for the last one.
/// Ordinary compaction forwards the region option uncapped; this executor caps
/// it so the working set stays inside the task budget.
pub(crate) const MAX_ROW_GROUP_SIZE: usize = 1024;

/// Admission includes every resident writer, not just concurrently polled tasks.
/// Codec/index thresholds are estimates; Arrow batches and output metadata have
/// separate runtime checks. Neither estimate nor buffer accounting measures RSS.
#[derive(Clone)]
pub(crate) struct MemoryLimits {
    pub(crate) batch: usize,
    pub(crate) group: usize,
    pub(crate) index: usize,
    /// Reuses index allocation only when the optional working set fits admission.
    pub(crate) extension_index_memory_limit: Option<usize>,
    pub(crate) writer: SstWriteLimits,
    pub(crate) estimated: u64,
}

impl MemoryLimits {
    pub(crate) fn new(plan: &MultiRegionCompactionPlan, schema: &SchemaRef) -> Result<Self> {
        let schema = crate::sst::maybe_wrap_schema(schema)?;
        let columns = ArrowSchemaConverter::new()
            .convert(&schema)
            .context(crate::error::WriteParquetSnafu)?
            .num_columns();
        let batch =
            plan.memory_limit / (64 * (plan.sources.len() + plan.targets.len() + BATCH_SIZE + 4));
        Ok(Self {
            batch,
            group: plan.memory_limit / 16,
            index: plan.memory_limit / (64 * plan.targets.len()),
            extension_index_memory_limit: None,
            writer: SstWriteLimits {
                // Parquet 59 preallocates a 1 MiB bit writer for each of the
                // timestamp/sequence delta encoders. Dictionary interner tables
                // also have a nonzero minimum, independent of batch row count.
                working_bytes: 2 * 1024 * 1024
                    + columns * 128 * 1024
                    + batch * (MAX_ROW_GROUP_SIZE / BATCH_SIZE),
                metadata_bytes: plan.memory_limit / (8 * plan.targets.len()),
            },
            estimated: 0,
        })
    }

    pub(crate) fn admit(
        &mut self,
        plan: &MultiRegionCompactionPlan,
        context: &MultiRegionCompactionContext,
        footers: &[Arc<ParquetMetaData>],
        extension_targets: usize,
    ) -> Result<()> {
        self.check_upload_minimum(context)?;
        let mut extra_delta = 0;
        for target in plan.targets.values() {
            extra_delta = extra_delta.max(extra_delta_encoder_bytes(target)?);
        }
        self.writer.working_bytes = self.writer.working_bytes.saturating_add(extra_delta);
        // Merge assembly can retain exhausted batches until an output batch is full.
        let mut bytes = 4_u64
            .saturating_mul((plan.sources.len() + BATCH_SIZE + 2) as u64)
            .saturating_mul(self.batch as u64)
            .saturating_add(6 * self.group as u64);
        for footer in footers {
            bytes = bytes.saturating_add(reader_bytes(footer));
        }
        for target in plan.targets.values() {
            bytes = bytes.saturating_add(self.target_bytes(target, &context.config)?);
        }
        ensure!(
            bytes <= plan.memory_limit as u64,
            MultiRegionCompactionResourceSnafu {
                reason: format!(
                    "Reader/writer/index minimum working set needs {bytes} bytes, budget is {}",
                    plan.memory_limit
                ),
            }
        );
        // Optional indexes must not turn an admitted data compaction into a
        // resource failure. Reserve all resident target states together or skip.
        let admission =
            admit_extension_indexes(bytes, plan.memory_limit, self.index, extension_targets);
        self.extension_index_memory_limit = admission.limit;
        self.estimated = admission.estimated;
        Ok(())
    }

    fn check_upload_minimum(&self, context: &MultiRegionCompactionContext) -> Result<()> {
        let minimum = context
            .access_layer
            .object_store()
            .info()
            .capability()
            .write_multi_min_size
            .unwrap_or(0);
        ensure!(
            self.batch >= minimum && self.batch > 0,
            MultiRegionCompactionResourceSnafu {
                reason: "Storage multipart minimum exceeds the task buffer budget",
            }
        );
        Ok(())
    }

    fn target_bytes(&self, target: &MultiRegionTarget, config: &MitoConfig) -> Result<u64> {
        let schema_bytes = target
            .metadata
            .to_json()
            .context(crate::error::InvalidMetadataSnafu)?
            .len();
        // Task/channel allocation plus captured schema and input-map copies.
        let control = 8 * 1024
            + 3 * schema_bytes
            + 3 * target.inputs.len() * std::mem::size_of::<crate::sst::file::FileMeta>();
        // Encoding/finish workspace, SST and index multipart buffers, result metadata.
        let writer = self.writer.working_bytes * 2 + self.batch * 4 + self.writer.metadata_bytes;
        // Creators are instantiated even in async-index mode. Inverted/skipping
        // thresholds are shared within their indexers, fulltext is per column.
        Ok((control as u64)
            .saturating_add(writer as u64)
            .saturating_add(4 * self.index as u64)
            .saturating_add(self.fulltext_bytes(target, &config.fulltext_index)?))
    }

    fn fulltext_bytes(
        &self,
        target: &MultiRegionTarget,
        config: &FulltextIndexConfig,
    ) -> Result<u64> {
        if config.create_on_compaction.disabled() {
            return Ok(0);
        }
        let threshold = self.index.min(config.mem_threshold_on_create());
        let mut bytes = 0_u64;
        for column in target
            .metadata
            .column_metadatas
            .iter()
            .filter(|column| column.semantic_type != SemanticType::Tag)
        {
            let Some(options) = column
                .column_schema
                .fulltext_options()
                .context(IndexOptionsSnafu {
                    column_name: &column.column_schema.name,
                })?
                .filter(|options| options.enable)
            else {
                continue;
            };
            let allocation = match options.backend {
                FulltextBackend::Tantivy => {
                    TantivyFulltextIndexCreator::sanitize_memory_limit(threshold)
                }
                FulltextBackend::Bloom => threshold,
            };
            bytes = bytes.saturating_add(2 * allocation as u64);
        }
        Ok(bytes)
    }
}

fn extra_delta_encoder_bytes(target: &MultiRegionTarget) -> Result<usize> {
    let mut bytes = 0;
    for column in target
        .metadata
        .column_metadatas
        .iter()
        .filter(|column| column.semantic_type != SemanticType::Timestamp)
    {
        if column
            .column_schema
            .compression_options()
            .context(crate::error::CompressionOptionsSnafu {
                column_name: &column.column_schema.name,
            })?
            .is_some_and(|options| options.encoding == Some(ColumnEncoding::DeltaBinaryPacked))
        {
            bytes += 1024 * 1024;
        }
    }
    Ok(bytes)
}

fn reader_bytes(footer: &ParquetMetaData) -> u64 {
    let pages = footer
        .row_groups()
        .iter()
        .map(|group| {
            (group.total_byte_size().max(0) as u64)
                .saturating_mul(RETAINED_AND_FINISH_SCRATCH_FACTOR)
                .saturating_add(group.compressed_size().max(0) as u64)
        })
        .max()
        .unwrap_or(0);
    pages.saturating_add(footer.memory_size() as u64)
}

/// Retained state and temporary finish allocations share the admission budget.
const RETAINED_AND_FINISH_SCRATCH_FACTOR: u64 = 2;

#[derive(Debug, PartialEq, Eq)]
struct ExtensionIndexAdmission {
    limit: Option<usize>,
    estimated: u64,
}

/// Reserves all target states together, or preserves the admitted data budget.
fn admit_extension_indexes(
    base: u64,
    budget: usize,
    index: usize,
    targets: usize,
) -> ExtensionIndexAdmission {
    let with_extension_indexes = base.saturating_add(
        (index as u64)
            .saturating_mul(RETAINED_AND_FINISH_SCRATCH_FACTOR)
            .saturating_mul(targets as u64),
    );
    if targets > 0 && index > 0 && with_extension_indexes <= budget as u64 {
        ExtensionIndexAdmission {
            limit: Some(index),
            estimated: with_extension_indexes,
        }
    } else {
        ExtensionIndexAdmission {
            limit: None,
            estimated: base,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ExtensionIndexAdmission, admit_extension_indexes};

    #[test]
    fn test_extension_indexes_do_not_reject_admitted_data() {
        // Both target writers are resident. Insufficient extra room leaves the
        // already-admitted data budget unchanged and explicitly disables optional indexes.
        assert_eq!(
            admit_extension_indexes(900, 1000, 30, 2),
            ExtensionIndexAdmission {
                limit: None,
                estimated: 900
            }
        );
        assert_eq!(
            admit_extension_indexes(880, 1000, 30, 2),
            ExtensionIndexAdmission {
                limit: Some(30),
                estimated: 1000
            }
        );
        assert_eq!(
            admit_extension_indexes(900, 1000, 30, 0),
            ExtensionIndexAdmission {
                limit: None,
                estimated: 900
            }
        );
        assert_eq!(
            admit_extension_indexes(900, 1000, 0, 2),
            ExtensionIndexAdmission {
                limit: None,
                estimated: 900
            }
        );
    }
}
