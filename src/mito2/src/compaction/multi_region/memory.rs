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
use object_store::ObjectStore;
use parquet::arrow::ArrowSchemaConverter;
use parquet::file::metadata::ParquetMetaData;
use snafu::{OptionExt, ResultExt, ensure};

use crate::compaction::multi_region::{MultiRegionCompactionPlan, MultiRegionTarget};
use crate::config::{FulltextIndexConfig, MitoConfig};
use crate::error::{IndexOptionsSnafu, MultiRegionCompactionResourceSnafu, Result};
use crate::sst::DEFAULT_WRITE_BUFFER_SIZE;
use crate::sst::parquet::max_row_group_value_bytes;
use crate::sst::parquet::writer::SstWriteLimits;
use crate::sst::upload::UploadLimits;

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
    pub(crate) arrow_batch_bytes: usize,
    pub(crate) group: usize,
    pub(crate) index: usize,
    upload_reserved_bytes: usize,
    compute_budget: usize,
    /// Reuses index allocation only when the optional working set fits admission.
    pub(crate) extension_index_memory_limit: Option<usize>,
    pub(crate) writer: SstWriteLimits,
    pub(crate) estimated: u64,
}

impl MemoryLimits {
    pub(crate) fn new(
        plan: &MultiRegionCompactionPlan,
        schema: &SchemaRef,
        upload: UploadBudget,
    ) -> Result<Self> {
        let schema = crate::sst::maybe_wrap_schema(schema)?;
        let columns = ArrowSchemaConverter::new()
            .convert(&schema)
            .context(crate::error::WriteParquetSnafu)?
            .num_columns();
        let targets = plan.targets.len();
        let batch_share = 64usize.saturating_mul(
            plan.sources
                .len()
                .saturating_add(targets)
                .saturating_add(BATCH_SIZE + 4),
        );
        let arrow_batch_bytes = upload.compute_bytes / batch_share;
        ensure!(
            arrow_batch_bytes > 0,
            MultiRegionCompactionResourceSnafu {
                reason: "No Arrow batch budget after reserving upload memory",
            }
        );
        Ok(Self {
            arrow_batch_bytes,
            group: upload.compute_bytes / 16,
            index: upload.compute_bytes / targets / 64,
            upload_reserved_bytes: upload.reserved_bytes,
            compute_budget: upload.compute_bytes,
            extension_index_memory_limit: None,
            writer: SstWriteLimits {
                // Parquet 59 preallocates a 1 MiB bit writer for each of the
                // timestamp/sequence delta encoders. Dictionary interner tables
                // also have a nonzero minimum, independent of batch row count.
                working_bytes: (2usize << 20)
                    .saturating_add(columns.saturating_mul(128 << 10))
                    .saturating_add(arrow_batch_bytes * (MAX_ROW_GROUP_SIZE / BATCH_SIZE)),
                metadata_bytes: upload.compute_bytes / targets / 8,
                upload_limits: upload.limits,
            },
            estimated: 0,
        })
    }

    pub(crate) fn admit(
        &mut self,
        plan: &MultiRegionCompactionPlan,
        config: &MitoConfig,
        footers: &[Arc<ParquetMetaData>],
        extension_targets: usize,
    ) -> Result<()> {
        let mut extra_delta = 0;
        for target in plan.targets.values() {
            extra_delta = extra_delta.max(extra_delta_encoder_bytes(target)?);
        }
        self.writer.working_bytes = self.writer.working_bytes.saturating_add(extra_delta);
        let bytes = self.compute_bytes(plan, config, footers)?;
        let upload = self.writer.upload_limits;
        ensure!(
            bytes <= self.compute_budget as u64,
            MultiRegionCompactionResourceSnafu {
                reason: format!(
                    "Reader/writer/index minimum working set needs {bytes} bytes, compute budget is {} ({} bytes reserved for {}-byte upload chunks out of {} total)",
                    self.compute_budget,
                    self.upload_reserved_bytes,
                    upload.chunk_bytes,
                    plan.memory_limit,
                ),
            }
        );
        // Optional indexes must not turn an admitted data compaction into a
        // resource failure. Reserve all resident target states together or skip.
        let admission =
            admit_extension_indexes(bytes, self.compute_budget, self.index, extension_targets);
        self.extension_index_memory_limit = admission.limit;
        self.estimated = admission.estimated + self.upload_reserved_bytes as u64;
        common_telemetry::debug!(
            memory_limit = plan.memory_limit,
            compute_budget = self.compute_budget,
            arrow_batch_bytes = self.arrow_batch_bytes,
            upload_chunk_bytes = upload.chunk_bytes,
            upload_max_object_bytes = upload.max_object_bytes,
            upload_reserved_bytes = self.upload_reserved_bytes,
            estimated_memory_bytes = self.estimated,
            "Admitted multi-region compaction working set",
        );
        Ok(())
    }

    fn compute_bytes(
        &self,
        plan: &MultiRegionCompactionPlan,
        config: &MitoConfig,
        footers: &[Arc<ParquetMetaData>],
    ) -> Result<u64> {
        // Merge assembly can retain exhausted batches until an output batch is full.
        let mut bytes = 4_u64
            .saturating_mul((plan.sources.len() as u64).saturating_add((BATCH_SIZE + 2) as u64))
            .saturating_mul(self.arrow_batch_bytes as u64)
            .saturating_add(6 * self.group as u64);
        for footer in footers {
            bytes = bytes.saturating_add(reader_bytes(footer));
        }
        for target in plan.targets.values() {
            bytes = bytes.saturating_add(self.target_bytes(target, config)?);
        }
        Ok(bytes)
    }

    fn target_bytes(&self, target: &MultiRegionTarget, config: &MitoConfig) -> Result<u64> {
        let schema_bytes = target
            .metadata
            .to_json()
            .context(crate::error::InvalidMetadataSnafu)?
            .len();
        // Task/channel allocation plus captured schema and input-map copies.
        let control = (8_u64 << 10)
            .saturating_add((schema_bytes as u64).saturating_mul(3))
            .saturating_add(
                (target.inputs.len() as u64)
                    .saturating_mul((3 * std::mem::size_of::<crate::sst::file::FileMeta>()) as u64),
            );
        // A routed batch can be queued while the writer encodes another batch.
        // Finish workspace includes the encoded Parquet block handed to the uploader.
        let writer = (self.writer.working_bytes as u64)
            .saturating_mul(RETAINED_AND_FINISH_SCRATCH_FACTOR)
            .saturating_add(2 * self.arrow_batch_bytes as u64)
            .saturating_add(self.writer.metadata_bytes as u64);
        // Creators are instantiated even in async-index mode. Inverted/skipping
        // thresholds are shared within their indexers, fulltext is per column.
        Ok(control
            .saturating_add(writer)
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

/// Each serial upload can retain three parts: local assembly, the OpenDAL chunker,
/// and the backend's pending or in-flight part. Both writers can hold parts while
/// Puffin finishes, before Parquet closes.
const UPLOAD_CHUNKS_PER_TARGET: usize = 6;
const LOCAL_CHUNK_BYTES: usize = 256 << 10;
const PUFFIN_COPY_BUFFER_BYTES: usize = 8 << 10;
/// Estimate for two writers' part receipts and completion requests, per part number.
const COMPLETION_BYTES_PER_PART: usize = 512;

#[derive(Clone, Copy)]
pub(crate) struct UploadBudget {
    limits: UploadLimits,
    reserved_bytes: usize,
    pub(crate) compute_bytes: usize,
}

struct UploadGeometry {
    min_part_bytes: usize,
    max_part_bytes: usize,
    max_parts: Option<usize>,
    max_object_bytes: u64,
}

impl UploadBudget {
    pub(crate) fn new(plan: &MultiRegionCompactionPlan, store: &ObjectStore) -> Result<Self> {
        let geometry = UploadGeometry::from_store(store)?;
        let targets = plan.targets.len();
        let chunk_bytes = geometry.choose_chunk_bytes(plan);
        let fixed_bytes =
            PUFFIN_COPY_BUFFER_BYTES + COMPLETION_BYTES_PER_PART * geometry.max_parts.unwrap_or(0);
        let reserved_bytes = chunk_bytes
            .checked_mul(UPLOAD_CHUNKS_PER_TARGET)
            .and_then(|bytes| bytes.checked_add(fixed_bytes))
            .and_then(|bytes| bytes.checked_mul(targets))
            .context(MultiRegionCompactionResourceSnafu {
                reason: "Table compaction upload reservation overflow",
            })?;
        let compute_bytes = plan.memory_limit.checked_sub(reserved_bytes).filter(|bytes| *bytes > 0)
            .with_context(|| MultiRegionCompactionResourceSnafu {
                reason: format!(
                    "Upload reservation of {reserved_bytes} bytes for {targets} resident targets with {chunk_bytes}-byte chunks leaves no compute budget (task budget is {})",
                    plan.memory_limit,
                ),
            })?;
        Ok(Self {
            limits: geometry.limits(chunk_bytes),
            reserved_bytes,
            compute_bytes,
        })
    }
}

impl UploadGeometry {
    fn from_store(store: &ObjectStore) -> Result<Self> {
        let store_info = store.info();
        let scheme = store_info.scheme();
        let caps = store_info.capability();
        let (max_parts, protocol_part_max, protocol_object_max) = match scheme {
            "s3" | "oss" => (Some(10_000), usize::MAX, u64::MAX),
            // GCS caps even multipart objects at 5 TiB: https://cloud.google.com/storage/quotas#objects
            "gcs" => (Some(10_000), usize::MAX, 5_u64 << 40),
            // OpenDAL 0.59 does not advertise Azure's Put Block limit.
            "azblob" => (Some(50_000), 4000 << 20, u64::MAX),
            "fs" => (None, usize::MAX, u64::MAX),
            // Writers that spool a whole object or have their own transfer queues
            // need different admission rules (e.g. MySQL and HDFS).
            _ => {
                return MultiRegionCompactionResourceSnafu {
                    reason: format!(
                        "Cannot bound table compaction uploads for storage backend {scheme}"
                    ),
                }
                .fail();
            }
        };
        let min_part_bytes = caps.write_multi_min_size.unwrap_or(1).max(1);
        let max_part_bytes = caps
            .write_multi_max_size
            .unwrap_or(protocol_part_max)
            .min(protocol_part_max);
        let max_object_bytes = caps
            .write_total_max_size
            .map_or(protocol_object_max, |size| {
                protocol_object_max.min(size as u64)
            });
        ensure!(
            caps.write
                && caps.write_can_multi
                && min_part_bytes <= max_part_bytes
                && max_object_bytes > 0,
            MultiRegionCompactionResourceSnafu {
                reason: format!(
                    "No supported upload chunk size for {scheme}: minimum {min_part_bytes}, maximum {max_part_bytes}, object limit {max_object_bytes}"
                ),
            }
        );
        Ok(Self {
            min_part_bytes,
            max_part_bytes,
            max_parts,
            max_object_bytes,
        })
    }

    fn limits(&self, chunk_bytes: usize) -> UploadLimits {
        let max_object_bytes = self.max_parts.map_or(self.max_object_bytes, |parts| {
            self.max_object_bytes
                .min((chunk_bytes as u64).saturating_mul(parts as u64))
        });
        UploadLimits {
            chunk_bytes,
            max_object_bytes,
        }
    }

    fn choose_chunk_bytes(&self, plan: &MultiRegionCompactionPlan) -> usize {
        let preferred = self.max_parts.map_or(LOCAL_CHUNK_BYTES, |parts| {
            // Leave room for a final part near the soft rollover target. The
            // hard upload limit still applies to a long series or large index.
            (DEFAULT_WRITE_BUFFER_SIZE.as_bytes() as usize)
                .max(plan.max_output_file_size.div_ceil(parts - 1))
        });
        // Prefer to spend at most one eighth of the task on part buffers
        // unless the backend minimum requires more.
        preferred
            .min(plan.memory_limit / plan.targets.len() / 8 / UPLOAD_CHUNKS_PER_TARGET)
            .clamp(self.min_part_bytes, self.max_part_bytes)
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

pub(super) fn reader_bytes(footer: &ParquetMetaData) -> u64 {
    // Dictionary-encoded values decode to more than the uncompressed row group size; the footer
    // records the largest decoded row group of the file.
    let decoded = max_row_group_value_bytes(footer);
    let pages = footer
        .row_groups()
        .iter()
        .map(|group| {
            (group.total_byte_size().max(0) as u64)
                .max(decoded)
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
    use object_store::ObjectStore;
    use object_store::layers::CapabilityOverrideLayer;
    use object_store::services::Gcs;

    use super::{ExtensionIndexAdmission, UploadGeometry, admit_extension_indexes};

    #[rstest::rstest]
    #[case::service_limit(1 << 30, None, 5_u64 << 40)]
    #[case::advertised_limit(1 << 30, Some(2 << 30), 2 << 30)]
    #[case::part_count_limit(5 << 20, None, 10_000 * (5_u64 << 20))]
    fn test_gcs_multipart_object_limit(
        #[case] chunk_bytes: usize,
        #[case] advertised: Option<usize>,
        #[case] expected: u64,
    ) {
        let store = ObjectStore::new(Gcs::default().bucket("test"))
            .unwrap()
            .layer(CapabilityOverrideLayer::new(move |mut caps| {
                caps.write_total_max_size = advertised;
                caps
            }));
        let geometry = UploadGeometry::from_store(&store).unwrap();
        assert_eq!(expected, geometry.limits(chunk_bytes).max_object_bytes);
    }

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
