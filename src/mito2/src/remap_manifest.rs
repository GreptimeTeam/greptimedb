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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use partition::expr::PartitionExpr;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::storage::{FileId, RegionId};

use crate::error;
pub use crate::error::{Error, Result};
use crate::manifest::action::{RegionManifest, RemovedFilesRecord};
use crate::sst::file::FileFormat;

/// Remaps file references from old region manifests to new region manifests.
pub struct RemapManifest {
    /// Old region manifests indexed by region ID
    old_manifests: HashMap<RegionId, RegionManifest>,
    /// New partition expressions indexed by region ID
    new_partition_exprs: HashMap<RegionId, PartitionExpr>,
    /// For each old region, which new regions should receive its files
    region_mapping: HashMap<RegionId, Vec<RegionId>>,
    /// Newly generated manifests for target regions
    new_manifests: HashMap<RegionId, RegionManifest>,
}

impl RemapManifest {
    pub fn new(
        old_manifests: HashMap<RegionId, RegionManifest>,
        new_partition_exprs: HashMap<RegionId, PartitionExpr>,
        region_mapping: HashMap<RegionId, Vec<RegionId>>,
    ) -> Self {
        Self {
            old_manifests,
            new_partition_exprs,
            region_mapping,
            new_manifests: HashMap::new(),
        }
    }

    /// Remaps manifests from old regions to new regions.
    ///
    /// Main entry point. It copies files from old regions to new regions based on
    /// partition expression overlaps.
    pub fn remap_manifests(&mut self) -> Result<RemapResult> {
        // A region may only be remapped while it holds files it produced itself.
        self.ensure_regions_own_their_files()?;

        // initialize new manifests
        self.initialize_new_manifests()?;

        // remap files
        self.do_remap()?;

        // merge and set metadata for all new manifests
        self.finalize_manifests()?;

        // validate and compute statistics
        let stats = self.compute_stats();
        self.validate_result(&stats)?;

        let new_manifests = std::mem::take(&mut self.new_manifests);

        Ok(RemapResult {
            new_manifests,
            stats,
        })
    }

    /// Rejects a merge while a participant holds a file another region produced.
    ///
    /// A reader drops the region predicate exactly when the region's expression equals the
    /// file's (`ScanRegion::should_skip_region_partition`), so once a holder materializes a copy
    /// under a new file id the same row sits in the shared file and in the copy, and publishing
    /// both reads it twice.
    ///
    /// The operator resolves the refusal by hand: abort the repartition (a region in a staging
    /// session is skipped by the compaction scheduler), compact, retry. Nothing triggers that
    /// compaction automatically.
    ///
    /// A split is exempt: each target receives only that source's files and no target combines
    /// copies, so the region predicate still keeps their rows apart.
    ///
    /// A file that records no creator is *not* exempt. It can still hold rows of both children of
    /// an earlier split, so it proves nothing about provenance: if both children inherited it and
    /// only one of them compacted, the copy and the inherited file both hold that child's rows and
    /// a merge would publish them twice. The region has to materialise (a compaction stamps its
    /// output) before a merge.
    fn ensure_regions_own_their_files(&self) -> Result<()> {
        if self.is_split() {
            return Ok(());
        }

        let mut offenders = Vec::new();
        for &from_region_id in self.region_mapping.keys() {
            let from_manifest = self.old_manifests.get(&from_region_id).context(
                error::MissingOldManifestSnafu {
                    region_id: from_region_id,
                },
            )?;
            collect_foreign_files(from_region_id, from_manifest, &mut offenders)?;
        }
        ensure!(
            offenders.is_empty(),
            error::ForeignFilePartitionExprSnafu {
                offenders: describe_offenders(&offenders),
            }
        );

        Ok(())
    }

    /// Whether this remap publishes every target from at most one source: the shape that is
    /// exempt from [`Self::ensure_regions_own_their_files`], because no target combines two
    /// sources' file sets.
    ///
    /// The predicate is stated per target, not per source count, and the pre-publication check of
    /// the repartition procedure reads it from the same place
    /// ([`store_api::region_engine::sources_sharing_a_target`]), so the two stages agree on every
    /// shape. One source feeding several targets (a split), one source feeding one target (a
    /// one-to-one repartition, which may still hold a file an ancestor created) and several
    /// sources feeding disjoint targets all hand each target a single source's files: the shared
    /// object is then filtered by that target's own expression, never recombined with a copy of
    /// itself. Only a target fed by two or more distinct sources can see both an inherited object
    /// and a copy materialized from it, so only that shape is checked.
    fn is_split(&self) -> bool {
        store_api::region_engine::every_target_has_a_single_source(&self.region_mapping)
    }

    /// Initializes empty manifests for all new regions.
    fn initialize_new_manifests(&mut self) -> Result<()> {
        let mut new_manifests = HashMap::new();

        // Get any old manifest as template (they all share the same table schema)
        let template_manifest = self
            .old_manifests
            .values()
            .next()
            .context(error::NoOldManifestsSnafu)?;
        let template_metadata = (*template_manifest.metadata).clone();
        let sst_format = template_manifest.sst_format;

        // Create empty manifest for each new region
        for region_id in self.new_partition_exprs.keys() {
            // Derive region metadata from any old manifest as template
            let mut new_metadata = template_metadata.clone();

            new_metadata.region_id = *region_id;
            let new_partition_expr = self
                .new_partition_exprs
                .get(region_id)
                .context(error::MissingPartitionExprSnafu {
                    region_id: *region_id,
                })?
                .as_json_str()
                .context(error::SerializePartitionExprSnafu)?;
            new_metadata.set_partition_expr(Some(new_partition_expr));

            let manifest = RegionManifest {
                metadata: Arc::new(new_metadata),
                files: HashMap::new(),
                removed_files: RemovedFilesRecord::default(),
                flushed_entry_id: 0,
                flushed_sequence: 0,
                committed_sequence: None,
                manifest_version: 0,
                truncated_entry_id: None,
                compaction_time_window: None,
                sst_format,
                append_mode: None,
                pending_tails: Default::default(),
                series_indexes: Default::default(),
                handoff_floors: Default::default(),
            };

            new_manifests.insert(*region_id, manifest);
        }

        self.new_manifests = new_manifests;

        Ok(())
    }

    /// Remaps files from old regions to new regions according to the region mapping.
    fn do_remap(&mut self) -> Result<()> {
        // For each old region and its target new regions
        for (&from_region_id, target_region_ids) in &self.region_mapping {
            // Get old manifest
            let from_manifest = self.old_manifests.get(&from_region_id).context(
                error::MissingOldManifestSnafu {
                    region_id: from_region_id,
                },
            )?;

            // Copy files to all target new regions
            for &to_region_id in target_region_ids {
                let target_manifest = self.new_manifests.get_mut(&to_region_id).context(
                    error::MissingNewManifestSnafu {
                        region_id: to_region_id,
                    },
                )?;

                Self::copy_files_to_region(from_manifest, target_manifest)?;
            }
        }

        Ok(())
    }

    /// Copies files from a source region to a target region.
    fn copy_files_to_region(
        source_manifest: &RegionManifest,
        target_manifest: &mut RegionManifest,
    ) -> Result<()> {
        ensure!(
            (source_manifest.sst_format == crate::sst::FormatType::Narthex)
                == (target_manifest.sst_format == crate::sst::FormatType::Narthex),
            error::InvalidRequestSnafu {
                region_id: target_manifest.metadata.region_id,
                reason: "Cannot remap between Parquet and Narthex regions",
            }
        );
        for (file_id, file_meta) in &source_manifest.files {
            if let Some(existing) = target_manifest.files.get(file_id)
                && source_manifest.sst_format == crate::sst::FormatType::Narthex
            {
                // Admission sequences may differ by logical region; physical
                // ownership and object inventory must still agree in all builds.
                let same_inventory = match (&existing.format, &file_meta.format) {
                    (FileFormat::NarthexSamples(left), FileFormat::NarthexSamples(right)) => {
                        left.manifest_size == right.manifest_size && left.parts == right.parts
                    }
                    _ => false,
                };
                ensure!(
                    existing.region_id == file_meta.region_id && same_inventory,
                    error::InconsistentFileSnafu {
                        file_id: *file_id,
                        reason: "narthex samples identity mismatch",
                    }
                );
            }
            let file_meta_clone = file_meta.clone();

            // Insert or merge into target manifest
            // Same file might be added from multiple overlapping old regions
            use std::collections::hash_map::Entry;
            match target_manifest.files.entry(*file_id) {
                Entry::Vacant(e) => {
                    e.insert(file_meta_clone);
                }
                #[cfg(debug_assertions)]
                Entry::Occupied(e) => {
                    // File already exists - verify it's the same physical file
                    Self::verify_file_consistency(e.get(), &file_meta_clone)?;
                }
                #[cfg(not(debug_assertions))]
                Entry::Occupied(_) => {}
            }
        }

        // Remap creates references, not physical copies: retain index owner,
        // inventory and exact source identities together with the samples.
        for (id, index) in &source_manifest.series_indexes {
            if let Some(existing) = target_manifest.series_indexes.get(id) {
                ensure!(
                    existing == index,
                    error::InconsistentFileSnafu {
                        file_id: *id,
                        reason: "narthex series identity mismatch",
                    }
                );
            } else {
                target_manifest.series_indexes.insert(*id, index.clone());
            }
        }
        Ok(())
    }

    /// Verifies that two file metadata entries are consistent.
    #[cfg(debug_assertions)]
    fn verify_file_consistency(
        existing: &crate::sst::file::FileMeta,
        new: &crate::sst::file::FileMeta,
    ) -> Result<()> {
        // When the same file appears from multiple overlapping old regions,
        // verify they are actually the same physical file with identical metadata

        ensure!(
            existing.region_id == new.region_id,
            error::InconsistentFileSnafu {
                file_id: existing.file_id,
                reason: "region_id mismatch",
            }
        );

        ensure!(
            existing.file_id == new.file_id,
            error::InconsistentFileSnafu {
                file_id: existing.file_id,
                reason: "file_id mismatch",
            }
        );

        ensure!(
            existing.time_range == new.time_range,
            error::InconsistentFileSnafu {
                file_id: existing.file_id,
                reason: "time_range mismatch",
            }
        );

        ensure!(
            existing.level == new.level,
            error::InconsistentFileSnafu {
                file_id: existing.file_id,
                reason: "level mismatch",
            }
        );

        ensure!(
            existing.file_size == new.file_size,
            error::InconsistentFileSnafu {
                file_id: existing.file_id,
                reason: "file_size mismatch",
            }
        );

        ensure!(
            existing.partition_expr == new.partition_expr,
            error::InconsistentFileSnafu {
                file_id: existing.file_id,
                reason: "partition_expr mismatch",
            }
        );

        Ok(())
    }

    /// Finalizes manifests by merging metadata from source regions.
    fn finalize_manifests(&mut self) -> Result<()> {
        for (region_id, manifest) in self.new_manifests.iter_mut() {
            if let Some(previous_manifest) = self.old_manifests.get(region_id) {
                manifest.flushed_entry_id = previous_manifest.flushed_entry_id;
                manifest.flushed_sequence = previous_manifest.flushed_sequence;
                manifest.manifest_version = previous_manifest.manifest_version;
                manifest.truncated_entry_id = previous_manifest.truncated_entry_id;
                manifest.compaction_time_window = previous_manifest.compaction_time_window;
                manifest.committed_sequence = previous_manifest.committed_sequence;
            } else {
                // new region
                manifest.flushed_entry_id = 0;
                manifest.flushed_sequence = 0;
                manifest.manifest_version = 0;
                manifest.truncated_entry_id = None;
                manifest.compaction_time_window = None;
                manifest.committed_sequence = None;
            }

            // removed_files are tracked by old manifests, don't copy
            manifest.removed_files = RemovedFilesRecord::default();
        }

        Ok(())
    }

    /// Computes statistics about the remapping.
    fn compute_stats(&self) -> RemapStats {
        let mut files_per_region = HashMap::with_capacity(self.new_manifests.len());
        let mut total_file_refs = 0;
        let mut empty_regions = Vec::new();
        let mut all_files = HashSet::new();

        for (&region_id, manifest) in &self.new_manifests {
            let file_count = manifest.files.len();
            files_per_region.insert(region_id, file_count);
            total_file_refs += file_count;

            if file_count == 0 {
                empty_regions.push(region_id);
            }

            for file_id in manifest.files.keys() {
                all_files.insert(*file_id);
            }
        }

        RemapStats {
            files_per_region,
            total_file_refs,
            empty_regions,
            unique_files: all_files.len(),
        }
    }

    /// Validates the remapping result.
    fn validate_result(&self, stats: &RemapStats) -> Result<()> {
        // all new regions have manifests
        for region_id in self.new_partition_exprs.keys() {
            ensure!(
                self.new_manifests.contains_key(region_id),
                error::MissingNewManifestSnafu {
                    region_id: *region_id
                }
            );
        }

        // no unique files were lost
        // Count unique files in old manifests (files may be duplicated across regions)
        let mut old_unique_files = HashSet::new();
        for manifest in self.old_manifests.values() {
            for file_id in manifest.files.keys() {
                old_unique_files.insert(*file_id);
            }
        }

        ensure!(
            stats.unique_files >= old_unique_files.len(),
            error::FilesLostSnafu {
                old_count: old_unique_files.len(),
                new_count: stats.unique_files,
            }
        );

        // 3. Log warning about empty regions (not an error)
        if !stats.empty_regions.is_empty() {
            common_telemetry::warn!(
                "Repartition resulted in {} empty regions: {:?}, new partition exprs: {:?}",
                stats.empty_regions.len(),
                stats.empty_regions,
                self.new_partition_exprs.keys().collect::<Vec<_>>()
            );
        }

        Ok(())
    }
}

/// Checks that `region_id` holds only the files it created itself.
///
/// This is the single comparison the ownership rule is made of, factored out of
/// [`RemapManifest::ensure_regions_own_their_files`] so a caller can run it *before* anything is
/// staged: it reads one persisted manifest and returns an error naming the region and every
/// offending file. It does not need a staging region, does not write and has no side effects.
///
/// A split is exempt at *group* scope: the caller that runs this early only asks the sources that
/// take part in a merge, because a one-to-many split hands each target only that source's files
/// and combines no copies.
pub(crate) fn ensure_region_owns_its_files(
    region_id: RegionId,
    manifest: &RegionManifest,
) -> Result<()> {
    let mut offenders = Vec::new();
    collect_foreign_files(region_id, manifest, &mut offenders)?;
    ensure!(
        offenders.is_empty(),
        error::ForeignFilePartitionExprSnafu {
            offenders: describe_offenders(&offenders),
        }
    );

    Ok(())
}

/// Collects the `(region, file)` pairs of `manifest` whose recorded creator is not `region_id`'s
/// expression.
fn collect_foreign_files(
    region_id: RegionId,
    manifest: &RegionManifest,
    offenders: &mut Vec<(RegionId, FileId)>,
) -> Result<()> {
    let region_expr =
        crate::region::parse_partition_expr(manifest.metadata.partition_expr.as_deref())?;
    for (file_id, file_meta) in &manifest.files {
        // Exact match, including the case where the region serves an expression and the file
        // records none: such a file may hold rows either child of an earlier split wrote, so it
        // is not evidence that this region produced it.
        if file_meta.partition_expr.as_deref() != region_expr.as_ref() {
            offenders.push((region_id, *file_id));
        }
    }

    Ok(())
}

/// Number of offending files an error message lists before it summarizes the rest.
const MAX_LISTED_OFFENDERS: usize = 8;

/// Renders the offending `(region, file)` pairs an error names.
///
/// A repartition can be refused by many files, so the message lists the first
/// [`MAX_LISTED_OFFENDERS`] and only counts the rest: it stays bounded while every listed entry
/// keeps the region and the file the operator has to settle.
fn describe_offenders(offenders: &[(RegionId, FileId)]) -> String {
    let mut message = offenders
        .iter()
        .take(MAX_LISTED_OFFENDERS)
        .map(|(region_id, file_id)| {
            format!("region {region_id} holds file {file_id} that another region created")
        })
        .collect::<Vec<_>>()
        .join("; ");
    if let Some(rest) = offenders
        .len()
        .checked_sub(MAX_LISTED_OFFENDERS)
        .filter(|rest| *rest > 0)
    {
        message.push_str(&format!("; and {rest} more"));
    }
    message
}

/// Result of manifest remapping, including new manifests and statistics.
#[derive(Debug)]
pub struct RemapResult {
    /// New manifests for all new regions
    pub new_manifests: HashMap<RegionId, RegionManifest>,
    /// Statistics about the remapping
    pub stats: RemapStats,
}

/// Statistical information about the manifest remapping.
#[derive(Debug)]
pub struct RemapStats {
    /// Number of files per region in new manifests
    pub files_per_region: HashMap<RegionId, usize>,
    /// Total number of file references created across all new regions
    pub total_file_refs: usize,
    /// Regions that received no files (potentially empty after repartition)
    pub empty_regions: Vec<RegionId>,
    /// Total number of unique files processed
    pub unique_files: usize,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::num::NonZeroU64;
    use std::sync::Arc;
    use std::time::Duration;

    use api::v1::SemanticType;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use datatypes::value::Value;
    use partition::expr::{PartitionExpr, col};
    use smallvec::SmallVec;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};
    use store_api::storage::{FileId, RegionId, SequenceNumber};

    use super::*;
    use crate::manifest::action::RegionManifest;
    use crate::sst::FormatType;
    use crate::sst::file::{FileMeta, FileTimeRange};
    use crate::wal::EntryId;

    /// Helper to create a basic region metadata for testing.
    fn create_region_metadata(region_id: RegionId) -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(region_id);
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("pk", ConcreteDataType::int64_datatype(), false),
                semantic_type: SemanticType::Tag,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "val",
                    ConcreteDataType::float64_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Field,
                column_id: 3,
            })
            .primary_key(vec![2]);
        Arc::new(builder.build().unwrap())
    }

    /// Helper to create a FileMeta for testing.
    fn create_file_meta(
        region_id: RegionId,
        file_id: FileId,
        partition_expr: Option<PartitionExpr>,
    ) -> FileMeta {
        FileMeta {
            region_id,
            file_id,
            time_range: FileTimeRange::default(),
            level: 0,
            file_size: 1024,
            max_row_group_uncompressed_size: 1024,
            available_indexes: SmallVec::new(),
            indexes: Default::default(),
            index_file_size: 0,
            index_version: 0,
            num_rows: 100,
            num_row_groups: 1,
            sequence: NonZeroU64::new(1),
            partition_expr: partition_expr.map(Into::into),
            num_series: 1,
            ..Default::default()
        }
    }

    /// Helper to create a manifest with specified number of files.
    fn create_manifest(
        region_id: RegionId,
        num_files: usize,
        partition_expr: Option<PartitionExpr>,
        flushed_entry_id: EntryId,
        flushed_sequence: SequenceNumber,
    ) -> RegionManifest {
        let mut files = HashMap::new();
        for _ in 0..num_files {
            let file_id = FileId::random();
            let file_meta = create_file_meta(region_id, file_id, partition_expr.clone());
            files.insert(file_id, file_meta.into());
        }

        // The files are stamped with `partition_expr`, so the region serving that same
        // expression produced them: a remap requires exactly that.
        let mut metadata = (*create_region_metadata(region_id)).clone();
        metadata.set_partition_expr(
            partition_expr
                .as_ref()
                .map(|expr| expr.as_json_str().unwrap()),
        );

        RegionManifest {
            metadata: Arc::new(metadata),
            files,
            removed_files: RemovedFilesRecord::default(),
            flushed_entry_id,
            flushed_sequence,
            manifest_version: 1,
            truncated_entry_id: None,
            compaction_time_window: None,
            committed_sequence: None,
            sst_format: FormatType::PrimaryKey,
            append_mode: None,
            pending_tails: Default::default(),
            series_indexes: Default::default(),
            handoff_floors: Default::default(),
        }
    }

    #[rstest::rstest]
    #[case(FormatType::Narthex, FormatType::Flat)]
    #[case(FormatType::PrimaryKey, FormatType::Narthex)]
    #[case(FormatType::Flat, FormatType::PrimaryKey)]
    #[test]
    fn test_remap_storage_format(
        #[case] source_format: FormatType,
        #[case] target_format: FormatType,
    ) {
        let mut source = create_manifest(RegionId::new(1, 0), 0, None, 0, 0);
        let mut target = create_manifest(RegionId::new(1, 1), 0, None, 0, 0);
        source.sst_format = source_format;
        target.sst_format = target_format;
        let result = RemapManifest::copy_files_to_region(&source, &mut target);
        if source_format == FormatType::Narthex || target_format == FormatType::Narthex {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("Cannot remap between")
            );
        } else {
            result.unwrap();
        }
        assert!(target.files.is_empty());
        assert_eq!(target.sst_format, target_format);
    }

    /// Helper to create partition expression: col >= start AND col < end
    fn range_expr(col_name: &str, start: i64, end: i64) -> PartitionExpr {
        col(col_name)
            .gt_eq(Value::Int64(start))
            .and(col(col_name).lt(Value::Int64(end)))
    }

    #[test]
    fn test_simple_split() {
        // One region [0, 100) splits into two regions: [0, 50) and [50, 100)
        let old_region_id = RegionId::new(1, 1);
        let new_region_id_1 = RegionId::new(1, 2);
        let new_region_id_2 = RegionId::new(1, 3);

        let old_expr = range_expr("x", 0, 100);
        let new_expr_1 = range_expr("x", 0, 50);
        let new_expr_2 = range_expr("x", 50, 100);

        let old_manifest = create_manifest(old_region_id, 10, Some(old_expr.clone()), 100, 200);

        let mut old_manifests = HashMap::new();
        old_manifests.insert(old_region_id, old_manifest);

        let mut new_partition_exprs = HashMap::new();
        new_partition_exprs.insert(new_region_id_1, new_expr_1);
        new_partition_exprs.insert(new_region_id_2, new_expr_2);

        // Direct mapping: old region -> both new regions
        let mut region_mapping = HashMap::new();
        region_mapping.insert(old_region_id, vec![new_region_id_1, new_region_id_2]);

        let mut remapper = RemapManifest::new(old_manifests, new_partition_exprs, region_mapping);

        let result = remapper.remap_manifests().unwrap();

        // Both new regions should have all 10 files
        assert_eq!(result.new_manifests.len(), 2);
        assert_eq!(result.new_manifests[&new_region_id_1].files.len(), 10);
        assert_eq!(result.new_manifests[&new_region_id_2].files.len(), 10);
        assert_eq!(result.stats.total_file_refs, 20);
        assert_eq!(result.stats.unique_files, 10);
        assert!(result.stats.empty_regions.is_empty());

        // Verify FileMeta is immutable - region_id stays as old_region_id
        for file_meta in result.new_manifests[&new_region_id_1].files.values() {
            assert_eq!(file_meta.region_id, old_region_id);
        }
        for file_meta in result.new_manifests[&new_region_id_2].files.values() {
            assert_eq!(file_meta.region_id, old_region_id);
        }
    }

    #[test]
    fn test_rejects_files_another_region_produced() {
        // Both participants of the merge hold a file the whole-domain source created: merging
        // them could publish a row of it twice once a holder materializes its own copy, and the
        // refusal has to name every offending region and file.
        let old_region_id_1 = RegionId::new(1, 1);
        let old_region_id_2 = RegionId::new(1, 2);
        let new_region_id = RegionId::new(1, 3);

        let creator_expr = range_expr("x", 0, 100);
        let mut old_manifests = HashMap::new();
        let mut file_ids = Vec::new();
        for old_region_id in [old_region_id_1, old_region_id_2] {
            let own_expr = if old_region_id == old_region_id_1 {
                range_expr("x", 0, 50)
            } else {
                range_expr("x", 50, 100)
            };
            let mut old_manifest = create_manifest(old_region_id, 1, Some(own_expr), 100, 200);
            let file_id = *old_manifest.files.keys().next().unwrap();
            old_manifest.files.insert(
                file_id,
                create_file_meta(old_region_id, file_id, Some(creator_expr.clone())).into(),
            );
            file_ids.push(file_id);
            old_manifests.insert(old_region_id, old_manifest);
        }

        let mut new_partition_exprs = HashMap::new();
        new_partition_exprs.insert(new_region_id, range_expr("x", 0, 100));

        let mut region_mapping = HashMap::new();
        region_mapping.insert(old_region_id_1, vec![new_region_id]);
        region_mapping.insert(old_region_id_2, vec![new_region_id]);

        let mut remapper = RemapManifest::new(old_manifests, new_partition_exprs, region_mapping);

        let err = remapper.remap_manifests().unwrap_err();
        assert!(
            matches!(&err, Error::ForeignFilePartitionExpr { .. }),
            "unexpected error: {err:?}"
        );
        let message = err.to_string();
        for (region_id, file_id) in [
            (old_region_id_1, file_ids[0]),
            (old_region_id_2, file_ids[1]),
        ] {
            assert!(message.contains(&region_id.to_string()), "{message}");
            assert!(message.contains(&file_id.to_string()), "{message}");
        }
    }

    #[test]
    fn test_one_to_one_repartition_is_exempt() {
        // A one-to-one repartition reuses the region id and may still hold a file an ancestor
        // created (a split installs the child's expression without rewriting the files it
        // clones). No target receives files from two sources, so nothing is combined and the
        // remap publishes the file under the region's own expression: the shape is exempt, which
        // is what keeps the pre-publication check and this guard from disagreeing.
        let old_region_id = RegionId::new(1, 1);
        let own_expr = range_expr("x", 0, 50);
        let ancestor_expr = range_expr("x", 0, 100);

        let mut old_manifest = create_manifest(old_region_id, 1, Some(own_expr.clone()), 100, 200);
        let file_id = *old_manifest.files.keys().next().unwrap();
        old_manifest.files.insert(
            file_id,
            create_file_meta(old_region_id, file_id, Some(ancestor_expr)).into(),
        );

        let mut remapper = RemapManifest::new(
            HashMap::from([(old_region_id, old_manifest)]),
            HashMap::from([(old_region_id, own_expr)]),
            HashMap::from([(old_region_id, vec![old_region_id])]),
        );
        let result = remapper.remap_manifests().unwrap();
        assert_eq!(result.new_manifests[&old_region_id].files.len(), 1);
        assert!(
            result.new_manifests[&old_region_id]
                .files
                .contains_key(&file_id)
        );
    }

    #[test]
    fn test_target_fed_by_two_sources_is_still_checked() {
        // The predicate is per target, not per source count: here each source feeds a target of
        // its own *and* the shared one. The shared target combines both file sets, so the guard
        // runs and reports the file the two sources would publish twice.
        let left_region_id = RegionId::new(1, 1);
        let right_region_id = RegionId::new(1, 2);
        let shared_region_id = RegionId::new(1, 3);
        let left_only_region_id = RegionId::new(1, 4);

        let left_expr = range_expr("x", 0, 50);
        let right_expr = range_expr("x", 50, 100);
        let shared_expr = range_expr("x", 0, 100);

        let mut left_manifest =
            create_manifest(left_region_id, 1, Some(left_expr.clone()), 100, 200);
        let left_file = *left_manifest.files.keys().next().unwrap();
        left_manifest.files.insert(
            left_file,
            create_file_meta(left_region_id, left_file, Some(shared_expr.clone())).into(),
        );
        let right_manifest = create_manifest(right_region_id, 1, Some(right_expr), 100, 200);

        let mut remapper = RemapManifest::new(
            HashMap::from([
                (left_region_id, left_manifest),
                (right_region_id, right_manifest),
            ]),
            HashMap::from([
                (shared_region_id, shared_expr),
                (left_only_region_id, left_expr),
            ]),
            HashMap::from([
                (left_region_id, vec![shared_region_id, left_only_region_id]),
                (right_region_id, vec![shared_region_id]),
            ]),
        );
        let err = remapper.remap_manifests().unwrap_err();
        assert!(
            matches!(&err, Error::ForeignFilePartitionExpr { .. }),
            "unexpected error: {err:?}"
        );
        let message = err.to_string();
        assert!(message.contains(&left_region_id.to_string()), "{message}");
        assert!(message.contains(&left_file.to_string()), "{message}");
    }

    #[test]
    fn test_early_check_needs_no_staging_region() {
        // `handle_remap` runs this predicate when the instruction asks for validation only, i.e.
        // before any route is published and therefore before the region enters staging: it reads
        // the ordinary manifest and reports the offenders it finds there.
        let region_id = RegionId::new(1, 1);
        let own_expr = range_expr("x", 0, 50);
        let mut manifest = create_manifest(region_id, 1, Some(own_expr), 100, 200);
        ensure_region_owns_its_files(region_id, &manifest).unwrap();

        // A file another region created: the early check refuses it exactly like the remap does.
        let foreign_expr = range_expr("x", 0, 100);
        let file_id = *manifest.files.keys().next().unwrap();
        manifest.files.insert(
            file_id,
            create_file_meta(region_id, file_id, Some(foreign_expr)).into(),
        );

        let err = ensure_region_owns_its_files(region_id, &manifest).unwrap_err();
        assert!(
            matches!(&err, Error::ForeignFilePartitionExpr { .. }),
            "unexpected error: {err:?}"
        );
        let message = err.to_string();
        assert!(message.contains(&region_id.to_string()), "{message}");
        assert!(message.contains(&file_id.to_string()), "{message}");
    }

    #[test]
    fn test_early_check_is_not_exempt_for_a_split_source() {
        // The shape exemption belongs to the repartition procedure, which asks only about the
        // sources of a target that two sources feed: a split maps every target from that single
        // source, so the procedure never asks, while the per-region predicate itself - the one a
        // checked source is judged by - reports such a file. Keeping the exemption out of this
        // predicate is what makes the pre-publication check as strict as the remap-time guard.
        let old_region_id = RegionId::new(1, 1);
        let new_region_id_1 = RegionId::new(1, 2);
        let new_region_id_2 = RegionId::new(1, 3);

        let own_expr = range_expr("x", 50, 100);
        let ancestor_expr = range_expr("x", 0, 100);
        let mut old_manifest = create_manifest(old_region_id, 1, Some(own_expr), 100, 200);
        let file_id = *old_manifest.files.keys().next().unwrap();
        old_manifest.files.insert(
            file_id,
            create_file_meta(old_region_id, file_id, Some(ancestor_expr)).into(),
        );

        // The split itself is accepted: both children inherit the file and each keeps its own
        // part of it visible through its own predicate.
        let mut remapper = RemapManifest::new(
            HashMap::from([(old_region_id, old_manifest.clone())]),
            HashMap::from([
                (new_region_id_1, range_expr("x", 50, 75)),
                (new_region_id_2, range_expr("x", 75, 100)),
            ]),
            HashMap::from([(old_region_id, vec![new_region_id_1, new_region_id_2])]),
        );
        remapper.remap_manifests().unwrap();

        // The predicate a merge source is checked with reports the very same file.
        let err = ensure_region_owns_its_files(old_region_id, &old_manifest).unwrap_err();
        assert!(
            matches!(&err, Error::ForeignFilePartitionExpr { .. }),
            "unexpected error: {err:?}"
        );
        assert!(err.to_string().contains(&file_id.to_string()), "{err}");
    }

    #[test]
    fn test_split_is_exempt_from_the_foreign_file_rule() {
        // The source serves [50, 100) and holds the file an ancestor [0, 100) region created.
        // The split hands that object to both targets — [50, 75) and [75, 100), which is a real
        // partition of the source — and stays exempt: each target keeps only its own part of the
        // object visible through its own predicate, and no target combines file sets.
        let old_region_id = RegionId::new(1, 1);
        let new_region_id_1 = RegionId::new(1, 2);
        let new_region_id_2 = RegionId::new(1, 3);

        let source_expr = range_expr("x", 50, 100);
        let mut old_manifest = create_manifest(old_region_id, 1, Some(source_expr), 100, 200);
        let file_id = *old_manifest.files.keys().next().unwrap();
        old_manifest.files.insert(
            file_id,
            // The file a wider [0, 100) region created, so the target domains are inside it.
            create_file_meta(old_region_id, file_id, Some(range_expr("x", 0, 100))).into(),
        );

        let mut old_manifests = HashMap::new();
        old_manifests.insert(old_region_id, old_manifest);

        let left_expr = range_expr("x", 50, 75);
        let right_expr = range_expr("x", 75, 100);
        let mut new_partition_exprs = HashMap::new();
        new_partition_exprs.insert(new_region_id_1, left_expr);
        new_partition_exprs.insert(new_region_id_2, right_expr);

        let mut region_mapping = HashMap::new();
        region_mapping.insert(old_region_id, vec![new_region_id_1, new_region_id_2]);

        let mut remapper = RemapManifest::new(old_manifests, new_partition_exprs, region_mapping);

        let result = remapper.remap_manifests().unwrap();
        // Both targets inherit the shared object, so the exemption has to be what lets the pair
        // through: the source's own file is foreign to it (creator [0, 100)).
        for region_id in [new_region_id_1, new_region_id_2] {
            let manifest = &result.new_manifests[&region_id];
            assert_eq!(
                vec![file_id],
                manifest.files.keys().copied().collect::<Vec<_>>()
            );
            // The targets' expressions are strict parts of the source interval, so their union
            // covers it and neither is the ancestry of the other.
            let target_expr =
                crate::region::parse_partition_expr(manifest.metadata.partition_expr.as_deref())
                    .unwrap()
                    .expect("a split target serves an expression");
            assert_ne!(target_expr, range_expr("x", 50, 100));
        }
        assert_eq!(2, result.stats.total_file_refs);
        assert_eq!(1, result.stats.unique_files);
    }

    #[test]
    fn test_legacy_files_without_a_creator_are_rejected_by_a_merge() {
        // Files written before the expression was recorded carry no creator, which proves
        // nothing about provenance: such a file can hold rows of both children of an earlier
        // split. The merge is refused; the region materialises first (a compaction stamps its
        // output) exactly like it does for a file whose creator belongs to another region. A
        // split stays exempt, so the first repartition of an upgraded table still works.
        let old_region_id_1 = RegionId::new(1, 1);
        let old_region_id_2 = RegionId::new(1, 2);
        let new_region_id = RegionId::new(1, 3);

        let mut old_manifests = HashMap::new();
        let mut file_ids = Vec::new();
        for (old_region_id, expr) in [
            (old_region_id_1, range_expr("x", 0, 50)),
            (old_region_id_2, range_expr("x", 50, 100)),
        ] {
            let mut old_manifest = create_manifest(old_region_id, 1, Some(expr), 100, 200);
            let file_id = *old_manifest.files.keys().next().unwrap();
            old_manifest.files.insert(
                file_id,
                create_file_meta(old_region_id, file_id, None).into(),
            );
            file_ids.push((old_region_id, file_id));
            old_manifests.insert(old_region_id, old_manifest);
        }

        let mut new_partition_exprs = HashMap::new();
        new_partition_exprs.insert(new_region_id, range_expr("x", 0, 100));

        let mut region_mapping = HashMap::new();
        region_mapping.insert(old_region_id_1, vec![new_region_id]);
        region_mapping.insert(old_region_id_2, vec![new_region_id]);

        let mut remapper = RemapManifest::new(old_manifests, new_partition_exprs, region_mapping);
        let err = remapper.remap_manifests().unwrap_err();
        assert!(
            matches!(&err, Error::ForeignFilePartitionExpr { .. }),
            "an unstamped file must not be exempt from the rule: {err:?}"
        );
        let message = err.to_string();
        for (region_id, file_id) in file_ids {
            assert!(message.contains(&region_id.to_string()), "{message}");
            assert!(message.contains(&file_id.to_string()), "{message}");
        }
    }

    #[test]
    fn test_merge_rejects_self_stamped_sibling_and_unstamped_sibling() {
        // The reviewer's shape: one child of an earlier split compacted, so it holds a file it
        // stamped itself; its sibling still holds the unstamped ancestor object. Merging them
        // would publish the compacted child's rows twice — once from its own file, once from the
        // ancestor object the sibling inherited — so the unstamped file has to be an offender.
        let left_region_id = RegionId::new(1, 1);
        let right_region_id = RegionId::new(1, 2);
        let merged_region_id = RegionId::new(1, 3);

        let left_expr = range_expr("x", 0, 50);
        let mut left_manifest =
            create_manifest(left_region_id, 1, Some(left_expr.clone()), 100, 200);
        let left_file = *left_manifest.files.keys().next().unwrap();
        // The left child materialised: its compaction output is stamped with its own expression.
        left_manifest.files.insert(
            left_file,
            create_file_meta(left_region_id, left_file, Some(left_expr)).into(),
        );

        let right_expr = range_expr("x", 50, 100);
        let mut right_manifest = create_manifest(right_region_id, 1, Some(right_expr), 100, 200);
        let right_file = *right_manifest.files.keys().next().unwrap();
        // The right child still holds the ancestor's object, which records no creator.
        right_manifest.files.insert(
            right_file,
            create_file_meta(right_region_id, right_file, None).into(),
        );

        let mut old_manifests = HashMap::new();
        old_manifests.insert(left_region_id, left_manifest);
        old_manifests.insert(right_region_id, right_manifest);

        let mut new_partition_exprs = HashMap::new();
        new_partition_exprs.insert(merged_region_id, range_expr("x", 0, 100));

        let mut region_mapping = HashMap::new();
        region_mapping.insert(left_region_id, vec![merged_region_id]);
        region_mapping.insert(right_region_id, vec![merged_region_id]);

        let mut remapper = RemapManifest::new(old_manifests, new_partition_exprs, region_mapping);
        let err = remapper.remap_manifests().unwrap_err();
        assert!(
            matches!(&err, Error::ForeignFilePartitionExpr { .. }),
            "unexpected error: {err:?}"
        );
        let message = err.to_string();
        // Only the unstamped file is an offender: the self-stamped one belongs to its region.
        assert!(message.contains(&right_file.to_string()), "{message}");
        assert!(!message.contains(&left_file.to_string()), "{message}");
    }

    #[test]
    fn test_refusal_message_lists_a_bounded_number_of_offenders() {
        // A merge can be refused by many files; the message keeps the per-file detail for the
        // first `MAX_LISTED_OFFENDERS` and only counts the rest, so it cannot grow without bound.
        // The shape has to be one the guard runs on, i.e. a target fed by two sources.
        let old_region_id = RegionId::new(1, 1);
        let sibling_region_id = RegionId::new(1, 3);
        let new_region_id = RegionId::new(1, 2);
        let offender_count = MAX_LISTED_OFFENDERS + 5;

        let mut old_manifest =
            create_manifest(old_region_id, 0, Some(range_expr("x", 0, 50)), 100, 200);
        let mut file_ids = Vec::new();
        for _ in 0..offender_count {
            let file_id = FileId::random();
            old_manifest.files.insert(
                file_id,
                create_file_meta(old_region_id, file_id, Some(range_expr("x", 0, 100))).into(),
            );
            file_ids.push(file_id);
        }
        // The sibling holds only its own file, so every offender comes from the region above.
        let sibling_manifest = create_manifest(
            sibling_region_id,
            1,
            Some(range_expr("x", 50, 100)),
            100,
            200,
        );

        let mut old_manifests = HashMap::new();
        old_manifests.insert(old_region_id, old_manifest);
        old_manifests.insert(sibling_region_id, sibling_manifest);

        let mut new_partition_exprs = HashMap::new();
        new_partition_exprs.insert(new_region_id, range_expr("x", 0, 100));

        let mut region_mapping = HashMap::new();
        region_mapping.insert(old_region_id, vec![new_region_id]);
        region_mapping.insert(sibling_region_id, vec![new_region_id]);

        let mut remapper = RemapManifest::new(old_manifests, new_partition_exprs, region_mapping);
        let message = remapper.remap_manifests().unwrap_err().to_string();
        // The manifest is a map, so which files the message lists is not ordered; what matters
        // is that it lists exactly `MAX_LISTED_OFFENDERS` of them and only counts the rest.
        let listed = file_ids
            .iter()
            .filter(|file_id| message.contains(&file_id.to_string()))
            .count();
        assert_eq!(
            MAX_LISTED_OFFENDERS, listed,
            "the message has to list exactly {MAX_LISTED_OFFENDERS} offenders: {message}"
        );
        assert!(
            message.contains("; and 5 more"),
            "the message has to summarize the rest by count: {message}"
        );
    }

    #[test]
    fn test_simple_merge() {
        // Two regions [0, 50) and [50, 100) merge into one region [0, 100)
        let old_region_id_1 = RegionId::new(1, 1);
        let old_region_id_2 = RegionId::new(1, 2);
        let new_region_id = RegionId::new(1, 3);

        let old_expr_1 = range_expr("x", 0, 50);
        let old_expr_2 = range_expr("x", 50, 100);
        let new_expr = range_expr("x", 0, 100);

        let manifest_1 = create_manifest(old_region_id_1, 5, Some(old_expr_1.clone()), 100, 200);
        let manifest_2 = create_manifest(old_region_id_2, 5, Some(old_expr_2.clone()), 150, 250);

        let mut old_manifests = HashMap::new();
        old_manifests.insert(old_region_id_1, manifest_1);
        old_manifests.insert(old_region_id_2, manifest_2);

        let mut new_partition_exprs = HashMap::new();
        new_partition_exprs.insert(new_region_id, new_expr);

        // Direct mapping: both old regions -> same new region
        let mut region_mapping = HashMap::new();
        region_mapping.insert(old_region_id_1, vec![new_region_id]);
        region_mapping.insert(old_region_id_2, vec![new_region_id]);

        let mut remapper = RemapManifest::new(old_manifests, new_partition_exprs, region_mapping);

        let result = remapper.remap_manifests().unwrap();

        // New region should have all 10 files
        assert_eq!(result.new_manifests.len(), 1);
        assert_eq!(result.new_manifests[&new_region_id].files.len(), 10);
        assert_eq!(result.stats.total_file_refs, 10);
        assert_eq!(result.stats.unique_files, 10);
        assert!(result.stats.empty_regions.is_empty());

        // Verify metadata falls back to defaults when no prior manifest exists for the region
        let new_manifest = &result.new_manifests[&new_region_id];
        assert_eq!(new_manifest.flushed_entry_id, 0);
        assert_eq!(new_manifest.flushed_sequence, 0);
        assert_eq!(new_manifest.manifest_version, 0);
        assert_eq!(new_manifest.truncated_entry_id, None);
        assert_eq!(new_manifest.compaction_time_window, None);
    }

    #[test]
    fn test_metadata_preserved_for_existing_region() {
        // Test that metadata is preserved when a previous manifest exists for the same region id
        let old_region_id_1 = RegionId::new(1, 1);
        let old_region_id_2 = RegionId::new(1, 2);
        let old_region_id_3 = RegionId::new(1, 3);
        let new_region_id = RegionId::new(1, 4);

        let new_expr = range_expr("x", 0, 100);

        let mut manifest_1 = create_manifest(old_region_id_1, 2, None, 10, 20);
        manifest_1.truncated_entry_id = Some(5);
        manifest_1.compaction_time_window = Some(Duration::from_secs(3600));

        let mut manifest_2 = create_manifest(old_region_id_2, 2, None, 25, 15); // Higher entry_id, lower sequence
        manifest_2.truncated_entry_id = Some(20);
        manifest_2.compaction_time_window = Some(Duration::from_secs(7200)); // Larger window

        let manifest_3 = create_manifest(old_region_id_3, 2, None, 15, 30); // Lower entry_id, higher sequence
        let mut previous_manifest = create_manifest(new_region_id, 0, None, 200, 300);
        previous_manifest.truncated_entry_id = Some(40);
        previous_manifest.compaction_time_window = Some(Duration::from_secs(1800));
        previous_manifest.manifest_version = 7;
        let expected_flushed_entry_id = previous_manifest.flushed_entry_id;
        let expected_flushed_sequence = previous_manifest.flushed_sequence;
        let expected_truncated_entry_id = previous_manifest.truncated_entry_id;
        let expected_compaction_window = previous_manifest.compaction_time_window;
        let expected_manifest_version = previous_manifest.manifest_version;

        let mut old_manifests = HashMap::new();
        old_manifests.insert(old_region_id_1, manifest_1);
        old_manifests.insert(old_region_id_2, manifest_2);
        old_manifests.insert(old_region_id_3, manifest_3);
        old_manifests.insert(new_region_id, previous_manifest);

        let mut new_partition_exprs = HashMap::new();
        new_partition_exprs.insert(new_region_id, new_expr);

        // Direct mapping: all old regions -> same new region
        let mut region_mapping = HashMap::new();
        region_mapping.insert(old_region_id_1, vec![new_region_id]);
        region_mapping.insert(old_region_id_2, vec![new_region_id]);
        region_mapping.insert(old_region_id_3, vec![new_region_id]);

        let mut remapper = RemapManifest::new(old_manifests, new_partition_exprs, region_mapping);

        let result = remapper.remap_manifests().unwrap();

        let new_manifest = &result.new_manifests[&new_region_id];
        // Should reuse metadata from previous manifest of the same region id
        assert_eq!(new_manifest.flushed_entry_id, expected_flushed_entry_id);
        assert_eq!(new_manifest.flushed_sequence, expected_flushed_sequence);
        assert_eq!(new_manifest.truncated_entry_id, expected_truncated_entry_id);
        assert_eq!(
            new_manifest.compaction_time_window,
            expected_compaction_window
        );
        assert_eq!(new_manifest.manifest_version, expected_manifest_version);
    }

    #[test]
    fn test_file_consistency_check() {
        // Test that duplicate files are verified for consistency
        let old_region_id_1 = RegionId::new(1, 1);
        let old_region_id_2 = RegionId::new(1, 2);
        let new_region_id = RegionId::new(1, 3);

        let new_expr = range_expr("x", 0, 100);

        // Create manifests with same file (overlapping regions)
        let shared_file_id = FileId::random();
        let file_meta = create_file_meta(old_region_id_1, shared_file_id, None);

        let mut manifest_1 = create_manifest(old_region_id_1, 0, None, 100, 200);
        manifest_1
            .files
            .insert(shared_file_id, file_meta.clone().into());

        let mut manifest_2 = create_manifest(old_region_id_2, 0, None, 100, 200);
        manifest_2.files.insert(shared_file_id, file_meta.into());

        let mut old_manifests = HashMap::new();
        old_manifests.insert(old_region_id_1, manifest_1);
        old_manifests.insert(old_region_id_2, manifest_2);

        let mut new_partition_exprs = HashMap::new();
        new_partition_exprs.insert(new_region_id, new_expr);

        // Direct mapping: both old regions -> same new region
        let mut region_mapping = HashMap::new();
        region_mapping.insert(old_region_id_1, vec![new_region_id]);
        region_mapping.insert(old_region_id_2, vec![new_region_id]);

        let mut remapper = RemapManifest::new(old_manifests, new_partition_exprs, region_mapping);

        let result = remapper.remap_manifests().unwrap();

        // Should succeed - same file is consistent
        assert_eq!(result.new_manifests[&new_region_id].files.len(), 1);
        assert_eq!(result.stats.total_file_refs, 1);
        assert_eq!(result.stats.unique_files, 1);
    }

    #[test]
    fn test_empty_regions() {
        // Test that empty regions are detected and logged (but not an error)
        let old_region_id = RegionId::new(1, 1);
        let new_region_id_1 = RegionId::new(1, 2);
        let new_region_id_2 = RegionId::new(1, 3);

        let old_expr = range_expr("x", 0, 50);
        let new_expr_1 = range_expr("x", 0, 50);
        let new_expr_2 = range_expr("x", 100, 200); // No overlap with old region

        let old_manifest = create_manifest(old_region_id, 5, Some(old_expr.clone()), 100, 200);

        let mut old_manifests = HashMap::new();
        old_manifests.insert(old_region_id, old_manifest);

        let mut new_partition_exprs = HashMap::new();
        new_partition_exprs.insert(new_region_id_1, new_expr_1);
        new_partition_exprs.insert(new_region_id_2, new_expr_2);

        // Direct mapping: old region -> new region 1 only (region 2 is isolated)
        let mut region_mapping = HashMap::new();
        region_mapping.insert(old_region_id, vec![new_region_id_1]);

        let mut remapper = RemapManifest::new(old_manifests, new_partition_exprs, region_mapping);

        let result = remapper.remap_manifests().unwrap();

        // Region 2 should be empty
        assert_eq!(result.new_manifests[&new_region_id_1].files.len(), 5);
        assert_eq!(result.new_manifests[&new_region_id_2].files.len(), 0);
        assert_eq!(result.stats.empty_regions, vec![new_region_id_2]);
    }

    #[test]
    fn test_n_to_m_complex_repartition() {
        // Test complex N-to-M transition: 2 old regions -> 3 new regions
        let old_region_1 = RegionId::new(1, 1);
        let old_region_2 = RegionId::new(1, 2);
        let new_region_1 = RegionId::new(1, 3);
        let new_region_2 = RegionId::new(1, 4);
        let new_region_3 = RegionId::new(1, 5);

        // Old: [0, 100), [100, 200)
        // New: [0, 50), [50, 150), [150, 250)
        let old_expr_1 = range_expr("u", 0, 100);
        let old_expr_2 = range_expr("u", 100, 200);
        let new_expr_1 = range_expr("u", 0, 50);
        let new_expr_2 = range_expr("u", 50, 150);
        let new_expr_3 = range_expr("u", 150, 250);

        let manifest_1 = create_manifest(old_region_1, 3, Some(old_expr_1.clone()), 100, 200);
        let manifest_2 = create_manifest(old_region_2, 4, Some(old_expr_2.clone()), 150, 250);

        let mut old_manifests = HashMap::new();
        old_manifests.insert(old_region_1, manifest_1);
        old_manifests.insert(old_region_2, manifest_2);

        let mut new_partition_exprs = HashMap::new();
        new_partition_exprs.insert(new_region_1, new_expr_1);
        new_partition_exprs.insert(new_region_2, new_expr_2);
        new_partition_exprs.insert(new_region_3, new_expr_3);

        // Direct mapping:
        // old region 1 -> new regions 1, 2
        // old region 2 -> new regions 2, 3
        let mut region_mapping = HashMap::new();
        region_mapping.insert(old_region_1, vec![new_region_1, new_region_2]);
        region_mapping.insert(old_region_2, vec![new_region_2, new_region_3]);

        let mut remapper = RemapManifest::new(old_manifests, new_partition_exprs, region_mapping);

        let result = remapper.remap_manifests().unwrap();

        assert_eq!(result.new_manifests.len(), 3);
        assert_eq!(result.new_manifests[&new_region_1].files.len(), 3);
        assert_eq!(result.new_manifests[&new_region_2].files.len(), 7); // 3 + 4
        assert_eq!(result.new_manifests[&new_region_3].files.len(), 4);
        assert_eq!(result.stats.total_file_refs, 14); // 3 + 7 + 4
        assert_eq!(result.stats.unique_files, 7); // 3 + 4 unique
    }
}
