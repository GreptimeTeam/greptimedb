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
use store_api::storage::RegionId;

use crate::error;
pub use crate::error::{Error, Result};
use crate::manifest::action::{RegionManifest, RemovedFilesRecord};

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
            new_metadata.partition_expr = Some(new_partition_expr);

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
        for (file_id, file_meta) in &source_manifest.files {
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
            available_indexes: SmallVec::new(),
            index_file_size: 0,
            index_file_id: None,
            num_rows: 100,
            num_row_groups: 1,
            sequence: NonZeroU64::new(1),
            partition_expr,
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
            files.insert(file_id, file_meta);
        }

        RegionManifest {
            metadata: create_region_metadata(region_id),
            files,
            removed_files: RemovedFilesRecord::default(),
            flushed_entry_id,
            flushed_sequence,
            manifest_version: 1,
            truncated_entry_id: None,
            compaction_time_window: None,
            committed_sequence: None,
            sst_format: FormatType::PrimaryKey,
        }
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
        manifest_1.files.insert(shared_file_id, file_meta.clone());

        let mut manifest_2 = create_manifest(old_region_id_2, 0, None, 100, 200);
        manifest_2.files.insert(shared_file_id, file_meta);

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
