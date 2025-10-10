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

//! Manifest remapping for repartition operations.
//!
//! This module provides functionality to remap file references from old region
//! manifests to new region manifests during repartition.
//!
//! # Overview
//!
//! During repartition, the partition rules change and regions may be split, merged,
//! or redistributed. The manifest remapping process takes old region manifests with
//! their partition rules and new partition rules as input, then generates new manifests
//! for each new region.
//!
//! # Key Principle
//!
//! Files are references (via FileId UUIDs), not physical copies. We remap which regions
//! reference which files, without moving data on disk. The `FileMeta.partition_expr` field
//! is immutable and records the partition rule when the file was created - only the
//! region's partition expression changes.
//!
//! # Example
//!
//! ```rust,ignore
//! use std::collections::HashMap;
//! use mito2::remap_manifest::RemapManifest;
//!
//! // Define which old regions map to which new regions
//! let mut region_mapping = HashMap::new();
//! region_mapping.insert(old_region_1, vec![new_region_1, new_region_2]);
//! region_mapping.insert(old_region_2, vec![new_region_2]);
//!
//! let remapper = RemapManifest::new(
//!     old_manifests,
//!     new_partition_exprs,
//!     region_mapping,
//! );
//!
//! let result = remapper.remap_manifests()?;
//! // Process result...
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use partition::expr::PartitionExpr;
use snafu::{OptionExt, Snafu, ensure};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{RegionId, SequenceNumber};

use crate::manifest::action::{RegionManifest, RemovedFilesRecord};
use crate::sst::file::{FileId, FileMeta};
use crate::wal::EntryId;

/// Error types for manifest remapping operations.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Old manifest missing for region {}", region_id))]
    MissingOldManifest { region_id: RegionId },

    #[snafu(display("New manifest missing for region {}", region_id))]
    MissingNewManifest { region_id: RegionId },

    #[snafu(display("File consistency check failed for file {}: {}", file_id, reason))]
    InconsistentFile { file_id: FileId, reason: String },

    #[snafu(display("Files lost during remapping: old={}, new={}", old_count, new_count))]
    FilesLost { old_count: usize, new_count: usize },

    #[snafu(display("No old manifests provided (need at least one for template)"))]
    NoOldManifests,
}

/// Result type for manifest remapping operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Remaps file references from old region manifests to new region manifests.
///
/// This struct takes old regions (with their manifests) and new regions
/// (with their partition rules), and generates new manifests by copying
/// file references according to a direct region-to-region mapping.
///
/// # Guarantees
///
/// * All new regions will have manifests
/// * No files will be lost (total references >= original file count)
/// * File metadata consistency is verified
/// * FileMeta.partition_expr remains unchanged (immutable)
pub struct RemapManifest {
    /// Old region manifests indexed by region ID
    old_manifests: HashMap<RegionId, RegionManifest>,
    /// New partition expressions indexed by region ID
    new_partition_exprs: HashMap<RegionId, PartitionExpr>,
    /// For each old region, which new regions should receive its files
    region_mapping: HashMap<RegionId, Vec<RegionId>>,
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

/// Merged metadata from multiple source regions.
struct MergedMetadata {
    flushed_entry_id: EntryId,
    flushed_sequence: SequenceNumber,
    truncated_entry_id: Option<EntryId>,
    compaction_time_window: Option<Duration>,
}

impl RemapManifest {
    /// Creates a new RemapManifest.
    ///
    /// # Arguments
    /// * `old_manifests` - Current manifests for old regions
    /// * `new_partition_exprs` - Partition expressions for new regions
    /// * `region_mapping` - For each old region ID, list of new region IDs to receive its files
    ///
    /// # Returns
    /// A new RemapManifest instance ready to perform remapping
    pub fn new(
        old_manifests: HashMap<RegionId, RegionManifest>,
        new_partition_exprs: HashMap<RegionId, PartitionExpr>,
        region_mapping: HashMap<RegionId, Vec<RegionId>>,
    ) -> Self {
        Self {
            old_manifests,
            new_partition_exprs,
            region_mapping,
        }
    }

    /// Remaps manifests from old regions to new regions.
    ///
    /// This is the main entry point for manifest remapping. It processes
    /// the subtask, copying files from old regions to new regions based on
    /// partition expression overlaps.
    ///
    /// # Returns
    /// * `Ok(RemapResult)` - Successfully remapped manifests with statistics
    /// * `Err(Error)` - Remapping failed (missing manifests, inconsistent data, etc.)
    ///
    /// # Guarantees
    /// * All new regions will have manifests
    /// * No files will be lost (total references >= original file count)
    /// * File metadata consistency is verified
    /// * FileMeta.partition_expr remains unchanged (immutable)
    pub fn remap_manifests(&self) -> Result<RemapResult> {
        // Step 1: Initialize new manifests
        let mut new_manifests = self.initialize_new_manifests()?;

        // Step 2: Process the subtask - remap files
        self.process_subtask(&mut new_manifests)?;

        // Step 3: Merge and set metadata for all new manifests
        self.finalize_manifests(&mut new_manifests)?;

        // Step 4: Validate and compute statistics
        let stats = self.compute_stats(&new_manifests);
        self.validate_result(&new_manifests, &stats)?;

        Ok(RemapResult {
            new_manifests,
            stats,
        })
    }

    /// Initializes empty manifests for all new regions.
    fn initialize_new_manifests(&self) -> Result<HashMap<RegionId, RegionManifest>> {
        let mut new_manifests = HashMap::new();

        // Create empty manifest for each new region
        for region_id in self.new_partition_exprs.keys() {
            // Derive region metadata from any old manifest as template
            let metadata = self.derive_region_metadata(*region_id)?;

            let manifest = RegionManifest {
                metadata,
                files: HashMap::new(),
                removed_files: RemovedFilesRecord::default(),
                flushed_entry_id: 0,
                flushed_sequence: 0,
                manifest_version: 0,
                truncated_entry_id: None,
                compaction_time_window: None,
            };

            new_manifests.insert(*region_id, manifest);
        }

        Ok(new_manifests)
    }

    /// Derives region metadata for a new region from old manifests.
    fn derive_region_metadata(&self, new_region_id: RegionId) -> Result<RegionMetadataRef> {
        // Get any old manifest as template (they all share the same table schema)
        let template_manifest = self
            .old_manifests
            .values()
            .next()
            .context(NoOldManifestsSnafu)?;

        // Clone metadata and update region_id
        let mut metadata = (*template_manifest.metadata).clone();
        metadata.region_id = new_region_id;

        Ok(Arc::new(metadata))
    }

    /// Remaps files from old regions to new regions according to the region mapping.
    fn process_subtask(&self, new_manifests: &mut HashMap<RegionId, RegionManifest>) -> Result<()> {
        // For each old region and its target new regions
        for (&from_region_id, target_region_ids) in &self.region_mapping {
            // Get old manifest
            let from_manifest =
                self.old_manifests
                    .get(&from_region_id)
                    .context(MissingOldManifestSnafu {
                        region_id: from_region_id,
                    })?;

            // Copy files to all target new regions
            for &to_region_id in target_region_ids {
                self.copy_files_to_region(
                    from_manifest,
                    from_region_id,
                    new_manifests.get_mut(&to_region_id).unwrap(),
                    to_region_id,
                )?;
            }
        }

        Ok(())
    }

    /// Copies files from a source region to a target region.
    fn copy_files_to_region(
        &self,
        source_manifest: &RegionManifest,
        _source_region_id: RegionId,
        target_manifest: &mut RegionManifest,
        target_region_id: RegionId,
    ) -> Result<()> {
        for (file_id, file_meta) in &source_manifest.files {
            // Clone file metadata with updated region_id
            let mut new_file_meta = file_meta.clone();

            // CRITICAL: Update the region_id to point to new region
            // The file_id (UUID) stays the same - this is a reference remap
            new_file_meta.region_id = target_region_id;

            // IMPORTANT: Do NOT update partition_expr
            // partition_expr is immutable and records the rule when file was created
            // Only the region's partition expression (in region metadata) changes

            // Insert or merge into target manifest
            // Same file might be added from multiple overlapping old regions
            use std::collections::hash_map::Entry;
            match target_manifest.files.entry(*file_id) {
                Entry::Vacant(e) => {
                    e.insert(new_file_meta);
                }
                Entry::Occupied(e) => {
                    // File already exists - verify it's the same physical file
                    self.verify_file_consistency(e.get(), &new_file_meta)?;
                }
            }
        }

        Ok(())
    }

    /// Verifies that two file metadata entries are consistent.
    fn verify_file_consistency(&self, existing: &FileMeta, new: &FileMeta) -> Result<()> {
        // When the same file appears from multiple overlapping old regions,
        // verify they are actually the same physical file

        ensure!(
            existing.file_id == new.file_id,
            InconsistentFileSnafu {
                file_id: existing.file_id,
                reason: "file_id mismatch",
            }
        );

        ensure!(
            existing.time_range == new.time_range,
            InconsistentFileSnafu {
                file_id: existing.file_id,
                reason: "time_range mismatch",
            }
        );

        ensure!(
            existing.level == new.level,
            InconsistentFileSnafu {
                file_id: existing.file_id,
                reason: "level mismatch",
            }
        );

        ensure!(
            existing.file_size == new.file_size,
            InconsistentFileSnafu {
                file_id: existing.file_id,
                reason: "file_size mismatch",
            }
        );

        // region_id is expected to differ (that's what we're remapping)
        // partition_expr should be the same (it's immutable)

        Ok(())
    }

    /// Finalizes manifests by merging metadata from source regions.
    fn finalize_manifests(
        &self,
        new_manifests: &mut HashMap<RegionId, RegionManifest>,
    ) -> Result<()> {
        for (region_id, manifest) in new_manifests.iter_mut() {
            // Find all old regions that contributed to this new region
            let source_region_ids = self.find_source_regions(region_id)?;

            // Merge metadata from all source regions
            let merged_metadata = self.merge_manifest_metadata(&source_region_ids)?;

            // Apply merged metadata
            manifest.flushed_entry_id = merged_metadata.flushed_entry_id;
            manifest.flushed_sequence = merged_metadata.flushed_sequence;
            manifest.manifest_version = 0; // Start fresh for new manifest
            manifest.truncated_entry_id = merged_metadata.truncated_entry_id;
            manifest.compaction_time_window = merged_metadata.compaction_time_window;

            // removed_files are tracked by old manifests, don't copy
            manifest.removed_files = RemovedFilesRecord::default();
        }

        Ok(())
    }

    /// Merges metadata from multiple source regions.
    fn merge_manifest_metadata(&self, source_region_ids: &[RegionId]) -> Result<MergedMetadata> {
        let mut max_flushed_entry_id = 0;
        let mut max_flushed_sequence = 0;
        let mut max_truncated_entry_id = None;
        let mut compaction_time_window = None;

        for &region_id in source_region_ids {
            if let Some(manifest) = self.old_manifests.get(&region_id) {
                // Take maximum of all entry IDs and sequences
                max_flushed_entry_id = max_flushed_entry_id.max(manifest.flushed_entry_id);
                max_flushed_sequence = max_flushed_sequence.max(manifest.flushed_sequence);

                if let Some(tid) = manifest.truncated_entry_id {
                    max_truncated_entry_id = Some(max_truncated_entry_id.unwrap_or(0).max(tid));
                }

                // Use the largest compaction window (conservative)
                if let Some(window) = manifest.compaction_time_window {
                    compaction_time_window =
                        Some(compaction_time_window.map_or(window, |w: Duration| w.max(window)));
                }
            }
        }

        Ok(MergedMetadata {
            flushed_entry_id: max_flushed_entry_id,
            flushed_sequence: max_flushed_sequence,
            truncated_entry_id: max_truncated_entry_id,
            compaction_time_window,
        })
    }

    /// Finds all source regions that contributed to a target region.
    fn find_source_regions(&self, target_region_id: &RegionId) -> Result<Vec<RegionId>> {
        let mut sources = Vec::new();

        // Find all old regions that map to this new region
        for (&source_region_id, target_regions) in &self.region_mapping {
            if target_regions.contains(target_region_id) {
                sources.push(source_region_id);
            }
        }

        Ok(sources)
    }

    /// Computes statistics about the remapping.
    fn compute_stats(&self, new_manifests: &HashMap<RegionId, RegionManifest>) -> RemapStats {
        let mut files_per_region = HashMap::new();
        let mut total_file_refs = 0;
        let mut empty_regions = Vec::new();
        let mut all_files = HashSet::new();

        for (&region_id, manifest) in new_manifests {
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
    fn validate_result(
        &self,
        new_manifests: &HashMap<RegionId, RegionManifest>,
        stats: &RemapStats,
    ) -> Result<()> {
        // 1. Verify all new regions have manifests
        for region_id in self.new_partition_exprs.keys() {
            ensure!(
                new_manifests.contains_key(region_id),
                MissingNewManifestSnafu {
                    region_id: *region_id
                }
            );
        }

        // 2. Verify no unique files were lost
        // Count unique files in old manifests (files may be duplicated across regions)
        let mut old_unique_files = HashSet::new();
        for manifest in self.old_manifests.values() {
            for file_id in manifest.files.keys() {
                old_unique_files.insert(*file_id);
            }
        }

        ensure!(
            stats.unique_files >= old_unique_files.len(),
            FilesLostSnafu {
                old_count: old_unique_files.len(),
                new_count: stats.unique_files,
            }
        );

        // 3. Log warning about empty regions (not an error)
        if !stats.empty_regions.is_empty() {
            common_telemetry::warn!(
                "Repartition resulted in {} empty regions: {:?}",
                stats.empty_regions.len(),
                stats.empty_regions
            );
        }

        Ok(())
    }
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
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::RegionId;

    use super::*;
    use crate::manifest::action::RegionManifest;
    use crate::sst::file::{FileId, FileMeta, FileTimeRange};

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

        let remapper = RemapManifest::new(old_manifests, new_partition_exprs, region_mapping);

        let result = remapper.remap_manifests().unwrap();

        // Both new regions should have all 10 files
        assert_eq!(result.new_manifests.len(), 2);
        assert_eq!(result.new_manifests[&new_region_id_1].files.len(), 10);
        assert_eq!(result.new_manifests[&new_region_id_2].files.len(), 10);
        assert_eq!(result.stats.total_file_refs, 20);
        assert_eq!(result.stats.unique_files, 10);
        assert!(result.stats.empty_regions.is_empty());

        // Verify all files have correct region_id
        for file_meta in result.new_manifests[&new_region_id_1].files.values() {
            assert_eq!(file_meta.region_id, new_region_id_1);
        }
        for file_meta in result.new_manifests[&new_region_id_2].files.values() {
            assert_eq!(file_meta.region_id, new_region_id_2);
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

        let remapper = RemapManifest::new(old_manifests, new_partition_exprs, region_mapping);

        let result = remapper.remap_manifests().unwrap();

        // New region should have all 10 files
        assert_eq!(result.new_manifests.len(), 1);
        assert_eq!(result.new_manifests[&new_region_id].files.len(), 10);
        assert_eq!(result.stats.total_file_refs, 10);
        assert_eq!(result.stats.unique_files, 10);
        assert!(result.stats.empty_regions.is_empty());

        // Verify metadata was merged (should take max values)
        let new_manifest = &result.new_manifests[&new_region_id];
        assert_eq!(new_manifest.flushed_entry_id, 150);
        assert_eq!(new_manifest.flushed_sequence, 250);
    }

    #[test]
    fn test_partition_expr_immutability() {
        // Verify that FileMeta.partition_expr is NOT modified during remapping
        let old_region_id = RegionId::new(1, 1);
        let new_region_id = RegionId::new(1, 2);

        let new_expr = range_expr("x", 0, 100);

        // Create files with partition expressions
        let file_partition_expr = range_expr("x", 10, 20);
        let old_manifest = create_manifest(
            old_region_id,
            5,
            Some(file_partition_expr.clone()),
            100,
            200,
        );

        let mut old_manifests = HashMap::new();
        old_manifests.insert(old_region_id, old_manifest);

        let mut new_partition_exprs = HashMap::new();
        new_partition_exprs.insert(new_region_id, new_expr);

        // Direct mapping: old region -> new region
        let mut region_mapping = HashMap::new();
        region_mapping.insert(old_region_id, vec![new_region_id]);

        let remapper = RemapManifest::new(old_manifests, new_partition_exprs, region_mapping);

        let result = remapper.remap_manifests().unwrap();

        // CRITICAL: Verify partition_expr remains unchanged
        for file_meta in result.new_manifests[&new_region_id].files.values() {
            assert_eq!(file_meta.partition_expr, Some(file_partition_expr.clone()));
            assert_eq!(file_meta.region_id, new_region_id); // Only region_id should change
        }
    }

    #[test]
    fn test_metadata_merge_max_values() {
        // Test that metadata merging takes maximum values from all sources
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

        let mut old_manifests = HashMap::new();
        old_manifests.insert(old_region_id_1, manifest_1);
        old_manifests.insert(old_region_id_2, manifest_2);
        old_manifests.insert(old_region_id_3, manifest_3);

        let mut new_partition_exprs = HashMap::new();
        new_partition_exprs.insert(new_region_id, new_expr);

        // Direct mapping: all old regions -> same new region
        let mut region_mapping = HashMap::new();
        region_mapping.insert(old_region_id_1, vec![new_region_id]);
        region_mapping.insert(old_region_id_2, vec![new_region_id]);
        region_mapping.insert(old_region_id_3, vec![new_region_id]);

        let remapper = RemapManifest::new(old_manifests, new_partition_exprs, region_mapping);

        let result = remapper.remap_manifests().unwrap();

        let new_manifest = &result.new_manifests[&new_region_id];
        // Should take max of all values
        assert_eq!(new_manifest.flushed_entry_id, 25); // max(10, 25, 15)
        assert_eq!(new_manifest.flushed_sequence, 30); // max(20, 15, 30)
        assert_eq!(new_manifest.truncated_entry_id, Some(20)); // max(5, 20, None)
        assert_eq!(
            new_manifest.compaction_time_window,
            Some(Duration::from_secs(7200))
        ); // max window
        assert_eq!(new_manifest.manifest_version, 0); // Starts fresh
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

        let remapper = RemapManifest::new(old_manifests, new_partition_exprs, region_mapping);

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

        let remapper = RemapManifest::new(old_manifests, new_partition_exprs, region_mapping);

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

        let remapper = RemapManifest::new(old_manifests, new_partition_exprs, region_mapping);

        let result = remapper.remap_manifests().unwrap();

        assert_eq!(result.new_manifests.len(), 3);
        assert_eq!(result.new_manifests[&new_region_1].files.len(), 3);
        assert_eq!(result.new_manifests[&new_region_2].files.len(), 7); // 3 + 4
        assert_eq!(result.new_manifests[&new_region_3].files.len(), 4);
        assert_eq!(result.stats.total_file_refs, 14); // 3 + 7 + 4
        assert_eq!(result.stats.unique_files, 7); // 3 + 4 unique
    }

    #[test]
    fn test_no_old_manifests_error() {
        // Should fail if no old manifests provided
        let new_region_id = RegionId::new(1, 1);
        let new_expr = range_expr("x", 0, 100);

        let old_manifests = HashMap::new();

        let mut new_partition_exprs = HashMap::new();
        new_partition_exprs.insert(new_region_id, new_expr);

        let region_mapping = HashMap::new();

        let remapper = RemapManifest::new(old_manifests, new_partition_exprs, region_mapping);

        let result = remapper.remap_manifests();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NoOldManifests));
    }
}
