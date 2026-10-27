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

use common_telemetry::debug;
use dashmap::{DashMap, Entry};
use store_api::storage::{FileRef, FileRefsManifest, RegionId};

use crate::error::Result;
use crate::metrics::GC_REF_FILE_CNT;
use crate::region::MitoRegionRef;
use crate::sst::file::FileMeta;

/// File references grouped by their physical source region.
#[derive(Debug, Clone, Default)]
struct RegionFileRefs {
    /// Counts independent metadata views of each physical file and index version.
    files: HashMap<FileRef, usize>,
}

/// Tracks reader references to SST metadata views in one datanode for object-store GC.
#[derive(Debug)]
pub struct FileReferenceManager {
    /// Datanode id. used to determine tmp ref file name.
    node_id: Option<u64>,
    files_per_region: DashMap<RegionId, RegionFileRefs>,
    /// Whether global GC is enabled.
    /// This is only meaningful when using object store (not local filesystem).
    gc_enabled: bool,
}

pub type FileReferenceManagerRef = Arc<FileReferenceManager>;

impl FileReferenceManager {
    pub fn new(node_id: Option<u64>) -> Self {
        Self {
            node_id,
            files_per_region: Default::default(),
            gc_enabled: false,
        }
    }

    /// Creates a new FileReferenceManager with GC configuration.
    pub fn with_gc_enabled(node_id: Option<u64>, gc_enabled: bool) -> Self {
        Self {
            node_id,
            files_per_region: Default::default(),
            gc_enabled,
        }
    }

    /// Returns whether global GC is enabled.
    pub fn is_gc_enabled(&self) -> bool {
        self.gc_enabled
    }

    fn ref_file_set(&self, region_id: RegionId) -> Option<HashSet<FileRef>> {
        let region_refs = self.files_per_region.get(&region_id)?.clone();
        let refs: HashSet<_> = region_refs.files.keys().cloned().collect();
        debug!(
            "Get file refs for region {}, node {:?}, {} files",
            region_id,
            self.node_id,
            region_refs.files.len(),
        );
        Some(refs)
    }

    /// Gets references from live SST metadata views and related regions' manifests.
    pub(crate) async fn get_snapshot_of_file_refs(
        &self,
        query_regions_for_mem: Vec<MitoRegionRef>,
        dst_region_to_src_regions: Vec<(MitoRegionRef, HashSet<RegionId>)>,
    ) -> Result<FileRefsManifest> {
        // Imported SSTs are tracked under their source region even if that region is
        // not open on this node. Capture their old readers before reading manifests.
        let region_ids: HashSet<_> = query_regions_for_mem
            .iter()
            .map(|r| r.region_id())
            .chain(
                dst_region_to_src_regions
                    .iter()
                    .flat_map(|(_, sources)| sources.iter().copied()),
            )
            .collect();
        let mut ref_files = HashMap::new();
        for region_id in region_ids {
            if let Some(files) = self.ref_file_set(region_id) {
                ref_files.insert(region_id, files);
            }
        }

        let mut manifest_version = HashMap::new();
        let mut cross_region_refs = HashMap::new();

        for (dst_region, src_regions) in &dst_region_to_src_regions {
            let manifest = dst_region.manifest_ctx.manifest().await;
            for meta in manifest.files.values() {
                if src_regions.contains(&meta.region_id) {
                    cross_region_refs
                        .entry(meta.region_id)
                        .or_insert_with(HashSet::new)
                        .insert(dst_region.region_id());
                    // GC and repartition hold region read locks, so staging manifests
                    // cannot change ownership while these references are collected.
                    ref_files
                        .entry(meta.region_id)
                        .or_insert_with(HashSet::new)
                        .insert(FileRef::new(
                            meta.region_id,
                            meta.file_id,
                            meta.index_version(),
                        ));
                }
            }
            manifest_version.insert(dst_region.region_id(), manifest.manifest_version);
        }

        for r in &query_regions_for_mem {
            let manifest = r.manifest_ctx.manifest().await;
            // The manifest only protects its current index generation; older readers
            // still need their exact-version temporary references.
            ref_files.entry(r.region_id()).and_modify(|refs| {
                refs.retain(|f| {
                    !manifest
                        .files
                        .get(&f.file_id)
                        .is_some_and(|meta| meta.index_version() == f.index_version)
                });
            });
            manifest_version.insert(r.region_id(), manifest.manifest_version);
        }

        Ok(FileRefsManifest {
            file_refs: ref_files,
            manifest_version,
            cross_region_refs,
        })
    }

    /// Registers a metadata view, paired with `remove_file` when its last reader leaves.
    pub(crate) fn register_file(&self, file_meta: &FileMeta) {
        let mut region = self
            .files_per_region
            .entry(file_meta.region_id)
            .or_default();
        let file_ref = FileRef::new(
            file_meta.region_id,
            file_meta.file_id,
            file_meta.index_version(),
        );
        let count = region.files.entry(file_ref).or_insert_with(|| {
            GC_REF_FILE_CNT.inc();
            0
        });
        *count += 1;
    }

    /// Releases a metadata view's reference, regardless of whether the SST was retired.
    pub(crate) fn remove_file(&self, file_meta: &FileMeta) {
        let file_ref = FileRef::new(
            file_meta.region_id,
            file_meta.file_id,
            file_meta.index_version(),
        );
        if let Entry::Occupied(mut region) = self.files_per_region.entry(file_meta.region_id) {
            if let std::collections::hash_map::Entry::Occupied(mut file) =
                region.get_mut().files.entry(file_ref)
            {
                *file.get_mut() -= 1;
                if *file.get() == 0 {
                    file.remove();
                    GC_REF_FILE_CNT.dec();
                }
            }
            if region.get().files.is_empty() {
                region.remove();
            }
        }
    }

    pub fn node_id(&self) -> Option<u64> {
        self.node_id
    }
}

#[cfg(test)]
mod tests {
    use store_api::storage::FileId;

    use super::*;
    use crate::sst::file::IndexType;

    #[test]
    fn test_index_view_references_release_generations_independently() {
        let manager = FileReferenceManager::new(None);
        let meta = FileMeta {
            region_id: RegionId::new(1, 1),
            file_id: FileId::random(),
            available_indexes: smallvec::smallvec![IndexType::InvertedIndex],
            ..Default::default()
        };
        let updated = FileMeta {
            index_version: 2,
            ..meta.clone()
        };
        manager.register_file(&meta);
        manager.register_file(&meta);
        manager.register_file(&updated);
        let expected: HashSet<_> = [0, 2]
            .into_iter()
            .map(|version| FileRef::new(meta.region_id, meta.file_id, Some(version)))
            .collect();
        assert_eq!(manager.ref_file_set(meta.region_id).unwrap(), expected);

        // Independent readers of the same index version must both release it.
        manager.remove_file(&meta);
        assert_eq!(manager.ref_file_set(meta.region_id).unwrap(), expected);
        manager.remove_file(&meta);
        assert_eq!(
            manager.ref_file_set(meta.region_id).unwrap(),
            HashSet::from([FileRef::new(meta.region_id, meta.file_id, Some(2))])
        );
        manager.remove_file(&updated);
        assert!(manager.ref_file_set(meta.region_id).is_none());
    }
}
