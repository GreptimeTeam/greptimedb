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
use store_api::storage::{FileId, FileRef, FileRefsManifest, IndexVersion, RegionId};

use crate::error::Result;
use crate::metrics::GC_REF_FILE_CNT;
use crate::region::MitoRegionRef;
use crate::sst::file::{FileMeta, RegionFileId};

/// Independent logical lifetimes referencing a physical SST.
#[derive(Debug, Clone)]
struct FileReference {
    count: usize,
    /// High-water mark retained until all lifetimes release this file.
    max_index_version: Option<IndexVersion>,
}

/// File references grouped by their physical source region.
#[derive(Debug, Clone, Default)]
struct RegionFileRefs {
    files: HashMap<FileId, FileReference>,
}

/// Manages file-level reader references in one datanode for object-store GC.
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

/// Older GC workers match index references by exact version. Expand file-level protection
/// into the existing wire format, including the no-index reference used by old SST metadata.
/// This is a conservative snapshot, not a reference count for each index version.
fn file_refs(
    region_id: RegionId,
    file_id: FileId,
    max_index_version: Option<IndexVersion>,
) -> impl Iterator<Item = FileRef> {
    std::iter::once(None)
        .chain(
            max_index_version
                .into_iter()
                .flat_map(|max| (0..=max).map(Some)),
        )
        .map(move |version| FileRef::new(region_id, file_id, version))
}

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
        // Copy before expanding so snapshot construction does not hold the registry lock.
        let region_refs = self.files_per_region.get(&region_id)?.clone();
        let refs: HashSet<_> = region_refs
            .files
            .iter()
            .flat_map(|(file_id, reference)| {
                file_refs(region_id, *file_id, reference.max_index_version)
            })
            .collect();
        debug!(
            "Get file refs for region {}, node {:?}, {} files",
            region_id,
            self.node_id,
            region_refs.files.len(),
        );
        Some(refs)
    }

    /// Gets references from open SST lifetimes and related regions' manifests.
    pub(crate) async fn get_snapshot_of_file_refs(
        &self,
        query_regions_for_mem: Vec<MitoRegionRef>,
        dst_region_to_src_regions: Vec<(MitoRegionRef, HashSet<RegionId>)>,
    ) -> Result<FileRefsManifest> {
        let mut ref_files = HashMap::new();
        for region_id in query_regions_for_mem.iter().map(|r| r.region_id()) {
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
                        .extend(file_refs(meta.region_id, meta.file_id, meta.index_version()));
                }
            }
            manifest_version.insert(dst_region.region_id(), manifest.manifest_version);
        }

        for r in &query_regions_for_mem {
            let manifest = r.manifest_ctx.manifest().await;
            // Keep older index versions in the snapshot for older GC workers, which
            // only protect the exact index version present in their manifest.
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

    /// Registers a logical SST lifetime, paired with `remove_file` when that lifetime ends.
    /// Index replacements reuse the reference; independent lifetimes register separately.
    pub(crate) fn register_file(&self, file_meta: &FileMeta) {
        let mut region = self.files_per_region.entry(file_meta.region_id).or_default();
        let file = region.files.entry(file_meta.file_id).or_insert_with(|| {
            GC_REF_FILE_CNT.inc();
            FileReference {
                count: 0,
                max_index_version: None,
            }
        });
        file.count += 1;
        file.max_index_version = file.max_index_version.max(file_meta.index_version());
    }

    /// Extends snapshot protection without registering another reader reference.
    pub(crate) fn update_index_version(
        &self,
        file_id: RegionFileId,
        index_version: Option<IndexVersion>,
    ) {
        if let Some(mut region) = self.files_per_region.get_mut(&file_id.region_id())
            && let Some(file) = region.files.get_mut(&file_id.file_id())
        {
            file.max_index_version = file.max_index_version.max(index_version);
        }
    }

    /// Releases a logical SST reference, regardless of whether the file was retired.
    pub(crate) fn remove_file(&self, file_id: RegionFileId) {
        if let Entry::Occupied(mut region) = self.files_per_region.entry(file_id.region_id()) {
            if let std::collections::hash_map::Entry::Occupied(mut file) =
                region.get_mut().files.entry(file_id.file_id())
            {
                file.get_mut().count -= 1;
                if file.get().count == 0 {
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
    use super::*;
    use crate::sst::file::IndexType;

    #[test]
    fn test_file_references_keep_all_versions_until_last_release() {
        let manager = FileReferenceManager::new(None);
        let meta = FileMeta {
            region_id: RegionId::new(1, 1),
            file_id: FileId::random(),
            ..Default::default()
        };
        manager.register_file(&meta);
        assert_eq!(
            manager.ref_file_set(meta.region_id).unwrap(),
            HashSet::from([FileRef::new(meta.region_id, meta.file_id, None)])
        );
        manager.update_index_version(meta.file_id(), Some(2));
        let updated = FileMeta {
            index_version: 2,
            available_indexes: smallvec::smallvec![IndexType::InvertedIndex],
            ..meta.clone()
        };
        manager.register_file(&updated);
        // A stale independent lifetime must not lower the protected index generation.
        manager.update_index_version(meta.file_id(), Some(0));
        let expected: HashSet<_> = [None, Some(0), Some(1), Some(2)]
            .into_iter()
            .map(|version| FileRef::new(meta.region_id, meta.file_id, version))
            .collect();
        assert_eq!(manager.ref_file_set(meta.region_id).unwrap(), expected);

        manager.remove_file(updated.file_id());
        assert_eq!(manager.ref_file_set(meta.region_id).unwrap(), expected);
        manager.remove_file(meta.file_id());
        assert!(manager.ref_file_set(meta.region_id).is_none());
    }
}
