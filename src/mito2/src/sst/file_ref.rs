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

/// File references for a region.
/// It contains all files referenced by the region.
#[derive(Debug, Clone, Default)]
pub struct RegionFileRefs {
    /// (FileRef, Ref Count) meaning how many FileHandleInner is opened for this file.
    pub files: HashMap<FileRef, usize>,
}

/// Manages all file references in one datanode.
/// It keeps track of which files are referenced and group by table ids.
/// This is useful for ensuring that files are not deleted while they are still in use by any
/// query.
#[derive(Debug)]
pub struct FileReferenceManager {
    /// Datanode id. used to determine tmp ref file name.
    node_id: Option<u64>,
    /// TODO(discord9): use no hash hasher since table id is sequential.
    files_per_region: DashMap<RegionId, RegionFileRefs>,
}

pub type FileReferenceManagerRef = Arc<FileReferenceManager>;

impl FileReferenceManager {
    pub fn new(node_id: Option<u64>) -> Self {
        Self {
            node_id,
            files_per_region: Default::default(),
        }
    }

    fn ref_file_set(&self, region_id: RegionId) -> Option<HashSet<FileRef>> {
        let file_refs = if let Some(file_refs) = self.files_per_region.get(&region_id) {
            file_refs.clone()
        } else {
            // region id not found.
            return None;
        };

        if file_refs.files.is_empty() {
            // still return an empty manifest to indicate no files are referenced.
            // and differentiate from error case where table_id not found.
            return Some(HashSet::new());
        }

        let ref_file_set: HashSet<FileRef> = file_refs.files.keys().cloned().collect();

        debug!(
            "Get file refs for region {}, node {:?}, {} files",
            region_id,
            self.node_id,
            ref_file_set.len(),
        );

        Some(ref_file_set)
    }

    /// Gets all ref files for the given regions, meaning all open FileHandles for those regions
    /// and from related regions' manifests.
    pub(crate) async fn get_snapshot_of_file_refs(
        &self,
        query_regions: Vec<MitoRegionRef>,
        related_regions: Vec<(MitoRegionRef, Vec<RegionId>)>,
    ) -> Result<FileRefsManifest> {
        let mut ref_files = HashMap::new();
        // get from in memory file handles
        for region_id in query_regions.iter().map(|r| r.region_id()) {
            if let Some(files) = self.ref_file_set(region_id) {
                ref_files.insert(region_id, files.into_iter().map(|f| f.file_id).collect());
            }
        }

        let mut manifest_version = HashMap::new();

        for r in &query_regions {
            let manifest = r.manifest_ctx.manifest().await;
            manifest_version.insert(r.region_id(), manifest.manifest_version);
        }

        // get file refs from related regions' manifests
        for (related_region, queries) in &related_regions {
            let queries = queries.iter().cloned().collect::<HashSet<_>>();
            let manifest = related_region.manifest_ctx.manifest().await;
            for meta in manifest.files.values() {
                if queries.contains(&meta.region_id) {
                    ref_files
                        .entry(meta.region_id)
                        .or_insert_with(HashSet::new)
                        .insert(meta.file_id);
                }
            }
            // not sure if related region's manifest version is needed, but record it for now.
            manifest_version.insert(related_region.region_id(), manifest.manifest_version);
        }

        // simply return all ref files, no manifest version filtering for now.
        Ok(FileRefsManifest {
            file_refs: ref_files,
            manifest_version,
        })
    }

    /// Adds a new file reference.
    /// Also records the access layer for the table if not exists.
    /// The access layer will be used to upload ref file to object storage.
    pub fn add_file(&self, file_meta: &FileMeta) {
        let region_id = file_meta.region_id;
        let mut is_new = false;
        {
            let file_ref = FileRef::new(file_meta.region_id, file_meta.file_id);
            self.files_per_region
                .entry(region_id)
                .and_modify(|refs| {
                    refs.files
                        .entry(file_ref.clone())
                        .and_modify(|count| *count += 1)
                        .or_insert_with(|| {
                            is_new = true;
                            1
                        });
                })
                .or_insert_with(|| RegionFileRefs {
                    files: HashMap::from_iter([(file_ref, 1)]),
                });
        }
        if is_new {
            GC_REF_FILE_CNT.inc();
        }
    }

    /// Removes a file reference.
    /// If the reference count reaches zero, the file reference will be removed from the manager.
    pub fn remove_file(&self, file_meta: &FileMeta) {
        let region_id = file_meta.region_id;
        let file_ref = FileRef::new(region_id, file_meta.file_id);

        let mut remove_table_entry = false;
        let mut remove_file_ref = false;
        let mut file_cnt = 0;

        let region_ref = self.files_per_region.entry(region_id).and_modify(|refs| {
            let entry = refs.files.entry(file_ref.clone()).and_modify(|count| {
                if *count > 0 {
                    *count -= 1;
                }
                if *count == 0 {
                    remove_file_ref = true;
                }
            });
            if let std::collections::hash_map::Entry::Occupied(o) = entry
                && remove_file_ref
            {
                o.remove_entry();
            }

            file_cnt = refs.files.len();

            if refs.files.is_empty() {
                remove_table_entry = true;
            }
        });

        if let Entry::Occupied(o) = region_ref
            && remove_table_entry
        {
            o.remove_entry();
        }
        if remove_file_ref {
            GC_REF_FILE_CNT.dec();
        }
    }

    pub fn node_id(&self) -> Option<u64> {
        self.node_id
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use smallvec::SmallVec;
    use store_api::storage::{FileId, RegionId};

    use super::*;
    use crate::sst::file::{ColumnIndexMetadata, FileMeta, FileTimeRange, IndexType, RegionFileId};

    #[tokio::test]
    async fn test_file_ref_mgr() {
        common_telemetry::init_default_ut_logging();

        let sst_file_id = RegionFileId::new(RegionId::new(0, 0), FileId::random());

        let file_ref_mgr = FileReferenceManager::new(None);

        let file_meta = FileMeta {
            region_id: sst_file_id.region_id(),
            file_id: sst_file_id.file_id(),
            time_range: FileTimeRange::default(),
            level: 0,
            file_size: 4096,
            available_indexes: SmallVec::from_iter([IndexType::InvertedIndex]),
            indexes: vec![ColumnIndexMetadata {
                column_id: 0,
                created_indexes: SmallVec::from_iter([IndexType::InvertedIndex]),
            }],
            index_file_size: 4096,
            index_file_id: None,
            num_rows: 1024,
            num_row_groups: 1,
            sequence: NonZeroU64::new(4096),
            partition_expr: None,
            num_series: 0,
        };

        file_ref_mgr.add_file(&file_meta);

        assert_eq!(
            file_ref_mgr
                .files_per_region
                .get(&file_meta.region_id)
                .unwrap()
                .files,
            HashMap::from_iter([(FileRef::new(file_meta.region_id, file_meta.file_id), 1)])
        );

        file_ref_mgr.add_file(&file_meta);

        let expected_region_ref_manifest =
            HashSet::from_iter([FileRef::new(file_meta.region_id, file_meta.file_id)]);

        assert_eq!(
            file_ref_mgr.ref_file_set(file_meta.region_id).unwrap(),
            expected_region_ref_manifest
        );

        assert_eq!(
            file_ref_mgr
                .files_per_region
                .get(&file_meta.region_id)
                .unwrap()
                .files,
            HashMap::from_iter([(FileRef::new(file_meta.region_id, file_meta.file_id), 2)])
        );

        assert_eq!(
            file_ref_mgr.ref_file_set(file_meta.region_id).unwrap(),
            expected_region_ref_manifest
        );

        file_ref_mgr.remove_file(&file_meta);

        assert_eq!(
            file_ref_mgr
                .files_per_region
                .get(&file_meta.region_id)
                .unwrap()
                .files,
            HashMap::from_iter([(FileRef::new(file_meta.region_id, file_meta.file_id), 1)])
        );

        assert_eq!(
            file_ref_mgr.ref_file_set(file_meta.region_id).unwrap(),
            expected_region_ref_manifest
        );

        file_ref_mgr.remove_file(&file_meta);

        assert!(
            file_ref_mgr
                .files_per_region
                .get(&file_meta.region_id)
                .is_none(),
            "{:?}",
            file_ref_mgr.files_per_region
        );

        assert!(
            file_ref_mgr.ref_file_set(file_meta.region_id).is_none(),
            "{:?}",
            file_ref_mgr.files_per_region
        );
    }
}
