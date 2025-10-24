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
use serde::{Deserialize, Serialize};
use store_api::ManifestVersion;
use store_api::storage::{FileId, RegionId, TableId};

use crate::error::Result;
use crate::metrics::GC_REF_FILE_CNT;
use crate::region::RegionMapRef;
use crate::sst::file::FileMeta;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FileRef {
    pub region_id: RegionId,
    pub file_id: FileId,
}

impl FileRef {
    pub fn new(region_id: RegionId, file_id: FileId) -> Self {
        Self { region_id, file_id }
    }
}

/// File references for a table.
/// It contains all files referenced by the table.
#[derive(Debug, Clone, Default)]
pub struct TableFileRefs {
    /// (FileRef, Ref Count) meaning how many FileHandleInner is opened for this file.
    pub files: HashMap<FileRef, usize>,
}

/// Manages all file references in one datanode.
/// It keeps track of which files are referenced and group by table ids.
/// And periodically update the references to tmp file in object storage.
/// This is useful for ensuring that files are not deleted while they are still in use by any
/// query.
#[derive(Debug)]
pub struct FileReferenceManager {
    /// Datanode id. used to determine tmp ref file name.
    node_id: Option<u64>,
    /// TODO(discord9): use no hash hasher since table id is sequential.
    files_per_table: DashMap<TableId, TableFileRefs>,
}

pub type FileReferenceManagerRef = Arc<FileReferenceManager>;

/// The tmp file uploaded to object storage to record one table's file references.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableFileRefsManifest {
    pub file_refs: HashSet<FileRef>,
    /// Manifest version when this manifest is read for it's files
    pub manifest_version: HashMap<RegionId, ManifestVersion>,
}

impl FileReferenceManager {
    pub fn new(node_id: Option<u64>) -> Self {
        Self {
            node_id,
            files_per_table: Default::default(),
        }
    }

    fn ref_file_set(&self, table_id: TableId) -> Option<HashSet<FileRef>> {
        let file_refs = if let Some(file_refs) = self.files_per_table.get(&table_id) {
            file_refs.clone()
        } else {
            // still return an empty manifest to indicate no files are referenced.
            // and differentiate from error case where table_id not found.
            return None;
        };

        if file_refs.files.is_empty() {
            // still return an empty manifest to indicate no files are referenced.
            // and differentiate from error case where table_id not found.
            return Some(HashSet::new());
        }

        let ref_file_set: HashSet<FileRef> = file_refs.files.keys().cloned().collect();

        debug!(
            "Get file refs for table {}, node {:?}, {} files",
            table_id,
            self.node_id,
            ref_file_set.len(),
        );

        Some(ref_file_set)
    }

    /// Gets all ref files for the given table id, excluding those already in region manifest.
    ///
    /// It's safe if manifest version became outdated when gc worker is called, as gc worker will check the changes between those two versions and act accordingly to make sure to get the real truly tmp ref file sets at the time of old manifest version.
    ///
    /// TODO(discord9): Since query will only possible refer to files in latest manifest when it's started, the only true risks is files removed from manifest between old version(when reading refs) and new version(at gc worker), so in case of having outdated manifest version, gc worker should make sure not to delete those files(Until next gc round which will use the latest manifest version and handle those files normally).
    /// or perhaps using a two-phase commit style process where it proposes a set of files for deletion and then verifies no new references have appeared before committing the delete.
    ///
    /// gc worker could do this:
    /// 1. if can get the files that got removed from old manifest to new manifest, then shouldn't delete those files even if they are not in tmp ref file, other files can be normally handled(deleted if not in use, otherwise keep)
    ///    and report back allow next gc round to handle those files with newer tmp ref file sets.
    /// 2. if can't get the files that got removed from old manifest to new manifest(possible if just did a checkpoint),
    ///    then can do nothing as can't sure whether a file is truly unused or just tmp ref file sets haven't report it, so need to report back and try next gc round to handle those files with newer tmp ref file sets.
    ///
    #[allow(unused)]
    pub(crate) async fn get_snapshot_of_unmanifested_refs(
        &self,
        table_id: TableId,
        region_map: &RegionMapRef,
    ) -> Result<TableFileRefsManifest> {
        let Some(ref_files) = self.ref_file_set(table_id) else {
            return Ok(Default::default());
        };
        let region_list = region_map.list_regions();
        let table_regions = region_list
            .iter()
            .filter(|r| r.region_id().table_id() == table_id)
            .collect::<Vec<_>>();

        let mut in_manifest_files = HashSet::new();
        let mut manifest_version = HashMap::new();

        for r in &table_regions {
            let manifest = r.manifest_ctx.manifest().await;
            let files = manifest.files.keys().cloned().collect::<Vec<_>>();
            in_manifest_files.extend(files);
            manifest_version.insert(r.region_id(), manifest.manifest_version);
        }

        let ref_files_excluding_in_manifest = ref_files
            .iter()
            .filter(|f| !in_manifest_files.contains(&f.file_id))
            .cloned()
            .collect::<HashSet<_>>();

        Ok(TableFileRefsManifest {
            file_refs: ref_files_excluding_in_manifest,
            manifest_version,
        })
    }

    /// Adds a new file reference.
    /// Also records the access layer for the table if not exists.
    /// The access layer will be used to upload ref file to object storage.
    pub fn add_file(&self, file_meta: &FileMeta) {
        let table_id = file_meta.region_id.table_id();
        let mut is_new = false;
        {
            let file_ref = FileRef::new(file_meta.region_id, file_meta.file_id);
            self.files_per_table
                .entry(table_id)
                .and_modify(|refs| {
                    refs.files
                        .entry(file_ref.clone())
                        .and_modify(|count| *count += 1)
                        .or_insert_with(|| {
                            is_new = true;
                            1
                        });
                })
                .or_insert_with(|| TableFileRefs {
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
        let table_id = file_meta.region_id.table_id();
        let file_ref = FileRef::new(file_meta.region_id, file_meta.file_id);

        let mut remove_table_entry = false;
        let mut remove_file_ref = false;
        let mut file_cnt = 0;

        let table_ref = self.files_per_table.entry(table_id).and_modify(|refs| {
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

        if let Entry::Occupied(o) = table_ref
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
    use store_api::storage::RegionId;

    use super::*;
    use crate::sst::file::{FileMeta, FileTimeRange, IndexType, RegionFileId};

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
            index_file_size: 4096,
            index_file_id: None,
            num_rows: 1024,
            num_row_groups: 1,
            sequence: NonZeroU64::new(4096),
            partition_expr: None,
        };

        file_ref_mgr.add_file(&file_meta);

        assert_eq!(
            file_ref_mgr.files_per_table.get(&0).unwrap().files,
            HashMap::from_iter([(FileRef::new(file_meta.region_id, file_meta.file_id), 1)])
        );

        file_ref_mgr.add_file(&file_meta);

        let expected_table_ref_manifest =
            HashSet::from_iter([FileRef::new(file_meta.region_id, file_meta.file_id)]);

        assert_eq!(
            file_ref_mgr.ref_file_set(0).unwrap(),
            expected_table_ref_manifest
        );

        assert_eq!(
            file_ref_mgr.files_per_table.get(&0).unwrap().files,
            HashMap::from_iter([(FileRef::new(file_meta.region_id, file_meta.file_id), 2)])
        );

        assert_eq!(
            file_ref_mgr.ref_file_set(0).unwrap(),
            expected_table_ref_manifest
        );

        file_ref_mgr.remove_file(&file_meta);

        assert_eq!(
            file_ref_mgr.files_per_table.get(&0).unwrap().files,
            HashMap::from_iter([(FileRef::new(file_meta.region_id, file_meta.file_id), 1)])
        );

        assert_eq!(
            file_ref_mgr.ref_file_set(0).unwrap(),
            expected_table_ref_manifest
        );

        file_ref_mgr.remove_file(&file_meta);

        assert!(
            file_ref_mgr.files_per_table.get(&0).is_none(),
            "{:?}",
            file_ref_mgr.files_per_table
        );

        assert!(
            file_ref_mgr.ref_file_set(0).is_none(),
            "{:?}",
            file_ref_mgr.files_per_table
        );
    }
}
