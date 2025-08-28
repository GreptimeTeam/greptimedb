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
use std::sync::{Arc, Mutex};

use common_telemetry::{error, info};
use object_store::EntryMode;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::region_request::PathType;
use store_api::storage::{RegionId, TableId};

use crate::access_layer::AccessLayerRef;
use crate::error::{OpenDalSnafu, Result, SerdeJsonSnafu};
use crate::sst::file::{FileId, FileMeta};

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
#[derive(Debug, Clone)]
pub struct TableFileRefs {
    /// (FileRef, Ref Count) meaning how many FileHandleInner is opened for this file.
    pub files: HashMap<FileRef, usize>,
    /// Access layer per table. Will be used to upload tmp file for recording references
    /// to the object storage.
    pub access_layer: AccessLayerRef,
}

pub const PURGER_REFS_PATH: &str = ".purger_refs";

/// Returns the path of the tmp ref file for given table id and datanode id.
pub fn ref_file_path(table_dir: &str, node_id: u64, path_type: PathType) -> String {
    let path_type_postfix = match path_type {
        PathType::Bare => "",
        PathType::Data => ".data",
        PathType::Metadata => ".metadata",
    };
    format!(
        "{}/{}/{:020}{}.refs",
        table_dir, PURGER_REFS_PATH, node_id, path_type_postfix
    )
}

pub fn ref_path_to_node_id_path_type(path: &str) -> Option<(u64, PathType)> {
    let parts: Vec<&str> = path.rsplitn(2, '/').collect();
    if parts.len() != 2 {
        return None;
    }
    let file_name = parts[0];
    let segments: Vec<&str> = file_name.split('.').collect();
    if segments.len() < 2 {
        return None;
    }
    let node_id_str = segments[0];
    let path_type = if segments.len() == 2 {
        PathType::Bare
    } else {
        match segments[1] {
            "data" => PathType::Data,
            "metadata" => PathType::Metadata,
            _ => return None,
        }
    };
    let node_id = node_id_str.parse::<u64>().ok()?;
    Some((node_id, path_type))
}

/// Returns the directory path to store all purger ref files.
pub fn ref_dir(table_dir: &str) -> String {
    object_store::util::join_dir(table_dir, PURGER_REFS_PATH)
}

/// Manages all file references in one datanode.
/// It keeps track of which files are referenced and group by table ids.
/// And periodically update the references to tmp file in object storage.
/// This is useful for ensuring that files are not deleted while they are still in use by any
/// query.
#[derive(Debug, Default)]
pub struct FileReferenceManager {
    /// Datanode id. used to determine tmp ref file name.
    node_id: u64,
    /// TODO(discord9): use no hash hasher since table id is sequential.
    files_per_table: Mutex<HashMap<TableId, TableFileRefs>>,
}

pub type FileReferenceManagerRef = Arc<FileReferenceManager>;

/// The tmp file uploaded to object storage to record one table's file references.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableFileRefsManifest {
    pub file_refs: HashSet<FileRef>,
    /// Unix timestamp in milliseconds sent by metasrv to indicate when the manifest is created. Used for gc worker to make sure it gets the latest version of the manifest.
    pub ts: i64,
}

/// Reads all ref files for a table from the given access layer.
/// Returns a list of `TableFileRefsManifest`.
pub async fn read_all_ref_files_for_table(
    access_layer: &AccessLayerRef,
) -> Result<Vec<(u64, TableFileRefsManifest)>> {
    let ref_dir = ref_dir(access_layer.table_dir());
    // list everything under the ref dir. And filter out by path type in `.<path-type>.refs` postfix.
    let entries = access_layer
        .object_store()
        .list(&ref_dir)
        .await
        .context(OpenDalSnafu)?;
    let mut manifests = Vec::new();

    for entry in entries {
        if entry.metadata().mode() != EntryMode::FILE {
            continue;
        }
        let Some((node_id, path_type)) = ref_path_to_node_id_path_type(entry.path()) else {
            continue;
        };
        if path_type != access_layer.path_type() {
            continue;
        }

        // read file and parse as `TableFileRefsManifest`.
        let buf = access_layer
            .object_store()
            .read(entry.path())
            .await
            .context(OpenDalSnafu)?
            .to_bytes();

        let manifest: TableFileRefsManifest =
            serde_json::from_slice(&buf).context(SerdeJsonSnafu)?;
        manifests.push((node_id, manifest));
    }
    Ok(manifests)
}

impl FileReferenceManager {
    pub fn new() -> Self {
        Self::default()
    }

    fn ref_file_manifest(
        &self,
        table_id: TableId,
        now: i64,
    ) -> Option<(TableFileRefsManifest, AccessLayerRef)> {
        let file_refs = self
            .files_per_table
            .lock()
            .unwrap()
            .get(&table_id)
            .cloned()?;

        if file_refs.files.is_empty() {
            return None;
        }

        let ref_manifest = TableFileRefsManifest {
            file_refs: file_refs.files.keys().cloned().collect(),
            ts: now,
        };
        let access_layer = &file_refs.access_layer;

        info!(
            "Preparing to upload ref file for table {}, node {}, path_type {:?}, {} files: {:?}",
            table_id,
            self.node_id,
            access_layer.path_type(),
            ref_manifest.file_refs.len(),
            ref_manifest.file_refs,
        );

        Some((ref_manifest, access_layer.clone()))
    }

    pub async fn upload_ref_file_for_table(&self, table_id: TableId, now: i64) -> Result<()> {
        let Some((ref_manifest, access_layer)) = self.ref_file_manifest(table_id, now) else {
            return Ok(());
        };

        let path = ref_file_path(
            access_layer.table_dir(),
            self.node_id,
            access_layer.path_type(),
        );

        let content = serde_json::to_string(&ref_manifest).context(SerdeJsonSnafu)?;

        if let Err(e) = access_layer.object_store().write(&path, content).await {
            error!(e; "Failed to upload ref file to {}, table {}", path, table_id);
            return Err(e).context(OpenDalSnafu);
        } else {
            info!(
                "Successfully uploaded ref files with {} refs to {}, table {}",
                ref_manifest.file_refs.len(),
                path,
                table_id
            );
        }

        Ok(())
    }

    /// Adds a new file reference.
    /// Also records the access layer for the table if not exists.
    /// The access layer will be used to upload ref file to object storage.
    pub fn add_file(&self, file_meta: &FileMeta, table_access_layer: AccessLayerRef) {
        let table_id = file_meta.region_id.table_id();
        {
            let mut guard = self.files_per_table.lock().unwrap();
            let file_ref = FileRef::new(file_meta.region_id, file_meta.file_id);
            guard
                .entry(table_id)
                .and_modify(|refs| {
                    refs.files
                        .entry(file_ref.clone())
                        .and_modify(|count| *count += 1)
                        .or_insert(1);
                })
                .or_insert_with(|| TableFileRefs {
                    files: HashMap::from_iter([(file_ref, 1)]),
                    access_layer: table_access_layer.clone(),
                });
        }
    }

    pub fn remove_file(&self, file_meta: &FileMeta) {
        let table_id = file_meta.region_id.table_id();
        let file_ref = FileRef::new(file_meta.region_id, file_meta.file_id);
        let mut guard = self.files_per_table.lock().unwrap();
        guard.entry(table_id).and_modify(|refs| {
            refs.files.entry(file_ref.clone()).and_modify(|count| {
                if *count > 0 {
                    *count -= 1;
                }
            });
        });

        // if the ref count is 0, meaning no existing FileHandleInner, remove the file ref from table.
        if guard
            .get(&table_id)
            .map(|table_refs| table_refs.files.get(&file_ref))
            == Some(Some(&0))
        {
            if let Some(table_refs) = guard.get_mut(&table_id) {
                table_refs.files.remove(&file_ref);
            }
        }

        // if no more files for the table, remove the table entry.
        if let Some(refs) = guard.get(&table_id) {
            if refs.files.is_empty() {
                guard.remove(&table_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use common_test_util::temp_dir::create_temp_dir;
    use object_store::services::Fs;
    use object_store::ObjectStore;
    use smallvec::SmallVec;
    use store_api::region_request::PathType;
    use store_api::storage::RegionId;

    use super::*;
    use crate::access_layer::AccessLayer;
    use crate::sst::file::{FileId, FileMeta, FileTimeRange, IndexType, RegionFileId};
    use crate::sst::index::intermediate::IntermediateManager;
    use crate::sst::index::puffin_manager::PuffinManagerFactory;

    #[tokio::test]
    async fn test_file_ref_mgr() {
        common_telemetry::init_default_ut_logging();

        let dir = create_temp_dir("file_ref_mgr");
        let dir_path = dir.path().display().to_string();
        let builder = Fs::default().root(&dir_path);
        let sst_file_id = RegionFileId::new(RegionId::new(0, 0), FileId::random());
        let sst_dir = "table1";

        let index_aux_path = dir.path().join("index_aux");
        let puffin_mgr = PuffinManagerFactory::new(&index_aux_path, 4096, None, None)
            .await
            .unwrap();
        let intm_mgr = IntermediateManager::init_fs(index_aux_path.to_str().unwrap())
            .await
            .unwrap();

        let object_store = ObjectStore::new(builder).unwrap().finish();

        let layer = Arc::new(AccessLayer::new(
            sst_dir,
            PathType::Bare,
            object_store.clone(),
            puffin_mgr,
            intm_mgr,
        ));

        let file_ref_mgr = FileReferenceManager::new();

        let file_meta = FileMeta {
            region_id: sst_file_id.region_id(),
            file_id: sst_file_id.file_id(),
            time_range: FileTimeRange::default(),
            level: 0,
            file_size: 4096,
            available_indexes: SmallVec::from_iter([IndexType::InvertedIndex]),
            index_file_size: 4096,
            num_rows: 1024,
            num_row_groups: 1,
            sequence: NonZeroU64::new(4096),
        };

        file_ref_mgr.add_file(&file_meta, layer.clone());

        assert_eq!(
            file_ref_mgr
                .files_per_table
                .lock()
                .unwrap()
                .get(&0)
                .unwrap()
                .files,
            HashMap::from_iter([(FileRef::new(file_meta.region_id, file_meta.file_id), 1)])
        );

        file_ref_mgr.add_file(&file_meta, layer.clone());

        let expected_table_ref_manifest = TableFileRefsManifest {
            file_refs: HashSet::from_iter([FileRef::new(file_meta.region_id, file_meta.file_id)]),
            ts: 0,
        };

        assert_eq!(
            file_ref_mgr.ref_file_manifest(0, 0).unwrap().0,
            expected_table_ref_manifest
        );

        assert_eq!(
            file_ref_mgr
                .files_per_table
                .lock()
                .unwrap()
                .get(&0)
                .unwrap()
                .files,
            HashMap::from_iter([(FileRef::new(file_meta.region_id, file_meta.file_id), 2)])
        );

        assert_eq!(
            file_ref_mgr.ref_file_manifest(0, 0).unwrap().0,
            expected_table_ref_manifest
        );

        file_ref_mgr.remove_file(&file_meta);

        assert_eq!(
            file_ref_mgr
                .files_per_table
                .lock()
                .unwrap()
                .get(&0)
                .unwrap()
                .files,
            HashMap::from_iter([(FileRef::new(file_meta.region_id, file_meta.file_id), 1)])
        );

        assert_eq!(
            file_ref_mgr.ref_file_manifest(0, 0).unwrap().0,
            expected_table_ref_manifest
        );

        file_ref_mgr.remove_file(&file_meta);

        assert!(
            file_ref_mgr
                .files_per_table
                .lock()
                .unwrap()
                .get(&0)
                .is_none(),
            "{:?}",
            file_ref_mgr.files_per_table.lock().unwrap()
        );

        assert!(
            file_ref_mgr.ref_file_manifest(0, 0).is_none(),
            "{:?}",
            file_ref_mgr.files_per_table.lock().unwrap()
        );
    }
}
