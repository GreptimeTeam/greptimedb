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

//! GC worker which periodically checks and removes unused/obsolete  SST files.
//!
//! `expel time`: the time when the file is considered as removed, as in removed from the manifest.
//! `lingering time`: the time duration before deleting files after they are removed from manifest.
//! `delta manifest`: the manifest files after the last checkpoint that contains the changes to the manifest.
//! `delete time`: the time when the file is actually deleted from the object store.
//! `unknown files`: files that are not recorded in the manifest, usually due to saved checkpoint which remove actions before the checkpoint.
//!

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use common_telemetry::warn;
use common_time::Timestamp;
use object_store::Entry;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt as _};
use store_api::storage::RegionId;
use store_api::{MAX_VERSION, MIN_VERSION};
use tokio_stream::StreamExt;

use crate::access_layer::AccessLayerRef;
use crate::error::{
    DurationOutOfRangeSnafu, JoinSnafu, OpenDalSnafu, RegionNotFoundSnafu, Result, UnexpectedSnafu,
};
use crate::manifest::action::{RegionManifestBuilder, RegionMetaAction, RegionMetaActionList};
use crate::manifest::manager::RegionManifestManager;
use crate::region::ManifestContextRef;
use crate::sst::file::{FileId, FileMeta, RegionFileId};
use crate::sst::location;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FileGcOption {
    /// Lingering time before deleting files.
    #[serde(with = "humantime_serde")]
    pub lingering_time: Duration,
    /// Lingering time before deleting unknown files(files with undetermine expel time).
    /// expel time is the time when the file is considered as removed, as in removed from the manifest.
    #[serde(with = "humantime_serde")]
    pub unknown_file_lingering_time: Duration,
}

impl Default for FileGcOption {
    fn default() -> Self {
        Self {
            lingering_time: Duration::from_secs(60 * 30), // 30 minutes
            unknown_file_lingering_time: Duration::from_secs(60 * 60 * 24), // 1 day
        }
    }
}
pub struct LocalGcWorker {
    pub(crate) access_layer: AccessLayerRef,
    pub(crate) manifest_ctxs: HashMap<RegionId, ManifestContextRef>,
    /// Lingering time before deleting files.
    pub(crate) opt: FileGcOption,
}

impl LocalGcWorker {
    /// concurrency of listing files per region.
    /// This is used to limit the number of concurrent listing operations and speed up listing
    pub const CONCURRENCY_LIST_PER_FILES: usize = 512;

    /// Perform GC for the region.
    /// 1. Get all the removed files in delta manifest files and their expel times
    /// 2. List all files in the region dir concurrently
    /// 3. Filter out the files that are still in use or may still be kept for a while
    /// 4. Delete the unused files
    ///
    /// Note that the files that are still in use or may still be kept for a while are not deleted
    /// to avoid deleting files that are still needed.
    pub async fn do_region_gc(&self, region_id: RegionId) -> Result<()> {
        // TODO(discord9): impl gc worker
        let manifest = self
            .manifest_ctxs
            .get(&region_id)
            .context(RegionNotFoundSnafu { region_id })?
            .manifest()
            .await;
        let region_id = manifest.metadata.region_id;
        let current_files = &manifest.files;

        let recently_removed_files = self.get_removed_files_expel_times(region_id).await?;

        let concurrency = current_files.len() / Self::CONCURRENCY_LIST_PER_FILES;
        let unused_files = self
            .list_unused_files(
                region_id,
                current_files.keys().cloned().collect(),
                recently_removed_files,
                concurrency,
            )
            .await?;

        self.access_layer
            .object_store()
            .delete_iter(unused_files.iter().map(|e| e.name()))
            .await
            .context(OpenDalSnafu)
    }

    /// Get all the removed files in delta manifest files and their expel times.
    /// The expel time is the time when the file is considered as removed.
    /// Which is the last modified time of delta manifest which contains the remove action.
    ///
    pub async fn get_removed_files_expel_times(
        &self,
        region_id: RegionId,
    ) -> Result<BTreeMap<Option<Timestamp>, HashSet<FileMeta>>> {
        let mut store = self
            .manifest_ctxs
            .get(&region_id)
            .context(RegionNotFoundSnafu { region_id })?
            .manifest_manager
            .read()
            .await
            .store();
        let last_checkpoint = RegionManifestManager::last_checkpoint(&mut store).await?;
        let min_version = if let Some((checkpoint, _)) = &last_checkpoint {
            checkpoint.last_version() + 1
        } else {
            MIN_VERSION
        };

        let mut manifest_builder = if let Some((checkpoint, _)) = last_checkpoint {
            RegionManifestBuilder::with_checkpoint(checkpoint.checkpoint)
        } else {
            RegionManifestBuilder::default()
        };

        // search in delta manifests for removed files and their expel times
        // TODO(discord9): remove twice scan
        let action_entries = store.scan(min_version, MAX_VERSION).await?;
        let delta_manifests = store.fetch_manifests(min_version, MAX_VERSION).await?;
        let delta_manifests = delta_manifests.into_iter().collect::<HashMap<_, _>>();

        let mut ret = BTreeMap::new();

        for (manifest_version, entry) in action_entries {
            let last_modified_time = entry.metadata().last_modified();
            let ts = last_modified_time.map(|t| Timestamp::new_millisecond(t.timestamp_millis()));
            let action_list = RegionMetaActionList::decode(
                delta_manifests
                    .get(&manifest_version)
                    .with_context(|| UnexpectedSnafu {
                        reason: format!(
                            "Failed to find manifest for version {} in all delta versions: {:?}",
                            manifest_version,
                            delta_manifests.keys()
                        ),
                    })?,
            )?;
            // try and collect all removed files& their expel times
            // set manifest size after last checkpoint
            let mut removed_files = HashSet::new();
            for action in action_list.actions {
                match action {
                    RegionMetaAction::Change(action) => {
                        manifest_builder.apply_change(manifest_version, action);
                    }
                    RegionMetaAction::Edit(action) => {
                        removed_files.extend(action.files_to_remove.clone());
                        manifest_builder.apply_edit(manifest_version, action);
                    }
                    RegionMetaAction::Remove(_) => {}
                    RegionMetaAction::Truncate(action) => {
                        match &action.kind {
                            crate::manifest::action::TruncateKind::All { .. } => {
                                removed_files.extend(manifest_builder.files().values().cloned());
                            }
                            crate::manifest::action::TruncateKind::Partial { files_to_remove } => {
                                removed_files.extend(files_to_remove.clone());
                            }
                        }
                        manifest_builder.apply_truncate(manifest_version, action);
                    }
                }
            }
            ret.entry(ts)
                .or_insert_with(HashSet::new)
                .extend(removed_files);
        }

        Ok(ret)
    }

    /// Concurrently list unused files in the region dir
    /// because there may be a lot of files in the region dir
    /// and listing them may take a long time.
    pub async fn list_unused_files(
        &self,
        region_id: RegionId,
        in_used: HashSet<FileId>,
        recently_removed_files: BTreeMap<Option<Timestamp>, HashSet<FileMeta>>,
        concurrency: usize,
    ) -> Result<Vec<Entry>> {
        let may_linger_until = chrono::Utc::now()
            - chrono::Duration::from_std(self.opt.lingering_time).with_context(|_| {
                DurationOutOfRangeSnafu {
                    input: self.opt.lingering_time,
                }
            })?;

        let unknown_file_may_linger_until = chrono::Utc::now()
            - chrono::Duration::from_std(self.opt.unknown_file_lingering_time).with_context(
                |_| DurationOutOfRangeSnafu {
                    input: self.opt.unknown_file_lingering_time,
                },
            )?;

        // files that may linger, which means they are not in use but may still be kept for a while
        let may_linger_filenames = recently_removed_files
            .iter()
            .filter_map(|(ts, files)| {
                if let Some(ts) = ts {
                    if *ts < Timestamp::new_millisecond(may_linger_until.timestamp_millis()) {
                        // if the expel time is before the may linger time, we can delete it
                        return None;
                    }
                }
                Some(files)
            })
            .flatten()
            .flat_map(|meta| {
                [
                    location::sst_file_path(
                        self.access_layer.table_dir(),
                        meta.file_id(),
                        self.access_layer.path_type(),
                    ),
                    location::index_file_path(
                        self.access_layer.table_dir(),
                        meta.file_id(),
                        self.access_layer.path_type(),
                    ),
                ]
            })
            .collect::<HashSet<_>>();

        let all_files_appear_in_delta_manifests = recently_removed_files
            .values()
            .flat_map(|files| {
                files.iter().flat_map(|meta| {
                    [
                        location::sst_file_path(
                            self.access_layer.table_dir(),
                            meta.file_id(),
                            self.access_layer.path_type(),
                        ),
                        location::index_file_path(
                            self.access_layer.table_dir(),
                            meta.file_id(),
                            self.access_layer.path_type(),
                        ),
                    ]
                })
            })
            .collect::<HashSet<_>>();

        // in use filenames, include sst and index files
        let in_use_filenames = in_used
            .iter()
            .flat_map(|id| {
                [
                    location::sst_file_path(
                        self.access_layer.table_dir(),
                        RegionFileId::new(region_id, *id),
                        self.access_layer.path_type(),
                    ),
                    location::index_file_path(
                        self.access_layer.table_dir(),
                        RegionFileId::new(region_id, *id),
                        self.access_layer.path_type(),
                    ),
                ]
            })
            .collect::<HashSet<_>>();

        let region_dir = self.access_layer.build_region_dir(region_id);

        let partitions = gen_partition_from_concurrency(concurrency);
        let bounds = vec![None]
            .into_iter()
            .chain(partitions.iter().map(|p| Some(p.clone())))
            .chain(vec![None])
            .collect::<Vec<_>>();
        let mut listers = vec![];
        for part in bounds.windows(2) {
            let start = part[0].clone();
            let end = part[1].clone();
            let mut lister = self.access_layer.object_store().lister_with(&region_dir);
            if let Some(s) = start {
                lister = lister.start_after(&s);
            }

            let lister = lister.await.context(OpenDalSnafu)?;

            listers.push((lister, end));
        }

        let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
        let mut handles = vec![];
        for (lister, end) in listers {
            let tx = tx.clone();
            let handle = tokio::spawn(async move {
                let stream = lister.take_while(|e| match e {
                    Ok(e) => {
                        if let Some(end) = &end {
                            // reach end, stop listing
                            e.name() < end.as_str()
                        } else {
                            // no end, take all entries
                            true
                        }
                    }
                    // entry went wrong, log and skip it
                    Err(err) => {
                        warn!("Failed to list entry: {}", err);
                        true
                    }
                });
                let stream = stream.collect::<Vec<_>>().await;
                // ordering of files doesn't matter here, so we can send them directly
                tx.send(stream).await.expect("Failed to send entries");
            });

            handles.push(handle);
        }
        // Wait for all listers to finish
        for handle in handles {
            handle.await.context(JoinSnafu)?;
        }

        drop(tx); // Close the channel to stop receiving

        // Collect all entries from the channel
        let mut entries = vec![];
        while let Some(stream) = rx.recv().await {
            entries.extend(
                stream
                    .into_iter()
                    .filter_map(Result::ok)
                    .filter(|e| !in_use_filenames.contains(e.name()))
                    .filter(|e| !may_linger_filenames.contains(e.name()))
                    .filter(|e| {
                        if !all_files_appear_in_delta_manifests.contains(e.name()) {
                            // if the file's expel time is unknown(because not appear in delta manifest), we keep it for a while
                            // using it's last modified time
                            // notice unknown files use a different lingering time
                            e.metadata()
                                .last_modified()
                                .map(|t| t < unknown_file_may_linger_until)
                                .unwrap_or(false)
                        } else {
                            // if the file did appear in manifest delta(and passes previous predicate), we can delete it immediately
                            true
                        }
                    }),
            );
        }

        Ok(entries)
    }
}

/// Generate partition prefixes based on concurrency and
/// assume file names are evenly-distributed uuid string,
/// to evenly distribute files across partitions.
/// For example, if concurrency is 2, partition prefixes will be:
/// ["8"] so it divide uuids into two partitions based on the first character.
/// If concurrency is 32, partition prefixes will be:
/// ["08", "10", "18", "20", "28", "30", "38" ..., "f0", "f8"]
/// if concurrency is 1, it returns an empty vector.
///
fn gen_partition_from_concurrency(concurrency: usize) -> Vec<String> {
    let n = concurrency.next_power_of_two();
    if n <= 1 {
        return vec![];
    }

    // `d` is the number of hex characters required to build the partition key.
    // `p` is the total number of possible values for a key of length `d`.
    // We need to find the smallest `d` such that 16^d >= n.
    let mut d = 0;
    let mut p: u128 = 1;
    while p < n as u128 {
        p *= 16;
        d += 1;
    }

    let total_space = p;
    let step = total_space / n as u128;

    (1..n)
        .map(|i| {
            let boundary = i as u128 * step;
            format!("{:0width$x}", boundary, width = d)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gen_partition_from_concurrency() {
        let partitions = gen_partition_from_concurrency(1);
        assert!(partitions.is_empty());

        let partitions = gen_partition_from_concurrency(2);
        assert_eq!(partitions, vec!["8"]);

        let partitions = gen_partition_from_concurrency(3);
        assert_eq!(partitions, vec!["4", "8", "c"]);

        let partitions = gen_partition_from_concurrency(4);
        assert_eq!(partitions, vec!["4", "8", "c"]);

        let partitions = gen_partition_from_concurrency(8);
        assert_eq!(partitions, vec!["2", "4", "6", "8", "a", "c", "e"]);

        let partitions = gen_partition_from_concurrency(16);
        assert_eq!(
            partitions,
            vec!["1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"]
        );

        let partitions = gen_partition_from_concurrency(32);
        assert_eq!(
            partitions,
            [
                "08", "10", "18", "20", "28", "30", "38", "40", "48", "50", "58", "60", "68", "70",
                "78", "80", "88", "90", "98", "a0", "a8", "b0", "b8", "c0", "c8", "d0", "d8", "e0",
                "e8", "f0", "f8",
            ]
        );
    }
}
