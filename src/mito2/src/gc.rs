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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use common_meta::datanode::GcStat;
use common_telemetry::{debug, error, info, warn};
use common_time::Timestamp;
use object_store::{Entry, Lister};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt as _, ensure};
use store_api::storage::{FileId, FileRefsManifest, GcReport, RegionId};
use tokio::sync::{OwnedSemaphorePermit, TryAcquireError};
use tokio_stream::StreamExt;

use crate::access_layer::AccessLayerRef;
use crate::cache::CacheManagerRef;
use crate::config::MitoConfig;
use crate::error::{
    DurationOutOfRangeSnafu, EmptyRegionDirSnafu, JoinSnafu, OpenDalSnafu, RegionNotFoundSnafu,
    Result, TooManyGcJobsSnafu, UnexpectedSnafu,
};
use crate::manifest::manager::{RegionManifestManager, RegionManifestOptions, RemoveFileOptions};
use crate::manifest::storage::manifest_compress_type;
use crate::metrics::GC_DEL_FILE_CNT;
use crate::region::opener::new_manifest_dir;
use crate::sst::file::delete_files;
use crate::sst::location::{self, region_dir_from_table_dir};

/// Limit the amount of concurrent GC jobs on the datanode
pub struct GcLimiter {
    pub gc_job_limit: Arc<tokio::sync::Semaphore>,
    gc_concurrency: usize,
}

pub type GcLimiterRef = Arc<GcLimiter>;

impl GcLimiter {
    pub fn new(gc_concurrency: usize) -> Self {
        Self {
            gc_job_limit: Arc::new(tokio::sync::Semaphore::new(gc_concurrency)),
            gc_concurrency,
        }
    }

    pub fn running_gc_tasks(&self) -> u32 {
        (self.gc_concurrency - self.gc_job_limit.available_permits()) as u32
    }

    pub fn gc_concurrency(&self) -> u32 {
        self.gc_concurrency as u32
    }

    pub fn gc_stat(&self) -> GcStat {
        GcStat::new(self.running_gc_tasks(), self.gc_concurrency())
    }

    /// Try to acquire a permit for a GC job.
    ///
    /// If no permit is available, returns an `TooManyGcJobs` error.
    pub fn permit(&self) -> Result<OwnedSemaphorePermit> {
        self.gc_job_limit
            .clone()
            .try_acquire_owned()
            .map_err(|e| match e {
                TryAcquireError::Closed => UnexpectedSnafu {
                    reason: format!("Failed to acquire gc permit: {e}"),
                }
                .build(),
                TryAcquireError::NoPermits => TooManyGcJobsSnafu {}.build(),
            })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcConfig {
    /// Whether GC is enabled.
    pub enable: bool,
    /// Lingering time before deleting files.
    /// Should be long enough to allow long running queries to finish.
    ///
    /// TODO(discord9): long running queries should actively write tmp manifest files
    /// to prevent deletion of files they are using.
    #[serde(with = "humantime_serde")]
    pub lingering_time: Duration,
    /// Lingering time before deleting unknown files(files with undetermine expel time).
    /// expel time is the time when the file is considered as removed, as in removed from the manifest.
    /// This should only occur rarely, as manifest keep tracks in `removed_files` field
    /// unless something goes wrong.
    #[serde(with = "humantime_serde")]
    pub unknown_file_lingering_time: Duration,
    /// Maximum concurrent list operations per GC job.
    /// This is used to limit the number of concurrent listing operations and speed up listing.
    pub max_concurrent_lister_per_gc_job: usize,
    /// Maximum concurrent GC jobs.
    /// This is used to limit the number of concurrent GC jobs running on the datanode
    /// to prevent too many concurrent GC jobs from overwhelming the datanode.
    pub max_concurrent_gc_job: usize,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            enable: false,
            // expect long running queries to be finished within a reasonable time
            lingering_time: Duration::from_secs(60 * 5),
            // 6 hours, for unknown expel time, which is when this file get removed from manifest, it should rarely happen, can keep it longer
            unknown_file_lingering_time: Duration::from_secs(60 * 60 * 6),
            max_concurrent_lister_per_gc_job: 32,
            max_concurrent_gc_job: 4,
        }
    }
}

pub struct LocalGcWorker {
    pub(crate) access_layer: AccessLayerRef,
    pub(crate) cache_manager: Option<CacheManagerRef>,
    pub(crate) manifest_mgrs: HashMap<RegionId, RegionManifestManager>,
    /// Lingering time before deleting files.
    pub(crate) opt: GcConfig,
    pub(crate) manifest_open_config: ManifestOpenConfig,
    /// Tmp ref files manifest, used to determine which files are still in use by ongoing queries.
    ///
    /// Also contains manifest versions of regions when the tmp ref files are generated.
    /// Used to determine whether the tmp ref files are outdated.
    pub(crate) file_ref_manifest: FileRefsManifest,
    _permit: OwnedSemaphorePermit,
    /// Whether to perform full file listing during GC.
    /// When set to false, GC will only delete files that are tracked in the manifest's removed_files,
    /// which can significantly improve performance by avoiding expensive list operations.
    /// When set to true, GC will perform a full listing to find and delete orphan files
    /// (files not tracked in the manifest).
    ///
    /// Set to false for regular GC operations to optimize performance.
    /// Set to true periodically or when you need to clean up orphan files.
    pub full_file_listing: bool,
}

pub struct ManifestOpenConfig {
    pub compress_manifest: bool,
    pub manifest_checkpoint_distance: u64,
    pub experimental_manifest_keep_removed_file_count: usize,
    pub experimental_manifest_keep_removed_file_ttl: Duration,
}

impl From<MitoConfig> for ManifestOpenConfig {
    fn from(mito_config: MitoConfig) -> Self {
        Self {
            compress_manifest: mito_config.compress_manifest,
            manifest_checkpoint_distance: mito_config.manifest_checkpoint_distance,
            experimental_manifest_keep_removed_file_count: mito_config
                .experimental_manifest_keep_removed_file_count,
            experimental_manifest_keep_removed_file_ttl: mito_config
                .experimental_manifest_keep_removed_file_ttl,
        }
    }
}

impl LocalGcWorker {
    /// Create a new LocalGcWorker, with `regions_to_gc` regions to GC.
    /// The regions are specified by their `RegionId` and should all belong to the same table.
    ///
    #[allow(clippy::too_many_arguments)]
    pub async fn try_new(
        access_layer: AccessLayerRef,
        cache_manager: Option<CacheManagerRef>,
        regions_to_gc: BTreeSet<RegionId>,
        opt: GcConfig,
        manifest_open_config: ManifestOpenConfig,
        file_ref_manifest: FileRefsManifest,
        limiter: &GcLimiterRef,
        full_file_listing: bool,
    ) -> Result<Self> {
        let table_id = regions_to_gc
            .first()
            .context(UnexpectedSnafu {
                reason: "Expect at least one region, found none",
            })?
            .table_id();
        let permit = limiter.permit()?;
        let mut zelf = Self {
            access_layer,
            cache_manager,
            manifest_mgrs: HashMap::new(),
            opt,
            manifest_open_config,
            file_ref_manifest,
            _permit: permit,
            full_file_listing,
        };

        // dedup just in case
        for region_id in regions_to_gc {
            ensure!(
                region_id.table_id() == table_id,
                UnexpectedSnafu {
                    reason: format!(
                        "All regions should belong to the same table, found region {} and table {}",
                        region_id, table_id
                    ),
                }
            );
            let mgr = zelf.open_mgr_for(region_id).await?;
            zelf.manifest_mgrs.insert(region_id, mgr);
        }

        Ok(zelf)
    }

    /// Get tmp ref files for all current regions
    ///
    /// Outdated regions are added to `outdated_regions` set
    pub async fn read_tmp_ref_files(
        &self,
        outdated_regions: &mut HashSet<RegionId>,
    ) -> Result<HashMap<RegionId, HashSet<FileId>>> {
        for (region_id, region_mgr) in &self.manifest_mgrs {
            let current_version = region_mgr.manifest().manifest_version;
            if &current_version
                > self
                    .file_ref_manifest
                    .manifest_version
                    .get(region_id)
                    .with_context(|| UnexpectedSnafu {
                        reason: format!(
                            "Region {} not found in tmp ref manifest version map",
                            region_id
                        ),
                    })?
            {
                outdated_regions.insert(*region_id);
            }
        }
        // TODO(discord9): verify manifest version before reading tmp ref files

        let mut tmp_ref_files = HashMap::new();
        for (region_id, file_refs) in &self.file_ref_manifest.file_refs {
            if outdated_regions.contains(region_id) {
                // skip outdated regions
                continue;
            }
            tmp_ref_files
                .entry(*region_id)
                .or_insert_with(HashSet::new)
                .extend(file_refs.clone());
        }

        Ok(tmp_ref_files)
    }

    /// Run the GC worker in serial mode,
    /// considering list files could be slow and run multiple regions in parallel
    /// may cause too many concurrent listing operations.
    ///
    /// TODO(discord9): consider instead running in parallel mode
    pub async fn run(self) -> Result<GcReport> {
        info!("LocalGcWorker started");
        let now = std::time::Instant::now();

        let mut outdated_regions = HashSet::new();
        let mut deleted_files = HashMap::new();
        let tmp_ref_files = self.read_tmp_ref_files(&mut outdated_regions).await?;
        for region_id in self.manifest_mgrs.keys() {
            debug!("Doing gc for region {}", region_id);
            let tmp_ref_files = tmp_ref_files
                .get(region_id)
                .cloned()
                .unwrap_or_else(HashSet::new);
            let files = self.do_region_gc(*region_id, &tmp_ref_files).await?;
            deleted_files.insert(*region_id, files);
            debug!("Gc for region {} finished", region_id);
        }
        info!(
            "LocalGcWorker finished after {} secs.",
            now.elapsed().as_secs()
        );
        let report = GcReport {
            deleted_files,
            need_retry_regions: outdated_regions.into_iter().collect(),
        };
        Ok(report)
    }
}

impl LocalGcWorker {
    /// concurrency of listing files per region.
    /// This is used to limit the number of concurrent listing operations and speed up listing
    pub const CONCURRENCY_LIST_PER_FILES: usize = 1024;

    /// Perform GC for the region.
    /// 1. Get all the removed files in delta manifest files and their expel times
    /// 2. List all files in the region dir concurrently
    /// 3. Filter out the files that are still in use or may still be kept for a while
    /// 4. Delete the unused files
    ///
    /// Note that the files that are still in use or may still be kept for a while are not deleted
    /// to avoid deleting files that are still needed.
    pub async fn do_region_gc(
        &self,
        region_id: RegionId,
        tmp_ref_files: &HashSet<FileId>,
    ) -> Result<Vec<FileId>> {
        debug!("Doing gc for region {}", region_id);
        let manifest = self
            .manifest_mgrs
            .get(&region_id)
            .context(RegionNotFoundSnafu { region_id })?
            .manifest();
        let region_id = manifest.metadata.region_id;
        let current_files = &manifest.files;

        let recently_removed_files = self.get_removed_files_expel_times(region_id).await?;

        if recently_removed_files.is_empty() {
            // no files to remove, skip
            debug!("No recently removed files to gc for region {}", region_id);
        }

        debug!(
            "Found {} recently removed files sets for region {}",
            recently_removed_files.len(),
            region_id
        );

        let concurrency = (current_files.len() / Self::CONCURRENCY_LIST_PER_FILES)
            .max(1)
            .min(self.opt.max_concurrent_lister_per_gc_job);

        let in_used = current_files
            .keys()
            .cloned()
            .chain(tmp_ref_files.clone().into_iter())
            .collect();

        let unused_files = self
            .list_to_be_deleted_files(region_id, in_used, recently_removed_files, concurrency)
            .await?;

        let unused_len = unused_files.len();

        debug!(
            "Found {} unused files to delete for region {}",
            unused_len, region_id
        );

        self.delete_files(region_id, &unused_files).await?;

        debug!(
            "Successfully deleted {} unused files for region {}",
            unused_len, region_id
        );

        Ok(unused_files)
    }

    async fn delete_files(&self, region_id: RegionId, file_ids: &[FileId]) -> Result<()> {
        delete_files(
            region_id,
            file_ids,
            true,
            &self.access_layer,
            &self.cache_manager,
        )
        .await?;

        // FIXME(discord9): if files are already deleted before calling delete_files, the metric will be inaccurate, no clean way to fix it now
        GC_DEL_FILE_CNT.add(file_ids.len() as i64);

        Ok(())
    }

    /// Get the manifest manager for the region.
    async fn open_mgr_for(&self, region_id: RegionId) -> Result<RegionManifestManager> {
        let table_dir = self.access_layer.table_dir();
        let path_type = self.access_layer.path_type();
        let mito_config = &self.manifest_open_config;

        let region_manifest_options = RegionManifestOptions {
            manifest_dir: new_manifest_dir(&region_dir_from_table_dir(
                table_dir, region_id, path_type,
            )),
            object_store: self.access_layer.object_store().clone(),
            compress_type: manifest_compress_type(mito_config.compress_manifest),
            checkpoint_distance: mito_config.manifest_checkpoint_distance,
            remove_file_options: RemoveFileOptions {
                keep_count: mito_config.experimental_manifest_keep_removed_file_count,
                keep_ttl: mito_config.experimental_manifest_keep_removed_file_ttl,
            },
        };

        RegionManifestManager::open(
            region_manifest_options,
            Default::default(),
            Default::default(),
        )
        .await?
        .context(EmptyRegionDirSnafu {
            region_id,
            region_dir: &region_dir_from_table_dir(table_dir, region_id, path_type),
        })
    }

    /// Get all the removed files in delta manifest files and their expel times.
    /// The expel time is the time when the file is considered as removed.
    /// Which is the last modified time of delta manifest which contains the remove action.
    ///
    pub async fn get_removed_files_expel_times(
        &self,
        region_id: RegionId,
    ) -> Result<BTreeMap<Timestamp, HashSet<FileId>>> {
        let region_manifest = self
            .manifest_mgrs
            .get(&region_id)
            .context(RegionNotFoundSnafu { region_id })?
            .manifest();

        let mut ret = BTreeMap::new();
        for files in &region_manifest.removed_files.removed_files {
            let expel_time = Timestamp::new_millisecond(files.removed_at);
            let set = ret.entry(expel_time).or_insert_with(HashSet::new);
            set.extend(files.file_ids.iter().cloned());
        }

        Ok(ret)
    }

    /// Create partitioned listers for concurrent file listing based on concurrency level.
    /// Returns a vector of (lister, end_boundary) pairs for parallel processing.
    async fn partition_region_files(
        &self,
        region_id: RegionId,
        concurrency: usize,
    ) -> Result<Vec<(Lister, Option<String>)>> {
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

        Ok(listers)
    }

    /// Concurrently list all files in the region directory using the provided listers.
    /// Returns a vector of all file entries found across all partitions.
    async fn list_region_files_concurrent(
        &self,
        listers: Vec<(Lister, Option<String>)>,
    ) -> Result<Vec<Entry>> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
        let mut handles = vec![];

        for (lister, end) in listers {
            let tx = tx.clone();
            let handle = tokio::spawn(async move {
                let stream = lister.take_while(|e: &std::result::Result<Entry, _>| match e {
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
                let stream = stream
                    .filter(|e| {
                        if let Ok(e) = &e {
                            // notice that we only care about files, skip dirs
                            e.metadata().is_file()
                        } else {
                            // error entry, take for further logging
                            true
                        }
                    })
                    .collect::<Vec<_>>()
                    .await;
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
        let mut all_entries = vec![];
        while let Some(stream) = rx.recv().await {
            all_entries.extend(stream.into_iter().filter_map(Result::ok));
        }

        Ok(all_entries)
    }

    /// Filter files to determine which ones can be deleted based on usage status and lingering time.
    /// Returns a vector of file IDs that are safe to delete.
    fn filter_deletable_files(
        &self,
        entries: Vec<Entry>,
        in_use_filenames: &HashSet<&FileId>,
        may_linger_filenames: &HashSet<&FileId>,
        eligible_for_removal: &HashSet<&FileId>,
        unknown_file_may_linger_until: chrono::DateTime<chrono::Utc>,
    ) -> (Vec<FileId>, HashSet<FileId>) {
        let mut all_unused_files_ready_for_delete = vec![];
        let mut all_in_exist_linger_files = HashSet::new();

        for entry in entries {
            let file_id = match location::parse_file_id_from_path(entry.name()) {
                Ok(file_id) => file_id,
                Err(err) => {
                    error!(err; "Failed to parse file id from path: {}", entry.name());
                    // if we can't parse the file id, it means it's not a sst or index file
                    // shouldn't delete it because we don't know what it is
                    continue;
                }
            };

            if may_linger_filenames.contains(&file_id) {
                all_in_exist_linger_files.insert(file_id);
            }

            let should_delete = !in_use_filenames.contains(&file_id)
                && !may_linger_filenames.contains(&file_id)
                && {
                    if !eligible_for_removal.contains(&file_id) {
                        // if the file's expel time is unknown(because not appear in delta manifest), we keep it for a while
                        // using it's last modified time
                        // notice unknown files use a different lingering time
                        entry
                            .metadata()
                            .last_modified()
                            .map(|t| t < unknown_file_may_linger_until)
                            .unwrap_or(false)
                    } else {
                        // if the file did appear in manifest delta(and passes previous predicate), we can delete it immediately
                        true
                    }
                };

            if should_delete {
                all_unused_files_ready_for_delete.push(file_id);
            }
        }

        (all_unused_files_ready_for_delete, all_in_exist_linger_files)
    }

    /// Concurrently list unused files in the region dir
    /// because there may be a lot of files in the region dir
    /// and listing them may take a long time.
    ///
    /// When `full_file_listing` is false, this method will only delete files tracked in
    /// `recently_removed_files` without performing expensive list operations, which significantly
    /// improves performance. When `full_file_listing` is true, it performs a full listing to
    /// find and delete orphan files.
    pub async fn list_to_be_deleted_files(
        &self,
        region_id: RegionId,
        in_used: HashSet<FileId>,
        recently_removed_files: BTreeMap<Timestamp, HashSet<FileId>>,
        concurrency: usize,
    ) -> Result<Vec<FileId>> {
        let start = tokio::time::Instant::now();
        let now = chrono::Utc::now();
        let may_linger_until = now
            - chrono::Duration::from_std(self.opt.lingering_time).with_context(|_| {
                DurationOutOfRangeSnafu {
                    input: self.opt.lingering_time,
                }
            })?;

        let unknown_file_may_linger_until = now
            - chrono::Duration::from_std(self.opt.unknown_file_lingering_time).with_context(
                |_| DurationOutOfRangeSnafu {
                    input: self.opt.unknown_file_lingering_time,
                },
            )?;

        // files that may linger, which means they are not in use but may still be kept for a while
        let threshold = Timestamp::new_millisecond(may_linger_until.timestamp_millis());
        let mut recently_removed_files = recently_removed_files;
        let may_linger_files = recently_removed_files.split_off(&threshold);
        let may_linger_filenames = may_linger_files.values().flatten().collect::<HashSet<_>>();

        let eligible_for_removal = recently_removed_files
            .values()
            .flatten()
            .collect::<HashSet<_>>();

        // in use filenames, include sst and index files
        let in_use_filenames = in_used.iter().collect::<HashSet<_>>();

        // When full_file_listing is false, skip expensive list operations and only delete
        // files that are tracked in recently_removed_files
        if !self.full_file_listing {
            // Only delete files that:
            // 1. Are in recently_removed_files (tracked in manifest)
            // 2. Are not in use
            // 3. Have passed the lingering time
            let files_to_delete: Vec<FileId> = eligible_for_removal
                .iter()
                .filter(|file_id| !in_use_filenames.contains(*file_id))
                .map(|&f| *f)
                .collect();

            info!(
                "gc: fast mode (no full listing) cost {} secs for region {}, found {} files to delete from manifest",
                start.elapsed().as_secs_f64(),
                region_id,
                files_to_delete.len()
            );

            return Ok(files_to_delete);
        }

        // Full file listing mode: perform expensive list operations to find orphan files
        // Step 1: Create partitioned listers for concurrent processing
        let listers = self.partition_region_files(region_id, concurrency).await?;
        let lister_cnt = listers.len();

        // Step 2: Concurrently list all files in the region directory
        let all_entries = self.list_region_files_concurrent(listers).await?;

        let cnt = all_entries.len();

        // Step 3: Filter files to determine which ones can be deleted
        let (all_unused_files_ready_for_delete, all_in_exist_linger_files) = self
            .filter_deletable_files(
                all_entries,
                &in_use_filenames,
                &may_linger_filenames,
                &eligible_for_removal,
                unknown_file_may_linger_until,
            );

        info!(
            "gc: full listing mode cost {} secs using {lister_cnt} lister for {cnt} files in region {}, found {} unused files to delete",
            start.elapsed().as_secs_f64(),
            region_id,
            all_unused_files_ready_for_delete.len()
        );
        debug!("All in exist linger files: {:?}", all_in_exist_linger_files);

        Ok(all_unused_files_ready_for_delete)
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
            vec![
                "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"
            ]
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
