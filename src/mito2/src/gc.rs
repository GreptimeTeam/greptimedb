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

use std::collections::HashSet;
use std::time::Duration;

use common_telemetry::warn;
use object_store::Entry;
use snafu::ResultExt as _;
use store_api::storage::RegionId;
use tokio_stream::StreamExt;

use crate::access_layer::AccessLayerRef;
use crate::error::{JoinSnafu, OpenDalSnafu, Result};
use crate::region::ManifestContextRef;
use crate::sst::file::{FileId, RegionFileId};
use crate::sst::location;

pub struct LocalGcWorker {
    pub(crate) access_layer: AccessLayerRef,
    pub(crate) manifest_ctx: ManifestContextRef,
    /// Lingering time before deleting files.
    pub(crate) lingering_time: Duration,
}

impl LocalGcWorker {
    /// concurrency of listing files per region.
    /// This is used to limit the number of concurrent listing operations and speed up listing
    pub const CONCURRENCY_LIST_PER_FILES: usize = 256;
    pub async fn do_region_gc(&self) -> Result<()> {
        // TODO(discord9): impl gc worker
        let manifest = self.manifest_ctx.manifest().await;
        let region_id = manifest.metadata.region_id;
        let current_files = &manifest.files;
        let concurrency = current_files.len() / Self::CONCURRENCY_LIST_PER_FILES;
        let unused_files = self
            .list_unused_files(
                region_id,
                current_files.keys().cloned().collect(),
                concurrency,
            )
            .await?;

        let old_unused_files = unused_files
            .into_iter()
            .filter(|e| {
                // TODO(discord9): is use last_modified time correct?
                if let Some(last_modified) = e.metadata().last_modified() {
                    last_modified
                        < (chrono::Utc::now()
                            - chrono::Duration::from_std(self.lingering_time).unwrap())
                } else {
                    // if last modified is not set, we consider it as old enough
                    return true;
                }
            })
            .collect::<Vec<_>>();

        self.access_layer
            .object_store()
            .delete_iter(old_unused_files.iter().map(|e| e.name()))
            .await
            .context(OpenDalSnafu)
    }

    /// Concurrently list unused files in the region dir
    /// because there may be a lot of files in the region dir
    /// and listing them may take a long time.
    pub async fn list_unused_files(
        &self,
        region_id: RegionId,
        in_used: HashSet<FileId>,
        concurrency: usize,
    ) -> Result<Vec<Entry>> {
        // in use filenames, include sst and index files
        let in_use_filenames = in_used
            .iter()
            .map(|id| {
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
            .flatten()
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
                    .filter(|e| !in_use_filenames.contains(e.name())),
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
