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

//! Temporary files of non-append writes, and how to reclaim them.

use std::time::Duration;

use common_telemetry::warn;
use opendal::raw::Timestamp;

use crate::{ErrorKind, ObjectStore};

/// Marker that follows the name of the target in a temporary file name.
const TEMPORARY_MARKER: &str = ".greptime-";

/// Suffix of a temporary file name.
const TEMPORARY_SUFFIX: &str = ".tmp";

/// Scheme of the only service that publishes non-append writes with a rename,
/// or `None` if that service isn't compiled in.
#[cfg(feature = "hdfs-object-store")]
const TEMPORARY_FILE_SCHEME: Option<&str> = Some(crate::services::HDFS_NATIVE_SCHEME);
#[cfg(not(feature = "hdfs-object-store"))]
const TEMPORARY_FILE_SCHEME: Option<&str> = None;

/// Returns true if `store` publishes non-append writes with a rename, which is
/// what leaves temporary files behind when the writer dies.
///
/// Only the HDFS compatibility layer does that; a write to any other service
/// appears under its final name.
pub fn writes_temporary_files(store: &ObjectStore) -> bool {
    TEMPORARY_FILE_SCHEME == Some(store.info().scheme())
}

/// Deletes the temporary files in `dirs` that are older than `older_than`.
///
/// A non-append write goes to a temporary file next to its target and is
/// renamed on close, so a writer that dies in between leaves the file behind
/// with nothing to reclaim it: GC can't parse the name, and the startup cleanup
/// of [`ATOMIC_WRITE_DIR`](crate::ATOMIC_WRITE_DIR) doesn't reach it.
///
/// The caller runs this for a region that just became its writable leader, so a
/// file that is old enough belongs to a writer that is gone. A store that
/// doesn't write temporary files is left untouched. Failures are logged and
/// don't stop the sweep. Returns the number of deleted files.
pub async fn sweep_temporary_files(
    store: &ObjectStore,
    dirs: &[String],
    older_than: Duration,
) -> usize {
    if !writes_temporary_files(store) {
        return 0;
    }

    let mut deleted = 0;
    for dir in dirs {
        deleted += sweep_dir(store, dir, older_than).await;
    }
    deleted
}

async fn sweep_dir(store: &ObjectStore, dir: &str, older_than: Duration) -> usize {
    let entries = match store.list(dir).await {
        Ok(entries) => entries,
        // Not every region has every directory, for example one that was never
        // repartitioned has no staging directory.
        Err(error) if error.kind() == ErrorKind::NotFound => return 0,
        Err(error) => {
            warn!(error; "Failed to list {dir} for temporary files");
            return 0;
        }
    };

    let expired_before = Timestamp::now()
        .into_inner()
        .as_millisecond()
        .saturating_sub(i64::try_from(older_than.as_millis()).unwrap_or(i64::MAX));

    let mut deleted = 0;
    for entry in entries {
        if !is_temporary_file(entry.name()) {
            continue;
        }

        // HDFS advances the modification time of a file under construction only
        // as its blocks complete, so an old file is not necessarily abandoned.
        // That is what the threshold covers.
        let is_expired = entry
            .metadata()
            .last_modified()
            .map(|last_modified| last_modified.into_inner().as_millisecond() < expired_before)
            // Without a modification time there is no way to tell whether the
            // writer is gone, so keep the file.
            .unwrap_or(false);
        if !is_expired {
            continue;
        }

        // Delete one file at a time: the bulk delete stops at the first
        // failure, and one file that can't be deleted must not keep the rest of
        // the directory from being swept.
        match store.delete(entry.path()).await {
            Ok(()) => deleted += 1,
            Err(error) => warn!(error; "Failed to delete temporary file {}", entry.path()),
        }
    }

    deleted
}

fn is_temporary_file(name: &str) -> bool {
    // `name` is the last segment of a path, so a directory name ends with `/`.
    name.starts_with('.') && name.contains(TEMPORARY_MARKER) && name.ends_with(TEMPORARY_SUFFIX)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use opendal::Operator;
    use opendal::services::Fs;
    use tempfile::TempDir;

    use super::*;
    use crate::layers::mock::{MockLayerBuilder, oio};
    use crate::{Error, Result};

    const EXPIRED: Duration = Duration::from_millis(1);

    fn test_store() -> (TempDir, ObjectStore) {
        let directory = tempfile::tempdir().unwrap();
        let store = Operator::new(Fs::default().root(directory.path().to_str().unwrap())).unwrap();
        (directory, store)
    }

    fn test_store_of_missing_dirs() -> (TempDir, ObjectStore) {
        // Fs lists a directory it doesn't have as an empty one, but HDFS reports
        // it as not found.
        struct MissingLister;

        impl oio::List for MissingLister {
            async fn next(&mut self) -> Result<Option<oio::Entry>> {
                Err(Error::new(ErrorKind::NotFound, "no such directory"))
            }
        }

        let directory = tempfile::tempdir().unwrap();
        let layer = MockLayerBuilder::default()
            .lister_factory(Arc::new(|_, _, _| Box::new(MissingLister)))
            .build()
            .unwrap();
        let store = Operator::new(Fs::default().root(directory.path().to_str().unwrap()))
            .unwrap()
            .layer(layer);
        (directory, store)
    }

    async fn wait_until_expired() {
        // A file written just before this call can carry the same modification
        // time in milliseconds as the sweep that follows it, which would make the
        // sweep keep it.
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    #[test]
    fn test_is_temporary_file() {
        assert!(is_temporary_file(
            ".00000000000000000042.json.greptime-53f0f4a6-3f3e-4a2f-9a2f-6fe4b0d5b5b0.tmp"
        ));
        assert!(is_temporary_file(".a5f2.parquet.greptime-53f0f4a6.tmp"));

        assert!(!is_temporary_file("00000000000000000042.json"));
        assert!(!is_temporary_file(".00000000000000000042.json"));
        assert!(!is_temporary_file(
            "00000000000000000042.json.greptime-53f0f4a6.tmp"
        ));
        assert!(!is_temporary_file(
            ".00000000000000000042.json.greptime-53f0f4a6"
        ));
        assert!(!is_temporary_file("index/"));
    }

    #[tokio::test]
    async fn test_sweep_dir_deletes_expired_temporary_file() {
        let (_directory, store) = test_store();
        let temporary = "data/.42.parquet.greptime-53f0f4a6.tmp";
        store.create_dir("data/").await.unwrap();
        store.write(temporary, "partial").await.unwrap();
        store.write("data/42.parquet", "data").await.unwrap();

        wait_until_expired().await;
        assert_eq!(1, sweep_dir(&store, "data/", EXPIRED).await);

        assert!(!store.exists(temporary).await.unwrap());
        assert!(store.exists("data/42.parquet").await.unwrap());
    }

    #[tokio::test]
    async fn test_sweep_dir_keeps_young_temporary_file() {
        let (_directory, store) = test_store();
        let temporary = "data/.42.parquet.greptime-53f0f4a6.tmp";
        store.create_dir("data/").await.unwrap();
        store.write(temporary, "partial").await.unwrap();

        assert_eq!(
            0,
            sweep_dir(&store, "data/", Duration::from_secs(24 * 60 * 60)).await
        );

        assert!(store.exists(temporary).await.unwrap());
    }

    #[tokio::test]
    async fn test_sweep_dir_of_missing_directory() {
        let (_directory, store) = test_store_of_missing_dirs();

        // A region that was never repartitioned has no staging directory, so a
        // missing directory is an expected answer rather than a failure.
        assert_eq!(
            0,
            sweep_dir(&store, "data/staging/blob/", Duration::ZERO).await
        );
    }

    #[tokio::test]
    async fn test_sweep_skips_service_without_temporary_files() {
        let (_directory, store) = test_store();
        let temporary = "data/.42.parquet.greptime-53f0f4a6.tmp";
        store.create_dir("data/").await.unwrap();
        store.write(temporary, "partial").await.unwrap();

        wait_until_expired().await;
        assert!(!writes_temporary_files(&store));
        assert_eq!(
            0,
            sweep_temporary_files(&store, &["data/".to_string()], EXPIRED).await
        );

        assert!(store.exists(temporary).await.unwrap());
    }
}
