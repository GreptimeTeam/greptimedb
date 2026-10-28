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

//! object storage utilities

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common_telemetry::{info, warn};
use futures::TryStreamExt;
use object_store::factory::new_raw_object_store;
use object_store::util::{clean_temp_dir, join_dir, with_instrument_layers, with_retry_layers};
use object_store::{ATOMIC_WRITE_DIR, Error as ObjectStoreError, ErrorKind, ObjectStore};
use snafu::{IntoError, prelude::*};

use crate::config::ObjectStoreConfig;
use crate::error::{self, Result};

/// Cleans up old LRU read cache directories that were removed.
fn clean_old_read_cache(store: &ObjectStoreConfig, data_home: &str) {
    if !store.is_object_storage() {
        return;
    }

    let Some(cache_config) = store.cache_config() else {
        return;
    };

    // Only cleans if read cache was enabled
    if !cache_config.enable_read_cache {
        return;
    }

    let cache_base_dir = if cache_config.cache_path.is_empty() {
        data_home
    } else {
        &cache_config.cache_path
    };

    // Cleans up the old read cache directory
    let old_read_cache_dir = join_dir(cache_base_dir, "cache/object/read");
    info!(
        "Cleaning up old read cache directory: {}",
        old_read_cache_dir
    );
    if let Err(e) = clean_temp_dir(&old_read_cache_dir) {
        warn!(e; "Failed to clean old read cache directory {}", old_read_cache_dir);
    }

    // Cleans up the atomic temp dir used by the cache layer
    let cache_atomic_temp_dir = join_dir(cache_base_dir, ATOMIC_WRITE_DIR);
    info!(
        "Cleaning up old cache atomic temp directory: {}",
        cache_atomic_temp_dir
    );
    if let Err(e) = clean_temp_dir(&cache_atomic_temp_dir) {
        warn!(e; "Failed to clean old cache atomic temp directory {}", cache_atomic_temp_dir);
    }
}

pub async fn new_object_store(store: ObjectStoreConfig, data_home: &str) -> Result<ObjectStore> {
    // Cleans up old LRU read cache directories.
    // TODO: Remove this line after the 1.0 release.
    clean_old_read_cache(&store, data_home);

    let object_store = new_raw_object_store(&store, data_home)
        .await
        .context(error::ObjectStoreSnafu)?;
    // Enables retry layer for non-fs object storages
    let object_store = if store.is_object_storage() {
        // Adds retry layer
        with_retry_layers(object_store)
    } else {
        object_store
    };

    let object_store = with_instrument_layers(object_store, true);
    Ok(object_store)
}

/// The content written by [`probe_object_store`].
const PROBE_CONTENT: &[u8] = b"greptimedb object store probe";

/// The content appended by [`probe_object_store`] to exercise read-modify-write.
const PROBE_SUFFIX: &[u8] = b" - read-modify-write";

/// The directory that holds the objects created by [`probe_object_store`].
const PROBE_DIR: &str = ".greptime_probe";

/// The budget of one [`probe_object_store`] call, including the retries an object
/// store makes on top of its 30s request timeout.
pub(crate) const PROBE_TIMEOUT: Duration = Duration::from_secs(180);

/// The maximum random delay before a probe starts. It is on the order of one
/// healthy probe (a few round trips), so providers that share an object storage are
/// spread out instead of starting together and startup waits at most this delay.
const PROBE_STAGGER: Duration = Duration::from_millis(500);

/// The budget of the best-effort cleanup after a timed-out probe, kept short so an
/// unresponsive store cannot block startup a second time.
const PROBE_CLEANUP_TIMEOUT: Duration = Duration::from_secs(10);

/// Returns a random delay in `[0, PROBE_STAGGER)`.
pub(crate) fn probe_start_delay() -> Duration {
    Duration::from_nanos(rand::random_range(0..PROBE_STAGGER.as_nanos() as u64))
}

/// Returns a unique key prefix for [`probe_object_store`], so datanodes sharing
/// the same bucket and root do not collide.
pub(crate) fn probe_key_prefix(node_id: u64) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{PROBE_DIR}/{node_id}-{}-{nanos}", std::process::id())
}

/// Returns the object key [`probe_object_store`] probes under `prefix`.
fn probe_object_key(prefix: &str) -> String {
    format!("{prefix}/probe")
}

/// Checks that `store` supports the operations the storage engines and the WAL rely
/// on: write, read, read-modify-write, list and delete.
///
/// The probe object is removed before returning, after a failed step too. A caller
/// that abandons the check early must call [`remove_probe_object`].
pub(crate) async fn probe_object_store(
    store: &ObjectStore,
    name: &str,
    prefix: &str,
) -> Result<()> {
    let key = probe_object_key(prefix);

    let check = async {
        store
            .write(&key, PROBE_CONTENT)
            .await
            .context(error::ObjectStoreProbeSnafu {
                name,
                step: "write",
            })?;
        verify_content(store, name, &key, PROBE_CONTENT).await?;

        // Object stores cannot append in place, so the engines and the WAL read the
        // object, append to it and overwrite it; this exercises that path.
        let mut content = store
            .read(&key)
            .await
            .context(error::ObjectStoreProbeSnafu { name, step: "read" })?
            .to_vec();
        content.extend_from_slice(PROBE_SUFFIX);
        store
            .write(&key, content.clone())
            .await
            .context(error::ObjectStoreProbeSnafu {
                name,
                step: "overwrite",
            })?;
        verify_content(store, name, &key, &content).await?;

        let mut lister = store
            .lister_with(&format!("{prefix}/"))
            .await
            .context(error::ObjectStoreProbeSnafu { name, step: "list" })?;
        while let Some(entry) = lister
            .try_next()
            .await
            .context(error::ObjectStoreProbeSnafu { name, step: "list" })?
        {
            if entry.path() == key {
                return Ok(());
            }
        }

        Err(
            error::ObjectStoreProbeSnafu { name, step: "list" }.into_error(ObjectStoreError::new(
                ErrorKind::Unexpected,
                format!("'{key}' is not visible in the listing"),
            )),
        )
    }
    .await;

    match store.delete(&key).await {
        Ok(()) => check,
        Err(err) if check.is_err() => {
            warn!(
                err;
                "Failed to remove the object storage probe object '{}' of provider '{}'",
                key,
                name
            );
            check
        }
        Err(err) => Err(err).context(error::ObjectStoreProbeSnafu {
            name,
            step: "delete",
        }),
    }
}

async fn verify_content(store: &ObjectStore, name: &str, key: &str, expected: &[u8]) -> Result<()> {
    let got = store
        .read(key)
        .await
        .context(error::ObjectStoreProbeSnafu { name, step: "read" })?;
    if got.to_vec() != expected {
        return Err(
            error::ObjectStoreProbeSnafu { name, step: "read" }.into_error(ObjectStoreError::new(
                ErrorKind::Unexpected,
                format!("content of '{key}' differs from what was written"),
            )),
        );
    }
    Ok(())
}

/// Removes the probe object of a probe that did not finish.
///
/// It has its own short budget, and failures only log: the caller reports the timeout.
pub(crate) async fn remove_probe_object(store: &ObjectStore, prefix: &str) {
    let key = probe_object_key(prefix);
    match tokio::time::timeout(PROBE_CLEANUP_TIMEOUT, store.delete(&key)).await {
        Ok(Ok(())) => {}
        Ok(Err(err)) => warn!(
            err;
            "Failed to remove the object storage probe object '{}'", key
        ),
        Err(_) => warn!(
            "Timed out removing the object storage probe object '{}'",
            key
        ),
    }
}

#[cfg(test)]
mod tests {
    use common_test_util::temp_dir::create_temp_dir;
    use object_store::services::Fs;

    use super::*;

    fn new_fs_store(root: &str) -> ObjectStore {
        ObjectStore::new(Fs::default().root(root)).unwrap()
    }

    #[tokio::test]
    async fn test_probe_object_store() {
        let dir = create_temp_dir("object-store-probe");
        let store = new_fs_store(dir.path().to_str().unwrap());
        let prefix = probe_key_prefix(1);

        probe_object_store(&store, "fs", &prefix).await.unwrap();

        // The lister also returns the listed directory itself, so only check that the
        // probe object is gone.
        let key = format!("{prefix}/probe");
        let mut lister = store.lister_with(&format!("{prefix}/")).await.unwrap();
        while let Some(entry) = lister.try_next().await.unwrap() {
            assert_ne!(entry.path(), key);
        }
    }

    #[tokio::test]
    async fn test_probe_object_store_write_failure() {
        let dir = create_temp_dir("object-store-probe-write-failure");
        let root = dir.path().join("not-a-directory");
        std::fs::write(&root, b"").unwrap();
        let store = new_fs_store(root.to_str().unwrap());

        let err = probe_object_store(&store, "broken", &probe_key_prefix(1))
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("probe for provider 'broken' at step 'write'")
        );
    }

    #[tokio::test]
    async fn test_remove_probe_object() {
        let dir = create_temp_dir("object-store-probe-cleanup");
        let store = new_fs_store(dir.path().to_str().unwrap());
        let prefix = probe_key_prefix(1);
        let key = probe_object_key(&prefix);
        store.write(&key, PROBE_CONTENT).await.unwrap();

        remove_probe_object(&store, &prefix).await;

        assert!(store.read(&key).await.is_err());
    }
}
