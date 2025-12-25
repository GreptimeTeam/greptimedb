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

use object_store::{Entry, ObjectStore};
use snafu::ResultExt;
use store_api::ManifestVersion;

use crate::cache::manifest_cache::ManifestCache;
use crate::error::{OpenDalSnafu, Result};

/// Gets a manifest file from cache.
/// Returns the file data if found in cache, None otherwise.
pub(crate) async fn get_from_cache(cache: Option<&ManifestCache>, key: &str) -> Option<Vec<u8>> {
    let cache = cache?;
    cache.get_file(key).await
}

/// Puts a manifest file into cache.
pub(crate) async fn put_to_cache(cache: Option<&ManifestCache>, key: String, data: &[u8]) {
    let Some(cache) = cache else {
        return;
    };
    cache.put_file(key, data.to_vec()).await
}

/// Removes a manifest file from cache.
pub(crate) async fn remove_from_cache(cache: Option<&ManifestCache>, key: &str) {
    let Some(cache) = cache else {
        return;
    };
    cache.remove(key).await
}

/// Writes data to object store and puts it into cache.
pub(crate) async fn write_and_put_cache(
    object_store: &ObjectStore,
    cache: Option<&ManifestCache>,
    path: &str,
    data: Vec<u8>,
) -> Result<()> {
    // Clone data for cache before writing, only if cache is enabled.
    let cache_data = if cache.is_some() {
        Some(data.clone())
    } else {
        None
    };

    // Write to object store
    object_store.write(path, data).await.context(OpenDalSnafu)?;

    // Put to cache if we cloned the data
    if let Some(data) = cache_data {
        put_to_cache(cache, path.to_string(), &data).await;
    }

    Ok(())
}

/// Sorts the manifest files.
pub(crate) fn sort_manifests(entries: &mut [(ManifestVersion, Entry)]) {
    entries.sort_unstable_by(|(v1, _), (v2, _)| v1.cmp(v2));
}
