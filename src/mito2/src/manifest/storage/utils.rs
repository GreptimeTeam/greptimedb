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

use bytes::{Buf, BufMut};
use common_datasource::compression::CompressionType;
use common_telemetry::warn;
use object_store::{Entry, ObjectStore};
use snafu::ResultExt;
use store_api::ManifestVersion;

use crate::cache::manifest_cache::ManifestCache;
use crate::error::{OpenDalSnafu, Result};

const CHUNK_SIZE: usize = 64 * 1024 * 1024; // 64MB

/// Gets and decodes the object payload cached under `key`.
///
/// Older remote fills cached decoded content. If decoding fails, discard the
/// entry and let the caller refill it from object storage. Remote decode and
/// checksum errors are still returned by the caller.
pub(crate) async fn get_from_cache_decoded(
    cache: Option<&ManifestCache>,
    key: &str,
    compress_type: CompressionType,
) -> Option<Vec<u8>> {
    let cache = cache?;
    let raw = cache.get_file(key).await?;
    match compress_type.decode(raw.as_slice()).await {
        Ok(data) => Some(data),
        Err(error) => {
            warn!(error; "Failed to decode cached manifest {}, removing entry", key);
            cache.remove(key).await;
            None
        }
    }
}

/// Caches the object-store payload verbatim, before decompression.
pub(crate) async fn put_to_cache(cache: Option<&ManifestCache>, key: String, data: impl Buf) {
    let Some(cache) = cache else {
        return;
    };
    let mut bytes = Vec::with_capacity(data.remaining());
    bytes.put(data);
    cache.put_file(key, bytes).await
}

/// Removes a manifest file from cache.
pub(crate) async fn remove_from_cache(cache: Option<&ManifestCache>, key: &str) {
    let Some(cache) = cache else {
        return;
    };
    cache.remove(key).await
}

/// Writes data to object store and caches the same encoded payload.
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
    object_store
        .write_with(path, data)
        .chunk(CHUNK_SIZE) // 64MB
        .await
        .context(OpenDalSnafu)?;

    // Put to cache if we cloned the data
    if let Some(data) = cache_data {
        put_to_cache(cache, path.to_string(), data.as_slice()).await;
    }

    Ok(())
}

/// Sorts the manifest files.
pub(crate) fn sort_manifests(entries: &mut [(ManifestVersion, Entry)]) {
    entries.sort_unstable_by_key(|(version, _)| *version);
}
