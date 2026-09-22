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

//! Restores installed usage and removes orphaned local outputs before readers start.

use std::collections::HashSet;

use object_store::ObjectStore;
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::error::{OpenDalSnafu, Result};
use crate::series_index::catalog::{self, RangeIndexCatalog, SeriesIndexCatalog};
use crate::series_index::maintenance::{SeriesIndexMaintenance, evict_index, eviction_key};

pub(crate) async fn recover(
    store: &ObjectStore,
    maintenance: &SeriesIndexMaintenance,
) -> Result<()> {
    let mut state = maintenance.state.lock().await;
    let files = store
        .list_with("")
        .recursive(true)
        .await
        .context(OpenDalSnafu)?
        .into_iter()
        .filter(|entry| !entry.metadata().is_dir())
        .map(|entry| entry.path().to_string())
        .collect::<HashSet<_>>();
    let regions = files
        .iter()
        .filter_map(|path| path.split('/').next()?.parse::<u64>().ok())
        .map(RegionId::from)
        .collect::<HashSet<_>>();
    let mut referenced = HashSet::new();
    for region in regions {
        let series_path = catalog::series_catalog_path(region);
        if let Some(mut catalog) =
            catalog::load_catalog::<SeriesIndexCatalog>(store, &series_path).await
        {
            catalog.indexes.retain(|entry| {
                files.contains(&catalog::series_index_path(region, entry.index_uuid))
                    && catalog.file_metadata.contains_key(&entry.index_uuid)
            });
            let ids = catalog
                .indexes
                .iter()
                .map(|entry| entry.index_uuid)
                .collect::<HashSet<_>>();
            catalog.file_metadata.retain(|id, _| ids.contains(id));
            for id in catalog.file_metadata.keys() {
                let path = catalog::series_index_path(region, *id);
                referenced.insert(path);
            }
            catalog::store_catalog(store, &series_path, &catalog).await?;
            referenced.insert(series_path);
        }
        let range_path = catalog::range_catalog_path(region);
        if let Some(mut catalog) =
            catalog::load_catalog::<RangeIndexCatalog>(store, &range_path).await
        {
            catalog.indexes.retain(|id| {
                files.contains(&catalog::range_index_path(region, *id))
                    && catalog.file_metadata.contains_key(id)
            });
            catalog
                .file_metadata
                .retain(|id, _| catalog.indexes.contains(id));
            for id in catalog.file_metadata.keys() {
                let path = catalog::range_index_path(region, *id);
                referenced.insert(path);
            }
            catalog::store_catalog(store, &range_path, &catalog).await?;
            referenced.insert(range_path);
        }
        let control = catalog::load_version_control(store, region, &maintenance.purger).await;
        state.register(region, control);
    }
    for path in files.difference(&referenced) {
        store.delete(path).await.context(OpenDalSnafu)?;
    }
    let mut candidates = state.candidates();
    candidates.sort_by(|(left, lmeta), (right, rmeta)| {
        eviction_key(left, *lmeta).cmp(&eviction_key(right, *rmeta))
    });
    for (path, _) in candidates {
        if state.used <= state.capacity {
            break;
        }
        evict_index(store, &mut state, &path).await?;
    }
    Ok(())
}
