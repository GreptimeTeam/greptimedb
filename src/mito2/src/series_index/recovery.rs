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

//! Startup inventory and trimming before any index snapshots are exposed.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use common_time::Timestamp;
use object_store::ObjectStore;
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::error::{OpenDalSnafu, Result, SerdeJsonSnafu};
use crate::series_index::catalog::{self, RangeIndexCatalog, SeriesIndexCatalog};
use crate::series_index::disk_budget::SeriesIndexDiskBudget;

#[derive(Default)]
struct Catalogs {
    series: SeriesIndexCatalog,
    range: RangeIndexCatalog,
}

impl Catalogs {
    fn outputs(&self, region: RegionId) -> Result<Vec<(String, Vec<u8>)>> {
        let mut outputs = Vec::new();
        if !self.series.indexes.is_empty() {
            outputs.push((
                catalog::series_catalog_path(region),
                serde_json::to_vec_pretty(&self.series).context(SerdeJsonSnafu)?,
            ));
        }
        if !self.range.indexes.is_empty() {
            outputs.push((
                catalog::range_catalog_path(region),
                serde_json::to_vec_pretty(&self.range).context(SerdeJsonSnafu)?,
            ));
        }
        Ok(outputs)
    }
}

pub(crate) async fn recover(
    store: &ObjectStore,
    budget: &Arc<SeriesIndexDiskBudget>,
) -> Result<()> {
    let mut files = HashMap::new();
    let mut regions = BTreeMap::<RegionId, Catalogs>::new();
    for entry in store
        .list_with("")
        .recursive(true)
        .await
        .context(OpenDalSnafu)?
    {
        if entry.metadata().is_dir() {
            continue;
        }
        let path = entry.path().to_string();
        let meta = store.stat(&path).await.context(OpenDalSnafu)?;
        if let Some(id) = path.split('/').next().and_then(|id| id.parse::<u64>().ok()) {
            regions.entry(RegionId::from(id)).or_default();
        }
        files.insert(path, meta.content_length());
    }
    let mut ages = HashMap::<String, Option<Timestamp>>::new();
    let mut intact = true;
    let mut catalog_paths = std::collections::HashSet::new();
    for (region, catalogs) in &mut regions {
        let series_path = catalog::series_catalog_path(*region);
        let range_path = catalog::range_catalog_path(*region);
        let series = catalog::load_catalog(store, &series_path).await;
        let range = catalog::load_catalog(store, &range_path).await;
        if series.is_some() {
            catalog_paths.insert(series_path.clone());
        }
        if range.is_some() {
            catalog_paths.insert(range_path.clone());
        }
        intact &= !files.contains_key(&series_path) || series.is_some();
        intact &= !files.contains_key(&range_path) || range.is_some();
        catalogs.series = series.unwrap_or_default();
        catalogs.range = range.unwrap_or_default();
        let original_count = catalogs.series.indexes.len() + catalogs.range.indexes.len();
        catalogs.series.indexes.retain(|entry| {
            files.contains_key(&catalog::series_index_path(*region, entry.index_uuid))
        });
        catalogs
            .range
            .indexes
            .retain(|id| files.contains_key(&catalog::range_index_path(*region, *id)));
        intact &= original_count == catalogs.series.indexes.len() + catalogs.range.indexes.len();
        for entry in &catalogs.series.indexes {
            ages.insert(
                catalog::series_index_path(*region, entry.index_uuid),
                Some(entry.bucket_end),
            );
            for id in &entry.source_file_ids {
                let end = ages
                    .entry(catalog::range_index_path(*region, *id))
                    .or_default();
                *end = Some(end.map_or(entry.bucket_end, |end| end.max(entry.bucket_end)));
            }
        }
        for id in &catalogs.range.indexes {
            ages.entry(catalog::range_index_path(*region, *id))
                .or_default();
        }
    }
    // Remove catalogs before rewriting so recovery itself needs no extra disk space.
    // All files here are derived local indexes, and no queries have opened them yet.
    let referenced = regions
        .iter()
        .flat_map(|(region, catalogs)| {
            catalogs
                .series
                .indexes
                .iter()
                .map(|entry| catalog::series_index_path(*region, entry.index_uuid))
                .chain(
                    catalogs
                        .range
                        .indexes
                        .iter()
                        .map(|id| catalog::range_index_path(*region, *id)),
                )
        })
        .collect::<std::collections::HashSet<_>>();
    if intact
        && files
            .keys()
            .all(|path| referenced.contains(path) || catalog_paths.contains(path))
        && files
            .values()
            .map(|bytes| bytes.div_ceil(1024))
            .sum::<u64>()
            <= budget.capacity_bytes() / 1024
    {
        for (path, bytes) in files {
            let end = ages.get(&path).copied().flatten();
            budget.register_file(path, bytes, end)?;
        }
        return Ok(());
    }
    let discard = files
        .keys()
        .filter(|path| !referenced.contains(*path))
        .cloned()
        .collect::<Vec<_>>();
    for path in discard {
        store.delete(&path).await.context(OpenDalSnafu)?;
        files.remove(&path);
    }
    let mut candidates = files.keys().cloned().collect::<Vec<_>>();
    candidates.sort_by_key(|path| (ages.get(path).copied().flatten(), path.clone()));
    let mut candidates = candidates.into_iter();
    loop {
        let mut units = files
            .values()
            .map(|bytes| bytes.div_ceil(1024))
            .sum::<u64>();
        for (region, catalogs) in &regions {
            for (_, bytes) in catalogs.outputs(*region)? {
                units += (bytes.len() as u64).div_ceil(1024);
            }
        }
        if units <= budget.capacity_bytes() / 1024 {
            break;
        }
        let Some(path) = candidates.next() else {
            break;
        };
        store.delete(&path).await.context(OpenDalSnafu)?;
        files.remove(&path);
        for (region, catalogs) in &mut regions {
            catalogs
                .series
                .indexes
                .retain(|entry| catalog::series_index_path(*region, entry.index_uuid) != path);
            catalogs
                .range
                .indexes
                .retain(|id| catalog::range_index_path(*region, *id) != path);
        }
    }
    for (path, bytes) in files {
        let end = ages.get(&path).copied().flatten();
        budget.register_file(path, bytes, end)?;
    }
    for (region, catalogs) in regions {
        for (path, bytes) in catalogs.outputs(region)? {
            let mut output = budget.output(store.clone(), &path).await?;
            output.write(bytes.into()).await?;
            output.close().await?;
        }
    }
    Ok(())
}
