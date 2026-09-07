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

//! Index catalog persistence, coverage metadata, and file paths.

use common_telemetry::warn;
use common_time::Timestamp;
use object_store::{ErrorKind, ObjectStore};
use parquet::file::metadata::KeyValue;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::storage::{FileId, RegionId};

use super::purger::IndexFilePurger;
use super::version::{SeriesIndexFileHandle, SeriesIndexVersion, SeriesIndexVersionControl};
use crate::error::{OpenDalSnafu, Result, SerdeJsonSnafu};
const SERIES_DIR: &str = "series";
const RANGE_CATALOG: &str = "range-index.json";
const SERIES_CATALOG: &str = "series-index.json";
const SERIES_METADATA_KEY: &str = "greptime.series_index";

/// Self-describing coverage stored in a series-index Parquet footer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SeriesIndexEntry {
    pub(crate) index_uuid: FileId,
    /// Inclusive bucket start.
    pub(crate) bucket_start: Timestamp,
    /// Exclusive bucket end.
    pub(crate) bucket_end: Timestamp,
    pub(crate) source_file_ids: Vec<FileId>,
    pub(crate) min_file_sequence: u64,
    pub(crate) max_file_sequence: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct SeriesIndexCatalog {
    pub(crate) indexes: Vec<SeriesIndexEntry>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct RangeIndexCatalog {
    pub(crate) indexes: Vec<FileId>,
}

pub(crate) fn range_catalog_path(region_id: RegionId) -> String {
    format!("{}/{RANGE_CATALOG}", region_id.as_u64())
}

pub(crate) fn series_index_path(region_id: RegionId, index_uuid: FileId) -> String {
    format!("{}/{SERIES_DIR}/{index_uuid}.parquet", region_id.as_u64())
}

pub(crate) fn series_catalog_path(region_id: RegionId) -> String {
    format!("{}/{SERIES_CATALOG}", region_id.as_u64())
}

pub(crate) fn same_series_coverage(left: &SeriesIndexEntry, right: &SeriesIndexEntry) -> bool {
    left.bucket_start == right.bucket_start
        && left.bucket_end == right.bucket_end
        && left.source_file_ids == right.source_file_ids
        && left.min_file_sequence == right.min_file_sequence
        && left.max_file_sequence == right.max_file_sequence
}

pub(crate) fn series_metadata(entry: &SeriesIndexEntry) -> Result<Vec<KeyValue>> {
    Ok(vec![KeyValue::new(
        SERIES_METADATA_KEY.to_string(),
        Some(serde_json::to_string(entry).context(SerdeJsonSnafu)?),
    )])
}

pub(crate) async fn load_catalog<T>(store: &ObjectStore, path: &str) -> T
where
    T: Default + DeserializeOwned,
{
    let bytes = match store.read(path).await {
        Ok(bytes) => bytes.to_bytes(),
        Err(error) if error.kind() == ErrorKind::NotFound => return T::default(),
        Err(error) => {
            warn!(error; "Failed to load series-index catalog, path: {path}");
            return T::default();
        }
    };
    match serde_json::from_slice(&bytes) {
        Ok(catalog) => catalog,
        Err(error) => {
            warn!(error; "Invalid series-index catalog, path: {path}, phase: load");
            T::default()
        }
    }
}

pub(crate) async fn store_catalog<T>(store: &ObjectStore, path: &str, catalog: &T) -> Result<()>
where
    T: Serialize,
{
    let bytes = serde_json::to_vec_pretty(catalog).context(SerdeJsonSnafu)?;
    store
        .write(path, bytes)
        .await
        .map(|_| ())
        .context(OpenDalSnafu)
}

/// Restores the in-memory snapshot once when opening a region.
pub(crate) async fn load_version_control(
    store: &ObjectStore,
    region_id: RegionId,
    purger: &IndexFilePurger,
) -> SeriesIndexVersionControl {
    let range = load_catalog::<RangeIndexCatalog>(store, &range_catalog_path(region_id)).await;
    let series = load_catalog::<SeriesIndexCatalog>(store, &series_catalog_path(region_id)).await;
    // TODO: Handle catalog entries whose index files are missing from storage.
    let version = SeriesIndexVersion {
        range_indexes: range.indexes.into_iter().collect(),
        series_indexes: series
            .indexes
            .into_iter()
            .map(|entry| {
                (
                    entry.index_uuid,
                    SeriesIndexFileHandle::new(region_id, entry, purger.clone()),
                )
            })
            .collect(),
    };
    let control = SeriesIndexVersionControl::default();
    control.publish(std::sync::Arc::new(version));
    control
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::layers::mock::{self, MockLayerBuilder};
    use object_store::services::Memory;

    use super::super::purger::series_index_channel;
    use super::*;
    struct FailingCatalogReader;

    impl mock::Read for FailingCatalogReader {
        async fn read(&mut self) -> mock::Result<mock::Buffer> {
            Err(mock::Error::new(
                mock::ErrorKind::Unexpected,
                "injected catalog read failure",
            ))
        }
    }

    #[tokio::test]
    async fn test_load_catalog_defaults_on_missing_invalid_or_unreadable_catalog() {
        let store = ObjectStore::new(Memory::default()).unwrap().finish();
        let region_id = RegionId::new(1, 1);
        let (purger, _receiver) = series_index_channel(store.clone());
        let control = load_version_control(&store, region_id, &purger).await;
        assert!(control.current().range_indexes.is_empty());
        assert!(control.current().series_indexes.is_empty());
        let file_id = store_api::storage::FileId::random();
        store
            .write(
                &range_catalog_path(region_id),
                serde_json::to_vec(&RangeIndexCatalog {
                    indexes: vec![file_id],
                })
                .unwrap(),
            )
            .await
            .unwrap();
        store
            .write(&series_catalog_path(region_id), "invalid")
            .await
            .unwrap();
        let control = load_version_control(&store, region_id, &purger).await;
        assert!(control.current().range_indexes.contains(&file_id));
        assert!(control.current().series_indexes.is_empty());
        let layer = MockLayerBuilder::default()
            .reader_factory(Arc::new(|_, _, _| Box::new(FailingCatalogReader)))
            .build()
            .unwrap();
        let control = load_version_control(&store.layer(layer), region_id, &purger).await;
        assert!(control.current().range_indexes.is_empty());
        assert!(control.current().series_indexes.is_empty());
    }

    #[tokio::test]
    async fn test_catalog_roundtrip_preserves_coverage() {
        let store = ObjectStore::new(Memory::default()).unwrap().finish();
        let region_id = RegionId::new(1, 1);
        let entry = SeriesIndexEntry {
            index_uuid: FileId::random(),
            bucket_start: Timestamp::new_second(0),
            bucket_end: Timestamp::new_second(100),
            source_file_ids: vec![FileId::random()],
            min_file_sequence: 1,
            max_file_sequence: 2,
        };
        store_catalog(
            &store,
            &series_catalog_path(region_id),
            &SeriesIndexCatalog {
                indexes: vec![entry.clone()],
            },
        )
        .await
        .unwrap();
        let (purger, _receiver) = series_index_channel(store.clone());
        let restored = load_version_control(&store, region_id, &purger)
            .await
            .current();
        assert_eq!(&entry, restored.series_indexes[&entry.index_uuid].entry());
        let metadata = series_metadata(&entry).unwrap();
        let decoded: SeriesIndexEntry =
            serde_json::from_str(metadata[0].value.as_ref().unwrap()).unwrap();
        assert_eq!(entry, decoded);
    }
    #[tokio::test]
    async fn test_region_open_restores_catalog_once() {
        use store_api::codec::PrimaryKeyEncoding;
        use store_api::metric_engine_consts::PRIMARY_KEY_ENCODING;
        use store_api::region_engine::RegionEngine;
        use store_api::region_request::RegionRequest;

        use crate::access_layer::new_fs_cache_store;
        use crate::config::MitoConfig;
        use crate::test_util::sst_util::sst_region_metadata_with_encoding;
        use crate::test_util::{CreateRequestBuilder, TestEnv, reopen_region};

        let mut env = TestEnv::with_prefix("series-open").await;
        let engine = env
            .create_engine(MitoConfig {
                experimental_series_index_root: "indexes".to_string(),
                ..Default::default()
            })
            .await;
        let metadata = sst_region_metadata_with_encoding(PrimaryKeyEncoding::Sparse);
        let region_id = RegionId::new(1, 1);
        let mut request = CreateRequestBuilder::new().build();
        request.column_metadatas = metadata.column_metadatas.clone();
        request.primary_key = metadata.primary_key.clone();
        request
            .options
            .insert(PRIMARY_KEY_ENCODING.to_string(), "sparse".to_string());
        let table_dir = request.table_dir.clone();
        let options = request.options.clone();
        engine
            .handle_request(region_id, RegionRequest::Create(request))
            .await
            .unwrap();
        assert!(
            engine
                .get_region(region_id)
                .unwrap()
                .series_index_version()
                .series_indexes
                .is_empty()
        );
        let store = new_fs_cache_store(env.data_home().join("indexes").to_str().unwrap())
            .await
            .unwrap();
        let entry = SeriesIndexEntry {
            index_uuid: FileId::random(),
            bucket_start: Timestamp::new_second(0),
            bucket_end: Timestamp::new_second(100),
            source_file_ids: vec![FileId::random()],
            min_file_sequence: 1,
            max_file_sequence: 2,
        };
        store_catalog(
            &store,
            &range_catalog_path(region_id),
            &RangeIndexCatalog {
                indexes: entry.source_file_ids.clone(),
            },
        )
        .await
        .unwrap();
        store_catalog(
            &store,
            &series_catalog_path(region_id),
            &SeriesIndexCatalog {
                indexes: vec![entry.clone()],
            },
        )
        .await
        .unwrap();
        // Index files deliberately do not exist: opening trusts the catalogs.
        reopen_region(
            &engine,
            region_id,
            table_dir.clone(),
            false,
            options.clone(),
        )
        .await;
        let region = engine.get_region(region_id).unwrap();
        let current = region.series_index_version();
        assert!(current.range_indexes.contains(&entry.source_file_ids[0]));
        assert_eq!(&entry, current.series_indexes[&entry.index_uuid].entry());
        store
            .write(&series_catalog_path(region_id), "invalid")
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&current, &region.series_index_version()));
        // Only a subsequent opening reloads the catalog and applies its empty fallback.
        reopen_region(&engine, region_id, table_dir, false, options).await;
        assert!(
            engine
                .get_region(region_id)
                .unwrap()
                .series_index_version()
                .series_indexes
                .is_empty()
        );
        engine.stop().await.unwrap();
    }
}
