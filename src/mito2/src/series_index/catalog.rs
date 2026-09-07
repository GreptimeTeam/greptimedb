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

use crate::error::{OpenDalSnafu, Result, SerdeJsonSnafu};
use crate::series_index::purger::IndexFilePurger;
use crate::series_index::version::{
    SeriesIndexFileHandle, SeriesIndexVersion, SeriesIndexVersionControl,
};
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

pub(crate) fn series_metadata(entry: &SeriesIndexEntry) -> Result<Vec<KeyValue>> {
    Ok(vec![KeyValue::new(
        SERIES_METADATA_KEY.to_string(),
        Some(serde_json::to_string(entry).context(SerdeJsonSnafu)?),
    )])
}

pub(crate) async fn load_catalog<T>(store: &ObjectStore, path: &str) -> Option<T>
where
    T: DeserializeOwned,
{
    let bytes = match store.read(path).await {
        Ok(bytes) => bytes.to_bytes(),
        Err(error) if error.kind() == ErrorKind::NotFound => return None,
        Err(error) => {
            warn!(error; "Failed to load series-index catalog, path: {path}");
            return None;
        }
    };
    match serde_json::from_slice(&bytes) {
        Ok(catalog) => Some(catalog),
        Err(error) => {
            warn!(error; "Invalid series-index catalog, path: {path}, phase: load");
            None
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
    let range = load_catalog::<RangeIndexCatalog>(store, &range_catalog_path(region_id))
        .await
        .unwrap_or_default();
    let series = load_catalog::<SeriesIndexCatalog>(store, &series_catalog_path(region_id))
        .await
        .unwrap_or_default();
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

    use common_time::Timestamp;
    use object_store::ObjectStore;
    use object_store::layers::mock::{self, MockLayerBuilder};
    use object_store::services::Memory;
    use store_api::storage::{FileId, RegionId};

    use crate::series_index::catalog::{
        RangeIndexCatalog, SeriesIndexCatalog, SeriesIndexEntry, load_catalog, range_catalog_path,
        series_catalog_path, series_metadata, store_catalog,
    };
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
    async fn test_load_catalog_returns_none_on_error() {
        let store = ObjectStore::new(Memory::default()).unwrap().finish();
        let path = series_catalog_path(RegionId::new(1, 1));
        // Missing catalog.
        assert!(
            load_catalog::<SeriesIndexCatalog>(&store, &path)
                .await
                .is_none()
        );
        store.write(&path, "invalid").await.unwrap();
        assert!(
            load_catalog::<SeriesIndexCatalog>(&store, &path)
                .await
                .is_none()
        );
        let layer = MockLayerBuilder::default()
            .reader_factory(Arc::new(|_, _, _| Box::new(FailingCatalogReader)))
            .build()
            .unwrap();
        let store = store.layer(layer);
        assert!(
            load_catalog::<SeriesIndexCatalog>(&store, &path)
                .await
                .is_none()
        );
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
        let metadata = series_metadata(&entry).unwrap();
        let decoded: SeriesIndexEntry =
            serde_json::from_str(metadata[0].value.as_ref().unwrap()).unwrap();
        assert_eq!(entry, decoded);
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
        let current = engine.get_region(region_id).unwrap().series_index_version();
        assert!(current.series_indexes.is_empty());
        assert!(current.range_indexes.contains(&entry.source_file_ids[0]));
        engine.stop().await.unwrap();
    }
}
