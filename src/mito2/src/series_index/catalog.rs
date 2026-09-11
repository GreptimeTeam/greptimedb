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

use std::collections::BTreeMap;

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
pub(crate) use crate::sst::range_index::range_index_path;

const SERIES_DIR: &str = "series";
const RANGE_CATALOG: &str = "range-index.json";
const SERIES_CATALOG: &str = "series-index.json";
const SERIES_METADATA_KEY: &str = "greptime.series_index";

/// Summary of SSTs sharing a compaction-window-aligned start.
///
/// New data changes must have sequences greater than those already indexed. A new
/// start adds a map entry; new data at an existing start raises its maximum sequence.
/// The maximum end and sequence may come from different files, so this summary
/// does not establish uniform sequence coverage throughout the interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WindowSequence {
    /// Inclusive start in epoch seconds, equal to the key in `window_sequences`.
    pub(crate) start: i64,
    /// Exclusive interval end in epoch seconds.
    pub(crate) end: i64,
    /// Maximum sequence among the SSTs sharing this aligned start.
    pub(crate) max_sequence: u64,
}

/// Self-describing coverage stored in a series-index Parquet footer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SeriesIndexEntry {
    pub(crate) index_uuid: FileId,
    /// Inclusive bucket start.
    pub(crate) bucket_start: Timestamp,
    /// Exclusive bucket end.
    pub(crate) bucket_end: Timestamp,
    /// Source SST IDs retained for debugging only. Compaction can replace these
    /// files without changing indexed data, so IDs must not determine index reuse.
    pub(crate) source_file_ids: Vec<FileId>,
    pub(crate) min_file_sequence: u64,
    pub(crate) max_file_sequence: u64,
    /// Width used to align the half-open compaction windows, in seconds.
    pub(crate) compaction_window_secs: i64,
    /// Source SST summaries keyed by aligned start; intervals may overlap.
    /// Each file contributes one summary regardless of its span. Equal starts merge
    /// by taking the maximum end and sequence. See [`WindowSequence`] for the
    /// sequence assumption used to detect new data. Compaction changing summary
    /// boundaries may conservatively trigger a rebuild.
    pub(crate) window_sequences: BTreeMap<i64, WindowSequence>,
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

/// Best-effort removal of both catalogs when dropping a region.
pub(crate) async fn delete_catalogs(store: &ObjectStore, region_id: RegionId) {
    for path in [
        series_catalog_path(region_id),
        range_catalog_path(region_id),
    ] {
        if let Err(error) = store.delete(&path).await
            && error.kind() != ErrorKind::NotFound
        {
            warn!(error; "Failed to delete index catalog, path: {path}");
        }
    }
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
    let version = SeriesIndexVersion::new(
        range.indexes.into_iter().collect(),
        series
            .indexes
            .into_iter()
            .map(|entry| {
                (
                    entry.index_uuid,
                    SeriesIndexFileHandle::new(region_id, entry, purger.clone()),
                )
            })
            .collect(),
    );
    let control = SeriesIndexVersionControl::default();
    control.publish(std::sync::Arc::new(version));
    control
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use common_time::Timestamp;
    use object_store::ObjectStore;
    use object_store::layers::mock::{self, MockLayerBuilder};
    use object_store::services::Memory;
    use store_api::storage::{FileId, RegionId};

    use crate::series_index::catalog::{
        RangeIndexCatalog, SeriesIndexCatalog, SeriesIndexEntry, WindowSequence, load_catalog,
        load_version_control, range_catalog_path, series_catalog_path, series_metadata,
        store_catalog,
    };
    use crate::series_index::purger::series_index_channel;

    struct FailingCatalogReader;

    impl mock::Read for FailingCatalogReader {
        async fn read(
            &self,
            _range: mock::BytesRange,
        ) -> mock::Result<(mock::RpRead, mock::Buffer)> {
            Err(mock::Error::new(
                mock::ErrorKind::Unexpected,
                "injected catalog read failure",
            ))
        }

        async fn open(
            &self,
            _range: mock::BytesRange,
        ) -> mock::Result<(mock::RpRead, Box<dyn mock::ReadStreamDyn>)> {
            Err(mock::Error::new(
                mock::ErrorKind::Unexpected,
                "injected catalog read failure",
            ))
        }
    }

    #[tokio::test]
    async fn test_load_catalog_defaults_on_missing_invalid_or_unreadable_catalog() {
        let store = ObjectStore::new(Memory::default()).unwrap();
        let region_id = RegionId::new(1, 1);
        let (purger, _receiver) = series_index_channel(store.clone());
        assert!(
            load_catalog::<SeriesIndexCatalog>(&store, &series_catalog_path(region_id))
                .await
                .is_none()
        );
        let control = load_version_control(&store, region_id, &purger).await;
        assert!(control.current().range_indexes.is_empty());
        assert!(control.current().series_indexes.is_empty());
        let file_id = FileId::random();
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
        assert!(
            load_catalog::<SeriesIndexCatalog>(&store, &series_catalog_path(region_id))
                .await
                .is_none()
        );
        let store = store.layer(layer);
        assert!(
            load_catalog::<RangeIndexCatalog>(&store, &range_catalog_path(region_id))
                .await
                .is_none()
        );
        let control = load_version_control(&store, region_id, &purger).await;
        assert!(control.current().range_indexes.is_empty());
        assert!(control.current().series_indexes.is_empty());
    }

    #[tokio::test]
    async fn test_catalog_roundtrip() {
        let store = ObjectStore::new(Memory::default()).unwrap();
        let region_id = RegionId::new(1, 1);
        let entry = SeriesIndexEntry {
            index_uuid: FileId::random(),
            bucket_start: Timestamp::new_second(0),
            bucket_end: Timestamp::new_second(100),
            source_file_ids: vec![FileId::random()],
            min_file_sequence: 1,
            max_file_sequence: 2,
            compaction_window_secs: 10,
            window_sequences: BTreeMap::from([
                (
                    0,
                    WindowSequence {
                        start: 0,
                        end: 20,
                        max_sequence: 1,
                    },
                ),
                (
                    20,
                    WindowSequence {
                        start: 20,
                        end: 100,
                        max_sequence: 2,
                    },
                ),
            ]),
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
        let current = load_version_control(&store, region_id, &purger)
            .await
            .current();
        assert!(current.range_indexes.is_empty());
        assert_eq!(&entry, current.series_indexes[&entry.index_uuid].entry());
        assert_eq!(current.index_buckets.len(), 1);
        let bucket = &current.index_buckets[&entry.bucket_start];
        assert_eq!(bucket.start, entry.bucket_start);
        assert_eq!(bucket.end, entry.bucket_end);
        assert_eq!(bucket.index_ids.as_slice(), &[entry.index_uuid]);
        assert_eq!(bucket.compaction_window_secs, entry.compaction_window_secs);
        assert_eq!(bucket.window_sequences, entry.window_sequences);

        let metadata = series_metadata(&entry).unwrap();
        let decoded: SeriesIndexEntry =
            serde_json::from_str(metadata[0].value.as_ref().unwrap()).unwrap();
        assert_eq!(entry, decoded);
    }
}
