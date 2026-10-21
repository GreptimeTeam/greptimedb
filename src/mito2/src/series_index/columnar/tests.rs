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

//! Wire fixtures, corruption handling, range selection and artifact lifecycle.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use common_recordbatch::filter::SimpleFilterEvaluator;
use datafusion_expr::Operator;
use datatypes::arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryDictionaryBuilder, StringArray, UInt8Array, UInt32Array,
    UInt64Array,
};
use datatypes::arrow::datatypes::{DataType, Field, Schema, UInt32Type};
use datatypes::arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use object_store::ObjectStore;
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::FileId;

use super::codec::{self, Compression};
use super::metadata::{self, PayloadReference, Trailer};
use super::{ColumnarSeriesFormat, Manifest};
use crate::series_index::catalog::SeriesIndexEntry;
use crate::series_index::format::{SeriesIndexFormat, SeriesReadRequest};
use crate::series_index::{
    MetricSeriesId, ROW_COUNT_COLUMN, SeriesIndexFileHandle, TABLE_ID_COLUMN, TSID_COLUMN,
    series_index_channel,
};
use crate::test_util::sst_util::{new_sparse_primary_key, sst_region_metadata_with_encoding};

fn metadata() -> RegionMetadataRef {
    Arc::new(sst_region_metadata_with_encoding(
        PrimaryKeyEncoding::Sparse,
    ))
}
fn store() -> ObjectStore {
    ObjectStore::new(object_store::services::Memory::default()).unwrap()
}
fn entry() -> SeriesIndexEntry {
    let value: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/compatibility/fixtures/series_index/legacy_catalog.json"
    ))
    .unwrap();
    let mut entry: SeriesIndexEntry = serde_json::from_value(value["indexes"][0].clone()).unwrap();
    entry.index_uuid = FileId::random();
    entry.file_size = 0;
    entry
}
fn batch(keys: &[Vec<u8>], dictionary: bool, timestamp: i64) -> RecordBatch {
    let keys: ArrayRef = if dictionary {
        let mut builder = BinaryDictionaryBuilder::<UInt32Type>::new();
        for key in keys {
            builder.append(key).unwrap();
        }
        Arc::new(builder.finish())
    } else {
        Arc::new(BinaryArray::from_iter_values(keys))
    };
    // A non-timestamp column deliberately proves this writer never decodes timestamps.
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("__primary_key", keys.data_type().clone(), false),
        Field::new("__sequence", DataType::UInt64, false),
        Field::new("__op_type", DataType::UInt8, false),
    ]));
    let n = keys.len();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(datatypes::arrow::array::Int64Array::from(vec![
                timestamp;
                n
            ])),
            keys,
            Arc::new(UInt64Array::from(vec![1; n])),
            Arc::new(UInt8Array::from(vec![0; n])),
        ],
    )
    .unwrap()
}
async fn build(
    format: &ColumnarSeriesFormat,
    store: &ObjectStore,
    batches: &[RecordBatch],
) -> SeriesIndexFileHandle {
    let meta = metadata();
    let mut entry = entry();
    let mut writer = format
        .writer(meta.clone(), store.clone(), &entry)
        .await
        .unwrap();
    for batch in batches {
        writer.write(batch).await.unwrap();
    }
    let artifact = writer.finish().await.unwrap();
    entry.file_size = artifact.file_size;
    entry.artifact = Some(artifact);
    let (purger, _) = series_index_channel(store.clone());
    SeriesIndexFileHandle::new(meta.region_id, entry, purger)
}
async fn manifest(store: &ObjectStore, handle: &SeriesIndexFileHandle) -> Manifest {
    let path = handle
        .entry()
        .artifact
        .as_ref()
        .unwrap()
        .objects
        .iter()
        .find(|p| p.ends_with("manifest.json"))
        .unwrap();
    serde_json::from_slice(&store.read(path).await.unwrap().to_bytes()).unwrap()
}
fn request(names: &[&str]) -> SeriesReadRequest {
    SeriesReadRequest {
        projection: names.iter().map(|n| (*n).to_owned()).collect(),
        ..Default::default()
    }
}

#[test]
fn integer_and_dictionary_wire() {
    let values = [0, 1, u64::MAX, 0, 1 << 63, 7, 7];
    let bytes = codec::encode_integers(&values);
    assert_eq!(
        codec::decode_integers(&bytes, values.len()).unwrap(),
        values
    );
    assert_eq!(codec::encode_integers(&[1, 3, 2]), [2, 4, 1]);
    assert!(codec::decode_integers(&[255; 10], 1).is_err());
    assert!(codec::decode_integers(&[0], 2).is_err());
    assert!(codec::decode_integers(&[0, 0], 1).is_err());
    let values = vec![
        None,
        Some(String::new()),
        Some("a".into()),
        None,
        Some("a".into()),
    ];
    let decoded = codec::decode_dictionary(&codec::encode_dictionary(&values).unwrap(), 5).unwrap();
    assert_eq!(decoded.codes, [0, 1, 2, 0, 2]);
    assert_eq!(
        decoded.values,
        [None, Some(String::new()), Some("a".into())]
    );
    let mut bytes = codec::encode_dictionary(&values).unwrap();
    *bytes.last_mut().unwrap() = 127;
    assert!(codec::decode_dictionary(&bytes, 5).is_err());
    assert!(codec::decode_dictionary(&[255; 32], usize::MAX).is_err());
}

#[test]
fn references_beyond_four_gib_and_bounded_decode() {
    let offset = (1u64 << 32) + 19;
    for compression in [Compression::None, Compression::Zstd] {
        let (reference, bytes) =
            metadata::pack(&[2, 4, 1], offset, 1, compression, 9, false).unwrap();
        assert_eq!(reference.unpack(&bytes).unwrap(), [2, 4, 1]);
        reference
            .validate(metadata::HEADER_SIZE, reference.end().unwrap(), false)
            .unwrap();
        let mut encoded = Vec::new();
        reference.encode(&mut encoded);
        assert_eq!(encoded.len(), 32);
        assert_eq!(
            PayloadReference::decode(&mut codec::Cursor::new(&encoded))
                .unwrap()
                .offset,
            offset
        );
        let mut invalid = reference;
        invalid.offset = u64::MAX;
        assert!(invalid.end().is_err());
        invalid = reference;
        invalid.decoded = u32::MAX;
        assert!(invalid.validate(0, u64::MAX, false).is_err());
        invalid = reference;
        invalid.decoded = 1;
        assert!(invalid.unpack(&bytes).is_err());
        invalid = reference;
        invalid.compression = 42;
        assert!(invalid.unpack(&bytes).is_err());
    }
    let bomb = codec::compress(&vec![0; 1024 * 1024], Compression::Zstd, 3).unwrap();
    assert!(codec::decompress(&bomb, 1, 16).is_err());
}

#[tokio::test]
async fn aggregation_projection_and_independent_reads() {
    let store = store();
    let meta = metadata();
    let format = ColumnarSeriesFormat {
        block_rows: 2,
        ..Default::default()
    };
    let a = new_sparse_primary_key(&["", "x"], &meta, 1, 10);
    let b = new_sparse_primary_key(&["b", "y"], &meta, 1, 20);
    let c = new_sparse_primary_key(&["c", "z"], &meta, 2, 10);
    let handle = build(
        &format,
        &store,
        &[
            batch(&[a.clone(), a.clone()], true, 1),
            batch(&[a, b, c], false, 999),
        ],
    )
    .await;
    let reader = format.reader(store.clone(), handle.clone()).await.unwrap();
    assert_eq!(reader.schema().fields().len(), 5);
    assert!(reader.schema().index_of("__series_min_ts").is_err());
    let req = request(&[TABLE_ID_COLUMN, TSID_COLUMN, ROW_COUNT_COLUMN, "tag_0"]);
    let one = reader.read(req.clone()).unwrap();
    let two = reader.read(req).unwrap();
    drop(reader);
    let one = one.try_collect::<Vec<_>>().await.unwrap();
    let two = two.try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(one, two);
    assert_eq!(one.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
    assert_eq!(
        one[0]
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values(),
        &[3, 1]
    );
    assert_eq!(
        one[1]
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .value(0),
        2
    );
    let reader = format.reader(store.clone(), handle).await.unwrap();
    let mut req = request(&["tag_0", ROW_COUNT_COLUMN]);
    req.filters
        .push(SimpleFilterEvaluator::new("tag_0".into(), "b", Operator::Eq).unwrap());
    let batches = reader
        .read(req)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "b"
    );
    let mut req = request(&[]);
    req.ids = Some(BTreeSet::from([MetricSeriesId {
        table_id: 2,
        tsid: 10,
    }]));
    let batches = reader
        .read(req)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(batches[0].num_columns(), 0);
    assert_eq!(batches[0].num_rows(), 1);
    let mut req = request(&[]);
    req.ids = Some(BTreeSet::new());
    assert!(
        reader
            .read(req)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
    assert!(reader.read(request(&["missing"])).is_err());
    let mut req = request(&[]);
    req.filters
        .push(SimpleFilterEvaluator::new("__series_min_ts".into(), 1i64, Operator::Eq).unwrap());
    assert!(reader.read(req).is_err());
}

#[tokio::test]
async fn empty_nullable_and_block_row_options() {
    use datatypes::value::ValueRef;
    use mito_codec::row_converter::PrimaryKeyCodec;
    use mito_codec::row_converter::SparsePrimaryKeyCodec;
    use store_api::storage::consts::ReservedColumnId;
    let meta = metadata();
    let store = store();
    let format = ColumnarSeriesFormat::default();
    let empty = build(&format, &store, &[]).await;
    assert!(manifest(&store, &empty).await.files.is_empty());
    assert_eq!(empty.entry().artifact.as_ref().unwrap().objects.len(), 1);
    assert!(
        format
            .reader(store.clone(), empty)
            .await
            .unwrap()
            .read(request(&[]))
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
    let codec = SparsePrimaryKeyCodec::new(&meta);
    let mut key = Vec::new();
    codec
        .encode_value_refs(
            &[
                (ReservedColumnId::table_id(), ValueRef::UInt32(1)),
                (ReservedColumnId::tsid(), ValueRef::UInt64(1)),
                (0, ValueRef::String("")),
            ],
            &mut key,
        )
        .unwrap();
    let handle = build(&format, &store, &[batch(&[key], false, 0)]).await;
    let reader = format.reader(store.clone(), handle).await.unwrap();
    let batches = reader
        .read(request(&["tag_0", "tag_1"]))
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(batches[0].column(0).null_count(), 0);
    assert_eq!(batches[0].column(1).null_count(), 1);
    let mut req = request(&[]);
    req.filters
        .push(SimpleFilterEvaluator::new("tag_1".into(), "", Operator::Eq).unwrap());
    assert!(
        reader
            .read(req)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
    for block_rows in [1, 16384] {
        let format = ColumnarSeriesFormat {
            block_rows,
            compression: Compression::None,
            ..Default::default()
        };
        let n = if block_rows == 1 { 3 } else { 16385 };
        let keys: Vec<_> = (0..n)
            .map(|tsid| new_sparse_primary_key(&["a", "x"], &meta, 1, tsid))
            .collect();
        let handle = build(&format, &store, &[batch(&keys, false, 0)]).await;
        let batches = format
            .reader(store.clone(), handle)
            .await
            .unwrap()
            .read(request(&[TSID_COLUMN]))
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            n as usize
        );
        assert_eq!(batches[0].num_rows(), block_rows);
    }
}

#[tokio::test]
async fn two_levels_rollover_and_corrupt_framing() {
    let store = store();
    let meta = metadata();
    // One descriptor per leaf and two leaves per root force rollover before depth 3.
    let format = ColumnarSeriesFormat {
        block_rows: 1,
        page_bytes: 64,
        root_capacity: 2,
        compression: Compression::None,
        ..Default::default()
    };
    let keys: Vec<_> = (0..5)
        .map(|tsid| new_sparse_primary_key(&["a", "x"], &meta, 1, tsid))
        .collect();
    let handle = build(&format, &store, &[batch(&keys, false, 0)]).await;
    let manifest = manifest(&store, &handle).await;
    assert_eq!(manifest.files.len(), 3);
    for part in &manifest.files {
        let bytes = store
            .read(&manifest.part_path(part.part_id))
            .await
            .unwrap()
            .to_bytes();
        let trailer = Trailer::decode(&bytes[bytes.len() - 96..], bytes.len() as u64).unwrap();
        assert_eq!(trailer.depth, if part.part_id == 2 { 1 } else { 2 });
        for depth in [0u32, 3] {
            let mut tail = bytes[bytes.len() - 96..].to_vec();
            tail[56..60].copy_from_slice(&depth.to_le_bytes());
            let sum = codec::checksum(&tail[..88]);
            tail[88..].copy_from_slice(&sum.to_le_bytes());
            assert!(Trailer::decode(&tail, bytes.len() as u64).is_err());
        }
        for length in 0..96 {
            assert!(Trailer::decode(&bytes[bytes.len() - length..], bytes.len() as u64).is_err());
        }
    }
    let reader = format.reader(store.clone(), handle.clone()).await.unwrap();
    assert_eq!(
        reader
            .read(request(&[TSID_COLUMN]))
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len(),
        5
    );
    let path = manifest.part_path(0);
    let mut bytes = store.read(&path).await.unwrap().to_vec();
    bytes[16] ^= 1;
    store.write(&path, bytes).await.unwrap();
    assert!(
        reader
            .read(request(&[TSID_COLUMN]))
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .is_err()
    );
    // Corrupt unrequested integer bytes do not cause unrelated projected columns to decode.
    assert!(
        reader
            .read(request(&["tag_0"]))
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn explicit_abort_drop_and_failed_finish_cleanup() {
    let store = store();
    let meta = metadata();
    let format = ColumnarSeriesFormat {
        block_rows: 1,
        part_bytes: 1,
        ..Default::default()
    };
    let keys: Vec<_> = (0..3)
        .map(|tsid| new_sparse_primary_key(&["a", "x"], &meta, 1, tsid))
        .collect();
    for drop_writer in [false, true] {
        let entry = entry();
        let prefix = format!("{}/series/{}/", meta.region_id.as_u64(), entry.index_uuid);
        let mut writer = format
            .writer(meta.clone(), store.clone(), &entry)
            .await
            .unwrap();
        writer.write(&batch(&keys, false, 0)).await.unwrap();
        assert!(
            store
                .exists(&format!("{prefix}part-0.series"))
                .await
                .unwrap()
        );
        if drop_writer {
            drop(writer);
        } else {
            writer.abort().await.unwrap();
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if store
                    .list_with(&prefix)
                    .recursive(true)
                    .await
                    .unwrap()
                    .iter()
                    .all(|e| e.metadata().is_dir())
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    let entry = entry();
    let prefix = format!("{}/series/{}/", meta.region_id.as_u64(), entry.index_uuid);
    let mut writer = format.writer(meta, store.clone(), &entry).await.unwrap();
    writer.write(&batch(&keys, false, 0)).await.unwrap();
    assert!(writer.write(&batch(&keys, false, 0)).await.is_err());
    assert!(writer.finish().await.is_err());
    assert!(
        store
            .list_with(&prefix)
            .recursive(true)
            .await
            .unwrap()
            .iter()
            .all(|e| e.metadata().is_dir())
    );
}

#[tokio::test]
async fn frozen_v1_fixture_and_selective_io() {
    use object_store::layers::mock::MockLayerBuilder;
    use std::sync::Mutex;
    const MANIFEST: &[u8] = include_bytes!(
        "../../../../../tests/compatibility/fixtures/series_index/columnar_v1/manifest.json"
    );
    const PART: &[u8] = include_bytes!(
        "../../../../../tests/compatibility/fixtures/series_index/columnar_v1/part-0.series"
    );
    let manifest: Manifest = serde_json::from_slice(MANIFEST).unwrap();
    let store = store();
    store.write(&manifest.path(), MANIFEST).await.unwrap();
    store.write(&manifest.part_path(0), PART).await.unwrap();
    let mut entry = manifest.coverage.clone();
    entry.file_size = (MANIFEST.len() + PART.len()) as u64;
    entry.artifact = Some(crate::series_index::artifact::ArtifactMetadata {
        format: crate::series_index::artifact::ArtifactFormat::ColumnarSeries,
        generation: manifest.generation,
        objects: vec![manifest.part_path(0), manifest.path()],
        file_size: entry.file_size,
    });
    let (purger, mut purges) = series_index_channel(store.clone());
    let handle = SeriesIndexFileHandle::new(
        store_api::storage::RegionId::from_u64(manifest.region_id),
        entry,
        purger,
    );
    let reads = Arc::new(Mutex::new(Vec::new()));
    let observed = reads.clone();
    let layer = MockLayerBuilder::default()
        .reader_factory(Arc::new(move |path, _, inner| {
            Box::new(RecordingRead {
                inner,
                path: path.to_owned(),
                reads: observed.clone(),
            })
        }))
        .build()
        .unwrap();
    let format = ColumnarSeriesFormat::default();
    let reader = format
        .reader(store.layer(layer), handle.clone())
        .await
        .unwrap();
    let all = reader
        .read(request(&[
            TABLE_ID_COLUMN,
            TSID_COLUMN,
            ROW_COUNT_COLUMN,
            "tag_0",
            "tag_1",
        ]))
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(
        all[0]
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values(),
        &[10, 11]
    );
    assert_eq!(
        all[0]
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values(),
        &[2, 3]
    );
    assert_eq!(
        all[0]
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap(),
        &StringArray::from(vec![None, Some("")])
    );
    assert_eq!(
        all[1]
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "é"
    );
    assert_eq!(all[0].column(4).null_count(), 2);
    reads.lock().unwrap().clear();
    let mut req = request(&["tag_0"]);
    req.table_id = Some(9);
    let stream = reader.read(req).unwrap();
    handle.mark_deleted();
    drop(handle);
    drop(reader);
    assert!(purges.try_recv().is_err());
    let result = stream.try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(result.len(), 1);
    assert!(purges.try_recv().is_ok());
    // Framing, root, one leaf, and one tag payload. No TSID/count chunks or other leaf.
    let reads = reads.lock().unwrap();
    assert_eq!(
        reads.iter().filter(|(p, _)| p.ends_with(".series")).count(),
        4
    );
}

#[tokio::test]
async fn real_format_reconciliation_and_fallback() {
    use crate::config::MitoConfig;
    use crate::series_index::format::SeriesFormats;
    use crate::series_index::maintenance::reconcile_series_indexes;
    use crate::test_util::TestEnv;
    use store_api::region_engine::RegionEngine;
    let mut env = TestEnv::with_prefix("columnar-reconcile").await;
    let timestamps = [1000, 2000, 3000, 4000, 101000, 102000, 103000, 104000];
    let (engine, region) = crate::series_index::tests::prepare_region_with_config(
        &mut env,
        &timestamps,
        MitoConfig {
            experimental_enable_series_index: true,
            experimental_series_scan_v2: true,
            experimental_series_index_maintenance_interval: Duration::from_secs(3600),
            min_compaction_interval: Duration::from_secs(3600),
            ..Default::default()
        },
    )
    .await;
    let format = Arc::new(ColumnarSeriesFormat::default());
    *region.series_index_version_control.formats.write().unwrap() = Arc::new(SeriesFormats {
        index: Some(format.clone()),
        ..Default::default()
    });
    let store = region.series_index_store.clone().unwrap();
    let (purger, mut purges) = series_index_channel(store.clone());
    let reconcile = || {
        reconcile_series_indexes(
            0,
            store.clone(),
            region.clone(),
            Duration::from_secs(100),
            0,
            purger.clone(),
            false,
            true,
        )
    };
    assert_eq!(reconcile().await.unwrap().built_series, 2);
    let snapshot = region.series_index_version();
    for handle in snapshot.series_indexes.values() {
        let reader = format.reader(store.clone(), handle.clone()).await.unwrap();
        let batches = reader
            .read(request(&[ROW_COUNT_COLUMN]))
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    }
    assert_eq!(reconcile().await.unwrap().built_series, 0);
    let reopened =
        crate::series_index::catalog::load_version_control(&store, region.region_id, &purger).await;
    assert_eq!(reopened.current().series_indexes.len(), 2);
    let scanner = engine
        .scanner(
            region.region_id,
            store_api::storage::ScanRequest {
                distribution: Some(store_api::storage::TimeSeriesDistribution::PerSeries),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let batches = scanner
        .scan()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        timestamps.len()
    );
    let catalog_path = crate::series_index::catalog::series_catalog_path(region.region_id);
    let old_catalog = store.read(&catalog_path).await.unwrap().to_bytes();
    let old = snapshot.series_indexes.values().next().unwrap();
    store
        .delete(&old.entry().artifact.as_ref().unwrap().objects[0])
        .await
        .unwrap();
    let layer = object_store::layers::mock::MockLayerBuilder::default()
        .writer_factory(Arc::new(|path, _, inner| {
            if path.ends_with("series-index.json") {
                Box::new(FailedOutput {
                    inner,
                    fail_write: false,
                })
            } else {
                inner
            }
        }))
        .build()
        .unwrap();
    assert!(
        reconcile_series_indexes(
            0,
            store.clone().layer(layer),
            region.clone(),
            Duration::from_secs(100),
            0,
            purger.clone(),
            false,
            true
        )
        .await
        .is_err()
    );
    assert!(Arc::ptr_eq(&snapshot, &region.series_index_version()));
    assert_eq!(
        old_catalog,
        store.read(&catalog_path).await.unwrap().to_bytes()
    );
    let cleanup = purges.try_recv().unwrap().objects.unwrap();
    assert_eq!(cleanup.len(), 2);
    assert!(cleanup.iter().all(|path| {
        !old.entry()
            .artifact
            .as_ref()
            .unwrap()
            .objects
            .contains(path)
    }));
    engine.stop().await.unwrap();
}

struct FailedOutput {
    inner: object_store::layers::mock::oio::Writer,
    fail_write: bool,
}
impl object_store::layers::mock::Write for FailedOutput {
    async fn write(&mut self, buffer: object_store::Buffer) -> object_store::Result<()> {
        if self.fail_write {
            return Err(object_store::Error::new(
                object_store::ErrorKind::Unexpected,
                "injected series write failure",
            ));
        }
        self.inner.write(buffer).await
    }
    async fn close(&mut self) -> object_store::Result<object_store::layers::mock::Metadata> {
        Err(object_store::Error::new(
            object_store::ErrorKind::Unexpected,
            "injected series close failure",
        ))
    }
    async fn abort(&mut self) -> object_store::Result<()> {
        self.inner.abort().await
    }
}

#[tokio::test]
async fn opendal_write_close_and_manifest_failures() {
    use object_store::layers::mock::MockLayerBuilder;
    let meta = metadata();
    let keys: Vec<_> = (0..3)
        .map(|tsid| new_sparse_primary_key(&["a", "x"], &meta, 1, tsid))
        .collect();
    for (manifest_only, fail_write) in [(false, true), (false, false), (true, false)] {
        let store = store();
        let layer = MockLayerBuilder::default()
            .writer_factory(Arc::new(move |path, _, inner| {
                if !manifest_only || path.ends_with("manifest.json") {
                    Box::new(FailedOutput { inner, fail_write })
                } else {
                    inner
                }
            }))
            .build()
            .unwrap();
        let entry = entry();
        let prefix = format!("{}/series/{}/", meta.region_id.as_u64(), entry.index_uuid);
        let format = ColumnarSeriesFormat {
            block_rows: 1,
            ..Default::default()
        };
        let mut writer = format
            .writer(meta.clone(), store.clone().layer(layer), &entry)
            .await
            .unwrap();
        let _ = writer.write(&batch(&keys, false, 0)).await;
        assert!(writer.finish().await.is_err());
        assert!(
            store
                .list_with(&prefix)
                .recursive(true)
                .await
                .unwrap()
                .iter()
                .all(|e| e.metadata().is_dir())
        );
    }
}

#[tokio::test]
async fn filesystem_drop_cleans_only_owned_atomic_output() {
    let directory = common_test_util::temp_dir::create_temp_dir("columnar-files");
    let root = directory.path().to_str().unwrap();
    let temporary = directory.path().join("tmp");
    let store = ObjectStore::new(
        object_store::services::Fs::default()
            .root(root)
            .atomic_write_dir(temporary.to_str().unwrap()),
    )
    .unwrap();
    let meta = metadata();
    // Exceed OpenDAL's 8 MiB buffer so both physical temporary files exist.
    let label = "x".repeat(16 * 1024);
    let keys: Vec<_> = (0..1025)
        .map(|tsid| new_sparse_primary_key(&[&format!("{tsid:04}{label}"), "x"], &meta, 1, tsid))
        .collect();
    let format = ColumnarSeriesFormat {
        block_rows: 1024,
        compression: Compression::None,
        ..Default::default()
    };
    let mut first = format
        .writer(meta.clone(), store.clone(), &entry())
        .await
        .unwrap();
    let mut second = format.writer(meta, store.clone(), &entry()).await.unwrap();
    first.write(&batch(&keys, false, 0)).await.unwrap();
    second.write(&batch(&keys, false, 0)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if std::fs::read_dir(&temporary).unwrap().count() == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(first);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if std::fs::read_dir(&temporary).unwrap().count() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let artifact = second.finish().await.unwrap();
    for path in artifact.objects {
        assert!(store.exists(&path).await.unwrap());
    }
    assert_eq!(std::fs::read_dir(&temporary).unwrap().count(), 0);
}

#[tokio::test]
async fn unknown_column_codec_can_only_be_skipped() {
    let store = store();
    let meta = metadata();
    let format = ColumnarSeriesFormat {
        compression: Compression::None,
        ..Default::default()
    };
    let handle = build(
        &format,
        &store,
        &[batch(
            &[new_sparse_primary_key(&["a", "x"], &meta, 1, 1)],
            false,
            0,
        )],
    )
    .await;
    let manifest = manifest(&store, &handle).await;
    let path = manifest.part_path(0);
    let mut bytes = store.read(&path).await.unwrap().to_vec();
    let trailer_start = bytes.len() - 96;
    let mut trailer = Trailer::decode(&bytes[trailer_start..], bytes.len() as u64).unwrap();
    assert_eq!(trailer.depth, 1);
    // Leaf prefix + block prefix + TSID name + encoding's offset in PayloadReference.
    let encoding = trailer.root.payload.offset as usize + 12 + 28 + 8 + TSID_COLUMN.len() + 24;
    bytes[encoding..encoding + 2].copy_from_slice(&99u16.to_le_bytes());
    trailer.root.payload.checksum =
        codec::checksum(&bytes[trailer.root.payload.offset as usize..trailer_start]);
    bytes[trailer_start..].copy_from_slice(&trailer.encode());
    store.write(&path, bytes).await.unwrap();
    let reader = format.reader(store, handle).await.unwrap();
    assert!(
        reader
            .read(request(&["tag_0"]))
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .is_ok()
    );
    let error = reader
        .read(request(&[TSID_COLUMN]))
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("encoding 99"), "{error}");
}

#[tokio::test]
async fn malformed_manifest_and_timestamp_independence() {
    let store = store();
    let meta = metadata();
    let format = ColumnarSeriesFormat::default();
    let keys = vec![new_sparse_primary_key(&["a", "x"], &meta, 1, 1)];
    let first = build(&format, &store, &[batch(&keys, false, -999)]).await;
    let second = build(&format, &store, &[batch(&keys, true, 999)]).await;
    let mut manifest = manifest(&store, &first).await;
    let other = self::manifest(&store, &second).await;
    assert_eq!(
        store.read(&manifest.part_path(0)).await.unwrap().to_bytes(),
        store.read(&other.part_path(0)).await.unwrap().to_bytes()
    );
    manifest.files[0].part_id = 1;
    store
        .write(&manifest.path(), serde_json::to_vec(&manifest).unwrap())
        .await
        .unwrap();
    assert!(format.reader(store.clone(), first).await.is_err());
    let mut entry = second.entry().clone();
    entry
        .artifact
        .as_mut()
        .unwrap()
        .objects
        .push("unowned".into());
    let (purger, _) = series_index_channel(store.clone());
    let handle = SeriesIndexFileHandle::new(meta.region_id, entry, purger);
    assert!(format.reader(store, handle).await.is_err());
}

struct PausedClose {
    inner: object_store::layers::mock::oio::Writer,
    closing: Arc<tokio::sync::Notify>,
}
impl object_store::layers::mock::Write for PausedClose {
    async fn write(&mut self, buffer: object_store::Buffer) -> object_store::Result<()> {
        self.inner.write(buffer).await
    }
    async fn close(&mut self) -> object_store::Result<object_store::layers::mock::Metadata> {
        self.closing.notify_one();
        futures::future::pending().await
    }
    async fn abort(&mut self) -> object_store::Result<()> {
        self.inner.abort().await
    }
}
#[tokio::test]
async fn cancelled_finish_aborts_live_writer() {
    use object_store::layers::mock::MockLayerBuilder;
    let store = store();
    let meta = metadata();
    let entry = entry();
    let prefix = format!("{}/series/{}/", meta.region_id.as_u64(), entry.index_uuid);
    let closing = Arc::new(tokio::sync::Notify::new());
    let observed = closing.clone();
    let layer = MockLayerBuilder::default()
        .writer_factory(Arc::new(move |_, _, inner| {
            Box::new(PausedClose {
                inner,
                closing: observed.clone(),
            })
        }))
        .build()
        .unwrap();
    let format = ColumnarSeriesFormat::default();
    let mut writer = format
        .writer(meta.clone(), store.clone().layer(layer), &entry)
        .await
        .unwrap();
    writer
        .write(&batch(
            &[new_sparse_primary_key(&["a", "x"], &meta, 1, 1)],
            false,
            0,
        ))
        .await
        .unwrap();
    let task = tokio::spawn(async move { writer.finish().await });
    tokio::time::timeout(Duration::from_secs(5), closing.notified())
        .await
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if store
                .list_with(&prefix)
                .recursive(true)
                .await
                .unwrap()
                .iter()
                .all(|e| e.metadata().is_dir())
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn opendal_reads_sparse_part_beyond_four_gib() {
    use super::metadata::Page;
    use std::io::{Seek, SeekFrom, Write};
    let directory = common_test_util::temp_dir::create_temp_dir("columnar-files");
    let store = ObjectStore::new(
        object_store::services::Fs::default().root(directory.path().to_str().unwrap()),
    )
    .unwrap();
    let meta = metadata();
    let format = ColumnarSeriesFormat {
        compression: Compression::None,
        ..Default::default()
    };
    let handle = build(
        &format,
        &store,
        &[batch(
            &[new_sparse_primary_key(&["a", "x"], &meta, 1, 1)],
            false,
            0,
        )],
    )
    .await;
    let mut manifest = manifest(&store, &handle).await;
    let path = manifest.part_path(0);
    let bytes = store.read(&path).await.unwrap().to_bytes();
    let mut trailer = Trailer::decode(&bytes[bytes.len() - 96..], bytes.len() as u64).unwrap();
    let names: Vec<_> = manifest
        .schema()
        .fields()
        .iter()
        .skip(1)
        .map(|f| f.name().clone())
        .collect();
    let root_bytes = trailer
        .root
        .payload
        .unpack(&bytes[trailer.root.payload.offset as usize..bytes.len() - 96])
        .unwrap();
    let Page::Leaf(mut blocks) =
        Page::decode(&root_bytes, &trailer.root, 1, trailer.payload_end, &names).unwrap()
    else {
        panic!("expected leaf");
    };
    let shift = 1u64 << 32;
    for block in &mut blocks {
        for column in &mut block.columns {
            column.payload.offset += shift;
        }
    }
    let leaf = Page::Leaf(blocks).encode();
    trailer.root.payload.offset += shift;
    trailer.root.payload.checksum = codec::checksum(&leaf);
    let old_payload_end = trailer.payload_end as usize;
    trailer.payload_end += shift;
    trailer.file_length += shift;
    // A sparse hole uses no 4 GiB allocation. All reads below go through OpenDAL.
    let mut file = std::fs::File::create(directory.path().join(&path)).unwrap();
    file.write_all(&bytes[..16]).unwrap();
    file.seek(SeekFrom::Start(shift + 16)).unwrap();
    file.write_all(&bytes[16..old_payload_end]).unwrap();
    file.write_all(&leaf).unwrap();
    file.write_all(&trailer.encode()).unwrap();
    drop(file);
    manifest.files[0].file_size += shift;
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let mut entry = handle.entry().clone();
    entry.file_size = manifest_bytes.len() as u64 + manifest.files[0].file_size;
    entry.artifact.as_mut().unwrap().file_size = entry.file_size;
    store.write(&manifest.path(), manifest_bytes).await.unwrap();
    let (purger, _) = series_index_channel(store.clone());
    let handle = SeriesIndexFileHandle::new(meta.region_id, entry, purger);
    let batches = format
        .reader(store, handle)
        .await
        .unwrap()
        .read(request(&[TSID_COLUMN, "tag_0"]))
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        1
    );
    assert_eq!(
        batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "a"
    );
}

struct RecordingRead {
    inner: object_store::layers::mock::oio::Reader,
    path: String,
    reads: Arc<std::sync::Mutex<Vec<(String, object_store::layers::mock::BytesRange)>>>,
}
impl object_store::layers::mock::Read for RecordingRead {
    async fn open(
        &self,
        range: object_store::layers::mock::BytesRange,
    ) -> object_store::Result<(
        object_store::layers::mock::RpRead,
        Box<dyn object_store::layers::mock::oio::ReadStreamDyn>,
    )> {
        self.reads.lock().unwrap().push((self.path.clone(), range));
        self.inner.open(range).await
    }
    async fn read(
        &self,
        range: object_store::layers::mock::BytesRange,
    ) -> object_store::Result<(object_store::layers::mock::RpRead, object_store::Buffer)> {
        self.reads.lock().unwrap().push((self.path.clone(), range));
        self.inner.read(range).await
    }
}

#[tokio::test]
async fn dictionary_expansion_is_batched_and_empty_string_filter_is_exact() {
    let store = store();
    let meta = metadata();
    let label = "x".repeat(1024);
    let format = ColumnarSeriesFormat {
        block_rows: 16384,
        ..Default::default()
    };
    let keys: Vec<_> = (0..16384)
        .map(|tsid| new_sparse_primary_key(&[&label, ""], &meta, 1, tsid))
        .collect();
    let handle = build(&format, &store, &[batch(&keys, false, 0)]).await;
    let reader = format.reader(store, handle).await.unwrap();
    let batches = reader
        .read(request(&["tag_0"]))
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert!(batches.len() > 1);
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 16384);
    assert!(
        batches
            .iter()
            .all(|b| b.num_rows() * 1029 <= 8 * 1024 * 1024)
    );
    // Empty strings remain non-null and match equality exactly.
    let mut req = request(&[]);
    req.filters
        .push(SimpleFilterEvaluator::new("tag_1".into(), "", Operator::Eq).unwrap());
    assert_eq!(
        reader
            .read(req)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>(),
        16384
    );
}
