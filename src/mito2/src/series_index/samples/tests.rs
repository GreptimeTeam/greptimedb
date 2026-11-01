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

//! Lossless codecs, selective time navigation, corruption and artifact lifecycle.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datatypes::arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryDictionaryBuilder, Float64Array, UInt8Array, UInt64Array,
};
use datatypes::arrow::datatypes::{DataType, Field, Schema, UInt32Type};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::ConcreteDataType;
use futures::TryStreamExt;
use object_store::ObjectStore;
use object_store::layers::mock::{self, MockLayerBuilder, oio};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::FileId;

use super::metadata::{Bounds, Page, Trailer};
use super::*;
use crate::series_index::artifact::{SamplesEntry, SamplesFileHandle};
use crate::series_index::format::{SamplesFormat, SamplesReadRequest};
use crate::series_index::purger::series_index_channel;
use crate::sst::file::RegionFileId;
use crate::test_util::sst_util::{new_sparse_primary_key, sst_region_metadata_with_encoding};

type Sample = (u32, u64, i64, u64, u8, Option<u64>);
fn metadata() -> RegionMetadataRef {
    let mut m = sst_region_metadata_with_encoding(PrimaryKeyEncoding::Sparse);
    m.column_metadatas
        .iter_mut()
        .find(|c| c.column_id == 2)
        .unwrap()
        .column_schema
        .data_type = ConcreteDataType::float64_datatype();
    Arc::new(m)
}
fn store() -> ObjectStore {
    ObjectStore::new(object_store::services::Memory::default()).unwrap()
}
fn batch(meta: &RegionMetadataRef, rows: &[Sample], dictionary: bool) -> RecordBatch {
    let keys: Vec<_> = rows
        .iter()
        .map(|r| new_sparse_primary_key(&["a", "b"], meta, r.0, r.1))
        .collect();
    let keys: ArrayRef = if dictionary {
        let mut builder = BinaryDictionaryBuilder::<UInt32Type>::new();
        for key in &keys {
            builder.append(key).unwrap();
        }
        Arc::new(builder.finish())
    } else {
        Arc::new(BinaryArray::from_iter_values(
            keys.iter().map(Vec::as_slice),
        ))
    };
    let unit = meta
        .time_index_column()
        .column_schema
        .data_type
        .as_timestamp()
        .unwrap()
        .unit();
    let ts: ArrayRef = match unit {
        common_time::timestamp::TimeUnit::Second => Arc::new(
            datatypes::arrow::array::TimestampSecondArray::from_iter_values(
                rows.iter().map(|r| r.2),
            ),
        ),
        common_time::timestamp::TimeUnit::Millisecond => Arc::new(
            datatypes::arrow::array::TimestampMillisecondArray::from_iter_values(
                rows.iter().map(|r| r.2),
            ),
        ),
        common_time::timestamp::TimeUnit::Microsecond => Arc::new(
            datatypes::arrow::array::TimestampMicrosecondArray::from_iter_values(
                rows.iter().map(|r| r.2),
            ),
        ),
        common_time::timestamp::TimeUnit::Nanosecond => Arc::new(
            datatypes::arrow::array::TimestampNanosecondArray::from_iter_values(
                rows.iter().map(|r| r.2),
            ),
        ),
    };
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("field_0", DataType::Float64, true),
            Field::new("ts", ts.data_type().clone(), false),
            Field::new("__primary_key", keys.data_type().clone(), false),
            Field::new(SEQUENCE, DataType::UInt64, false),
            Field::new(OPERATION, DataType::UInt8, false),
        ])),
        vec![
            Arc::new(Float64Array::from_iter(
                rows.iter().map(|r| r.5.map(f64::from_bits)),
            )),
            ts,
            keys,
            Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.3))),
            Arc::new(UInt8Array::from_iter_values(rows.iter().map(|r| r.4))),
        ],
    )
    .unwrap()
}
async fn build(
    format: &ColumnarSamplesFormat,
    store: &ObjectStore,
    meta: RegionMetadataRef,
    batches: &[RecordBatch],
) -> SamplesFileHandle {
    let file_id = FileId::random();
    let generation = FileId::random();
    let prefix = format!(
        "{}/samples/{file_id}/{generation}/",
        meta.region_id.as_u64()
    );
    let mut writer = format
        .writer(meta.clone(), store.clone(), &prefix, file_id, generation)
        .await
        .unwrap();
    for batch in batches {
        writer.write(batch).await.unwrap();
    }
    let artifact = writer.finish().await.unwrap();
    let (purger, _) = series_index_channel(store.clone());
    SamplesFileHandle::new(
        RegionFileId::new(meta.region_id, file_id),
        SamplesEntry { file_id, artifact },
        purger,
    )
}
async fn manifest(store: &ObjectStore, handle: &SamplesFileHandle) -> Manifest {
    let path = &handle
        .entry()
        .artifact
        .objects
        .iter()
        .find(|o| o.path.ends_with("manifest.json"))
        .unwrap()
        .path;
    serde_json::from_slice(&store.read(path).await.unwrap().to_bytes()).unwrap()
}
fn request(names: &[&str]) -> SamplesReadRequest {
    SamplesReadRequest {
        projection: names.iter().map(|n| n.to_string()).collect(),
        ..Default::default()
    }
}
fn full_request() -> SamplesReadRequest {
    request(&[
        TABLE_ID_COLUMN,
        TSID_COLUMN,
        "ts",
        SEQUENCE,
        OPERATION,
        "field_0",
    ])
}
fn collect_rows(batches: &[RecordBatch]) -> Vec<Sample> {
    batches
        .iter()
        .flat_map(|b| {
            let ids = b
                .column(0)
                .as_any()
                .downcast_ref::<datatypes::arrow::array::UInt32Array>()
                .unwrap();
            let tsids = b.column(1).as_any().downcast_ref::<UInt64Array>().unwrap();
            let (times, _) =
                datatypes::timestamp::timestamp_array_to_primitive(b.column(2)).unwrap();
            let seq = b.column(3).as_any().downcast_ref::<UInt64Array>().unwrap();
            let op = b.column(4).as_any().downcast_ref::<UInt8Array>().unwrap();
            let values = b.column(5).as_any().downcast_ref::<Float64Array>().unwrap();
            (0..b.num_rows())
                .map(|i| {
                    (
                        ids.value(i),
                        tsids.value(i),
                        times.value(i),
                        seq.value(i),
                        op.value(i),
                        if values.is_null(i) {
                            None
                        } else {
                            Some(values.value(i).to_bits())
                        },
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

#[test]
fn sample_codecs_preserve_bits_and_reject_corruption() {
    for times in [
        vec![0],
        vec![1, 1, 1],
        vec![-10, -5, 0],
        vec![i64::MIN, i64::MAX],
        vec![-100, 0, 1, 999],
    ] {
        let (id, bytes) = codec::encode_time(&times);
        assert_eq!(
            codec::decode_numbers(&bytes, id, times.len())
                .unwrap()
                .iter()
                .map(|v| *v as i64)
                .collect::<Vec<_>>(),
            times
        );
        assert!(codec::decode_numbers(&bytes[..bytes.len() - 1], id, times.len()).is_err());
    }
    for seq in [vec![0, 0, 0], vec![u64::MAX, 0, 1 << 63, 3, 2]] {
        let (id, bytes) = codec::encode_sequence(&seq);
        assert_eq!(codec::decode_numbers(&bytes, id, seq.len()).unwrap(), seq);
    }
    assert_eq!(
        codec::encode_sequence(&[10, 9, 7]),
        (4, vec![10, 0, 0, 0, 0, 0, 0, 0, 1, 3])
    );
    let bits = vec![
        Some(0),
        None,
        Some(1 << 63),
        Some(0x7ff8000000000042),
        None,
        Some(u64::MAX),
        Some(1),
    ];
    let bytes = codec::encode_float(&bits);
    assert_eq!(codec::decode_float(&bytes, bits.len(), 2).unwrap(), bits);
    let mut bad = bytes.clone();
    bad[1] |= 128;
    assert!(codec::decode_float(&bad, bits.len(), 2).is_err());
    assert!(codec::decode_float(&bytes, bits.len(), 1).is_err());
    let mut bad = bytes.clone();
    bad.push(0);
    assert!(codec::decode_float(&bad, bits.len(), 2).is_err());
    assert!(codec::decode_numbers(&[255; 18], 4, 2).is_err());
    assert!(codec::decode_operations(&[2], 6, 2).is_err());
    assert!(codec::decode_operations(&[1], 7, 2).is_err());
    assert!(codec::decode_float(&[0], 8193, 0).is_err());
    for ops in [vec![1, 1], vec![0, 1, 0]] {
        let (id, bytes) = codec::encode_operations(&ops);
        assert_eq!(
            codec::decode_operations(&bytes, id, ops.len()).unwrap(),
            ops
        );
    }
}
#[tokio::test]
async fn lossless_round_trip_all_units_and_input_batches() {
    for timestamp_type in [
        ConcreteDataType::timestamp_second_datatype(),
        ConcreteDataType::timestamp_millisecond_datatype(),
        ConcreteDataType::timestamp_microsecond_datatype(),
        ConcreteDataType::timestamp_nanosecond_datatype(),
    ] {
        let mut m = (*metadata()).clone();
        m.column_metadatas
            .iter_mut()
            .find(|c| c.column_id == 3)
            .unwrap()
            .column_schema
            .data_type = timestamp_type;
        let m = Arc::new(m);
        let store = store();
        let format = ColumnarSamplesFormat {
            block_rows: 2,
            ..Default::default()
        };
        let rows = vec![
            (1, 42, -1, 9, 1, Some(1 << 63)),
            (1, 42, 0, 8, 0, None),
            (1, 42, 0, 8, 1, Some(0x7ff8000000000042)),
            (1, 42, 0, 7, 1, Some(u64::MAX)),
            (2, 42, 5, u64::MAX, 1, None),
        ];
        let handle = build(
            &format,
            &store,
            m.clone(),
            &[batch(&m, &rows[..2], false), batch(&m, &rows[2..], true)],
        )
        .await;
        let reader = format.reader(store.clone(), handle.clone()).await.unwrap();
        let result = reader
            .read(full_request())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(collect_rows(&result), rows);
        assert!(result.iter().all(|b| b.num_rows() <= 2));
        let no_columns = reader
            .read(request(&[]))
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            no_columns.iter().map(|b| b.num_rows()).sum::<usize>(),
            rows.len()
        );
        for object in &handle.entry().artifact.objects {
            assert_eq!(
                store.read(&object.path).await.unwrap().len() as u64,
                object.file_size
            );
        }
    }
}
#[tokio::test]
async fn equal_timestamps_cross_leaves_and_parts() {
    let store = store();
    let m = metadata();
    let format = ColumnarSamplesFormat {
        block_rows: 1,
        page_bytes: 232,
        root_capacity: 2,
        ..Default::default()
    };
    let rows: Vec<_> = (0..7)
        .map(|i| (7, 42, 100, 100 - i, 1, Some(i)))
        .chain([(7, 43, 50, 1, 1, Some(9)), (8, 42, 100, 1, 1, None)])
        .collect();
    let handle = build(&format, &store, m.clone(), &[batch(&m, &rows, true)]).await;
    let manifest = manifest(&store, &handle).await;
    assert!(manifest.files.len() >= 4);
    let first = store.read(&manifest.part_path(0)).await.unwrap().to_bytes();
    assert_eq!(
        Trailer::decode(&first[first.len() - 112..], first.len() as u64)
            .unwrap()
            .depth,
        2
    );
    let reader = format.reader(store, handle).await.unwrap();
    for (start, end) in [(99, 100), (100, 101), (101, 102), (50, 101)] {
        let mut req = full_request();
        req.ids = Some(BTreeSet::from([MetricSeriesId {
            table_id: 7,
            tsid: 42,
        }]));
        req.start = Some(start);
        req.end = Some(end);
        let out = reader
            .read(req)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            collect_rows(&out),
            rows.iter()
                .copied()
                .filter(|r| r.0 == 7 && r.1 == 42 && r.2 >= start && r.2 < end)
                .collect::<Vec<_>>()
        );
    }
    let mut req = request(&["field_0", TSID_COLUMN]);
    req.table_id = Some(8);
    let out = reader
        .read(req)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].column(0).null_count(), 1);
}
#[tokio::test]
async fn empty_selection_and_validation() {
    let store = store();
    let m = metadata();
    let format = ColumnarSamplesFormat::default();
    let handle = build(&format, &store, m, &[]).await;
    assert!(manifest(&store, &handle).await.files.is_empty());
    assert_eq!(handle.entry().artifact.objects.len(), 1);
    let reader = format.reader(store, handle).await.unwrap();
    assert!(
        reader
            .read(full_request())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
    assert!(reader.read(request(&["unknown"])).is_err());
    assert!(reader.read(request(&["ts", "ts"])).is_err());
    let mut req = request(&[]);
    req.start = Some(2);
    req.end = Some(1);
    assert!(reader.read(req).is_err());
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
}
#[tokio::test]
async fn failed_writer_and_abort_remove_outputs() {
    let store = store();
    let m = metadata();
    let format = ColumnarSamplesFormat {
        block_rows: 1,
        part_bytes: 1,
        ..Default::default()
    };
    for drop_writer in [false, true] {
        let id = FileId::random();
        let generation = FileId::random();
        let prefix = format!("{}/samples/{id}/{generation}/", m.region_id.as_u64());
        let mut writer = format
            .writer(m.clone(), store.clone(), &prefix, id, generation)
            .await
            .unwrap();
        writer
            .write(&batch(
                &m,
                &[(1, 1, 1, 1, 1, None), (1, 1, 2, 1, 1, Some(1))],
                false,
            ))
            .await
            .unwrap();
        if drop_writer {
            drop(writer);
        } else {
            assert!(
                writer
                    .write(&batch(&m, &[(1, 1, 0, 1, 1, None)], false))
                    .await
                    .is_err()
            );
            assert!(writer.finish().await.is_err());
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
    assert!(
        format
            .writer(m, store, "wrong", FileId::random(), FileId::random())
            .await
            .is_err()
    );
}

struct RecordingRead {
    inner: oio::Reader,
    path: String,
    reads: Arc<Mutex<Vec<(String, mock::BytesRange)>>>,
}
impl mock::Read for RecordingRead {
    async fn open(
        &self,
        range: mock::BytesRange,
    ) -> mock::Result<(mock::RpRead, Box<dyn oio::ReadStreamDyn>)> {
        self.reads.lock().unwrap().push((self.path.clone(), range));
        self.inner.open(range).await
    }
    async fn read(&self, range: mock::BytesRange) -> mock::Result<(mock::RpRead, mock::Buffer)> {
        self.reads.lock().unwrap().push((self.path.clone(), range));
        self.inner.read(range).await
    }
}
#[tokio::test]
async fn selective_reads_pin_the_artifact() {
    let store = store();
    let m = metadata();
    let format = ColumnarSamplesFormat::default();
    let built = build(
        &format,
        &store,
        m.clone(),
        &[batch(
            &m,
            &[(1, 1, 0, 1, 1, Some(7)), (2, 1, 10, 1, 1, None)],
            false,
        )],
    )
    .await;
    let (purger, mut purges) = series_index_channel(store.clone());
    let handle = SamplesFileHandle::new(built.file_id(), built.entry().clone(), purger);
    drop(built);
    let reads = Arc::new(Mutex::new(Vec::new()));
    let observed = reads.clone();
    let layer = MockLayerBuilder::default()
        .reader_factory(Arc::new(move |path, _, inner| {
            Box::new(RecordingRead {
                inner,
                path: path.into(),
                reads: observed.clone(),
            })
        }))
        .build()
        .unwrap();
    let reader = format
        .reader(store.layer(layer), handle.clone())
        .await
        .unwrap();
    reads.lock().unwrap().clear();
    let mut req = request(&["field_0"]);
    req.table_id = Some(2);
    let stream = reader.read(req).unwrap();
    handle.mark_deleted();
    drop(handle);
    drop(reader);
    assert!(purges.try_recv().is_err());
    let result = stream.try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].column(0).null_count(), 1);
    assert!(purges.try_recv().is_ok());
    // Header, trailer, one leaf/root. The all-null projected field has no payload.
    assert_eq!(reads.lock().unwrap().len(), 3);
}
#[test]
fn payload_limits_and_timestamp_envelopes() {
    assert!(valid_rows(8192, 1));
    assert!(!valid_rows(8193, 1));
    assert!(valid_rows(31, 1024));
    assert!(!valid_rows(32, 1024));
    let mut bounds = Bounds {
        first: MetricSeriesId {
            table_id: 1,
            tsid: 1,
        },
        last: MetricSeriesId {
            table_id: 1,
            tsid: 1,
        },
        min: 100,
        max: 200,
    };
    bounds.extend(Bounds {
        first: MetricSeriesId {
            table_id: 1,
            tsid: 2,
        },
        last: MetricSeriesId {
            table_id: 1,
            tsid: 2,
        },
        min: 0,
        max: 50,
    });
    assert_eq!((bounds.min, bounds.max), (0, 200));
    let (mut reference, bytes) = metadata::pack(&[1, 2, 3], (1u64 << 32) + 5, 1, false).unwrap();
    reference
        .validate(16, reference.end().unwrap(), false)
        .unwrap();
    assert_eq!(reference.unpack(&bytes).unwrap(), [1, 2, 3]);
    reference.offset = u64::MAX;
    assert!(reference.end().is_err());
    let (mut reference, bytes) = metadata::pack(&vec![0; 1024], 16, 1, false).unwrap();
    reference.decoded = 1000;
    assert!(reference.unpack(&bytes).is_err());
}
#[tokio::test]
async fn malformed_framing_and_payload_corruption() {
    let store = store();
    let m = metadata();
    let format = ColumnarSamplesFormat::default();
    let handle = build(
        &format,
        &store,
        m.clone(),
        &[batch(&m, &[(1, 1, 0, 1, 1, Some(42))], false)],
    )
    .await;
    let manifest = manifest(&store, &handle).await;
    let path = manifest.part_path(0);
    let original = store.read(&path).await.unwrap().to_vec();
    let end = original.len() - 112;
    let trailer = Trailer::decode(&original[end..], original.len() as u64).unwrap();
    for (encoding, compression) in [(99, 0), (1, 99)] {
        let mut unsupported = Trailer::decode(&original[end..], original.len() as u64).unwrap();
        unsupported.root.payload.encoding = encoding;
        unsupported.root.payload.compression = compression;
        assert!(Trailer::decode(&unsupported.encode(), original.len() as u64).is_err());
    }
    let decoded = trailer
        .root
        .payload
        .unpack(&original[trailer.root.payload.offset as usize..end])
        .unwrap();
    let Page::Leaf(blocks) =
        Page::decode(&decoded, &trailer.root, 1, trailer.payload_end, &manifest).unwrap()
    else {
        panic!()
    };
    let reader = format.reader(store.clone(), handle.clone()).await.unwrap();
    let mut bad = original.clone();
    bad[blocks[0].columns[3].payload.offset as usize] ^= 1;
    store.write(&path, bad).await.unwrap();
    assert!(
        reader
            .read(request(&[TABLE_ID_COLUMN]))
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .is_ok()
    );
    assert!(
        reader
            .read(request(&["field_0"]))
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .is_err()
    );
    for offset in [0, end + 72, end + 104] {
        let mut bad = original.clone();
        bad[offset] ^= 3;
        store.write(&path, bad).await.unwrap();
        assert!(
            reader
                .read(full_request())
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .is_err()
        );
    }
    for length in 0..decoded.len() {
        assert!(
            Page::decode(
                &decoded[..length],
                &trailer.root,
                1,
                trailer.payload_end,
                &manifest
            )
            .is_err()
        );
    }
    let mut entry = handle.entry().clone();
    entry
        .artifact
        .objects
        .push(entry.artifact.objects[0].clone());
    let (purger, _) = series_index_channel(store.clone());
    let bad = SamplesFileHandle::new(handle.file_id(), entry, purger);
    assert!(format.reader(store, bad).await.is_err());
}

#[tokio::test]
async fn real_samples_reconcile_reopen_and_retire() {
    use crate::config::MitoConfig;
    use crate::series_index::format::SeriesFormats;
    use crate::series_index::maintenance::reconcile_series_indexes;
    use crate::test_util::TestEnv;
    let mut env = TestEnv::with_prefix("columnar-samples-reconcile").await;
    let (engine, region) = crate::series_index::tests::prepare_region_with_metadata(
        &mut env,
        &[1000, 2000],
        MitoConfig {
            experimental_enable_series_index: true,
            experimental_series_index_maintenance_interval: Duration::from_secs(3600),
            min_compaction_interval: Duration::from_secs(3600),
            ..Default::default()
        },
        metadata(),
    )
    .await;
    let format = Arc::new(ColumnarSamplesFormat::default());
    *region.series_index_version_control.formats.write().unwrap() = Arc::new(SeriesFormats {
        samples: Some(format.clone()),
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
    assert_eq!(reconcile().await.unwrap().built_samples, 2);
    assert_eq!(reconcile().await.unwrap().built_samples, 0);
    let reopened =
        crate::series_index::catalog::load_version_control(&store, region.region_id, &purger).await;
    assert_eq!(reopened.current().samples.len(), 2);
    let mut timestamps = Vec::new();
    for handle in reopened.current().samples.values() {
        let reader = format.reader(store.clone(), handle.clone()).await.unwrap();
        let batches = reader
            .read(full_request())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        timestamps.extend(collect_rows(&batches).into_iter().map(|row| row.2));
    }
    timestamps.sort_unstable();
    assert_eq!(timestamps, vec![1000, 2000]);
    // A missing artifact triggers rebuilding; failure to publish its replacement
    // must retain the prior snapshot/catalog and retire only the new generation.
    let before = region.series_index_version();
    let catalog_path = crate::series_index::catalog::samples_catalog_path(region.region_id);
    let catalog = store.read(&catalog_path).await.unwrap().to_bytes();
    let old = before.samples.values().next().unwrap();
    store
        .delete(&old.entry().artifact.objects[0].path)
        .await
        .unwrap();
    let layer = MockLayerBuilder::default()
        .writer_factory(Arc::new(|path, _, inner| {
            if path.ends_with("samples.json") {
                Box::new(FaultyWriter {
                    inner,
                    fail_write: true,
                    closing: None,
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
    assert!(Arc::ptr_eq(&before, &region.series_index_version()));
    assert_eq!(catalog, store.read(&catalog_path).await.unwrap().to_bytes());
    let cleanup = purges.try_recv().unwrap().objects.unwrap();
    assert_eq!(cleanup.len(), 2);
    assert!(cleanup.iter().all(|path| {
        !old.entry()
            .artifact
            .objects
            .iter()
            .any(|object| object.path == *path)
    }));
    assert!(purges.try_recv().is_err());
    use store_api::region_engine::RegionEngine;
    engine.stop().await.unwrap();
}

async fn fixture(store: &ObjectStore) -> SamplesFileHandle {
    let bytes =
        include_str!("../../../../../tests/compatibility/wire/mito_columnar_samples/v1-part.hex")
            .split_whitespace()
            .collect::<String>();
    let bytes = (0..bytes.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&bytes[i..i + 2], 16).unwrap())
        .collect::<Vec<_>>();
    let json = include_str!(
        "../../../../../tests/compatibility/wire/mito_columnar_samples/v1-manifest.json"
    );
    let manifest: Manifest = serde_json::from_str(json).unwrap();
    store
        .write(&manifest.part_path(0), bytes.clone())
        .await
        .unwrap();
    store.write(&manifest.path(), json).await.unwrap();
    let objects = vec![
        crate::series_index::artifact::ArtifactObject {
            path: manifest.path(),
            file_size: json.len() as u64,
        },
        crate::series_index::artifact::ArtifactObject {
            path: manifest.part_path(0),
            file_size: bytes.len() as u64,
        },
    ];
    let artifact = crate::series_index::artifact::ArtifactMetadata {
        format: ArtifactFormat::ColumnarSamples,
        generation: manifest.generation,
        file_size: (json.len() + bytes.len()) as u64,
        objects,
    };
    let (purger, _) = series_index_channel(store.clone());
    SamplesFileHandle::new(
        RegionFileId::new(
            store_api::storage::RegionId::from_u64(manifest.region_id),
            manifest.source_file_id,
        ),
        SamplesEntry {
            file_id: manifest.source_file_id,
            artifact,
        },
        purger,
    )
}
#[tokio::test]
async fn frozen_v1_samples_fixture_and_unknown_codec() {
    let store = store();
    let handle = fixture(&store).await;
    let format = ColumnarSamplesFormat::default();
    let reader = format.reader(store.clone(), handle.clone()).await.unwrap();
    let batches = reader
        .read(full_request())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        collect_rows(&batches),
        vec![(7, 42, 100, 9, 1, Some(1 << 63)), (7, 42, 100, 8, 0, None)]
    );
    let manifest = manifest(&store, &handle).await;
    let path = manifest.part_path(0);
    let mut bytes = store.read(&path).await.unwrap().to_vec();
    let end = bytes.len() - 112;
    let mut trailer = Trailer::decode(&bytes[end..], bytes.len() as u64).unwrap();
    let leaf_start = trailer.root.payload.offset as usize;
    let encoding = leaf_start + 12 + 44 + 3 * 44 + 8 + 24;
    bytes[encoding..encoding + 2].copy_from_slice(&99u16.to_le_bytes());
    trailer.root.payload.checksum = codec::checksum(&bytes[leaf_start..end]);
    bytes[end..].copy_from_slice(&trailer.encode());
    store.write(&path, bytes).await.unwrap();
    assert!(
        reader
            .read(request(&["ts"]))
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .is_ok()
    );
    assert!(
        reader
            .read(request(&["field_0"]))
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .is_err()
    );
}

#[tokio::test]
async fn wide_schema_byte_bound_and_unsupported_input() {
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    let m = metadata();
    let store = store();
    let format = ColumnarSamplesFormat::default();
    let mut builder = RegionMetadataBuilder::from_existing((*m).clone());
    for i in 1..1024 {
        builder.push_column_metadata(ColumnMetadata {
            column_id: 100 + i,
            semantic_type: api::v1::SemanticType::Field,
            column_schema: ColumnSchema::new(
                format!("field_{i}"),
                ConcreteDataType::float64_datatype(),
                true,
            ),
        });
    }
    let wide = Arc::new(builder.build().unwrap());
    let rows: Vec<_> = (0..65).map(|i| (1, 1, i, 1, 1, Some(i as u64))).collect();
    let input = batch(&wide, &rows, false);
    let mut fields = input.schema().fields().to_vec();
    let mut arrays = input.columns().to_vec();
    for i in 1..1024 {
        fields.insert(
            i,
            Arc::new(Field::new(format!("field_{i}"), DataType::Float64, true)),
        );
        arrays.insert(i, input.column(0).clone());
    }
    let input = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
    let handle = build(&format, &store, wide, &[input]).await;
    let batches = format
        .reader(store.clone(), handle)
        .await
        .unwrap()
        .read(request(&["field_1023", "ts"]))
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        batches.iter().map(|b| b.num_rows()).collect::<Vec<_>>(),
        vec![31, 31, 3]
    );
    let wrong = Arc::new(sst_region_metadata_with_encoding(
        PrimaryKeyEncoding::Sparse,
    ));
    let id = FileId::random();
    let generation = FileId::random();
    let prefix = format!("{}/samples/{id}/{generation}/", m.region_id.as_u64());
    assert!(
        format
            .writer(wrong, store.clone(), &prefix, id, generation)
            .await
            .is_err()
    );
    let mut writer = format
        .writer(m.clone(), store, &prefix, id, generation)
        .await
        .unwrap();
    let mut bad = (*m).clone();
    bad.column_metadatas
        .iter_mut()
        .find(|c| c.column_id == 3)
        .unwrap()
        .column_schema
        .data_type = ConcreteDataType::timestamp_second_datatype();
    assert!(
        writer
            .write(&batch(&Arc::new(bad), &rows, false))
            .await
            .is_err()
    );
    writer.abort().await.unwrap();
}

struct FaultyWriter {
    inner: oio::Writer,
    fail_write: bool,
    closing: Option<Arc<tokio::sync::Notify>>,
}
impl mock::Write for FaultyWriter {
    async fn write(&mut self, buffer: mock::Buffer) -> mock::Result<()> {
        if self.fail_write {
            return Err(object_store::Error::new(
                object_store::ErrorKind::Unexpected,
                "injected sample write failure",
            ));
        }
        self.inner.write(buffer).await
    }
    async fn close(&mut self) -> mock::Result<mock::Metadata> {
        if let Some(closing) = &self.closing {
            closing.notify_one();
            std::future::pending::<()>().await;
        }
        Err(object_store::Error::new(
            object_store::ErrorKind::Unexpected,
            "injected sample close failure",
        ))
    }
    async fn abort(&mut self) -> mock::Result<()> {
        self.inner.abort().await
    }
}
#[tokio::test]
async fn output_failures_and_cancelled_finish_clean_up() {
    let m = metadata();
    let format = ColumnarSamplesFormat::default();
    for (manifest_only, fail_write, cancel) in [
        (false, true, false),
        (false, false, false),
        (true, false, false),
        (false, false, true),
    ] {
        let store = store();
        let closing = Arc::new(tokio::sync::Notify::new());
        let observed = closing.clone();
        let layer = MockLayerBuilder::default()
            .writer_factory(Arc::new(move |path, _, inner| {
                if !manifest_only || path.ends_with("manifest.json") {
                    Box::new(FaultyWriter {
                        inner,
                        fail_write,
                        closing: cancel.then(|| observed.clone()),
                    })
                } else {
                    inner
                }
            }))
            .build()
            .unwrap();
        let id = FileId::random();
        let generation = FileId::random();
        let prefix = format!("{}/samples/{id}/{generation}/", m.region_id.as_u64());
        let mut writer = format
            .writer(
                m.clone(),
                store.clone().layer(layer),
                &prefix,
                id,
                generation,
            )
            .await
            .unwrap();
        writer
            .write(&batch(&m, &[(1, 1, 0, 1, 1, Some(7))], false))
            .await
            .unwrap();
        if cancel {
            let task = tokio::spawn(async move { writer.finish().await });
            tokio::time::timeout(Duration::from_secs(5), closing.notified())
                .await
                .unwrap();
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            assert!(writer.finish().await.is_err());
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
}

#[tokio::test]
async fn batches_column_io_reuses_readers_and_releases_reservations() {
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use std::sync::atomic::{AtomicUsize, Ordering};

    let store = store();
    let m = metadata();
    let format = ColumnarSamplesFormat {
        block_rows: 2,
        ..Default::default()
    };
    let input = vec![
        (1, 1, 0, 4, 1, Some(7)),
        (1, 1, 10, 3, 1, Some(8)),
        (1, 1, 20, 2, 1, Some(9)),
        (1, 1, 30, 1, 1, Some(10)),
    ];
    let handle = build(&format, &store, m.clone(), &[batch(&m, &input, false)]).await;
    let part_size = handle
        .entry()
        .artifact
        .objects
        .iter()
        .find(|object| object.path.ends_with(".samples"))
        .unwrap()
        .file_size;
    let reads = Arc::new(Mutex::new(Vec::new()));
    let creations = Arc::new(AtomicUsize::new(0));
    let observed = reads.clone();
    let opened = creations.clone();
    let reader = format
        .reader(
            store.layer(
                MockLayerBuilder::default()
                    .reader_factory(Arc::new(move |path, _, inner| {
                        if path.ends_with(".samples") {
                            opened.fetch_add(1, Ordering::Relaxed);
                        }
                        Box::new(RecordingRead {
                            inner,
                            path: path.into(),
                            reads: observed.clone(),
                        })
                    }))
                    .build()
                    .unwrap(),
            ),
            handle,
        )
        .await
        .unwrap();
    reads.lock().unwrap().clear();
    let left = reader.read(full_request()).unwrap().try_collect::<Vec<_>>();
    let right = reader.read(full_request()).unwrap().try_collect::<Vec<_>>();
    let (left, right) = tokio::join!(left, right);
    let left = left.unwrap();
    assert_eq!(input, collect_rows(&left));
    assert_eq!(left, right.unwrap());
    assert_eq!(1, creations.load(Ordering::Relaxed));
    {
        let reads = reads.lock().unwrap();
        // Each stream reads framing/root plus one coalesced request per four-column block.
        assert_eq!(10, reads.len());
        assert_eq!(
            2 * part_size,
            reads
                .iter()
                .map(|(_, range)| range.size().unwrap())
                .sum::<u64>()
        );
    }
    reads.lock().unwrap().clear();
    let sparse = reader
        .read(request(&["field_0"]))
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(4, sparse.iter().map(RecordBatch::num_rows).sum::<usize>());
    assert!(
        reads
            .lock()
            .unwrap()
            .iter()
            .map(|(_, range)| range.size().unwrap())
            .sum::<u64>()
            < part_size
    );
    assert_eq!(1, creations.load(Ordering::Relaxed));

    let pool = Arc::new(GreedyMemoryPool::new(1024 * 1024));
    let mut request = full_request();
    request.memory_pool = Some(pool.clone());
    let mut stream = reader.read(request).unwrap();
    assert!(stream.try_next().await.unwrap().is_some());
    assert!(pool.reserved() > 0);
    drop(stream);
    assert_eq!(0, pool.reserved());
    let pool = Arc::new(GreedyMemoryPool::new(1));
    let mut request = full_request();
    request.memory_pool = Some(pool.clone());
    let error = reader.read(request).unwrap().try_next().await.unwrap_err();
    assert_eq!(
        common_error::ext::ErrorExt::status_code(&error),
        common_error::status_code::StatusCode::RuntimeResourcesExhausted
    );
    assert_eq!(0, pool.reserved());
}

#[tokio::test]
async fn boundary_blocks_fetch_remaining_columns_only_for_selected_rows() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let store = store();
    let m = metadata();
    let format = ColumnarSamplesFormat {
        block_rows: 3,
        part_bytes: 1,
        ..Default::default()
    };
    let input = vec![
        (1, 1, 0, 6, 1, Some(7)),
        (1, 1, 10, 5, 1, Some(8)),
        (1, 1, 20, 4, 1, Some(9)),
        (1, 1, 30, 3, 1, Some(10)),
        (1, 1, 40, 2, 1, Some(11)),
        (1, 1, 50, 1, 1, Some(12)),
    ];
    let handle = build(&format, &store, m.clone(), &[batch(&m, &input, false)]).await;
    assert_eq!(2, manifest(&store, &handle).await.files.len());
    let reads = Arc::new(Mutex::new(Vec::new()));
    let creations = Arc::new(AtomicUsize::new(0));
    let observed = reads.clone();
    let opened = creations.clone();
    let reader = format
        .reader(
            store.layer(
                MockLayerBuilder::default()
                    .reader_factory(Arc::new(move |path, _, inner| {
                        if path.ends_with(".samples") {
                            opened.fetch_add(1, Ordering::Relaxed);
                        }
                        Box::new(RecordingRead {
                            inner,
                            path: path.into(),
                            reads: observed.clone(),
                        })
                    }))
                    .build()
                    .unwrap(),
            ),
            handle,
        )
        .await
        .unwrap();
    reads.lock().unwrap().clear();
    let mut empty = full_request();
    empty.start = Some(1);
    empty.end = Some(9);
    let batches = reader
        .read(empty)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert!(batches.is_empty());
    assert_eq!(1, creations.load(Ordering::Relaxed));
    let timestamp_read = {
        let reads = reads.lock().unwrap();
        // Header, trailer, root, then timestamps only. The second part is pruned.
        assert_eq!(4, reads.len());
        reads[3].clone()
    };

    reads.lock().unwrap().clear();
    let mut partial = full_request();
    partial.start = Some(5);
    partial.end = Some(45);
    let batches = reader
        .read(partial)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(input[1..5], collect_rows(&batches));
    assert_eq!(2, creations.load(Ordering::Relaxed));
    let reads = reads.lock().unwrap();
    // Both boundary blocks read timestamps before fetching remaining columns.
    assert_eq!(10, reads.len());
    assert_eq!(timestamp_read, reads[3]);
}

#[cfg(unix)]
#[tokio::test]
async fn local_parts_read_like_object_store_parts() {
    let dir = common_test_util::temp_dir::create_temp_dir("samples-local-read");
    let local =
        ObjectStore::new(object_store::services::Fs::default().root(dir.path().to_str().unwrap()))
            .unwrap();
    let remote = store();
    let m = metadata();
    let format = ColumnarSamplesFormat {
        block_rows: 3,
        part_bytes: 1,
        ..Default::default()
    };
    let input = vec![
        (1, 1, 0, 6, 1, Some(7)),
        (1, 1, 10, 5, 0, None),
        (1, 1, 20, 4, 1, Some(9)),
        (1, 2, 30, 3, 1, Some(10)),
        (1, 2, 40, 2, 1, None),
        (1, 2, 50, 1, 1, Some(12)),
    ];
    let mut readers = Vec::new();
    for store in [&local, &remote] {
        let handle = build(&format, store, m.clone(), &[batch(&m, &input, false)]).await;
        readers.push(format.reader(store.clone(), handle).await.unwrap());
    }
    let mut bounded = full_request();
    bounded.start = Some(5);
    bounded.end = Some(45);
    // `ts` and `field_0` are not byte-adjacent once sequences and operations are skipped.
    let mut gapped = request(&["field_0", "ts"]);
    gapped.start = Some(10);
    for request in [full_request(), bounded, gapped] {
        let mut outputs = Vec::new();
        for reader in &readers {
            outputs.push(
                reader
                    .read(request.clone())
                    .unwrap()
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap(),
            );
        }
        assert!(!outputs[0].is_empty());
        assert_eq!(outputs[0], outputs[1]);
    }
}
