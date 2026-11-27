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

use super::metadata::{Bounds, KEY_CHECKSUM_BYTES, Page};
use super::*;
use crate::series_index::artifact::{SamplesEntry, SamplesFileHandle};
use crate::series_index::columnar::codec as columnar_codec;
use crate::series_index::format::{SamplesBatch, SamplesFormat, SamplesReadRequest};
use crate::series_index::identity::ExactSeriesKey;
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

#[test]
fn test_narthex_requires_one_value_field() {
    use crate::region::options::RegionOptions;
    use crate::sst::FormatType;

    for field_count in [0, 1, 2] {
        let mut metadata = metadata().as_ref().clone();
        let field = metadata.field_columns().next().unwrap().clone();
        metadata
            .column_metadatas
            .retain(|column| column.semantic_type != api::v1::SemanticType::Field);
        for i in 0..field_count {
            let mut column = field.clone();
            column.column_id = 100 + i;
            column.column_schema.name = format!("value_{i}");
            metadata.column_metadatas.push(column);
        }
        let metadata = store_api::metadata::RegionMetadataBuilder::from_existing(metadata)
            .build()
            .unwrap();
        let options = RegionOptions {
            sst_format: Some(FormatType::Narthex),
            ..Default::default()
        };
        let result = options.validate_schema(&metadata);
        if field_count == 1 {
            result.unwrap();
        } else {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("exactly one value field")
            );
        }
        let parquet_options = RegionOptions {
            sst_format: Some(FormatType::Flat),
            ..Default::default()
        };
        parquet_options.validate_schema(&metadata).unwrap();
    }
}

fn store() -> ObjectStore {
    ObjectStore::new(object_store::services::Memory::default()).unwrap()
}
fn batch(meta: &RegionMetadataRef, rows: &[Sample], dictionary: bool) -> RecordBatch {
    let keys: Vec<_> = rows
        .iter()
        .map(|r| new_sparse_primary_key(&["a", "b"], meta, r.0, r.1))
        .collect();
    sample_batch(
        meta,
        &keys,
        rows.iter().map(|r| r.2).collect(),
        rows.iter().map(|r| r.3).collect(),
        rows.iter().map(|r| r.4).collect(),
        rows.iter().map(|r| r.5.map(f64::from_bits)).collect(),
        dictionary,
    )
}

/// Builds one sample batch from explicit sparse keys, so a test can give two
/// distinct series the same `tsid` and different labels.
fn keyed_batch(
    meta: &RegionMetadataRef,
    keys: &[Vec<u8>],
    times: &[i64],
    dictionary: bool,
) -> RecordBatch {
    sample_batch(
        meta,
        keys,
        times.to_vec(),
        vec![1; keys.len()],
        vec![0; keys.len()],
        vec![Some(0.0); keys.len()],
        dictionary,
    )
}

fn sample_batch(
    meta: &RegionMetadataRef,
    keys: &[Vec<u8>],
    times: Vec<i64>,
    sequences: Vec<u64>,
    operations: Vec<u8>,
    values: Vec<Option<f64>>,
    dictionary: bool,
) -> RecordBatch {
    let keys: ArrayRef = if dictionary {
        let mut builder = BinaryDictionaryBuilder::<UInt32Type>::new();
        for key in keys {
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
        common_time::timestamp::TimeUnit::Second => {
            Arc::new(datatypes::arrow::array::TimestampSecondArray::from_iter_values(times))
        }
        common_time::timestamp::TimeUnit::Millisecond => {
            Arc::new(datatypes::arrow::array::TimestampMillisecondArray::from_iter_values(times))
        }
        common_time::timestamp::TimeUnit::Microsecond => {
            Arc::new(datatypes::arrow::array::TimestampMicrosecondArray::from_iter_values(times))
        }
        common_time::timestamp::TimeUnit::Nanosecond => {
            Arc::new(datatypes::arrow::array::TimestampNanosecondArray::from_iter_values(times))
        }
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
            Arc::new(Float64Array::from_iter(values)),
            ts,
            keys,
            Arc::new(UInt64Array::from_iter_values(sequences)),
            Arc::new(UInt8Array::from_iter_values(operations)),
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
    let prefix = format!("{}/samples/{generation}/", meta.region_id.as_u64());
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
        SamplesEntry {
            file_id,
            artifact,
            source_file_ids: Vec::new(),
            window: None,
            source_windows: Default::default(),
        },
        purger,
    )
}
async fn manifest(store: &ObjectStore, handle: &SamplesFileHandle) -> SamplesMetadata {
    let bytes = store.read(&handle.index_path()).await.unwrap().to_bytes();
    let footer = index_footer(&bytes);
    let (schema, files) = index::decode_metadata(
        &footer.metadata,
        &bytes[footer.metadata.offset as usize..footer.metadata.end().unwrap() as usize],
    )
    .unwrap();
    SamplesMetadata {
        timestamp: schema.timestamp,
        fields: schema.fields,
        files,
        generation: footer.generation,
        source_file_id: footer.source,

        prefix: handle
            .index_path()
            .strip_suffix("samples.idx")
            .unwrap()
            .into(),
    }
}

fn index_footer(bytes: &[u8]) -> index::Footer {
    index::Footer::decode(
        &bytes[bytes.len() - index::FOOTER_SIZE..],
        bytes.len() as u64,
    )
    .unwrap()
}

async fn read_leaf(
    store: &ObjectStore,
    handle: &SamplesFileHandle,
    leaf: u64,
) -> (SamplesMetadata, index::LeafEntry, Page, Vec<u8>) {
    let manifest = manifest(store, handle).await;
    let bytes = store.read(&handle.index_path()).await.unwrap().to_vec();
    let footer = index_footer(&bytes);
    let offset = (footer.directory + leaf * index::ENTRY_SIZE) as usize;
    let entry = index::LeafEntry::decode(
        &bytes[offset..offset + index::ENTRY_SIZE as usize],
        footer.directory,
    )
    .unwrap();
    let r = entry.page.payload;
    let page = Page::decode(
        &r.unpack(&bytes[r.offset as usize..r.end().unwrap() as usize])
            .unwrap(),
        &entry.page,
        entry.keys(),
        &manifest,
    )
    .unwrap();
    (manifest, entry, page, bytes)
}

async fn first_leaf(
    store: &ObjectStore,
    handle: &SamplesFileHandle,
) -> (SamplesMetadata, index::LeafEntry, Page, Vec<u8>) {
    read_leaf(store, handle, 0).await
}

/// Repack the single leaf of a fixture after editing its block descriptors.
async fn replace_leaf(
    store: &ObjectStore,
    handle: SamplesFileHandle,
    page: Page,
) -> SamplesFileHandle {
    let (_, mut entry, _, mut bytes) = first_leaf(store, &handle).await;
    let mut footer = index_footer(&bytes);
    assert_eq!(footer.leaves, 1);
    let metadata =
        bytes[footer.metadata.offset as usize..footer.metadata.end().unwrap() as usize].to_vec();
    bytes.truncate(entry.page.payload.offset as usize);
    let (payload, stored) =
        super::metadata::pack(&page.encode(), bytes.len() as u64, 1, true).unwrap();
    entry.page.payload = payload;
    bytes.extend(stored);
    footer.directory = bytes.len() as u64;
    bytes.extend(entry.encode());
    footer.metadata.offset = bytes.len() as u64;
    bytes.extend(metadata);
    bytes.extend(footer.encode());
    let mut inventory = handle.entry().clone();
    let object = inventory
        .artifact
        .objects
        .iter_mut()
        .find(|o| o.path == handle.index_path())
        .unwrap();
    inventory.artifact.file_size =
        inventory.artifact.file_size - object.file_size + bytes.len() as u64;
    object.file_size = bytes.len() as u64;
    store.write(&object.path, bytes).await.unwrap();
    let (purger, _) = series_index_channel(store.clone());
    SamplesFileHandle::new(handle.file_id(), inventory, purger)
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
fn collect_rows(batches: &[SamplesBatch]) -> Vec<Sample> {
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
        // One block descriptor plus header; a block now also stores its exact series key.
        page_bytes: 288,
        part_bytes: 42,
        ..Default::default()
    };
    let rows: Vec<_> = (0..7)
        .map(|i| (7, 42, 100, 100 - i, 1, Some(i)))
        .chain([(7, 43, 50, 1, 1, Some(9)), (8, 42, 100, 1, 1, None)])
        .collect();
    let handle = build(&format, &store, m.clone(), &[batch(&m, &rows, true)]).await;
    let manifest = manifest(&store, &handle).await;
    assert!(manifest.files.len() >= 2);
    let index = store.read(&manifest.path()).await.unwrap().to_bytes();
    assert_eq!(rows.len() as u64, index_footer(&index).leaves);
    let reader = format.reader(store, handle).await.unwrap();
    for (start, end) in [(99, 100), (100, 101), (101, 102), (50, 101)] {
        let mut req = full_request();
        req.keys = Some(BTreeSet::from([exact(&m, &["a", "b"], 7, 42)]));
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
/// Wraps one encoded primary key in the canonical exact identity.
fn exact(meta: &RegionMetadataRef, tags: &[&str; 2], table_id: u32, tsid: u64) -> ExactSeriesKey {
    let mut offsets = mito_codec::row_converter::SparseOffsetsCache::new();
    ExactSeriesKey::from_sparse_key(
        &new_sparse_primary_key(tags, meta, table_id, tsid),
        &mut offsets,
    )
    .unwrap()
}

/// Reads the samples selected by one exact series key.
async fn read_key(reader: &dyn SamplesFormatReader, key: ExactSeriesKey) -> Vec<Sample> {
    let mut req = full_request();
    req.keys = Some(BTreeSet::from([key]));
    let out = reader
        .read(req)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    collect_rows(&out)
}

/// A key whose label is long enough to push the encoded key past 8 KiB round-trips like any
/// other: the writer and the reader agree because neither imposes a length cap.
#[tokio::test]
async fn long_keys_round_trip() {
    let store = store();
    let meta = metadata();
    let format = ColumnarSamplesFormat::default();
    let label = "x".repeat(8 * 1024);
    let key = new_sparse_primary_key(&[label.as_str(), "z"], &meta, 1, 7);
    assert!(key.len() > 8 * 1024, "{}", key.len());
    let handle = build(
        &format,
        &store,
        meta.clone(),
        &[keyed_batch(
            &meta,
            &[key.clone(), key.clone()],
            &[100, 200],
            false,
        )],
    )
    .await;
    let reader = format.reader(store.clone(), handle.clone()).await.unwrap();
    let selection = exact(&meta, &[label.as_str(), "z"], 1, 7);
    assert_eq!(selection.bytes(), key.as_slice());
    assert_eq!(
        read_key(reader.as_ref(), selection).await,
        vec![(1, 7, 100, 1, 0, Some(0)), (1, 7, 200, 1, 0, Some(0)),]
    );
    // The key area keeps the key byte for byte, so a read returns the same bytes the writer
    // received; the descriptor only references it.
    let (_, leaf_entry, page, bytes) = first_leaf(&store, &handle).await;
    let Page(blocks) = page;
    assert_eq!(1, blocks.len());
    let reference = blocks[0].key;
    reference.validate(leaf_entry.keys()).unwrap();
    let end = reference.offset + u64::from(reference.length);
    assert_eq!(
        key.as_slice(),
        &bytes[reference.offset as usize..end as usize]
    );
}

/// The captured pair in the samples artifact: two distinct series with one
/// `table_id`/`tsid` whose timestamps restart. Every block records which exact key
/// owns it, so each series keeps its own samples and never sees the partner's.
#[tokio::test]
async fn colliding_tsids_are_retained_and_owned_exactly() {
    let store = store();
    let meta = metadata();
    let format = ColumnarSamplesFormat::default();
    let first = new_sparse_primary_key(&["host-020078", "z"], &meta, 1, 7);
    let partner = new_sparse_primary_key(&["host-029838", "z"], &meta, 1, 7);
    for dictionary in [false, true] {
        let handle = build(
            &format,
            &store,
            meta.clone(),
            &[
                keyed_batch(
                    &meta,
                    &[first.clone(), first.clone()],
                    &[100, 200],
                    dictionary,
                ),
                keyed_batch(
                    &meta,
                    &[partner.clone(), partner.clone()],
                    &[100, 200],
                    dictionary,
                ),
            ],
        )
        .await;
        let reader = format.reader(store.clone(), handle).await.unwrap();
        let first_key = exact(&meta, &["host-020078", "z"], 1, 7);
        let partner_key = exact(&meta, &["host-029838", "z"], 1, 7);
        assert_eq!(
            read_key(reader.as_ref(), first_key.clone()).await,
            vec![(1, 7, 100, 1, 0, Some(0)), (1, 7, 200, 1, 0, Some(0)),]
        );
        assert_eq!(
            read_key(reader.as_ref(), partner_key.clone()).await,
            vec![(1, 7, 100, 1, 0, Some(0)), (1, 7, 200, 1, 0, Some(0)),]
        );
        // Selecting both owners reads both, even though they share one identifier.
        let mut req = full_request();
        req.keys = Some(BTreeSet::from([first_key.clone(), partner_key.clone()]));
        let out = reader
            .read(req)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(collect_rows(&out).len(), 4);
        // Selecting one owner never reads the partner's block.
        let mut req = full_request();
        req.keys = Some(BTreeSet::from([partner_key]));
        let out = reader
            .read(req)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(collect_rows(&out).len(), 2);
    }
}

/// The silent-loss variant: timestamps keep increasing across the collision, so the
/// old writer accepted it and both series' samples landed under one identity.
#[tokio::test]
async fn colliding_tsids_with_increasing_timestamps_are_retained() {
    let store = store();
    let meta = metadata();
    let format = ColumnarSamplesFormat::default();
    let first = new_sparse_primary_key(&["host-020078", "z"], &meta, 1, 7);
    let partner = new_sparse_primary_key(&["host-029838", "z"], &meta, 1, 7);
    for dictionary in [false, true] {
        let handle = build(
            &format,
            &store,
            meta.clone(),
            &[
                keyed_batch(
                    &meta,
                    &[first.clone(), first.clone()],
                    &[100, 200],
                    dictionary,
                ),
                keyed_batch(
                    &meta,
                    &[partner.clone(), partner.clone()],
                    &[300, 400],
                    dictionary,
                ),
            ],
        )
        .await;
        let reader = format.reader(store.clone(), handle).await.unwrap();
        let first_key = exact(&meta, &["host-020078", "z"], 1, 7);
        let partner_key = exact(&meta, &["host-029838", "z"], 1, 7);
        assert_eq!(
            read_key(reader.as_ref(), first_key).await,
            vec![(1, 7, 100, 1, 0, Some(0)), (1, 7, 200, 1, 0, Some(0)),]
        );
        assert_eq!(
            read_key(reader.as_ref(), partner_key).await,
            vec![(1, 7, 300, 1, 0, Some(0)), (1, 7, 400, 1, 0, Some(0)),]
        );
    }
}

/// Many distinct keys under one forced tsid, through both primary-key inputs.
#[tokio::test]
async fn many_distinct_keys_under_one_tsid_are_retained() {
    let store = store();
    let meta = metadata();
    let format = ColumnarSamplesFormat::default();
    let keys: Vec<_> = (0..8)
        .map(|index| {
            let label = format!("host-{index:06}");
            new_sparse_primary_key(&[label.as_str(), "z"], &meta, 1, 5)
        })
        .collect();
    let times: Vec<_> = (0..8).map(|index| 100 + index as i64).collect();
    for dictionary in [false, true] {
        let handle = build(
            &format,
            &store,
            meta.clone(),
            &[keyed_batch(&meta, &keys, &times, dictionary)],
        )
        .await;
        let reader = format.reader(store.clone(), handle).await.unwrap();
        let all = reader
            .read(full_request())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(collect_rows(&all).len(), keys.len());
        // Each owner sees exactly one row, at its own timestamp.
        for index in 0..8 {
            let label = format!("host-{index:06}");
            let key = exact(&meta, &[label.as_str(), "z"], 1, 5);
            let rows = read_key(reader.as_ref(), key).await;
            assert_eq!(rows, vec![(1, 5, 100 + index as i64, 1, 0, Some(0))]);
        }
        // An absent owner selects nothing, even though its identifier is present.
        let absent = exact(&meta, &["host-999999", "z"], 1, 5);
        assert!(read_key(reader.as_ref(), absent).await.is_empty());
    }
}

/// A regression in sparse-key order is refused; the same rules that guard one series
/// still apply to a stream of distinct series.
#[tokio::test]
async fn out_of_order_series_are_refused() {
    let store = store();
    let meta = metadata();
    let format = ColumnarSamplesFormat::default();
    let first = new_sparse_primary_key(&["a", "b"], &meta, 1, 1);
    let second = new_sparse_primary_key(&["a", "b"], &meta, 1, 2);
    let file_id = FileId::random();
    let generation = FileId::random();
    let prefix = format!("{}/samples/{generation}/", meta.region_id.as_u64());
    let mut writer = format
        .writer(meta.clone(), store.clone(), &prefix, file_id, generation)
        .await
        .unwrap();
    writer
        .write(&keyed_batch(
            &meta,
            &[first.clone(), second.clone()],
            &[100, 200],
            false,
        ))
        .await
        .unwrap();
    let error = writer
        .write(&keyed_batch(&meta, &[first], &[300], false))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("not sorted by primary key"), "{error}");
    writer.abort().await.unwrap();
    // Timestamps are still monotonic inside one exact series.
    let generation = FileId::random();
    let prefix = format!("{}/samples/{generation}/", meta.region_id.as_u64());
    let mut writer = format
        .writer(
            meta.clone(),
            store.clone(),
            &prefix,
            FileId::random(),
            generation,
        )
        .await
        .unwrap();
    writer
        .write(&keyed_batch(
            &meta,
            std::slice::from_ref(&second),
            &[200],
            false,
        ))
        .await
        .unwrap();
    let error = writer
        .write(&keyed_batch(&meta, &[second], &[100], false))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("sample input is not sorted"), "{error}");
    writer.abort().await.unwrap();
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
    req.keys = Some(BTreeSet::new());
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
        let prefix = format!("{}/samples/{generation}/", m.region_id.as_u64());
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
    assert!(
        reads
            .lock()
            .unwrap()
            .iter()
            .all(|(path, _)| path.ends_with("samples.idx"))
    );
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
    let (_, entry, page, index_bytes) = first_leaf(&store, &handle).await;
    let Page(blocks) = page;
    let decoded = Page(blocks.clone()).encode();
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
    let footer_start = index_bytes.len() - index::FOOTER_SIZE;
    for offset in [footer_start, index_bytes.len() - 1] {
        let mut bad = index_bytes.clone();
        bad[offset] ^= 3;
        store.write(&manifest.path(), bad).await.unwrap();
        assert!(format.reader(store.clone(), handle.clone()).await.is_err());
    }
    let mut footer = index_footer(&index_bytes);
    footer.leaves = u64::MAX;
    assert!(index::Footer::decode(&footer.encode(), index_bytes.len() as u64).is_err());
    let mut footer = index_footer(&index_bytes);
    footer.metadata.decoded = (MANIFEST_LIMIT + 1) as u32;
    assert!(index::Footer::decode(&footer.encode(), index_bytes.len() as u64).is_err());
    store.write(&manifest.path(), index_bytes).await.unwrap();
    for length in 0..decoded.len() {
        assert!(Page::decode(&decoded[..length], &entry.page, entry.keys(), &manifest).is_err());
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

#[rstest::rstest]
#[case(vec![1000, 2000], 1, true)]
#[case(vec![-1, 0, 86_399_999, 86_400_000], 3, true)]
#[case(vec![86_399_999, 86_400_000], 2, false)]
#[case((1000..1065).collect(), 2, true)]
#[tokio::test]
async fn real_samples_reconcile_reopen_and_retire(
    #[case] expected_times: Vec<i64>,
    #[case] windows: usize,
    #[case] flush_each: bool,
) {
    use crate::config::MitoConfig;
    use crate::series_index::format::SeriesFormats;
    use crate::series_index::maintenance::reconcile_series_indexes;
    use crate::test_util::TestEnv;
    let mut env = TestEnv::with_prefix("columnar-samples-reconcile").await;
    let (engine, region) = crate::series_index::tests::prepare_region_with_metadata_and_flush(
        &mut env,
        &expected_times,
        MitoConfig {
            experimental_enable_series_index: true,
            experimental_series_index_maintenance_interval: Duration::from_secs(3600),
            min_compaction_interval: Duration::from_secs(3600),
            ..Default::default()
        },
        metadata(),
        flush_each,
    )
    .await;
    if !flush_each {
        assert!(
            region
                .version()
                .ssts
                .levels()
                .iter()
                .flat_map(|l| l.files())
                .any(|f| {
                    let (a, b) = f.time_range();
                    crate::series_index::builder::sample_day(a)
                        != crate::series_index::builder::sample_day(b)
                })
        );
    }
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
    assert_eq!(reconcile().await.unwrap().built_samples, windows);
    assert_eq!(reconcile().await.unwrap().built_samples, 0);
    let reopened =
        crate::series_index::catalog::load_version_control(&store, region.region_id, &purger).await;
    assert_eq!(reopened.current().samples.len(), windows);
    let mut timestamps = Vec::new();
    for handle in reopened.current().samples.values() {
        let day = handle.entry().window.unwrap();
        let manifest = manifest(&store, handle).await;
        // All source chunks for this series/day are repacked into one block.
        assert_eq!(manifest.files.iter().map(|p| p.num_blocks).sum::<u64>(), 1);
        assert!(
            manifest
                .files
                .iter()
                .all(|p| p.min_timestamp.div_euclid(86_400_000) == day
                    && p.max_timestamp.div_euclid(86_400_000) == day)
        );
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
    assert_eq!(timestamps, expected_times);
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
    let rebuilt_days = before
        .samples
        .values()
        .filter(|h| h.entry().source_file_ids == old.entry().source_file_ids)
        .count();
    for _ in 0..rebuilt_days {
        let cleanup = purges.try_recv().unwrap().objects.unwrap();
        assert_eq!(cleanup.len(), 2);
        assert!(cleanup.iter().all(|path| before.samples.values().all(|h| {
            h.entry()
                .artifact
                .objects
                .iter()
                .all(|object| object.path != *path)
        })));
    }
    assert!(purges.try_recv().is_err());

    // A successful rebuild replaces the generation it supersedes, and that generation is retired:
    // the published snapshot keeps its objects alive, so it is purged only once the last reader of
    // the superseded generation is dropped. This pass also rebuilds the day whose generation the
    // failed publish above left behind.
    drop(before);
    let current = region.series_index_version();
    let retired = current.samples.values().next().unwrap().clone();
    let retired_paths: BTreeSet<String> = retired
        .entry()
        .artifact
        .objects
        .iter()
        .map(|object| object.path.clone())
        .collect();
    store
        .delete(&retired.entry().artifact.objects[0].path)
        .await
        .unwrap();
    let rebuilt = reconcile().await.unwrap();
    assert!(rebuilt.built_samples > 0);
    let next = region.series_index_version();
    assert!(!Arc::ptr_eq(&current, &next));
    assert!(
        next.samples
            .values()
            .all(|handle| handle.entry().file_id != retired.entry().file_id)
    );
    assert!(next.samples.values().all(|handle| {
        handle
            .entry()
            .artifact
            .objects
            .iter()
            .all(|object| !retired_paths.contains(&object.path))
    }));
    // Nothing is released while the superseded snapshot and its handle are still alive.
    assert!(purges.try_recv().is_err());
    // A reader outlives the snapshot it was taken from, so hold a handle on every generation the
    // superseded snapshot published: a rebuild can replace several of them at once, and releasing
    // the snapshot must release none while those handles are alive.
    let readers: Vec<_> = current
        .samples
        .values()
        .filter(|handle| handle.entry().file_id != retired.entry().file_id)
        .cloned()
        .collect();
    drop(current);
    // The purge follows the last reader handle, not the snapshot: `retired` and `readers` still
    // hold the superseded generations, so dropping the published snapshot that owned one of their
    // handles each must not release them.
    assert!(purges.try_recv().is_err());
    drop(retired);
    let mut purged = BTreeSet::new();
    while let Ok(request) = purges.try_recv() {
        purged.extend(request.objects.unwrap());
    }
    assert!(retired_paths.is_subset(&purged));
    // The generations kept alive above are superseded as well, so they are released with their
    // last reader handle, not with the snapshot.
    drop(readers);
    while let Ok(request) = purges.try_recv() {
        purged.extend(request.objects.unwrap());
    }
    // Only superseded generations are released; nothing the snapshot still points at is purged.
    assert!(
        region
            .series_index_version()
            .samples
            .values()
            .all(|handle| handle
                .entry()
                .artifact
                .objects
                .iter()
                .all(|object| !purged.contains(&object.path)))
    );
    use store_api::region_engine::RegionEngine;
    engine.stop().await.unwrap();
}

/// Rewrites the stored root page so every field column carries an unknown codec, and returns a
/// handle whose descriptor sizes match the rewritten bytes. Everything else stays valid.
async fn unknown_field_codec(store: &ObjectStore, handle: SamplesFileHandle) -> SamplesFileHandle {
    let (_, _, page, _) = first_leaf(store, &handle).await;
    let Page(mut blocks) = page;
    let mut patched = 0;
    for column in blocks.iter_mut().flat_map(|block| block.columns.iter_mut()) {
        if column.role == 4 {
            column.payload.encoding = 99;
            patched += 1;
        }
    }
    assert_eq!(1, patched, "the fixture must store one field column");
    replace_leaf(store, handle, Page(blocks)).await
}

/// A column whose codec the format does not know can only be skipped: a query that does not
/// project it reads normally, and projecting it is an error rather than silent garbage.
#[tokio::test]
async fn unknown_field_codec_can_only_be_skipped() {
    let store = store();
    let m = metadata();
    let format = ColumnarSamplesFormat::default();
    let handle = build(
        &format,
        &store,
        m.clone(),
        &[batch(&m, &[(1, 1, 100, 1, 1, Some(7))], false)],
    )
    .await;
    let handle = unknown_field_codec(&store, handle).await;
    let reader = format.reader(store, handle).await.unwrap();
    let timestamps = reader
        .read(request(&["ts"]))
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(1, timestamps.iter().map(|b| b.num_rows()).sum::<usize>());
    let error = reader
        .read(request(&["field_0"]))
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("samples column encoding"),
        "{error}"
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
    let prefix = format!("{}/samples/{generation}/", m.region_id.as_u64());
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
                if !manifest_only || path.ends_with("samples.idx") {
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
        let prefix = format!("{}/samples/{generation}/", m.region_id.as_u64());
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
        let payload: Vec<_> = reads
            .iter()
            .filter(|(path, _)| path.ends_with(".samples"))
            .collect();
        assert_eq!(2, payload.len());
        assert_eq!(
            2 * part_size,
            payload.iter().map(|(_, r)| r.size().unwrap()).sum::<u64>()
        );
    }
    reads.lock().unwrap().clear();
    let sparse = reader
        .read(request(&["field_0"]))
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(4, sparse.iter().map(|b| b.num_rows()).sum::<usize>());
    assert!(
        reads
            .lock()
            .unwrap()
            .iter()
            .filter(|(path, _)| path.ends_with(".samples"))
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
    // Anchor the timestamp read by provenance rather than by position: the timestamp chunk is
    // the first chunk of a part, so its read starts at the payload header. Positional indexing
    // would silently point at the key-area read instead.
    let timestamp_read = {
        let reads = reads.lock().unwrap();
        // Header, trailer, root, the selected block's key, then timestamps only. The other part
        // is pruned.
        assert_eq!(
            1,
            reads
                .iter()
                .filter(|(path, _)| path.ends_with(".samples"))
                .count()
        );
        reads
            .iter()
            .find(|(path, range)| path.ends_with(".samples") && range.offset() == 0)
            .cloned()
            .expect("the timestamp chunk must be read")
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
    // Both boundary blocks read timestamps before fetching remaining columns, and each part
    // reads its own keys.
    assert_eq!(
        4,
        reads
            .iter()
            .filter(|(path, _)| path.ends_with(".samples"))
            .count()
    );
    assert_eq!(
        Some(&timestamp_read),
        reads
            .iter()
            .find(|(path, range)| path.ends_with(".samples") && range.offset() == 0)
    );
}

#[tokio::test]
async fn series_read_windows_are_bounded_and_gap_rows_are_not_returned() {
    let store = store();
    let m = metadata();
    let format = ColumnarSamplesFormat::default();
    let input = (0..260)
        .map(|id| (1, id, 0, 1, 1, Some(id)))
        .collect::<Vec<_>>();
    let handle = build(&format, &store, m.clone(), &[batch(&m, &input, false)]).await;
    let reads = Arc::new(Mutex::new(Vec::new()));
    let observed = reads.clone();
    let reader = format
        .reader(
            store.layer(
                MockLayerBuilder::default()
                    .reader_factory(Arc::new(move |path, _, inner| {
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
    let all = reader
        .read(full_request())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(input, collect_rows(&all));
    let (full_bytes, first_window_bytes) = {
        let reads = reads.lock().unwrap();
        // The 260 payload blocks require three windows of at most 128 blocks.
        assert_eq!(
            3,
            reads
                .iter()
                .filter(|(path, _)| path.ends_with(".samples"))
                .count()
        );
        // The initial read-ahead window is the first read of block 0's first column: it starts
        // at the payload header, while the header, trailer, and page reads do not. Anchor on
        // that range rather than on an index so a reordered traversal cannot point the
        // assertions below at some later window.
        let (_, window) = reads
            .iter()
            .find(|(path, range)| path.ends_with(".samples") && range.offset() == 0)
            .expect("the initial payload window must be read");
        let window_size = window.size().unwrap() as usize;
        // One row per block and four payload columns: 8 + 8 + 1 + 8 bytes each. A read that
        // spans the whole 128-block window is far larger than a single block's columns.
        assert!(
            window_size >= 128 * (8 + 8 + 1 + 8),
            "the anchored read must be the initial 128-block window, not one block: {window_size}"
        );
        (
            reads.iter().map(|(_, r)| r.size().unwrap()).sum::<u64>(),
            window_size,
        )
    };
    reads.lock().unwrap().clear();
    let mut request = full_request();
    request.keys = Some(
        [0, 259]
            .into_iter()
            .map(|tsid| exact(&m, &["a", "b"], 1, tsid))
            .collect(),
    );
    let selected = reader
        .read(request)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(vec![input[0], input[259]], collect_rows(&selected));
    assert!(
        reads
            .lock()
            .unwrap()
            .iter()
            .map(|(_, r)| r.size().unwrap())
            .sum::<u64>()
            < full_bytes
    );

    reads.lock().unwrap().clear();
    let mut request = full_request();
    request.keys = Some(
        [0, 2]
            .into_iter()
            .map(|tsid| exact(&m, &["a", "b"], 1, tsid))
            .collect(),
    );
    let selected = reader
        .read(request)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(vec![input[0], input[2]], collect_rows(&selected));
    // The intervening series is read as a gap, but never decoded or returned: only the
    // leaf that owns both selected blocks and one payload window are fetched.
    assert_eq!(
        1,
        reads
            .lock()
            .unwrap()
            .iter()
            .filter(|(path, _)| path.ends_with(".samples"))
            .count()
    );

    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    // The first pool rejects read-ahead outright. The second admits the first 128-block,
    // four-column window but leaves no room for block decoding. Both also hold the part's
    // retained exact keys, which are charged to the scan pool before the window is attempted:
    // 260 series x (key bytes + the struct + allocator rounding).
    let keys_bytes = 260 * (new_sparse_primary_key(&["a", "b"], &m, 1, 0).len() + 32 + 64);
    for capacity in [
        64 * 1024,
        2 * first_window_bytes + 128 * 4 * 256 + keys_bytes,
    ] {
        reads.lock().unwrap().clear();
        let pool = Arc::new(GreedyMemoryPool::new(capacity));
        let mut request = full_request();
        request.memory_pool = Some(pool.clone());
        let all = reader
            .read(request.clone())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(input, collect_rows(&all));
        assert_eq!(0, pool.reserved());
        {
            let reads = reads.lock().unwrap();
            assert!(reads.len() > 6);
            // The fallback reads that same first column one block at a time, so only the larger
            // pool's first payload read spans the whole window. Read order depends on the pool,
            // which is why the anchor is the range rather than an index.
            let initial = reads
                .iter()
                .find(|(path, range)| path.ends_with(".samples") && range.offset() == 0)
                .map(|(_, range)| range.size().unwrap() as usize);
            assert_eq!(
                capacity != 64 * 1024,
                initial == Some(first_window_bytes),
                "only the larger pool should admit the initial 128-block prefetch: {initial:?}"
            );
        }
        let mut stream = reader.read(request).unwrap();
        assert!(stream.try_next().await.unwrap().is_some());
        drop(stream);
        assert_eq!(0, pool.reserved());
    }
}

/// Keys live in the part's key area, not in the metadata pages: a page keeps a fixed-size
/// reference whatever the key length is, and the area holds one copy per covered series.
#[tokio::test]
async fn keys_are_kept_out_of_metadata_pages() {
    let store = store();
    let meta = metadata();
    let format = ColumnarSamplesFormat::default();
    let label = "x".repeat(1024);
    let keys = (0..3)
        .map(|tsid| new_sparse_primary_key(&[label.as_str(), "z"], &meta, 1, tsid))
        .collect::<Vec<_>>();
    let handle = build(
        &format,
        &store,
        meta.clone(),
        &[keyed_batch(&meta, &keys, &[100, 100, 100], false)],
    )
    .await;
    let (_, leaf_entry, page, _) = first_leaf(&store, &handle).await;
    assert!(
        page.retained_size() < 3 * 1024,
        "retained size {} still stores the keys in the page",
        page.retained_size()
    );
    assert_eq!(
        keys.iter()
            .map(|key| key.len() as u64 + KEY_CHECKSUM_BYTES)
            .sum::<u64>(),
        leaf_entry.page.payload.offset - leaf_entry.keys_start,
        "the key area holds one copy of each key and its checksum"
    );
}

/// One stream builds one decoder and one scratch, however many compressed blocks it reads.
///
/// Nothing else observes that reuse: a fresh decoder or scratch per block returns the same
/// rows, so the codec's context counter and the scratch counter are what pin it.
#[tokio::test]
async fn stream_reuses_one_decoder_and_scratch_across_compressed_blocks() {
    let store = store();
    let meta = metadata();
    // Four blocks of one series, each with a payload large enough to be stored compressed.
    let format = ColumnarSamplesFormat {
        block_rows: 1024,
        ..Default::default()
    };
    let rows: Vec<_> = (0..4096)
        .map(|index| (1, 7, index as i64, 1, 1, Some(0)))
        .collect();
    let handle = build(&format, &store, meta.clone(), &[batch(&meta, &rows, false)]).await;
    let manifest = manifest(&store, &handle).await;
    assert_eq!(1, manifest.files.len());
    assert_eq!(4, manifest.files[0].num_blocks);
    // Every block must decode a compressed payload, or a decoder rebuilt per block would build
    // no context and the assertion below could not fail.
    let (_, _, page, _) = first_leaf(&store, &handle).await;
    let Page(blocks) = page;
    assert!(blocks.iter().all(|block| {
        block
            .columns
            .iter()
            .any(|column| column.payload.compression == 1)
    }));
    let reader = format.reader(store, handle).await.unwrap();

    let contexts = columnar_codec::contexts_built();
    let scratches = reader::scratches_built();
    let batches = reader
        .read(full_request())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    // Every block was read once, and all of them shared the stream's single decoder.
    assert_eq!(4, batches.len());
    assert_eq!(rows, collect_rows(&batches));
    assert_eq!(
        contexts + 1,
        columnar_codec::contexts_built(),
        "all blocks of one stream must share one zstd context"
    );
    assert_eq!(
        scratches + 1,
        reader::scratches_built(),
        "a stream must build its scratch once, not once per block"
    );
}

/// Reuse is per stream, never shared: a second stream over the same artifact builds its own
/// decoder and scratch and returns the same rows.
#[tokio::test]
async fn each_stream_owns_its_decoder_and_scratch() {
    let store = store();
    let meta = metadata();
    let format = ColumnarSamplesFormat {
        block_rows: 1024,
        ..Default::default()
    };
    let rows: Vec<_> = (0..2048)
        .map(|index| (1, 7, index as i64, 1, 1, Some(0)))
        .collect();
    let handle = build(&format, &store, meta.clone(), &[batch(&meta, &rows, false)]).await;
    let reader = format.reader(store, handle).await.unwrap();
    let contexts = columnar_codec::contexts_built();
    let scratches = reader::scratches_built();
    let first = reader
        .read(full_request())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let second = reader
        .read(full_request())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(2, first.len());
    assert_eq!(first.len(), second.len());
    assert_eq!(collect_rows(&first), collect_rows(&second));
    // Two streams build two decoders and two scratches: one each, not one shared.
    assert_eq!(contexts + 2, columnar_codec::contexts_built());
    assert_eq!(scratches + 2, reader::scratches_built());
}

/// A block belongs to exactly one exact series, so its key is emitted once per batch. The
/// scan reservation therefore budgets one key per block instead of one per row: a block of
/// 8192 rows with a 1 KiB key costs 1 KiB of identity, not 8 MiB.
#[tokio::test]
async fn exact_key_is_emitted_once_per_batch() {
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};

    let store = store();
    let m = metadata();
    let format = ColumnarSamplesFormat::default();
    let label = "x".repeat(1024);
    let keys = vec![new_sparse_primary_key(&[label.as_str(), "z"], &m, 1, 7); 8192];
    let times = (0..8192_i64).collect::<Vec<_>>();
    let handle = build(
        &format,
        &store,
        m.clone(),
        &[keyed_batch(&m, &keys, &times, false)],
    )
    .await;
    let reader = format.reader(store, handle).await.unwrap();
    let mut request = request(&[TABLE_ID_COLUMN, TSID_COLUMN, "ts"]);
    request.keys = Some(BTreeSet::from([exact(&m, &[label.as_str(), "z"], 1, 7)]));
    // A pooled read that only has to hold one key per batch succeeds; a per-row key column of
    // 8 MiB would not fit here.
    let pool = Arc::new(GreedyMemoryPool::new(4 * 1024 * 1024));
    request.memory_pool = Some(pool.clone());
    let batches = reader
        .read(request)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(8192, batches.iter().map(|b| b.num_rows()).sum::<usize>());
    let expected = exact(&m, &[label.as_str(), "z"], 1, 7);
    assert!(batches.iter().all(|batch| batch.key == expected));
    // The key is the batch's, not a column: the artifact schema keeps it, output batches do not.
    assert!(
        batches
            .iter()
            .all(|batch| batch.batch.column_by_name(PRIMARY_KEY).is_none())
    );
    assert_eq!(0, pool.reserved());
}

/// The reservation must cover what a batch actually retains, and identity must not be charged
/// per row: 2135 rows under a 1 KiB key would be over 2 MiB of keys alone.
#[tokio::test]
async fn exact_key_output_reservation_covers_the_retained_array() {
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};

    let store = store();
    let m = metadata();
    let format = ColumnarSamplesFormat::default();
    let label = "x".repeat(1024);
    let rows = 2135;
    let keys = vec![new_sparse_primary_key(&[label.as_str(), "z"], &m, 1, 7); rows];
    let key_len = keys[0].len();
    let times = (0..rows as i64).collect::<Vec<_>>();
    let handle = build(
        &format,
        &store,
        m.clone(),
        &[keyed_batch(&m, &keys, &times, false)],
    )
    .await;
    let reader = format.reader(store.clone(), handle).await.unwrap();
    // The key is returned with the batch, so projecting it as a column is refused instead of
    // silently repeating it once per row.
    assert!(
        reader
            .read(SamplesReadRequest {
                projection: vec![PRIMARY_KEY.to_string()],
                ..Default::default()
            })
            .is_err()
    );
    let pool = Arc::new(GreedyMemoryPool::new(128 * 1024 * 1024));
    let mut request = request(&[TABLE_ID_COLUMN, TSID_COLUMN, "ts"]);
    request.keys = Some(BTreeSet::from([exact(&m, &[label.as_str(), "z"], 1, 7)]));
    request.memory_pool = Some(pool.clone());
    let mut stream = reader.read(request).unwrap();
    let batch = stream.try_next().await.unwrap().expect("one batch");
    assert_eq!(rows, batch.num_rows());
    // The batch is yielded while its reservation is still held, so the two are comparable.
    let retained = batch.get_array_memory_size();
    let reserved = pool.reserved();
    assert!(
        reserved >= retained,
        "reservation {reserved} is below the {retained} bytes the yielded batch retains"
    );
    // No per-row identity term survives: the estimate is far below one key per row, and the
    // key itself is retained once rather than once per row.
    assert!(
        reserved < rows * key_len,
        "reservation {reserved} still charges {rows} keys of {key_len} bytes"
    );
    assert!(
        reserved < retained + rows * key_len,
        "reservation {reserved} dwarfs the {retained} bytes retained"
    );
    drop(batch);
    drop(stream);
    assert_eq!(0, pool.reserved());
}

/// Sparse selections must bound aligned cache reads as well as selected column bytes.
#[rstest::rstest]
#[case(1024 * 1024)]
#[case(128 * 1024)]
// Retained exact keys are charged to the scan pool: 128 selected series cost
// 128 * (key bytes + ExactSeriesKey + allocator rounding) ~= 14.3 KB, so a 16 KiB pool can no
// longer hold both the keys and one block's payload reservation.
#[case(64 * 1024)]
#[tokio::test]
async fn sparse_cached_reads_fit_the_scan_pool(#[case] capacity: usize) {
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    let store = store();
    let m = metadata();
    let input = (0..256_u64)
        .flat_map(|id| {
            let count = if id % 2 == 0 { 1 } else { 2000 };
            (0..count).map(move |t| {
                (
                    1,
                    id,
                    t,
                    1,
                    1,
                    Some((t as u64).wrapping_mul(0x9e3779b97f4a7c15)),
                )
            })
        })
        .collect::<Vec<_>>();
    let handle = build(
        &ColumnarSamplesFormat::default(),
        &store,
        m.clone(),
        &[batch(&m, &input, false)],
    )
    .await;
    let reader = reader::SamplesReader::open(store, handle).await.unwrap();
    let pool = Arc::new(GreedyMemoryPool::new(capacity));
    let mut request = full_request();
    request.memory_pool = Some(pool.clone());
    request.keys = Some(
        (0..256)
            .step_by(2)
            .map(|tsid| exact(&m, &["a", "b"], 1, tsid))
            .collect(),
    );
    let rows = reader
        .read(request.clone())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        collect_rows(&rows),
        input
            .iter()
            .copied()
            .filter(|r| r.1 % 2 == 0)
            .collect::<Vec<_>>()
    );
    assert_eq!(pool.reserved(), 0);
}

mod keys;

/// Narthex readers use the SST lifetime, not the local sidecar purger.
#[tokio::test]
async fn narthex_file_reader_pins_sst_lifetime() {
    use crate::sst::file::{FileFormat, FileHandle, FileMeta, NarthexPartMeta, NarthexSamplesMeta};
    use crate::sst::file_purger::FilePurger;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct CountingPurger(Arc<AtomicUsize>);
    impl FilePurger for CountingPurger {
        fn purge_file(&self, _: FileMeta) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let store = store();
    let meta = metadata();
    let id = FileId::random();
    let prefix = format!("table/region/{id}/");
    let rows = vec![
        (1, 2, 100, 7, 0, Some(1.5f64.to_bits())),
        (1, 2, 101, 8, 0, None),
    ];
    let mut writer = ColumnarSamplesFormat::default()
        .writer(meta.clone(), store.clone(), &prefix, id, id)
        .await
        .unwrap();
    writer.write(&batch(&meta, &rows, false)).await.unwrap();
    let artifact = writer.finish().await.unwrap();
    let (manifest, parts) = artifact.objects.split_first().unwrap();
    let narthex = NarthexSamplesMeta {
        index_size: manifest.file_size,
        parts: parts
            .iter()
            .map(|part| NarthexPartMeta {
                file_size: part.file_size,
            })
            .collect(),
    };
    let file_meta = FileMeta {
        region_id: meta.region_id,
        file_id: id,
        file_size: artifact.file_size,
        num_rows: rows.len() as u64,
        format: FileFormat::NarthexSamples(Box::new(narthex)),
        ..Default::default()
    };
    let restored: FileMeta =
        serde_json::from_str(&serde_json::to_string(&file_meta).unwrap()).unwrap();
    assert_eq!(file_meta, restored);
    let purged = Arc::new(AtomicUsize::new(0));
    let file = FileHandle::new(restored, Arc::new(CountingPurger(purged.clone())));
    let handle = SamplesFileHandle::from_narthex(file.clone(), "table/region/").unwrap();
    let reader = reader::SamplesReader::open(store, handle).await.unwrap();
    file.mark_deleted();
    drop(file);
    assert_eq!(0, purged.load(Ordering::Relaxed));
    let batches: Vec<_> = reader
        .read(full_request())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(rows, collect_rows(&batches));
    drop(reader);
    assert_eq!(1, purged.load(Ordering::Relaxed));
}

#[test]
fn narthex_region_schema_rejected_before_writes() {
    let options = crate::region::options::RegionOptions {
        sst_format: Some(crate::sst::FormatType::Narthex),
        ..Default::default()
    };
    options.validate_schema(&metadata()).unwrap();
    let unsupported_field = sst_region_metadata_with_encoding(PrimaryKeyEncoding::Sparse);
    assert!(options.validate_schema(&unsupported_field).is_err());
    let dense = sst_region_metadata_with_encoding(PrimaryKeyEncoding::Dense);
    assert!(options.validate_schema(&dense).is_err());
}

#[tokio::test]
async fn narthex_write_error_cleans_completed_parts_before_returning() {
    let store = store();
    let metadata = metadata();
    let format = ColumnarSamplesFormat {
        block_rows: 1,
        part_bytes: 1,
        ..Default::default()
    };
    let mut writer = format
        .writer(
            metadata.clone(),
            store.clone(),
            "narthex-error/",
            FileId::random(),
            FileId::random(),
        )
        .await
        .unwrap();
    writer
        .write(&batch(
            &metadata,
            &[
                (1, 1, 1000, 1, 0, Some(1.0_f64.to_bits())),
                (1, 2, 1000, 1, 0, Some(2.0_f64.to_bits())),
                (1, 3, 1000, 1, 0, Some(3.0_f64.to_bits())),
            ],
            true,
        ))
        .await
        .unwrap();
    assert!(
        !store
            .list_with("narthex-error/")
            .recursive(true)
            .await
            .unwrap()
            .is_empty()
    );
    let error = crate::series_index::columnar::invalid("injected source error");
    let error = crate::sst::narthex::abort_narthex_write(writer, error).await;
    assert!(error.to_string().contains("injected source error"));
    assert!(
        store
            .list_with("narthex-error/")
            .recursive(true)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn narthex_prepared_discovery_empty_projection_skips_payload() {
    let store = store();
    let metadata = metadata();
    let rows = (1..=2)
        .flat_map(|id| (0..10).map(move |time| (1, id, time, 1, 1, Some(1.0f64.to_bits()))))
        .collect::<Vec<_>>();
    let handle = build(
        &ColumnarSamplesFormat::default(),
        &store,
        metadata.clone(),
        &[batch(&metadata, &rows, false)],
    )
    .await;
    let reads = Arc::new(Mutex::new(Vec::new()));
    let observed = reads.clone();
    let store = store.layer(
        MockLayerBuilder::default()
            .reader_factory(Arc::new(move |path, _, inner| {
                Box::new(RecordingRead {
                    inner,
                    path: path.into(),
                    reads: observed.clone(),
                })
            }))
            .build()
            .unwrap(),
    );
    let reader = reader::SamplesReader::open(store, handle).await.unwrap();
    let discovered = reader
        .read(SamplesReadRequest::default())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        20,
        discovered
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>()
    );
    assert!(discovered.iter().all(|batch| batch.num_columns() == 0));
    assert_eq!(
        2,
        discovered
            .iter()
            .map(|batch| &batch.key)
            .collect::<BTreeSet<_>>()
            .len()
    );
    let reads = reads.lock().unwrap();
    assert!(reads.iter().any(|(path, _)| path.ends_with("samples.idx")));
    assert!(reads.iter().all(|(path, _)| !path.ends_with(".samples")));
}

/// Native histogram fields round-trip through an artifact. A block ends before its histogram
/// chunk would outgrow the chunk limit, and the stored chunk is only readable as the field type
/// the manifest records.
#[tokio::test]
async fn native_histogram_fields_round_trip_and_split_by_chunk_limit() {
    use datatypes::arrow::compute::concat;

    use super::histogram::tests::{TestHistogram, fixture, histogram_array};

    let mut m = sst_region_metadata_with_encoding(PrimaryKeyEncoding::Sparse);
    m.column_metadatas
        .iter_mut()
        .find(|c| c.column_id == 2)
        .unwrap()
        .column_schema
        .data_type = native_histogram_value_type().clone();
    let m = Arc::new(m);
    let small = histogram_array(&fixture());
    // Bucket counts whose XOR deltas stay wide, so the rows together encode past one chunk.
    let large = histogram_array(
        &(0..24)
            .map(|i| {
                Some(TestHistogram {
                    sum: i as f64,
                    positive_spans: vec![(0, 8000)],
                    floats: Some((
                        8000.0,
                        0.0,
                        (0..8000).map(|j| ((i * 8000 + j) as f64).sqrt()).collect(),
                        vec![],
                    )),
                    ..Default::default()
                })
            })
            .collect::<Vec<_>>(),
    );
    assert!(histogram::encode(&large).unwrap().len() > metadata::CHUNK_LIMIT);
    let values = concat(&[&small as &dyn Array, &large]).unwrap();
    let keys = (0..values.len())
        .map(|i| new_sparse_primary_key(&["a", "b"], &m, 1, if i < small.len() { 1 } else { 2 }))
        .collect::<Vec<_>>();
    let times = (0..small.len() as i64)
        .chain(0..large.len() as i64)
        .collect::<Vec<_>>();
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("field_0", values.data_type().clone(), true),
            Field::new(
                "ts",
                DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("__primary_key", DataType::Binary, false),
            Field::new(SEQUENCE, DataType::UInt64, false),
            Field::new(OPERATION, DataType::UInt8, false),
        ])),
        vec![
            values.clone(),
            Arc::new(datatypes::arrow::array::TimestampMillisecondArray::from(
                times,
            )),
            Arc::new(BinaryArray::from_iter_values(
                keys.iter().map(Vec::as_slice),
            )),
            Arc::new(UInt64Array::from(vec![1; values.len()])),
            Arc::new(UInt8Array::from(vec![1; values.len()])),
        ],
    )
    .unwrap();
    let store = store();
    let format = ColumnarSamplesFormat::default();
    // Two input batches, so a block also concatenates rows borrowed from different batches.
    let handle = build(
        &format,
        &store,
        m.clone(),
        &[input.slice(0, 10), input.slice(10, input.num_rows() - 10)],
    )
    .await;
    let read = |handle: SamplesFileHandle| {
        let store = store.clone();
        let format = format.clone();
        async move {
            format
                .reader(store, handle)
                .await
                .unwrap()
                .read(request(&["field_0"]))
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
        }
    };
    let batches = read(handle.clone()).await.unwrap();
    assert!(
        batches.len() > 2,
        "the large series spans more than one block"
    );
    let decoded = concat(
        &batches
            .iter()
            .map(|b| b.column(0).as_ref())
            .collect::<Vec<_>>(),
    )
    .unwrap();
    assert_eq!(&values, &decoded);

    let mut stored = manifest(&store, &handle).await;
    stored.fields[0].data_type = FLOAT64_TYPE.to_string();
    let mut bytes = store
        .read(&stored.path())
        .await
        .unwrap()
        .to_bytes()
        .to_vec();
    let mut footer = index_footer(&bytes);
    bytes.truncate(footer.metadata.offset as usize);
    let (reference, metadata) = index::encode_metadata(
        &index::Schema {
            timestamp: stored.timestamp.clone(),
            fields: stored.fields.clone(),
        },
        &stored.files,
        footer.metadata.offset,
    )
    .unwrap();
    footer.metadata = reference;
    bytes.extend(metadata);
    bytes.extend(footer.encode());
    let index_size = bytes.len() as u64;
    store.write(&stored.path(), bytes).await.unwrap();
    let mut entry = handle.entry().clone();
    for object in &mut entry.artifact.objects {
        if object.path == stored.path() {
            object.file_size = index_size;
        }
    }
    entry.artifact.file_size = entry.artifact.objects.iter().map(|o| o.file_size).sum();
    let (purger, _receiver) = series_index_channel(store.clone());
    let error = read(SamplesFileHandle::new(handle.file_id(), entry, purger))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("does not match its field type"),
        "{error}"
    );
}

/// Read-ahead must not fail a histogram block that fits the pool on its own. The histogram
/// reservation is only known once the chunk header is read, after the block's first reservation.
#[tokio::test]
async fn histogram_reservation_releases_read_ahead() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool, MemoryReservation};

    #[derive(Debug)]
    struct PeakPool {
        inner: GreedyMemoryPool,
        peak: AtomicUsize,
    }
    impl std::fmt::Display for PeakPool {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "peak")
        }
    }
    impl MemoryPool for PeakPool {
        fn name(&self) -> &str {
            "peak"
        }
        fn grow(&self, reservation: &MemoryReservation, additional: usize) {
            self.inner.grow(reservation, additional);
            self.peak
                .fetch_max(self.inner.reserved(), Ordering::Relaxed);
        }
        fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
            self.inner.shrink(reservation, shrink);
        }
        fn try_grow(
            &self,
            reservation: &MemoryReservation,
            additional: usize,
        ) -> datafusion::common::Result<()> {
            self.inner.try_grow(reservation, additional)?;
            self.peak
                .fetch_max(self.inner.reserved(), Ordering::Relaxed);
            Ok(())
        }
        fn reserved(&self) -> usize {
            self.inner.reserved()
        }
    }

    let mut m = sst_region_metadata_with_encoding(PrimaryKeyEncoding::Sparse);
    m.column_metadatas
        .iter_mut()
        .find(|c| c.column_id == 2)
        .unwrap()
        .column_schema
        .data_type = native_histogram_value_type().clone();
    let m = Arc::new(m);
    // Equal, compressible rows: several equal blocks whose chunks all fit one read-ahead window.
    let values = histogram_array(
        &(0..300)
            .map(|_| {
                Some(TestHistogram {
                    positive_spans: vec![(0, 1000)],
                    floats: Some((1000.0, 0.0, vec![1.0; 1000], vec![])),
                    ..Default::default()
                })
            })
            .collect::<Vec<_>>(),
    );
    let store = store();
    let format = ColumnarSamplesFormat::default();
    let rows = values.len();
    let key = new_sparse_primary_key(&["a", "b"], &m, 1, 1);
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("field_0", values.data_type().clone(), true),
            Field::new(
                "ts",
                DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("__primary_key", DataType::Binary, false),
            Field::new(SEQUENCE, DataType::UInt64, false),
            Field::new(OPERATION, DataType::UInt8, false),
        ])),
        vec![
            Arc::new(values.clone()),
            Arc::new(
                datatypes::arrow::array::TimestampMillisecondArray::from_iter_values(
                    0..rows as i64,
                ),
            ),
            Arc::new(BinaryArray::from_iter_values(vec![key.as_slice(); rows])),
            Arc::new(UInt64Array::from(vec![1; rows])),
            Arc::new(UInt8Array::from(vec![1; rows])),
        ],
    )
    .unwrap();
    let handle = build(&format, &store, m.clone(), &[input]).await;
    let reader = format.reader(store, handle).await.unwrap();
    let read = |pool: Arc<dyn MemoryPool>| {
        let mut request = request(&["field_0"]);
        request.memory_pool = Some(pool);
        reader.read(request).unwrap().try_collect::<Vec<_>>()
    };
    let peak = Arc::new(PeakPool {
        inner: GreedyMemoryPool::new(usize::MAX),
        peak: AtomicUsize::new(0),
    });
    assert!(read(peak.clone()).await.unwrap().len() > 2);
    // The peak decodes a block while the window is held. The same block needs less without the
    // window, so a pool one byte smaller must still read everything.
    let pool = Arc::new(GreedyMemoryPool::new(peak.peak.load(Ordering::Relaxed) - 1));
    let batches = read(pool.clone()).await.unwrap();
    let decoded = datatypes::arrow::compute::concat(
        &batches
            .iter()
            .map(|b| b.column(0).as_ref())
            .collect::<Vec<_>>(),
    )
    .unwrap();
    assert_eq!(&values as &dyn Array, decoded.as_ref());
    drop(batches);
    assert_eq!(0, pool.reserved());
}
