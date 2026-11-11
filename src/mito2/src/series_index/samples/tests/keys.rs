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

//! Exact series keys stored outside every bounded page.
//!
//! A block descriptor names its series with a fixed-size reference into the part's key area,
//! so the descriptor — and therefore the metadata page that holds it — never carries a key.
//! These tests pin both directions of that choice: keys far longer than a page must round-trip
//! (they used to exceed the leaf limit and fail the build), and a corrupt reference must be
//! rejected before any row is emitted.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use datatypes::arrow::array::{Array, RecordBatch, UInt32Array, UInt64Array};
use datatypes::arrow::datatypes::{DataType, Field, Schema};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use futures::TryStreamExt;
use object_store::ObjectStore;
use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};

use crate::series_index::artifact::SamplesFileHandle;
use crate::series_index::format::{SamplesBatch, SamplesFormat, SamplesReadRequest};
use crate::series_index::identity::ExactSeriesKey;
use crate::series_index::purger::series_index_channel;
use crate::series_index::samples::codec;
use crate::series_index::samples::metadata::{
    self, KeyRef, MAGIC, Page, PayloadReference, TRAILER_SIZE, Trailer,
};
use crate::series_index::samples::{ColumnarSamplesFormat, Manifest};
use crate::series_index::{TABLE_ID_COLUMN, TSID_COLUMN};
use crate::test_util::sst_util::new_sparse_primary_key;

use super::{RecordingRead, build, exact, keyed_batch, request, root_page, store};

/// `(table_id, tsid, timestamp)` of every returned row, in output order.
fn collect_times(batches: &[SamplesBatch]) -> Vec<(u32, u64, i64)> {
    batches
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap();
            let tsids = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let (times, _) =
                datatypes::timestamp::timestamp_array_to_primitive(batch.column(2)).unwrap();
            (0..batch.num_rows())
                .map(|i| (ids.value(i), tsids.value(i), times.value(i)))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn exact_keys(batches: &[SamplesBatch]) -> Vec<ExactSeriesKey> {
    batches.iter().map(|batch| batch.key.clone()).collect()
}

/// A region with the 1,024 fields the manifest allows, so the widest possible descriptor and
/// its old, key-length-dependent page budget are exercised.
fn wide_metadata() -> RegionMetadataRef {
    let base = super::metadata();
    let mut builder = RegionMetadataBuilder::from_existing((*base).clone());
    for index in 1..1024 {
        builder.push_column_metadata(ColumnMetadata {
            column_id: 100 + index,
            semantic_type: api::v1::SemanticType::Field,
            column_schema: ColumnSchema::new(
                format!("field_{index}"),
                ConcreteDataType::float64_datatype(),
                true,
            ),
        });
    }
    Arc::new(builder.build().unwrap())
}

/// One block whose series has the given key and every one of the 1,024 fields present.
fn wide_batch(meta: &RegionMetadataRef, key: &[u8], times: &[i64]) -> RecordBatch {
    let base = keyed_batch(meta, &vec![key.to_vec(); times.len()], times, false);
    let mut fields = base.schema().fields().to_vec();
    let mut arrays = base.columns().to_vec();
    for index in 1..1024 {
        fields.insert(
            index,
            Arc::new(Field::new(
                format!("field_{index}"),
                DataType::Float64,
                true,
            )),
        );
        arrays.insert(index, base.column(0).clone());
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
}

/// Replaces the part and manifest of `handle` with `bytes` and their recorded sizes.
///
/// Everything else stays valid, so a test can isolate one rewritten descriptor.
async fn rewrite_part(
    store: &ObjectStore,
    handle: SamplesFileHandle,
    mut manifest: Manifest,
    bytes: Vec<u8>,
) -> SamplesFileHandle {
    let part_path = manifest.part_path(0);
    store.write(&part_path, bytes.clone()).await.unwrap();
    manifest.files[0].file_size = bytes.len() as u64;
    let manifest_path = manifest.path();
    let json = serde_json::to_vec(&manifest).unwrap();
    store.write(&manifest_path, json.clone()).await.unwrap();
    let mut entry = handle.entry().clone();
    for object in &mut entry.artifact.objects {
        if object.path == part_path {
            object.file_size = bytes.len() as u64;
        } else if object.path == manifest_path {
            object.file_size = json.len() as u64;
        } else {
            panic!("unexpected samples object {}", object.path);
        }
    }
    entry.artifact.file_size = entry.artifact.objects.iter().map(|o| o.file_size).sum();
    let (purger, _receiver) = series_index_channel(store.clone());
    SamplesFileHandle::new(handle.file_id(), entry, purger)
}

/// A single label this long encodes to a key well past the 65,536-byte page limit. The old
/// descriptor had to fit `48 + 44 * C + key_length` bytes plus the leaf prefix in one page,
/// so this build failed with "sample descriptor exceeds leaf limit"; the key now lives in its
/// own area and only a 12-byte reference stays in the descriptor.
#[tokio::test]
async fn key_longer_than_one_page_round_trips() {
    let store = store();
    let meta = super::metadata();
    let format = ColumnarSamplesFormat::default();
    let label = "x".repeat(65 * 1024);
    let key = new_sparse_primary_key(&[label.as_str(), "z"], &meta, 1, 7);
    assert!(key.len() > 65_536, "key is {} bytes", key.len());
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
    let mut query = request(&[TABLE_ID_COLUMN, TSID_COLUMN, "ts"]);
    query.keys = Some(BTreeSet::from([selection.clone()]));
    let batches = reader
        .read(query)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    // Both rows belong to one exact series, so they come back as one batch carrying that key.
    assert_eq!(vec![(1, 7, 100), (1, 7, 200)], collect_times(&batches));
    assert_eq!(vec![selection], exact_keys(&batches));
    // The stored key is the writer's key byte for byte.
    let (manifest, trailer, page, bytes) = root_page(&store, &handle).await;
    let Page::Leaf(blocks) = page else {
        panic!("one leaf page holds the fixture")
    };
    assert_eq!(1, blocks.len());
    let reference = blocks[0].key;
    assert_eq!(reference.offset, trailer.payload_end);
    assert_eq!(reference.length as usize, key.len());
    assert_eq!(trailer.key_end - trailer.payload_end, key.len() as u64);
    assert_eq!(
        key.as_slice(),
        &bytes[reference.offset as usize..trailer.key_end as usize]
    );
    // A key longer than a page still leaves a descriptor far below the page limit.
    assert_eq!(
        56 + 44 * 4,
        metadata::descriptor_bytes(blocks[0].columns.len())
    );
    assert!(metadata::descriptor_bytes(blocks[0].columns.len()) + 12 < 65_536 / 2);
    // The key area is part of the part, not of any page.
    assert!(trailer.key_end > trailer.payload_end);
    assert!(manifest.files[0].file_size > trailer.key_end);
}

/// At the 1,024-field maximum the old layout left `65,536 - (12 + 48 + 44 * 1,027) = 20,288`
/// bytes for the key. This key is longer than that and still round-trips with every field
/// present in the block.
#[tokio::test]
async fn many_fields_round_trip_a_key_past_the_old_ceiling() {
    let store = store();
    let meta = wide_metadata();
    let format = ColumnarSamplesFormat::default();
    let label = "x".repeat(20 * 1024);
    let key = new_sparse_primary_key(&[label.as_str(), "z"], &meta, 3, 11);
    assert!(key.len() > 20_288, "key is {} bytes", key.len());
    let handle = build(
        &format,
        &store,
        meta.clone(),
        &[wide_batch(&meta, &key, &[10, 20])],
    )
    .await;
    let reader = format.reader(store.clone(), handle.clone()).await.unwrap();
    let selection = exact(&meta, &[label.as_str(), "z"], 3, 11);
    let mut query = request(&[TABLE_ID_COLUMN, TSID_COLUMN, "ts"]);
    query.keys = Some(BTreeSet::from([selection.clone()]));
    let batches = reader
        .read(query)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(vec![(3, 11, 10), (3, 11, 20)], collect_times(&batches));
    assert_eq!(vec![selection], exact_keys(&batches));
    // The widest descriptor stays a fixed number of bytes whatever the key's length is.
    let (_, trailer, page, _) = root_page(&store, &handle).await;
    let Page::Leaf(blocks) = page else {
        panic!("one leaf page holds the fixture")
    };
    assert_eq!(
        56 + 44 * 1027,
        metadata::descriptor_bytes(blocks[0].columns.len())
    );
    assert_eq!(3 + 1024, blocks[0].columns.len());
    assert_eq!(trailer.key_end - trailer.payload_end, key.len() as u64);
}

/// A reference that points outside the key area is a corrupt artifact, not a reason to read
/// some other series' bytes: decoding the page it appears in must fail, and a read must fail
/// before it emits any row.
#[tokio::test]
async fn corrupt_key_reference_is_rejected_before_rows() {
    let store = store();
    let meta = super::metadata();
    let format = ColumnarSamplesFormat::default();
    let label = "y".repeat(8 * 1024);
    let key = new_sparse_primary_key(&[label.as_str(), "z"], &meta, 1, 9);
    let handle = build(
        &format,
        &store,
        meta.clone(),
        &[keyed_batch(
            &meta,
            std::slice::from_ref(&key),
            &[100],
            false,
        )],
    )
    .await;
    let (manifest, trailer, page, bytes) = root_page(&store, &handle).await;
    let Page::Leaf(blocks) = page else {
        panic!("one leaf page holds the fixture")
    };
    let area = trailer.keys();
    // Past the end of the area, before its start, and a zero length: each one addresses
    // descriptor bytes that cannot be a key of this part.
    let corrupt = [
        KeyRef {
            offset: area.end - 1,
            length: 4,
        },
        KeyRef {
            offset: area.start - 4,
            length: 4,
        },
        KeyRef {
            offset: area.start,
            length: 0,
        },
    ];
    let start = trailer.root.payload.offset as usize;
    let mut encoded = Page::Leaf(blocks.clone()).encode();
    for bad in corrupt {
        let mut patched = blocks.clone();
        patched[0].key = bad;
        encoded = Page::Leaf(patched).encode();
        assert!(
            Page::decode(
                &encoded,
                &trailer.root,
                trailer.depth,
                trailer.areas(),
                &manifest
            )
            .is_err(),
            "{bad:?} must not decode"
        );
    }
    // Install the last patch and read it back through the reader.
    let mut rewritten = bytes;
    rewritten.truncate(start);
    rewritten.extend_from_slice(&encoded);
    let mut trailer = trailer;
    trailer.root.payload = PayloadReference {
        offset: start as u64,
        stored: encoded.len() as u32,
        decoded: encoded.len() as u32,
        checksum: codec::checksum(&encoded),
        encoding: 1,
        compression: 0,
    };
    trailer.file_length = (rewritten.len() + TRAILER_SIZE as usize) as u64;
    rewritten.extend_from_slice(&trailer.encode());
    let handle = rewrite_part(&store, handle, manifest, rewritten).await;
    let reader = format.reader(store, handle).await.unwrap();
    let mut stream = reader.read(request(&[TABLE_ID_COLUMN])).unwrap();
    let error = stream
        .try_next()
        .await
        .expect_err("a corrupt key reference must fail the read");
    assert!(
        error.to_string().contains("key area"),
        "unexpected error: {error}"
    );
}

/// Two distinct series that share one `(table_id, tsid)`, one with a long label. Each block
/// still carries its own exact key, so neither read can see the partner's samples, and the
/// key area holds exactly one copy per series.
#[tokio::test]
async fn colliding_series_keep_their_own_exact_keys() {
    let store = store();
    let meta = super::metadata();
    let format = ColumnarSamplesFormat::default();
    let long_label = format!("host-{}", "z".repeat(300));
    let first = new_sparse_primary_key(&["host-020078", "z"], &meta, 1, 7);
    let partner = new_sparse_primary_key(&[long_label.as_str(), "z"], &meta, 1, 7);
    let handle = build(
        &format,
        &store,
        meta.clone(),
        &[
            keyed_batch(&meta, &[first.clone(), first.clone()], &[100, 200], false),
            keyed_batch(
                &meta,
                &[partner.clone(), partner.clone()],
                &[100, 200],
                false,
            ),
        ],
    )
    .await;
    let reader = format.reader(store.clone(), handle.clone()).await.unwrap();
    let first_key = exact(&meta, &["host-020078", "z"], 1, 7);
    let partner_key = exact(&meta, &[long_label.as_str(), "z"], 1, 7);
    assert_ne!(first_key, partner_key);
    assert_eq!(first_key.id(), partner_key.id());
    for (selection, expected) in [
        (first_key.clone(), vec![(1, 7, 100), (1, 7, 200)]),
        (partner_key.clone(), vec![(1, 7, 100), (1, 7, 200)]),
    ] {
        let mut query = request(&[TABLE_ID_COLUMN, TSID_COLUMN, "ts"]);
        query.keys = Some(BTreeSet::from([selection.clone()]));
        let batches = reader
            .read(query)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(expected, collect_times(&batches));
        assert_eq!(vec![selection], exact_keys(&batches));
    }
    // One copy per series: two references, in block order, and the area holds both keys.
    let (_, trailer, page, bytes) = root_page(&store, &handle).await;
    let Page::Leaf(blocks) = page else {
        panic!("one leaf page holds the fixture")
    };
    assert_eq!(2, blocks.len());
    assert_eq!(first_key.id(), blocks[0].id);
    assert_eq!(partner_key.id(), blocks[0].id);
    assert_ne!(blocks[0].key, blocks[1].key);
    assert!(blocks[0].key.end().unwrap() <= blocks[1].key.offset);
    assert_eq!(trailer.payload_end, blocks[0].key.offset);
    assert_eq!(
        first.len() + partner.len(),
        (trailer.key_end - trailer.payload_end) as usize
    );
    assert_eq!(
        first.as_slice(),
        &bytes[blocks[0].key.offset as usize..blocks[0].key.end().unwrap() as usize]
    );
    assert_eq!(
        partner.as_slice(),
        &bytes[blocks[1].key.offset as usize..blocks[1].key.end().unwrap() as usize]
    );
}

/// A part written before the key area has a 112-byte trailer and no `key_end`. The reader locates
/// a trailer from the recorded object size, so it always reads the last 120 bytes: the shifted
/// frame must be rejected, never decoded as if it were current. There is no version bump to tell
/// the two shapes apart, so an artifact from an earlier build of this branch is unreadable and
/// has to be rebuilt.
#[tokio::test]
async fn legacy_112_byte_trailer_is_rejected() {
    let store = store();
    let meta = super::metadata();
    let format = ColumnarSamplesFormat::default();
    let key = new_sparse_primary_key(&["a", "b"], &meta, 1, 7);
    let handle = build(
        &format,
        &store,
        meta.clone(),
        &[keyed_batch(&meta, &[key], &[100], false)],
    )
    .await;
    let (_, trailer, _, _) = root_page(&store, &handle).await;
    assert_eq!(120, TRAILER_SIZE);
    // The pre-key-area frame: the same fields without `key_end`, which is 112 bytes.
    let mut legacy = trailer.encode();
    // Drop `key_end` (96..104) and the current magic/checksum, then seal the old way: the old
    // checksum covers its 96 body bytes plus the magic, 104 in total.
    legacy.drain(96..104);
    legacy.truncate(96);
    legacy.extend_from_slice(MAGIC);
    let sum = codec::checksum(&legacy);
    legacy.extend_from_slice(&sum.to_le_bytes());
    assert_eq!(112, legacy.len());
    // A 112-byte slice is not a trailer of this format at all.
    let error = Trailer::decode(&legacy, trailer.file_length)
        .err()
        .expect("a 112-byte frame is not a trailer");
    assert!(error.to_string().contains("truncated"), "{error}");
    // What a reader actually does with an old part: the last 120 bytes of the file, which start
    // 8 bytes before the old trailer. The magic still lands where this format expects it, so the
    // frame is caught by the checksum over the shifted prefix rather than by the magic.
    let mut window: Vec<u8> = vec![0; 8];
    window.extend_from_slice(&legacy);
    assert_eq!(TRAILER_SIZE as usize, window.len());
    assert!(Trailer::decode(&window, trailer.file_length).is_err());
}

/// One request's key-area traffic: `(reads, bytes)` plus the rows it returned.
async fn key_area_traffic(
    store: &ObjectStore,
    format: &ColumnarSamplesFormat,
    handle: &SamplesFileHandle,
    query: SamplesReadRequest,
) -> (usize, u64, usize) {
    use object_store::layers::mock::{MockLayerBuilder, oio};

    let reads = Arc::new(Mutex::new(Vec::new()));
    let observed = reads.clone();
    let layer = MockLayerBuilder::default()
        .reader_factory(Arc::new(move |path, _, inner: oio::Reader| {
            Box::new(RecordingRead {
                inner,
                path: path.into(),
                reads: observed.clone(),
            })
        }))
        .build()
        .unwrap();
    let reader = format
        .reader(store.clone().layer(layer), handle.clone())
        .await
        .unwrap();
    // The manifest was read above; only the query's own reads matter here.
    reads.lock().unwrap().clear();
    let batches = reader
        .read(query)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
    let (_, trailer, _, _) = root_page(store, handle).await;
    let area = trailer.keys();
    let reads = reads.lock().unwrap();
    let (mut count, mut bytes) = (0usize, 0u64);
    for (_, range) in reads.iter() {
        let start = range.offset();
        let end = start + range.size().unwrap();
        if start < end && start >= area.start && end <= area.end {
            count += 1;
            bytes += end - start;
        }
    }
    (count, bytes, rows)
}

/// The reader stays lazy and does not pay one key read per series.
///
/// 260 one-row series share one part, so a full scan touches 260 distinct keys. Reading each of
/// them from the key area costs 260 tiny reads next to the three coalesced payload windows the
/// same scan issues; merging the selected blocks of a page into bounded reads keeps the traffic
/// at a handful of reads while still fetching exactly the bytes of the selected keys.
#[tokio::test]
async fn key_reads_are_merged_per_page() {
    let store = store();
    let meta = super::metadata();
    let format = ColumnarSamplesFormat::default();
    let input = (0..260u64)
        .map(|id| (1, id, 0, 1, 1, Some(id)))
        .collect::<Vec<_>>();
    let handle = build(
        &format,
        &store,
        meta.clone(),
        &[super::batch(&meta, &input, false)],
    )
    .await;
    let key_len = new_sparse_primary_key(&["a", "b"], &meta, 1, 0).len() as u64;
    let (reads, bytes, rows) =
        key_area_traffic(&store, &format, &handle, request(&[TABLE_ID_COLUMN])).await;
    assert_eq!(260, rows);
    // Every selected key is fetched exactly once: no duplicate bytes, no whole-area read.
    assert_eq!(260 * key_len, bytes, "key bytes fetched");
    // One read for the whole page: before the keys of a page were merged, this scan issued one
    // key-area read per series (260 of them, about 30 bytes each).
    assert_eq!(
        1, reads,
        "{reads} key-area reads for 260 series is per-series amplification"
    );

    // Selection stays lazy: two series cost a bounded read, not the whole area.
    let mut query = request(&[TABLE_ID_COLUMN]);
    query.keys = Some(BTreeSet::from([
        exact(&meta, &["a", "b"], 1, 0),
        exact(&meta, &["a", "b"], 1, 259),
    ]));
    let (reads, bytes, rows) = key_area_traffic(&store, &format, &handle, query).await;
    assert_eq!(2, rows);
    // The two keys are ~8 KiB apart, past the 4 KiB gap the reader is allowed to over-read, so
    // they stay two bounded reads rather than one whole-area read.
    assert_eq!(2, reads, "{reads} key-area reads for two selected series");
    assert_eq!(
        2 * key_len,
        bytes,
        "{bytes} bytes for two keys means the whole key area was read"
    );
}

/// A key area whose bytes disagree with the descriptor that references them is a corrupt part.
///
/// The reserved `(table_id, tsid)` prefix is the first 12 bytes of a canonical sparse key, so
/// flipping one byte there keeps the key parseable while it names another series.
#[tokio::test]
async fn key_bytes_that_disagree_with_the_descriptor_are_rejected() {
    let store = store();
    let meta = super::metadata();
    let format = ColumnarSamplesFormat::default();
    let key = new_sparse_primary_key(&["a", "b"], &meta, 1, 7);
    let handle = build(
        &format,
        &store,
        meta.clone(),
        &[keyed_batch(&meta, &[key], &[100], false)],
    )
    .await;
    let (manifest, trailer, page, mut bytes) = root_page(&store, &handle).await;
    let Page::Leaf(blocks) = page else {
        panic!("one leaf page holds the fixture")
    };
    let reference = blocks[0].key;
    assert_eq!(reference.offset, trailer.payload_end);
    // The canonical sparse prefix is `[column id 0..4][table_id marker 4][table_id 5..9]`
    // `[tsid column id 9..13][tsid marker 13][tsid 14..22]`, then the labels. Byte 16 is in the
    // middle of the tsid value, so the framing still parses and only the identifier changes.
    bytes[reference.offset as usize + 16] ^= 1;
    store.write(&manifest.part_path(0), bytes).await.unwrap();
    let reader = format.reader(store, handle).await.unwrap();
    let mut stream = reader.read(request(&[TABLE_ID_COLUMN])).unwrap();
    let error = stream
        .try_next()
        .await
        .expect_err("a key that disagrees with its descriptor must fail the read");
    assert!(
        error.to_string().contains("disagrees with its identifier"),
        "unexpected error: {error}"
    );
}
