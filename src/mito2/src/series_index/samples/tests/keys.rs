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

use super::{RecordingRead, build, exact, keyed_batch, request, root_page, sample_batch, store};

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
    // One copy is the key plus its stored checksum, which the reference does not count.
    assert_eq!(
        trailer.key_end - trailer.payload_end,
        key.len() as u64 + metadata::KEY_CHECKSUM_BYTES
    );
    assert_eq!(
        key.as_slice(),
        &bytes[reference.offset as usize..reference.end().unwrap() as usize]
    );
    assert_eq!(
        codec::checksum(key.as_slice()),
        u64::from_le_bytes(
            bytes[reference.end().unwrap() as usize..reference.checksum_end().unwrap() as usize]
                .try_into()
                .unwrap()
        )
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
    assert_eq!(
        trailer.key_end - trailer.payload_end,
        key.len() as u64 + metadata::KEY_CHECKSUM_BYTES
    );
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
        first.len() + partner.len() + 2 * metadata::KEY_CHECKSUM_BYTES as usize,
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
    let key_len = new_sparse_primary_key(&["a", "b"], &meta, 1, 0).len() as u64
        + metadata::KEY_CHECKSUM_BYTES;
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
    // Repair the stored checksum: this test is about the key disagreeing with its descriptor, not
    // about the bytes being corrupted, so the checksum must not be what rejects the part.
    let sum = codec::checksum(&bytes[reference.offset as usize..reference.end().unwrap() as usize]);
    let checksum_start = reference.end().unwrap() as usize;
    bytes[checksum_start..reference.checksum_end().unwrap() as usize]
        .copy_from_slice(&sum.to_le_bytes());
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

/// The trailer and root page of one part, plus its bytes.
///
/// `root_page` insists on a single-part artifact; the part-boundary tests need one part at a time.
async fn part_root(
    store: &ObjectStore,
    handle: &SamplesFileHandle,
    part: u32,
) -> (Trailer, Page, Vec<u8>) {
    let manifest = super::manifest(store, handle).await;
    let bytes = store
        .read(&manifest.part_path(part))
        .await
        .unwrap()
        .to_bytes()
        .to_vec();
    let start = bytes.len() - TRAILER_SIZE as usize;
    let trailer = Trailer::decode(&bytes[start..], bytes.len() as u64).unwrap();
    assert_eq!(1, trailer.depth, "one leaf keeps the root page addressable");
    let offset = trailer.root.payload.offset as usize;
    let stored = &bytes[offset..offset + trailer.root.payload.stored as usize];
    let page = Page::decode(
        &trailer.root.payload.unpack(stored).unwrap(),
        &trailer.root,
        trailer.depth,
        trailer.areas(),
        &manifest,
    )
    .unwrap();
    (trailer, page, bytes)
}

/// Rewrites the two stored key entries of a two-series part in place.
///
/// The descriptors stay untouched and the part keeps its length, so whatever rejects the part
/// must be the exact keys themselves, not the framing or the reference order.
async fn rewrite_entries(
    store: &ObjectStore,
    handle: SamplesFileHandle,
    edit: impl FnOnce(&mut [u8], [std::ops::Range<usize>; 2]),
) {
    let (manifest, _, page, mut bytes) = root_page(store, &handle).await;
    let Page::Leaf(blocks) = page else {
        panic!("one leaf page holds the fixture")
    };
    assert_eq!(2, blocks.len(), "the fixture holds two series");
    let entries = [&blocks[0].key, &blocks[1].key]
        .map(|reference| reference.offset as usize..reference.checksum_end().unwrap() as usize);
    edit(&mut bytes, entries);
    store.write(&manifest.part_path(0), bytes).await.unwrap();
}

/// One query's key-area reads - `(reads, bytes)` - and row count, run through a recording layer.
///
/// A read counts when it *intersects* the key area, not only when it lies inside it: an aligned
/// read is clipped at the area end but may start before the area, and it still fetches key bytes.
async fn recorded_key_area_reads(
    store: &ObjectStore,
    format: &ColumnarSamplesFormat,
    handle: &SamplesFileHandle,
    query: SamplesReadRequest,
) -> (usize, u64, crate::error::Result<usize>) {
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
    reads.lock().unwrap().clear();
    let result = reader
        .read(query)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .map(|batches| batches.iter().map(|batch| batch.num_rows()).sum::<usize>());
    let (_, trailer, _, _) = root_page(store, handle).await;
    let area = trailer.keys();
    let reads = reads.lock().unwrap();
    let (mut count, mut bytes) = (0usize, 0u64);
    for (_, range) in reads.iter() {
        let start = range.offset();
        let end = start + range.size().unwrap();
        if start < end && start < area.end && end > area.start {
            count += 1;
            bytes += end - start;
        }
    }
    (count, bytes, result)
}

/// One exact series may span parts, and every part lays out its own key area.
///
/// The two copies of one series therefore live at unrelated offsets. Comparing references across
/// the boundary reads those offsets as one area, so a legitimate artifact looks overlapping (or
/// like a new series that skipped its ordinal). Only the exact key bytes can tell a series that
/// continues from a series that starts.
#[tokio::test]
async fn same_series_across_parts_with_different_key_offsets_is_readable() {
    let store = store();
    let meta = super::metadata();
    let format = ColumnarSamplesFormat {
        block_rows: 1,
        part_bytes: 1,
        ..Default::default()
    };
    // The second row has no field value, so its block stores one chunk fewer and the part it
    // opens places its key area before the previous part did.
    let input = vec![
        (1, 7, 10, 1, 1, Some(7)),
        (1, 7, 20, 1, 1, None),
        (1, 7, 30, 1, 1, Some(9)),
    ];
    let handle = build(
        &format,
        &store,
        meta.clone(),
        &[super::batch(&meta, &input, false)],
    )
    .await;
    assert_eq!(
        3,
        super::manifest(&store, &handle).await.files.len(),
        "every block rolls its part"
    );
    let (first, _, _) = part_root(&store, &handle, 0).await;
    let (second, _, _) = part_root(&store, &handle, 1).await;
    assert_ne!(
        first.payload_end, second.payload_end,
        "the parts must place their key areas differently"
    );
    let reader = format.reader(store, handle).await.unwrap();
    let batches = reader
        .read(request(&[TABLE_ID_COLUMN, TSID_COLUMN, "ts"]))
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        vec![(1, 7, 10), (1, 7, 20), (1, 7, 30)],
        collect_times(&batches)
    );
    // One series, so every batch reports the same exact key.
    assert_eq!(
        vec![exact(&meta, &["a", "b"], 1, 7); 3],
        exact_keys(&batches)
    );
}

/// Two parts' key areas are unrelated, so equal references in them name different series.
///
/// Both series are alone in their part and encode to the same length, so both key areas start at
/// the same offset and the two references are the same number. Treating an equal reference as one
/// series would then demand a continuation the artifact never promised: the second series starts
/// its own timestamps and ordinal.
#[tokio::test]
async fn equal_key_references_across_parts_are_different_series() {
    let store = store();
    let meta = super::metadata();
    let format = ColumnarSamplesFormat {
        block_rows: 1,
        part_bytes: 1,
        ..Default::default()
    };
    let first = new_sparse_primary_key(&["host-000001", "z"], &meta, 1, 7);
    let partner = new_sparse_primary_key(&["host-000002", "z"], &meta, 1, 7);
    assert_eq!(first.len(), partner.len(), "the references must coincide");
    let handle = build(
        &format,
        &store,
        meta.clone(),
        &[keyed_batch(&meta, &[first, partner], &[100, 50], false)],
    )
    .await;
    assert_eq!(2, super::manifest(&store, &handle).await.files.len());
    let (_, first_page, _) = part_root(&store, &handle, 0).await;
    let (_, second_page, _) = part_root(&store, &handle, 1).await;
    let Page::Leaf(first_blocks) = first_page else {
        panic!("one leaf page holds the first part")
    };
    let Page::Leaf(second_blocks) = second_page else {
        panic!("one leaf page holds the second part")
    };
    assert_eq!(
        first_blocks[0].key, second_blocks[0].key,
        "the two series' references must be the same number"
    );
    assert!(
        second_blocks[0].min < first_blocks[0].max,
        "the second series must restart its timestamps"
    );
    let reader = format.reader(store, handle).await.unwrap();
    let batches = reader
        .read(request(&[TABLE_ID_COLUMN, TSID_COLUMN, "ts"]))
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(vec![(1, 7, 100), (1, 7, 50)], collect_times(&batches));
    assert_eq!(
        vec![
            exact(&meta, &["host-000001", "z"], 1, 7),
            exact(&meta, &["host-000002", "z"], 1, 7),
        ],
        exact_keys(&batches)
    );
}

/// A label byte is not covered by the key's own framing, so only a stored checksum protects it.
///
/// Flipping one byte of a label value keeps the sparse framing and the reserved `(table_id, tsid)`
/// prefix valid, so the key still parses and still claims the descriptor's identifier. Without a
/// per-key checksum the block's rows would be attributed to another series and returned as a
/// perfectly ordinary batch.
#[tokio::test]
async fn corrupted_label_byte_is_rejected() {
    let store = store();
    let meta = super::metadata();
    let format = ColumnarSamplesFormat::default();
    let key = new_sparse_primary_key(&["host-000001", "z"], &meta, 1, 7);
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
    let (manifest, trailer, page, mut bytes) = root_page(&store, &handle).await;
    let Page::Leaf(blocks) = page else {
        panic!("one leaf page holds the fixture")
    };
    let reference = blocks[0].key;
    assert_eq!(reference.offset, trailer.payload_end);
    // Find a label byte whose flip keeps the key well framed and under the same identifier.
    let mut offsets = mito_codec::row_converter::SparseOffsetsCache::new();
    let position = (22..key.len())
        .rev()
        .find(|position| {
            let mut candidate = key.clone();
            candidate[*position] ^= 1;
            ExactSeriesKey::from_sparse_key(&candidate, &mut offsets)
                .is_ok_and(|decoded| decoded.id() == blocks[0].id)
        })
        .expect("a label byte whose flip keeps the key valid");
    bytes[reference.offset as usize + position] ^= 1;
    store.write(&manifest.part_path(0), bytes).await.unwrap();
    let reader = format.reader(store, handle).await.unwrap();
    let error = reader
        .read(request(&[TABLE_ID_COLUMN]))
        .unwrap()
        .try_next()
        .await
        .expect_err("a rewritten label must not silently name another series");
    assert!(
        error.to_string().contains("sample key checksum mismatch"),
        "unexpected error: {error}"
    );
}

/// References can be ordered and disjoint while the keys they point at are reversed.
///
/// The structural check only sees descriptor fields, so an increasing reference holding a smaller
/// exact key than its predecessor passes it. Two distinct series under one `(table_id, tsid)` are
/// exactly this case once the key area is corrupted: downstream reads rely on the batches arriving
/// in exact-key order, so the loaded keys themselves must be ordered.
#[tokio::test]
async fn reversed_exact_keys_under_ordered_references_are_rejected() {
    let store = store();
    let meta = super::metadata();
    let format = ColumnarSamplesFormat::default();
    let first = new_sparse_primary_key(&["host-000001", "z"], &meta, 1, 7);
    let partner = new_sparse_primary_key(&["host-000002", "z"], &meta, 1, 7);
    let handle = build(
        &format,
        &store,
        meta.clone(),
        &[keyed_batch(
            &meta,
            &[first.clone(), partner.clone()],
            &[100, 200],
            false,
        )],
    )
    .await;
    // Swapping whole entries keeps both references, both checksums, and both lengths valid: the
    // only thing wrong is that the first block is now the larger key.
    rewrite_entries(&store, handle.clone(), |bytes, [left, right]| {
        let left_entry = bytes[left.clone()].to_vec();
        bytes.copy_within(right.clone(), left.start);
        bytes[right].copy_from_slice(&left_entry);
    })
    .await;
    let reader = format.reader(store, handle).await.unwrap();
    // The stream is lazy, so the second block is only visited after the first is yielded.
    let error = reader
        .read(request(&[TABLE_ID_COLUMN, TSID_COLUMN, "ts"]))
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .expect_err("reversed exact keys must not be read as ordered series");
    assert!(
        error
            .to_string()
            .contains("unordered sample exact series keys"),
        "unexpected error: {error}"
    );
}

/// Two references that point at one exact key are one series, so it must continue.
///
/// A different reference alone used to mean "a new series", which let a second copy of the same
/// key restart its ordinal and its timestamps. The key bytes say otherwise: the same series is
/// one series, and a block that starts at ordinal zero and goes back in time is corrupt.
#[tokio::test]
async fn repeated_exact_key_under_new_reference_must_continue() {
    let store = store();
    let meta = super::metadata();
    let format = ColumnarSamplesFormat::default();
    let first = new_sparse_primary_key(&["host-000001", "z"], &meta, 1, 7);
    let partner = new_sparse_primary_key(&["host-000002", "z"], &meta, 1, 7);
    assert_eq!(first.len(), partner.len());
    let handle = build(
        &format,
        &store,
        meta.clone(),
        &[keyed_batch(
            &meta,
            &[first.clone(), partner],
            &[100, 50],
            false,
        )],
    )
    .await;
    // The second entry becomes a second copy of the first series' key, with a valid checksum.
    rewrite_entries(&store, handle.clone(), |bytes, [left, right]| {
        let key = bytes[left.start..left.end - metadata::KEY_CHECKSUM_BYTES as usize].to_vec();
        let sum = codec::checksum(&key);
        bytes[right.start..right.end - metadata::KEY_CHECKSUM_BYTES as usize].copy_from_slice(&key);
        bytes[right.end - metadata::KEY_CHECKSUM_BYTES as usize..right.end]
            .copy_from_slice(&sum.to_le_bytes());
    })
    .await;
    let reader = format.reader(store, handle).await.unwrap();
    let error = reader
        .read(request(&[TABLE_ID_COLUMN, TSID_COLUMN, "ts"]))
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .expect_err("a repeated key that restarts its series must not be read");
    assert!(
        error.to_string().contains("unordered sample blocks"),
        "unexpected error: {error}"
    );
}

/// A pool that cannot pay for one key must fail before the key area is read.
///
/// The fetched entry and the owned key coexist, so the pool has to hold both before the read. A
/// reader that fetches first and charges afterwards has already issued I/O for bytes it cannot
/// keep, which is what "no I/O once the pool cannot pay" rules out.
#[tokio::test]
async fn unpayable_key_prefetch_issues_no_key_read() {
    use common_error::ext::ErrorExt;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};

    let store = store();
    let meta = super::metadata();
    let format = ColumnarSamplesFormat::default();
    let label = "x".repeat(4096);
    let keys = (0..32u64)
        .map(|tsid| new_sparse_primary_key(&[label.as_str(), "z"], &meta, 1, tsid))
        .collect::<Vec<_>>();
    let times = vec![100; keys.len()];
    let handle = build(
        &format,
        &store,
        meta.clone(),
        &[keyed_batch(&meta, &keys, &times, false)],
    )
    .await;
    // Four KiB cannot hold one 4 KiB key entry plus the owned key decoded from it.
    let pool = Arc::new(GreedyMemoryPool::new(4 * 1024));
    let mut query = super::full_request();
    query.memory_pool = Some(pool.clone());
    let (reads, _, result) = recorded_key_area_reads(&store, &format, &handle, query).await;
    let error = result.expect_err("an unpayable key must fail the read");
    assert_eq!(
        common_error::status_code::StatusCode::RuntimeResourcesExhausted,
        error.status_code()
    );
    assert_eq!(
        0, reads,
        "{reads} key-area reads happened although the pool cannot hold one key"
    );
    assert_eq!(0, pool.reserved());
}

/// A cached part answers a key from the payload cache, never from the key area.
///
/// Keys use the same aligned blocks as column chunks, so a part that is already cached must not
/// pay a key-area read - including when the scan pool shapes how the read is issued.
///
/// This is a cache-behaviour guard, not a proof of the key-fetch budget: it passes without the
/// peak reservation too, because a warm part issues no read either way. The budget itself is
/// pinned by `unpayable_key_prefetch_issues_no_key_read` and
/// `per_key_fallback_reserves_the_aligned_peak`.
#[tokio::test]
async fn cached_parts_need_no_key_read() {
    use crate::cache::{CacheManager, CacheStrategy};
    use crate::series_index::format::SamplesFormatReader;
    use crate::series_index::samples::reader;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use store_api::storage::FileId;

    let store = store();
    let meta = super::metadata();
    let format = ColumnarSamplesFormat::default();
    let keys = (0..4u64)
        .map(|tsid| new_sparse_primary_key(&["a", "b"], &meta, 1, tsid))
        .collect::<Vec<_>>();
    let handle = build(
        &format,
        &store,
        meta.clone(),
        &[keyed_batch(&meta, &keys, &[100, 100, 100, 100], false)],
    )
    .await;
    let strategy = CacheStrategy::EnableAll(Arc::new(
        CacheManager::builder()
            .page_cache_size(16 * 1024 * 1024)
            .build(),
    ));
    let cache = strategy.columnar_payload(FileId::random());
    let open = |store: &ObjectStore| {
        let store = store.clone();
        let handle = handle.clone();
        let cache = cache.clone();
        async move {
            reader::SamplesReader::open(store, handle)
                .await
                .unwrap()
                .with_payload_cache(cache)
        }
    };
    // Warm every aligned block of the part, keys included.
    let warm = open(&store).await;
    warm.read(super::full_request())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let reads = Arc::new(Mutex::new(Vec::new()));
    let observed = reads.clone();
    let layer = object_store::layers::mock::MockLayerBuilder::default()
        .reader_factory(Arc::new(
            move |path, _, inner: object_store::layers::mock::oio::Reader| {
                Box::new(RecordingRead {
                    inner,
                    path: path.into(),
                    reads: observed.clone(),
                })
            },
        ))
        .build()
        .unwrap();
    let layered = store.clone().layer(layer);
    let reader = open(&layered).await;
    reads.lock().unwrap().clear();
    let pool = Arc::new(GreedyMemoryPool::new(1024 * 1024));
    let mut query = super::full_request();
    query.memory_pool = Some(pool.clone());
    // One selected series still needs its key, which this part already has cached.
    query.keys = Some(BTreeSet::from([exact(&meta, &["a", "b"], 1, 2)]));
    let batches = reader
        .read(query)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(vec![(1, 2, 100)], collect_times(&batches));
    // Counting any read that intersects the key area, so an aligned read starting before it is
    // counted too: a warm part must answer the key from the payload cache either way.
    let (_, trailer, _, _) = root_page(&store, &handle).await;
    let area = trailer.keys();
    let reads = reads.lock().unwrap();
    let key_reads = reads
        .iter()
        .filter(|(_, range)| {
            let start = range.offset();
            let end = start + range.size().unwrap();
            start < end && start < area.end && end > area.start
        })
        .count();
    assert_eq!(
        0, key_reads,
        "{key_reads} key-area reads on a part whose blocks are already cached"
    );
    assert_eq!(0, pool.reserved());
}

/// One writer over one long-keyed series, returning the accounting after the second block flushes
/// plus the size of one key copy.
///
/// `root_capacity` is one and `page_bytes` admits exactly one maximal descriptor per leaf, so the
/// second block starts a new part while the series continues into it.
async fn cross_rollover_accounting(part_bytes: u64) -> ((u64, u64, u64, usize), u64) {
    use crate::series_index::format::FormatWriter;
    use crate::series_index::samples::writer::SamplesWriter;
    use store_api::storage::FileId;

    let store = store();
    let meta = super::metadata();
    let label = "x".repeat(512);
    let key = new_sparse_primary_key(&[label.as_str(), "z"], &meta, 1, 7);
    let copy_bytes = key.len() as u64 + metadata::KEY_CHECKSUM_BYTES;
    let format = ColumnarSamplesFormat {
        block_rows: 1,
        part_bytes,
        page_bytes: 244,
        root_capacity: 1,
        ..Default::default()
    };
    let generation = FileId::random();
    let prefix = format!("{}/samples/{generation}/", meta.region_id.as_u64());
    let mut writer = SamplesWriter::new(
        format,
        meta.clone(),
        store,
        &prefix,
        FileId::random(),
        generation,
    )
    .unwrap();
    // A row is buffered before the next one flushes its block, so three single-row batches flush
    // two blocks: the second one is the block that starts the new part.
    for (time, value) in [(10, None), (20, Some(0.0)), (30, Some(0.0))] {
        let batch = sample_batch(
            &meta,
            std::slice::from_ref(&key),
            vec![time],
            vec![1],
            vec![0],
            vec![value],
            false,
        );
        writer.write(&batch).await.unwrap();
    }
    (writer.part_accounting(), copy_bytes)
}

/// A series that crosses a metadata rollover must have its new part's key copy counted.
///
/// `finish_part` clears the part's key accounting, so a "is this the first copy" decision taken
/// before the rollover skips the copy the new part makes. The part target then never sees the key
/// bytes and defers the part boundary by one part's worth of keys.
#[tokio::test]
async fn series_across_a_metadata_rollover_counts_its_key_copy() {
    let ((position, counted, blocks, parts), copy_bytes) =
        cross_rollover_accounting(u64::MAX).await;
    assert_eq!(1, parts, "the metadata rule must complete one part");
    assert_eq!(1, blocks, "the second block opens the new part");
    assert_eq!(
        copy_bytes, counted,
        "the new part must count its own copy of the series key"
    );
    // `flush_block` compares exactly `position + part_key_bytes` with the part target, so a part
    // target of that sum rolls the new part at once - only when the copy is counted.
    let ((_, _, blocks_again, parts_again), _) =
        cross_rollover_accounting(position + counted).await;
    assert_eq!(
        2, parts_again,
        "the counted key copy must be what reaches the part target"
    );
    assert_eq!(
        0, blocks_again,
        "the part target rolls the new part at once"
    );
}

/// A memory pool that remembers the highest reservation it ever held and how much each named
/// consumer held, so a test can see that a scan charged for something while it was alive.
#[derive(Debug)]
struct WatchingPool {
    inner: datafusion::execution::memory_pool::GreedyMemoryPool,
    high: std::sync::atomic::AtomicUsize,
    consumers: Mutex<std::collections::HashMap<String, usize>>,
}

impl std::fmt::Display for WatchingPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.inner, f)
    }
}

impl WatchingPool {
    fn new(limit: usize) -> Self {
        Self {
            inner: datafusion::execution::memory_pool::GreedyMemoryPool::new(limit),
            high: std::sync::atomic::AtomicUsize::new(0),
            consumers: Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// The most bytes any scan using this pool ever had reserved at once.
    fn high_water(&self) -> usize {
        self.high.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Bytes currently reserved by one named consumer.
    fn consumer(&self, name: &str) -> usize {
        self.consumers
            .lock()
            .unwrap()
            .get(name)
            .copied()
            .unwrap_or(0)
    }

    fn record(&self, name: &str, bytes: usize, grow: bool) {
        {
            let mut consumers = self.consumers.lock().unwrap();
            let slot = consumers.entry(name.to_string()).or_default();
            if grow {
                *slot += bytes;
            } else {
                *slot = slot.saturating_sub(bytes);
            }
        }
        self.high.fetch_max(
            datafusion::execution::memory_pool::MemoryPool::reserved(&self.inner),
            std::sync::atomic::Ordering::Relaxed,
        );
    }
}

impl datafusion::execution::memory_pool::MemoryPool for WatchingPool {
    fn name(&self) -> &str {
        datafusion::execution::memory_pool::MemoryPool::name(&self.inner)
    }

    fn register(&self, consumer: &datafusion::execution::memory_pool::MemoryConsumer) {
        datafusion::execution::memory_pool::MemoryPool::register(&self.inner, consumer)
    }

    fn unregister(&self, consumer: &datafusion::execution::memory_pool::MemoryConsumer) {
        datafusion::execution::memory_pool::MemoryPool::unregister(&self.inner, consumer)
    }

    fn grow(
        &self,
        reservation: &datafusion::execution::memory_pool::MemoryReservation,
        additional: usize,
    ) {
        datafusion::execution::memory_pool::MemoryPool::grow(&self.inner, reservation, additional);
        self.record(reservation.consumer().name(), additional, true);
    }

    fn shrink(
        &self,
        reservation: &datafusion::execution::memory_pool::MemoryReservation,
        shrink: usize,
    ) {
        datafusion::execution::memory_pool::MemoryPool::shrink(&self.inner, reservation, shrink);
        self.record(reservation.consumer().name(), shrink, false);
    }

    fn try_grow(
        &self,
        reservation: &datafusion::execution::memory_pool::MemoryReservation,
        additional: usize,
    ) -> std::result::Result<(), datafusion::error::DataFusionError> {
        datafusion::execution::memory_pool::MemoryPool::try_grow(
            &self.inner,
            reservation,
            additional,
        )?;
        self.record(reservation.consumer().name(), additional, true);
        Ok(())
    }

    fn reserved(&self) -> usize {
        datafusion::execution::memory_pool::MemoryPool::reserved(&self.inner)
    }
}

/// One recorded read: its path, offset, length, and the key reservation held before it.
type PeekedRead = (String, u64, u64, usize);

/// A reader that records each read together with the key reservation held when it is issued.
struct PeekingRead {
    inner: object_store::layers::mock::oio::Reader,
    path: String,
    reads: Arc<Mutex<Vec<PeekedRead>>>,
    pool: Arc<WatchingPool>,
}

impl object_store::layers::mock::Read for PeekingRead {
    async fn open(
        &self,
        range: object_store::layers::mock::BytesRange,
    ) -> object_store::layers::mock::Result<(
        object_store::layers::mock::RpRead,
        Box<dyn object_store::layers::mock::oio::ReadStreamDyn>,
    )> {
        let start = range.offset();
        let length = range.size().unwrap();
        self.reads.lock().unwrap().push((
            self.path.clone(),
            start,
            length,
            self.pool.consumer("ColumnarSamplesKeys"),
        ));
        self.inner.open(range).await
    }

    async fn read(
        &self,
        range: object_store::layers::mock::BytesRange,
    ) -> object_store::layers::mock::Result<(
        object_store::layers::mock::RpRead,
        object_store::layers::mock::Buffer,
    )> {
        let start = range.offset();
        let length = range.size().unwrap();
        self.reads.lock().unwrap().push((
            self.path.clone(),
            start,
            length,
            self.pool.consumer("ColumnarSamplesKeys"),
        ));
        self.inner.read(range).await
    }
}

/// A key the page-level window leaves over is read one at a time, and that path must charge the
/// same peak as the page-level one.
///
/// The aligned payload cache turns one key into a whole aligned block plus whatever assembling it
/// needs. Reserving only the key entry and then reading through the cache over-commits the pool by
/// more than an order of magnitude: every key-area read must be covered by what the key
/// reservation already holds when the read is issued, whichever path serves it.
#[tokio::test]
async fn per_key_fallback_reserves_the_aligned_peak() {
    use crate::cache::{CacheManager, CacheStrategy};
    use crate::series_index::format::SamplesFormatReader;
    use crate::series_index::samples::reader;
    use datafusion::execution::memory_pool::MemoryPool;
    use object_store::layers::mock::MockLayerBuilder;
    use store_api::storage::FileId;

    let store = store();
    let meta = super::metadata();
    let format = ColumnarSamplesFormat::default();
    // Long keys keep the window's peak well above the pool while a single key still fits it, so
    // the window is halved and the leftover keys take the per-key path.
    let label = "x".repeat(1000);
    let rows = 20u64;
    let keys = (0..rows)
        .map(|tsid| new_sparse_primary_key(&[label.as_str(), "z"], &meta, 1, tsid))
        .collect::<Vec<_>>();
    let key_len = keys[0].len();
    let times = vec![100; keys.len()];
    let handle = build(
        &format,
        &store,
        meta.clone(),
        &[keyed_batch(&meta, &keys, &times, false)],
    )
    .await;
    let (_, trailer, _, _) = root_page(&store, &handle).await;
    let area = trailer.keys();
    let strategy = CacheStrategy::EnableAll(Arc::new(
        CacheManager::builder()
            .page_cache_size(16 * 1024 * 1024)
            .build(),
    ));
    let cache = strategy.columnar_payload(FileId::random());
    // What the reader charges for one retained key, plus room for one block's payload buffers.
    let entry = std::mem::size_of::<ExactSeriesKey>() + 64;
    let limit = rows as usize * (key_len + entry) + 8 * 1024;
    let pool = Arc::new(WatchingPool::new(limit));
    let reads = Arc::new(Mutex::new(Vec::new()));
    let observed = reads.clone();
    let watching = pool.clone();
    let layer = MockLayerBuilder::default()
        .reader_factory(Arc::new(move |path, _, inner| {
            Box::new(PeekingRead {
                inner,
                path: path.into(),
                reads: observed.clone(),
                pool: watching.clone(),
            })
        }))
        .build()
        .unwrap();
    let reader = reader::SamplesReader::open(store.clone().layer(layer), handle.clone())
        .await
        .unwrap()
        .with_payload_cache(cache);
    let mut query = super::full_request();
    query.memory_pool = Some(pool.clone());
    reads.lock().unwrap().clear();
    let batches = reader
        .read(query)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        rows as usize,
        batches.iter().map(|b| b.num_rows()).sum::<usize>()
    );
    // Every key-area read was paid for before it was issued: the key reservation already held at
    // least the bytes the read fetches.
    let key_reads = reads
        .lock()
        .unwrap()
        .iter()
        .filter(|(path, start, length, _)| {
            path.ends_with(".samples") && *start < area.end && *start + *length > area.start
        })
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        key_reads.len() > 1,
        "{} key-area reads means the leftover keys never took the per-key path",
        key_reads.len()
    );
    for (_, start, length, reserved) in &key_reads {
        assert!(
            *reserved >= *length as usize,
            "a {length} byte key-area read at {start} was issued while the key reservation held \
             only {reserved} bytes"
        );
    }
    // The pool has to hold every retained key, and the scan never asks for more than it has.
    assert!(
        pool.high_water() >= rows as usize * (key_len + entry),
        "high water {} is below the {} bytes of retained keys",
        pool.high_water(),
        rows as usize * (key_len + entry)
    );
    assert_eq!(0, pool.reserved());

    // A pool that cannot pay for one key's peak must fail before any key-area read, even with the
    // aligned cache enabled.
    let pool = Arc::new(WatchingPool::new(key_len));
    let mut query = super::full_request();
    query.memory_pool = Some(pool.clone());
    reads.lock().unwrap().clear();
    let error = reader
        .read(query)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .expect_err("a pool below one key's peak must refuse the read");
    assert_eq!(
        common_error::status_code::StatusCode::RuntimeResourcesExhausted,
        common_error::ext::ErrorExt::status_code(&error)
    );
    let key_reads = reads
        .lock()
        .unwrap()
        .iter()
        .filter(|(path, start, length, _)| {
            path.ends_with(".samples") && *start < area.end && *start + *length > area.start
        })
        .count();
    assert_eq!(
        0, key_reads,
        "{key_reads} key-area reads while the pool cannot pay"
    );
    assert_eq!(0, pool.reserved());
}

/// A series that spans two parts is recognised by the last loaded key of the first part.
///
/// That copy outlives every per-part reservation, so it has a consumer of its own: charged while
/// the boundary key is alive and given back once the boundary is behind the scan. Copying it from
/// a block's key would retain bytes no reservation pays for.
#[tokio::test]
async fn boundary_key_is_charged_while_it_is_alive() {
    use crate::series_index::format::SamplesFormatReader;
    use crate::series_index::samples::reader;
    use datafusion::execution::memory_pool::MemoryPool;

    let store = store();
    let meta = super::metadata();
    let format = ColumnarSamplesFormat {
        block_rows: 1,
        part_bytes: 1,
        ..Default::default()
    };
    let label = "x".repeat(1024);
    let key = new_sparse_primary_key(&[label.as_str(), "z"], &meta, 1, 7);
    // One series, two blocks, one block per part: the series continues across the boundary.
    let handle = build(
        &format,
        &store,
        meta.clone(),
        &[keyed_batch(&meta, &vec![key.clone(); 2], &[10, 20], false)],
    )
    .await;
    assert_eq!(2, super::manifest(&store, &handle).await.files.len());
    let pool = Arc::new(WatchingPool::new(8 * 1024 * 1024));
    let reader = reader::SamplesReader::open(store, handle).await.unwrap();
    let mut query = request(&[TABLE_ID_COLUMN, TSID_COLUMN, "ts"]);
    query.memory_pool = Some(pool.clone());
    let mut stream = reader.read(query).unwrap();
    // The first part is not finished yet: nothing has been carried across a boundary.
    assert!(
        stream.try_next().await.unwrap().is_some(),
        "the first part holds one block"
    );
    assert_eq!(
        0,
        pool.consumer("ColumnarSamplesBoundaryKey"),
        "the boundary key must not be charged before the part ends"
    );
    // The second batch is only produced after the first part handed its last loaded key over.
    assert!(
        stream.try_next().await.unwrap().is_some(),
        "the series continues in the second part"
    );
    let charged = pool.consumer("ColumnarSamplesBoundaryKey");
    assert!(
        charged >= key.len(),
        "the boundary key's {charged} charged bytes do not cover its {} bytes",
        key.len()
    );
    // Past the last part the boundary key is no longer needed, so its charge goes back.
    assert!(stream.try_next().await.unwrap().is_none());
    assert_eq!(0, pool.consumer("ColumnarSamplesBoundaryKey"));
    drop(stream);
    assert_eq!(0, pool.reserved());
}

/// A selection that prunes a block must not cut the order check between the keys around it.
///
/// The pruned block's key is never read, so the check has to chain over the last *loaded* key.
/// Comparing against the last visited block instead silently drops the comparison, and two
/// reversed exact keys under one `(table_id, tsid)` pass as ordered.
#[tokio::test]
async fn skipped_block_does_not_hide_reversed_keys() {
    let store = store();
    let meta = super::metadata();
    let format = ColumnarSamplesFormat::default();
    let first = new_sparse_primary_key(&["host-000001", "z"], &meta, 1, 7);
    let middle = new_sparse_primary_key(&["host-000002", "z"], &meta, 1, 7);
    let last = new_sparse_primary_key(&["host-000003", "z"], &meta, 1, 7);
    let handle = build(
        &format,
        &store,
        meta.clone(),
        &[keyed_batch(
            &meta,
            &[first, middle, last],
            &[100, 5_000, 110],
            false,
        )],
    )
    .await;
    // Swapping the first and last series' stored entries keeps both checksums and identifiers
    // valid; only the order of the two keys the request loads is wrong. The middle block is
    // outside the requested time range, so its key is never read.
    let (manifest, _, page, mut bytes) = root_page(&store, &handle).await;
    let Page::Leaf(blocks) = page else {
        panic!("one leaf page holds the fixture")
    };
    assert_eq!(3, blocks.len());
    let entries = [&blocks[0].key, &blocks[2].key]
        .map(|reference| reference.offset as usize..reference.checksum_end().unwrap() as usize);
    let first_entry = bytes[entries[0].clone()].to_vec();
    bytes.copy_within(entries[1].clone(), entries[0].start);
    bytes[entries[1].clone()].copy_from_slice(&first_entry);
    store.write(&manifest.part_path(0), bytes).await.unwrap();
    let reader = format.reader(store, handle).await.unwrap();
    let mut query = request(&[TABLE_ID_COLUMN, TSID_COLUMN, "ts"]);
    query.end = Some(1_000);
    let error = reader
        .read(query)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .expect_err("a skipped block must not hide reversed keys");
    assert!(
        error
            .to_string()
            .contains("unordered sample exact series keys"),
        "unexpected error: {error}"
    );
}

/// The same gap across a part boundary: a part whose last block was pruned must still hand its
/// last *loaded* key to the next part.
///
/// The skipped block ends the first part, so a boundary key taken from "the last visited block"
/// is missing and the check would silently compare nothing across the parts.
#[tokio::test]
async fn pruned_block_does_not_hide_the_boundary_key() {
    let store = store();
    let meta = super::metadata();
    // One leaf holds two descriptors and the root holds one leaf, so the first two blocks share a
    // page in the first part and the third block starts the second part. The pruned block is
    // therefore traversed without being loaded, which is what used to lose the boundary key.
    let format = ColumnarSamplesFormat {
        block_rows: 1,
        page_bytes: 476,
        root_capacity: 1,
        ..Default::default()
    };
    let first = new_sparse_primary_key(&["host-000001", "z"], &meta, 1, 7);
    let middle = new_sparse_primary_key(&["host-000002", "z"], &meta, 1, 7);
    let last = new_sparse_primary_key(&["host-000003", "z"], &meta, 1, 7);
    let handle = build(
        &format,
        &store,
        meta.clone(),
        &[keyed_batch(
            &meta,
            &[first, middle, last.clone()],
            &[100, 5_000, 110],
            false,
        )],
    )
    .await;
    assert_eq!(2, super::manifest(&store, &handle).await.files.len());
    // Rewrite the second part's key with a smaller key of the same length and identifier, so the
    // two loaded keys are reversed while everything else about the part stays valid.
    let smaller = new_sparse_primary_key(&["host-000000", "z"], &meta, 1, 7);
    assert_eq!(last.len(), smaller.len());
    let (_, page, mut bytes) = part_root(&store, &handle, 1).await;
    let manifest = super::manifest(&store, &handle).await;
    let Page::Leaf(blocks) = page else {
        panic!("one leaf page holds the second part")
    };
    let reference = blocks[0].key;
    let start = reference.offset as usize;
    bytes[start..start + smaller.len()].copy_from_slice(&smaller);
    let sum = codec::checksum(&smaller);
    bytes[start + smaller.len()..reference.checksum_end().unwrap() as usize]
        .copy_from_slice(&sum.to_le_bytes());
    store.write(&manifest.part_path(1), bytes).await.unwrap();
    let reader = format.reader(store, handle).await.unwrap();
    let mut query = request(&[TABLE_ID_COLUMN, TSID_COLUMN, "ts"]);
    query.end = Some(1_000);
    let error = reader
        .read(query)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .expect_err("a pruned block must not hide the boundary key");
    assert!(
        error
            .to_string()
            .contains("unordered sample exact series keys"),
        "unexpected error: {error}"
    );
}
