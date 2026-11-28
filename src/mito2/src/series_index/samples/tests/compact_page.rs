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

//! Compact internal pages: exact bytes, capacity, restart search, corruption, and the version gate.
//!
//! The page exists to name thousands of children in one 64 KiB page. These tests pin both halves
//! of that: the encoding is exactly what the wire contract says, and the search that avoids
//! decoding the whole page finds precisely the children a linear scan would.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use futures::TryStreamExt;
use object_store::ObjectStore;
use object_store::layers::mock::{self, MockLayerBuilder, oio};
use store_api::metadata::RegionMetadataRef;

use super::*;
use crate::error::Result;
use crate::series_index::artifact::{ArtifactFormat, SamplesEntry, SamplesFileHandle};
use crate::series_index::format::{SamplesFormat, SamplesReadRequest};
use crate::series_index::identity::{ExactSeriesKey, IdentityEncoding, series_key_suffix};
use crate::series_index::purger::series_index_channel;
use crate::series_index::samples::codec;
use crate::series_index::samples::compact::{self, ChildEntry, CompactPage, Entries};
use crate::series_index::samples::metadata::{
    Areas, Bounds, KEY_CHECKSUM_BYTES, KeyArea, PAGE_LIMIT, Page, PageReference, PayloadReference,
    TRAILER_SIZE, Trailer, descriptor_bytes,
};
use crate::series_index::samples::reader::matches;
use crate::series_index::samples::{
    COMPACT_VERSION, Manifest, MetricSeriesId, TimestampColumn, TimestampUnit, VERSION, ValueColumn,
};
use crate::sst::file::RegionFileId;

fn id(table_id: u32, tsid: u64) -> MetricSeriesId {
    MetricSeriesId { table_id, tsid }
}

fn child(
    (first, last): ((u32, u64), (u32, u64)),
    (min, max): (i64, i64),
    (offset, stored, decoded, compression): (u64, u32, u32, u16),
) -> ChildEntry {
    ChildEntry {
        bounds: Bounds {
            first: id(first.0, first.1),
            last: id(last.0, last.1),
            min,
            max,
        },
        offset,
        stored,
        decoded,
        checksum: 0x1122_3344_5566_7788,
        compression,
    }
}

fn aggregate<T>(items: &[T], bounds: impl Fn(&T) -> Bounds) -> Bounds {
    let mut result = bounds(&items[0]);
    for item in &items[1..] {
        result.extend(bounds(item));
    }
    result
}

fn entry_bounds(entry: &ChildEntry) -> Bounds {
    entry.bounds
}

fn reference_bounds(reference: &PageReference) -> Bounds {
    reference.bounds
}

/// The area a page of these children lives in: its children start where the metadata area does,
/// and its own payload is stored after the last of them.
fn areas_of(entries: &[ChildEntry]) -> Areas {
    let start = entries.iter().map(|entry| entry.offset).min().unwrap();
    Areas {
        payload_end: 16,
        keys: KeyArea {
            start: 16,
            end: start,
        },
        metadata_start: start,
    }
}

/// The reference a parent page or the trailer holds for a page of these children.
fn parent_of(entries: &[ChildEntry], encoding: u16) -> PageReference {
    let end = entries
        .iter()
        .map(|entry| entry.offset + u64::from(entry.stored))
        .max()
        .unwrap();
    PageReference {
        bounds: aggregate(entries, entry_bounds),
        payload: PayloadReference {
            offset: end + 64,
            stored: 32,
            decoded: 32,
            checksum: 0,
            encoding,
            compression: 0,
        },
    }
}

fn compact_of(entries: &[ChildEntry]) -> Arc<CompactPage> {
    let bytes = compact::encode(entries, compact::CHILD_LEAF).unwrap();
    Arc::new(
        CompactPage::decode(
            bytes,
            &parent_of(entries, compact::ENCODING),
            2,
            areas_of(entries),
        )
        .unwrap(),
    )
}

fn manifest_fixture() -> Manifest {
    Manifest {
        prefix: None,
        format: ArtifactFormat::ColumnarSamples,
        version: VERSION,
        identity_encoding: IdentityEncoding::CURRENT_SAMPLES,
        region_id: 1,
        source_file_id: FileId::random(),
        generation: FileId::random(),
        timestamp: TimestampColumn {
            column_id: 3,
            name: "ts".into(),
            unit: TimestampUnit::Millisecond,
        },
        fields: vec![ValueColumn {
            column_id: 2,
            name: "field_0".into(),
            data_type: "float64".into(),
            nullable: true,
        }],
        files: Vec::new(),
    }
}

// ---------------------------------------------------------------------------------------------
// Raw page assembly, for the tests that must write a page the codec would refuse.
// ---------------------------------------------------------------------------------------------

/// A compact page assembled from explicit parts.
fn raw_page(count: u64, child_encoding: u16, stream: &[u8], restarts: &[u16]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&1u32.to_le_bytes());
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&child_encoding.to_le_bytes());
    out.push(compact::RESTART_INTERVAL as u8);
    out.push(0);
    out.extend_from_slice(stream);
    for restart in restarts {
        out.extend_from_slice(&restart.to_le_bytes());
    }
    out
}

/// The entry stream and restart offsets of a page the codec wrote.
fn parts_of(bytes: &[u8], count: usize) -> (Vec<u8>, Vec<u16>) {
    let groups = count.div_ceil(compact::RESTART_INTERVAL as usize);
    let end = bytes.len() - 2 * groups;
    let stream = bytes[16..end].to_vec();
    let restarts = (0..groups)
        .map(|group| u16::from_le_bytes([bytes[end + 2 * group], bytes[end + 2 * group + 1]]))
        .collect();
    (stream, restarts)
}

/// The body of a restart entry: absolute identifiers, an absolute time, a span, an absolute
/// offset, a stored length, an optional decoded length, and a checksum.
fn restart_body(
    (table, tsid): (u32, u64),
    min: i64,
    span: u64,
    (offset, stored, decoded): (u64, u64, Option<u64>),
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&table.to_le_bytes());
    body.extend_from_slice(&tsid.to_le_bytes());
    body.extend_from_slice(&min.to_le_bytes());
    codec::put_varint(&mut body, span);
    body.extend_from_slice(&offset.to_le_bytes());
    codec::put_varint(&mut body, stored);
    if let Some(decoded) = decoded {
        codec::put_varint(&mut body, decoded);
    }
    body.extend_from_slice(&0u64.to_le_bytes());
    body
}

fn entry(control: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![control];
    out.extend_from_slice(body);
    out
}

/// Decodes a page whose parent is derived from the page itself, so a test corrupts the entry it
/// means to check rather than the envelope around it.
fn decode_raw(bytes: Vec<u8>) -> Result<CompactPage> {
    let reference = PageReference {
        bounds: Bounds {
            first: id(0, 0),
            last: id(u32::MAX, u64::MAX),
            min: i64::MIN,
            max: i64::MAX,
        },
        payload: PayloadReference {
            offset: u64::MAX - 1,
            stored: 1,
            decoded: 1,
            checksum: 0,
            encoding: compact::ENCODING,
            compression: 0,
        },
    };
    CompactPage::decode(
        bytes,
        &reference,
        2,
        Areas {
            payload_end: 16,
            keys: KeyArea { start: 16, end: 16 },
            metadata_start: 16,
        },
    )
}

/// Decodes a page that passes the parent's bounds, for a page built by hand.
fn decode_checked(bytes: Vec<u8>, reference: &PageReference, areas: Areas) -> Result<CompactPage> {
    CompactPage::decode(bytes, reference, 2, areas)
}

// ---------------------------------------------------------------------------------------------

/// The encoded bytes are exactly what the contract lists, field by field.
#[test]
fn golden_compact_page_bytes() {
    let entries = [
        // Restart: absolute identifiers, an absolute time, an absolute offset.
        child(((7, 42), (7, 45)), (100, 200), (1 << 32, 300, 300, 0)),
        // Same table as the previous last, then a new table with a lower tsid; Zstd.
        child(((7, 45), (8, 3)), (90, 1000), ((1 << 32) + 300, 100, 50, 1)),
        // Equal endpoints, a negative minimum, the full signed span, and a nonzero offset gap.
        child(
            ((8, 10), (8, 10)),
            (-5, i64::MAX),
            ((1 << 32) + 500, 4096, 4096, 0),
        ),
    ];
    let mut expected = Vec::new();
    expected.extend_from_slice(&[1, 0, 0, 0]);
    expected.extend_from_slice(&[3, 0, 0, 0, 0, 0, 0, 0]);
    expected.extend_from_slice(&[1, 0]);
    expected.extend_from_slice(&[32, 0]);
    // Entry 0: absolute table/tsid, a last tsid delta of three, an absolute minimum, a span of
    // 100, an absolute offset, a stored length, and the checksum. Raw, so no decoded length.
    expected.push(0x02);
    expected.extend_from_slice(&7u32.to_le_bytes());
    expected.extend_from_slice(&42u64.to_le_bytes());
    expected.push(3);
    expected.extend_from_slice(&100i64.to_le_bytes());
    expected.push(100);
    expected.extend_from_slice(&(1u64 << 32).to_le_bytes());
    expected.extend_from_slice(&[0xAC, 0x02]);
    expected.extend_from_slice(&0x1122_3344_5566_7788u64.to_le_bytes());
    // Entry 1: same table (zero delta), a new table for the last id, a time step of -10, a span of
    // 910, no gap, a stored length, and a decoded length because the child is Zstd.
    expected.push(0x16);
    expected.push(0);
    expected.push(1);
    expected.push(3);
    expected.push(0x13);
    expected.extend_from_slice(&[0x8E, 0x07]);
    expected.push(100);
    expected.push(50);
    expected.extend_from_slice(&0x1122_3344_5566_7788u64.to_le_bytes());
    // Entry 2: an equal endpoint pair, a time step of -95, the widest possible span, and a gap.
    expected.push(0x08);
    expected.push(7);
    expected.extend_from_slice(&[0xBD, 0x01]);
    expected.extend_from_slice(&[0x84, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01]);
    expected.push(100);
    expected.extend_from_slice(&[0x80, 0x20]);
    expected.extend_from_slice(&0x1122_3344_5566_7788u64.to_le_bytes());
    // One restart group, at the start of the entry stream.
    expected.extend_from_slice(&[0, 0]);

    let encoded = compact::encode(&entries, compact::CHILD_LEAF).unwrap();
    assert_eq!(
        expected, encoded,
        "the encoded page must match the contract"
    );

    // The fields decode exactly, and re-encoding them reproduces the same bytes.
    let page = CompactPage::decode(
        encoded.clone(),
        &parent_of(&entries, compact::ENCODING),
        2,
        areas_of(&entries),
    )
    .unwrap();
    let decoded = page.entries().unwrap();
    assert_eq!(3, decoded.len());
    assert_eq!(aggregate(&entries, entry_bounds), page.bounds());
    for (decoded, expected) in decoded.iter().zip(&entries) {
        assert_eq!(expected.bounds, decoded.bounds);
        assert_eq!(expected.offset, decoded.payload.offset);
        assert_eq!(expected.stored, decoded.payload.stored);
        assert_eq!(expected.decoded, decoded.payload.decoded);
        assert_eq!(expected.checksum, decoded.payload.checksum);
        assert_eq!(expected.compression, decoded.payload.compression);
        assert_eq!(compact::CHILD_LEAF, decoded.payload.encoding);
    }
    let reencoded = compact::encode(
        &decoded.iter().map(ChildEntry::from).collect::<Vec<_>>(),
        compact::CHILD_LEAF,
    )
    .unwrap();
    assert_eq!(encoded, reencoded);
    assert_eq!(encoded, page.bytes());
}

/// Every field shape the contract names survives a round trip: raw, Zstd, and mixed children,
/// positive gaps, offsets above 4 GiB, a table change with a lower tsid, equal endpoints,
/// negative times, and the full signed span.
#[test]
fn compact_page_round_trips_every_field_shape() {
    let base = 1 << 32;
    let entries = [
        child(((1, 5), (1, 5)), (0, 0), (base, 10, 10, 0)),
        child(((1, 6), (1, 300)), (-7, 9), (base + 10, 40, 20, 1)),
        child(((2, 1), (2, 1)), (i64::MIN, i64::MAX), (base + 90, 7, 7, 0)),
        child(
            ((2, 2), (9, u64::MAX)),
            (i64::MAX, i64::MAX),
            (base + 97, 1, 1, 0),
        ),
        child(
            ((9, u64::MAX), (9, u64::MAX)),
            (0, 1),
            (base + 200, 5, 2, 1),
        ),
    ];
    let page = compact_of(&entries);
    let decoded = page.entries().unwrap();
    for (decoded, expected) in decoded.iter().zip(&entries) {
        assert_eq!(expected.bounds, decoded.bounds);
        assert_eq!(expected.offset, decoded.payload.offset);
        assert_eq!(expected.stored, decoded.payload.stored);
        assert_eq!(expected.decoded, decoded.payload.decoded);
        assert_eq!(expected.compression, decoded.payload.compression);
    }
    assert_eq!(aggregate(&entries, entry_bounds), page.bounds());
    let reencoded = compact::encode(
        &decoded.iter().map(ChildEntry::from).collect::<Vec<_>>(),
        compact::CHILD_LEAF,
    )
    .unwrap();
    assert_eq!(page.bytes(), reencoded);
}

/// One logical tree, decoded both ways: the compact page must name the same children, derive the
/// same bounds, and select the same candidates as the fixed 72-byte page.
#[test]
fn compact_and_fixed_pages_agree() {
    let mut children = Vec::new();
    let mut offset = 1024;
    for index in 0..40u32 {
        let table_id = 7 + index / 20;
        let tsid = u64::from(index % 20);
        children.push(PageReference {
            bounds: Bounds {
                first: id(table_id, tsid),
                last: id(table_id, tsid + 1),
                min: i64::from(index) * 10 - 100,
                max: i64::from(index) * 10 + 500,
            },
            payload: PayloadReference {
                offset,
                // A raw child's stored length is its decoded length; a Zstd child's is smaller.
                stored: if index % 2 == 0 {
                    64 + index
                } else {
                    50 + index
                },
                decoded: 64 + index,
                checksum: u64::from(index),
                encoding: 1,
                compression: u16::from(index % 2 != 0),
            },
        });
        offset += u64::from(64 + index);
    }
    let entries: Vec<_> = children.iter().map(ChildEntry::from).collect();
    let main = metadata();
    let manifest = manifest_fixture();
    let areas = Areas {
        payload_end: 16,
        keys: KeyArea {
            start: 16,
            end: 1024,
        },
        metadata_start: 1024,
    };
    let parent = PageReference {
        bounds: aggregate(&children, reference_bounds),
        payload: PayloadReference {
            offset,
            stored: 1,
            decoded: 1,
            checksum: 0,
            encoding: 1,
            compression: 0,
        },
    };
    let fixed = Page::decode(
        &Page::Internal(children.clone()).encode(),
        &parent,
        2,
        areas,
        &manifest,
    )
    .unwrap();
    let Page::Internal(fixed_children) = &fixed else {
        panic!("the fixed page must decode as an internal page")
    };
    let compact_page = compact_of(&entries);
    assert_eq!(*fixed_children, compact_page.entries().unwrap());
    assert_eq!(fixed.bounds().unwrap(), compact_page.bounds());
    assert_eq!(
        aggregate(&children, reference_bounds),
        compact_page.bounds()
    );

    // Selection: every child the fixed page keeps for a request, the compact page visits too.
    let mut keys = BTreeSet::new();
    keys.insert(exact(&main, &["a", "b"], 7, 3));
    let mut requests = vec![SamplesReadRequest::default()];
    let mut keyed = full_request();
    keyed.keys = Some(keys);
    requests.push(keyed);
    let mut ranged = full_request();
    ranged.start = Some(0);
    ranged.end = Some(200);
    requests.push(ranged);
    let mut tabled = full_request();
    tabled.table_id = Some(8);
    requests.push(tabled);
    let mut isolated = full_request();
    isolated.table_id = Some(7);
    isolated.start = Some(100);
    requests.push(isolated);
    for request in requests {
        let ids = request
            .keys
            .as_ref()
            .map(|keys| keys.iter().map(|key| key.id()).collect::<BTreeSet<_>>());
        let expected: Vec<_> = fixed_children
            .iter()
            .filter(|child| matches(child.bounds, &request, ids.as_ref()))
            .map(|child| child.bounds)
            .collect();
        let targets: Vec<_> = match (ids.as_ref(), request.table_id) {
            (Some(ids), _) => ids.iter().map(|id| (*id, *id)).collect(),
            (None, Some(table_id)) => vec![(id(table_id, 0), id(table_id, u64::MAX))],
            (None, None) => vec![(id(0, 0), id(u32::MAX, u64::MAX))],
        };
        let mut visited = Vec::new();
        for window in compact_page.windows(targets).unwrap() {
            let mut cursor = Entries::start(&compact_page, window.start).unwrap();
            while let Some((index, entry)) = cursor.next().unwrap() {
                if index >= window.end {
                    break;
                }
                if matches(entry.bounds, &request, ids.as_ref()) {
                    visited.push(entry.bounds);
                }
            }
        }
        assert_eq!(expected, visited, "candidates of {request:?}");
    }
}

/// The children the search visits are exactly the children a linear scan keeps.
#[test]
fn restart_search_matches_a_linear_oracle() {
    for count in [1usize, 31, 32, 33, 64, 100] {
        let entries: Vec<_> = (0..count)
            .map(|index| {
                let tsid = index as u64 * 2;
                child(
                    ((7, tsid), (7, tsid + 1)),
                    (index as i64, index as i64),
                    (1024 + index as u64 * 10, 10, 10, 0),
                )
            })
            .collect();
        let page = compact_of(&entries);
        assert_eq!(count as u64, page.count());
        // Below the page, above it, in the gap between two children, and on a restart boundary.
        let mut targets = vec![0u64, 1, 2, 3];
        for index in 0..count as u64 {
            targets.extend([index * 2, index * 2 + 1, index * 2 + 2]);
        }
        targets.extend([u64::from(u32::MAX - 1), u64::MAX]);
        for tsid in targets {
            let target = id(7, tsid);
            let windows = page.windows([(target, target)]).unwrap();
            let visited: Vec<u64> = windows.iter().flat_map(|window| window.clone()).collect();
            let expected: Vec<u64> = entries
                .iter()
                .enumerate()
                .filter(|(_, entry)| entry.bounds.first <= target && target <= entry.bounds.last)
                .map(|(index, _)| index as u64)
                .collect();
            assert_eq!(expected, visited, "target {tsid} of {count} entries");
        }
        // Several targets merge into one pass, in file order, each entry once.
        let targets = [
            (id(7, 0), id(7, 3)),
            (id(7, 10), id(7, 11)),
            (id(7, 0), id(7, 1)),
        ];
        let windows = page.windows(targets).unwrap();
        let visited: Vec<u64> = windows.iter().flat_map(|window| window.clone()).collect();
        let expected: Vec<u64> = entries
            .iter()
            .enumerate()
            .filter(|(_, entry)| {
                targets
                    .iter()
                    .any(|(first, last)| entry.bounds.first <= *last && *first <= entry.bounds.last)
            })
            .map(|(index, _)| index as u64)
            .collect();
        assert_eq!(expected, visited, "merged windows of {count} entries");
        assert!(
            windows.windows(2).all(|pair| pair[0].end < pair[1].start),
            "merged windows must be disjoint and ordered"
        );
    }
}

/// A run of equal identifiers longer than one group is still found from its first group.
#[test]
fn equal_identifier_runs_span_several_groups() {
    let entries: Vec<_> = (0..100u64)
        .map(|index| {
            child(
                ((7, 42), (7, 42)),
                (index as i64, index as i64),
                (1024 + index * 10, 10, 10, 0),
            )
        })
        .collect();
    let page = compact_of(&entries);
    assert_eq!(
        vec![0..100],
        page.windows([(id(7, 42), id(7, 42))]).unwrap()
    );
    assert!(page.windows([(id(7, 41), id(7, 41))]).unwrap().is_empty());
    assert!(page.windows([(id(7, 43), id(7, 43))]).unwrap().is_empty());
    // A run of equal identifiers in the middle of the page, longer than two groups.
    let entries: Vec<_> = (0..100u64)
        .map(|index| {
            let tsid = match index {
                0..30 => 1,
                30..70 => 50,
                _ => 90,
            };
            child(
                ((7, tsid), (7, tsid)),
                (index as i64, index as i64),
                (1024 + index * 10, 10, 10, 0),
            )
        })
        .collect();
    let page = compact_of(&entries);
    assert_eq!(
        vec![30..70],
        page.windows([(id(7, 50), id(7, 50))]).unwrap()
    );
    assert!(page.windows([(id(7, 51), id(7, 51))]).unwrap().is_empty());
    assert_eq!(vec![0..30], page.windows([(id(7, 1), id(7, 1))]).unwrap());
    assert_eq!(
        vec![70..100],
        page.windows([(id(7, 90), id(7, 90))]).unwrap()
    );
}

/// A page whose count cannot fit its bytes is refused before anything is allocated for it.
#[test]
fn page_count_is_bounded_by_its_bytes() {
    let entries = [
        child(((7, 1), (7, 1)), (0, 1), (1024, 10, 10, 0)),
        child(((7, 2), (7, 2)), (0, 1), (1034, 10, 10, 0)),
    ];
    let bytes = compact::encode(&entries, compact::CHILD_LEAF).unwrap();
    let (stream, restarts) = parts_of(&bytes, entries.len());
    for count in [3u64, 30, 1_000, u64::MAX] {
        let error = decode_raw(raw_page(count, compact::CHILD_LEAF, &stream, &restarts))
            .expect_err("a count larger than the page cannot be decoded");
        let message = error.to_string();
        assert!(
            message.contains("restart array")
                || message.contains("entry count")
                || message.contains("truncated"),
            "{count}: {message}"
        );
    }
    // A count whose restart array is missing entirely.
    let error = decode_raw(raw_page(40, compact::CHILD_LEAF, &stream, &[0]))
        .expect_err("a truncated restart array must be refused");
    let message = error.to_string();
    assert!(
        message.contains("restart array") || message.contains("entry count"),
        "{message}"
    );
}

/// Corruption inside a page is refused however the invalidity is spelled.
#[test]
fn malformed_compact_pages_are_rejected() {
    let valid = [
        child(((7, 10), (7, 12)), (100, 200), (1024, 30, 30, 0)),
        child(((7, 12), (7, 14)), (200, 300), (1054, 30, 30, 0)),
        child(((7, 14), (7, 16)), (300, 400), (1084, 30, 30, 0)),
    ];
    let bytes = compact::encode(&valid, compact::CHILD_LEAF).unwrap();
    let (stream, restarts) = parts_of(&bytes, valid.len());
    // Where the second entry starts, from a page that holds only the first entry.
    let single = compact::encode(&valid[..1], compact::CHILD_LEAF).unwrap();
    let second = parts_of(&single, 1).0.len();

    // One restart entry, hand-written, so every case below can spell one field differently.
    let restart = restart_body((7, 42), 100, 100, (1024, 300, None));
    let restart_page = |control: u8, body: &[u8]| raw_page(1, 1, &entry(control, body), &[0]);

    let mut cases: Vec<(&'static str, Vec<u8>)> = Vec::new();
    // An unknown control bit.
    let mut unknown = stream.clone();
    unknown[0] |= 0x20;
    cases.push(("unknown flag", raw_page(3, 1, &unknown, &restarts)));
    // A flag that depends on a predecessor, on the restart entry that has none.
    let mut dependent = stream.clone();
    dependent[0] |= 0x01;
    cases.push(("dependent restart", raw_page(3, 1, &dependent, &restarts)));
    // A last-table flag on an equal endpoint pair.
    let mut last_table = stream.clone();
    last_table[0] |= 0x04;
    cases.push(("dependent last id", raw_page(3, 1, &last_table, &restarts)));
    // An out-of-order pair: the second entry claims identifiers below the first one's last.
    let mut crossed = stream[..second].to_vec();
    crossed.extend_from_slice(&[0x01, 0x00, 11, 0x00, 0x00, 0x01]);
    crossed.extend_from_slice(&[0; 8]);
    cases.push(("crossed ranges", raw_page(2, 1, &crossed, &restarts)));
    // A restart that does not point at an entry start.
    cases.push(("restart start", raw_page(3, 1, &stream, &[1])));
    // Trailing bytes after the last entry.
    let mut trailing = stream.clone();
    trailing.push(0);
    cases.push(("trailing", raw_page(3, 1, &trailing, &restarts)));
    // An unknown child metadata encoding in the header.
    cases.push(("child encoding", raw_page(3, 3, &stream, &restarts)));
    // A child whose stored length is zero.
    let mut empty = restart.clone();
    empty.truncate(29);
    empty.push(0);
    empty.extend_from_slice(&[0; 8]);
    cases.push(("empty child", restart_page(0, &empty)));
    // A decoded length past the page limit.
    cases.push((
        "decoded length",
        restart_page(
            0,
            &restart_body((7, 42), 100, 100, (1024, 300, Some(u64::from(u32::MAX)))),
        ),
    ));
    // A restart offset inside an entry.
    cases.push(("restart start inside", raw_page(1, 1, &restart, &[7])));
    for (name, bytes) in cases {
        decode_raw(bytes).expect_err(name);
    }

    // A non-minimal varint: a span of zero written with a redundant continuation byte.
    let mut non_minimal = Vec::new();
    non_minimal.extend_from_slice(&7u32.to_le_bytes());
    non_minimal.extend_from_slice(&42u64.to_le_bytes());
    non_minimal.extend_from_slice(&0i64.to_le_bytes());
    non_minimal.extend_from_slice(&[0x80, 0x00]);
    non_minimal.extend_from_slice(&1024u64.to_le_bytes());
    non_minimal.push(1);
    non_minimal.extend_from_slice(&[0; 8]);
    let error = decode_raw(restart_page(0, &non_minimal))
        .expect_err("a non-minimal varint must be refused");
    assert!(error.to_string().contains("non-minimal"), "{error}");

    // A gap flag on an entry that names no gap.
    let mut redundant = stream[..second].to_vec();
    redundant.extend_from_slice(&[0x08, 0x00, 0x00, 0x00, 0x00, 0x01]);
    redundant.extend_from_slice(&[0; 8]);
    let error = decode_raw(raw_page(2, 1, &redundant, &[0]))
        .expect_err("a zero offset gap must be refused");
    assert!(error.to_string().contains("gap"), "{error}");

    // An identifier overflow: a same-table delta that the previous tsid cannot absorb.
    let mut overflow = stream[..second].to_vec();
    overflow.push(0x00);
    overflow.extend_from_slice(&[0xFF; 9]);
    overflow.push(0x01);
    overflow.extend_from_slice(&[0x00, 0x00, 0x01]);
    overflow.extend_from_slice(&[0; 8]);
    let error = decode_raw(raw_page(2, 1, &overflow, &[0]))
        .expect_err("an identifier overflow must be refused");
    assert!(error.to_string().contains("overflow"), "{error}");

    // A truncated entry: the declared count cannot be read out of the stream.
    let error = decode_raw(raw_page(3, 1, &stream[..stream.len() - 4], &restarts))
        .expect_err("a truncated entry must be refused");
    assert!(!error.to_string().is_empty());

    // Bounds that disagree with the parent.
    let bytes = compact::encode(&valid, compact::CHILD_LEAF).unwrap();
    let mut reference = parent_of(&valid, compact::ENCODING);
    reference.bounds.max += 1;
    let error = decode_checked(bytes, &reference, areas_of(&valid))
        .expect_err("the parent's bounds are the aggregate the page must derive");
    assert!(error.to_string().contains("disagree"), "{error}");

    // A child outside the area its parent allows.
    let bytes = compact::encode(&valid, compact::CHILD_LEAF).unwrap();
    let mut areas = areas_of(&valid);
    areas.metadata_start += 1;
    let error = decode_checked(bytes, &parent_of(&valid, compact::ENCODING), areas)
        .expect_err("a child before the metadata area must be refused");
    assert!(error.to_string().contains("outside area"), "{error}");

    // A page deeper than version 1 allows.
    let bytes = compact::encode(&valid, compact::CHILD_LEAF).unwrap();
    let error = CompactPage::decode(
        bytes,
        &parent_of(&valid, compact::ENCODING),
        1,
        areas_of(&valid),
    )
    .expect_err("a compact page is the depth-two root");
    assert!(error.to_string().contains("depth"), "{error}");
}

/// A page is split by encoded bytes: the last entry that fits, the first that does not, and the
/// restart directory that grows at every group boundary.
#[test]
fn capacity_is_exact_and_restart_growth_is_charged() {
    let leaf = 12 + descriptor_bytes(4) * 200;
    let entries: Vec<_> = (0..4096u64)
        .map(|index| {
            child(
                ((7, index * 200), (7, index * 200 + 199)),
                (0, 1000),
                (1024 + index * leaf as u64, leaf as u32, leaf as u32, 1),
            )
        })
        .collect();
    let fits = |count: usize| compact::encode(&entries[..count], compact::CHILD_LEAF).is_ok();
    let (mut low, mut high) = (1usize, entries.len());
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if fits(middle) {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    assert!(fits(low), "the largest fitting page must encode");
    assert!(!fits(low + 1), "one entry more must not encode");
    let bytes = compact::encode(&entries[..low], compact::CHILD_LEAF).unwrap();
    assert!(bytes.len() <= PAGE_LIMIT);
    assert!(
        bytes.len() + compact::MAX_ENTRY_BYTES > PAGE_LIMIT,
        "the page must be full, not merely within its limit"
    );
    // The restart directory is part of the budget, and it grows by two bytes at every group.
    assert_eq!(1, parts_of(&bytes, 32).1.len());
    assert_eq!(2, parts_of(&bytes, 33).1.len());
    assert_eq!(
        low.div_ceil(compact::RESTART_INTERVAL as usize),
        parts_of(&bytes, low).1.len()
    );
    assert_eq!(0, parts_of(&bytes, low).1[0]);

    // Worst-width entries stay within the reservation a writer makes for an unfinished leaf.
    let worst = child(
        ((1, u64::MAX), (u32::MAX, u64::MAX)),
        (i64::MIN, i64::MAX),
        (u64::MAX - 8, 3, 3, 1),
    );
    let single = compact::encode(&[worst], compact::CHILD_LEAF).unwrap();
    assert!(
        single.len() - 16 - 2 <= compact::MAX_ENTRY_BYTES,
        "{} bytes for one entry",
        single.len() - 18
    );
}

/// The density the page exists for: children per page for one block per series and for eight
/// blocks per series.
#[test]
fn reference_shapes_name_thousands_of_children() {
    let blocks_per_leaf = (PAGE_LIMIT - 12) / descriptor_bytes(4);
    let leaf_decoded = 12 + blocks_per_leaf * descriptor_bytes(4);
    assert!(leaf_decoded <= PAGE_LIMIT);
    for (name, blocks_per_series) in [
        ("one block per series", 1usize),
        ("eight blocks per series", 8),
    ] {
        let entries: Vec<_> = (0..4096u64)
            .map(|index| {
                let first = index * blocks_per_leaf as u64 / blocks_per_series as u64;
                let last = ((index + 1) * blocks_per_leaf as u64 - 1) / blocks_per_series as u64;
                child(
                    ((7, first), (7, last)),
                    (0, 1000),
                    (
                        1024 + index * leaf_decoded as u64,
                        leaf_decoded as u32,
                        leaf_decoded as u32,
                        1,
                    ),
                )
            })
            .collect();
        let fits = |count: usize| compact::encode(&entries[..count], compact::CHILD_LEAF).is_ok();
        let (mut low, mut high) = (1usize, entries.len());
        while low < high {
            let middle = low + (high - low).div_ceil(2);
            if fits(middle) {
                low = middle;
            } else {
                high = middle - 1;
            }
        }
        // A leaf of one value field holds 282 blocks, so one block per series makes each child
        // span 282 identifiers while eight blocks per series makes it span about 35. That is
        // 21.88 and 20.89 encoded bytes per child, against 72 for a fixed descriptor.
        let expected = if blocks_per_series == 1 {
            2_900..=3_100
        } else {
            3_100..=3_300
        };
        assert!(expected.contains(&low), "{name}: {low} children per page");
    }
}

/// The writer's default capacity must not clamp a compact page back to the fixed layout's count:
/// a part may name far more leaves than 910 before the encoded size splits it.
#[test]
fn default_capacity_admits_more_than_the_fixed_layout() {
    let format = ColumnarSamplesFormat::default();
    assert!(
        format.root_capacity >= 5_000,
        "the default capacity clamps the page to {} children",
        format.root_capacity
    );
    assert!(format.root_capacity < usize::MAX);
    // The rollover estimate allows a part whose root names several times the fixed capacity.
    let leaf = 12 + descriptor_bytes(4) * 200;
    let leaves: Vec<compact::LeafPlan> = (0..4_000u64)
        .map(|index| compact::LeafPlan {
            bounds: Bounds {
                first: id(7, index * 200),
                last: id(7, index * 200 + 199),
                min: 0,
                max: 1000,
            },
            decoded: leaf,
        })
        .collect();
    for count in [911usize, 2_000] {
        assert!(
            compact::planned_bytes(&leaves[..count], compact::MAX_ENTRY_BYTES).unwrap()
                <= PAGE_LIMIT,
            "{count} leaves must still fit one page"
        );
    }
    assert!(
        compact::planned_bytes(&leaves[..3_000], compact::MAX_ENTRY_BYTES).is_err(),
        "the estimate must eventually refuse another leaf"
    );
}

/// The writer emits compact internal pages, records the new manifest version, and leaves a
/// one-leaf part on version 1 in the unchanged layout.
#[tokio::test]
async fn writer_emits_compact_internal_pages() {
    let store = store();
    let m = metadata();
    let format = ColumnarSamplesFormat {
        block_rows: 1,
        page_bytes: 4096,
        ..Default::default()
    };
    let rows: Vec<Sample> = (0..600u64)
        .map(|index| (7, 42, 100, 600 - index, 1, Some(index)))
        .collect();
    let handle = build(&format, &store, m.clone(), &[batch(&m, &rows[..], false)]).await;
    let manifest = manifest(&store, &handle).await;
    assert_eq!(COMPACT_VERSION, manifest.version);
    let bytes = store.read(&manifest.part_path(0)).await.unwrap().to_bytes();
    let trailer = Trailer::decode(
        &bytes[bytes.len() - TRAILER_SIZE as usize..],
        bytes.len() as u64,
    )
    .unwrap();
    assert_eq!(2, trailer.depth);
    assert_eq!(compact::ENCODING, trailer.root.payload.encoding);
    let root = Page::decode(
        &trailer
            .root
            .payload
            .unpack(part_page(&bytes, &trailer))
            .unwrap(),
        &trailer.root,
        trailer.depth,
        trailer.areas(),
        &manifest,
    )
    .unwrap();
    let Page::Compact(page) = &root else {
        panic!("the root of a depth-two part must be a compact page")
    };
    assert!(page.count() > 32, "the root must hold several groups");

    // The same writer keeps a single-leaf part on version 1, with the unchanged leaf layout.
    let handle = build(
        &format,
        &store,
        m.clone(),
        &[batch(&m, &[(7, 42, 100, 1, 1, Some(1))], false)],
    )
    .await;
    let small = super::manifest(&store, &handle).await;
    assert_eq!(VERSION, small.version);
    let bytes = store.read(&small.part_path(0)).await.unwrap().to_bytes();
    let trailer = Trailer::decode(
        &bytes[bytes.len() - TRAILER_SIZE as usize..],
        bytes.len() as u64,
    )
    .unwrap();
    assert_eq!(1, trailer.depth);
    assert_eq!(1, trailer.root.payload.encoding);
}

fn part_page<'a>(bytes: &'a [u8], trailer: &Trailer) -> &'a [u8] {
    let start = trailer.root.payload.offset as usize;
    &bytes[start..start + trailer.root.payload.stored as usize]
}

/// A request against a part whose root has several restart groups returns exactly the rows a full
/// scan of the same artifact would.
#[tokio::test]
async fn artifact_search_matches_a_full_scan() {
    let store = store();
    let m = metadata();
    let format = ColumnarSamplesFormat {
        block_rows: 1,
        page_bytes: 4096,
        ..Default::default()
    };
    // The first series spans the leaf that starts the second restart group, so an equal-timestamp
    // run crosses a restart boundary.
    let mut rows: Vec<Sample> = (0..520)
        .map(|index| (7, 42, 100, 520 - index, 1, Some(index)))
        .collect();
    rows.extend((0..100u64).map(|index| (7, 43, 50, 100 - index, 1, Some(1000 + index))));
    rows.extend((0..100u64).map(|index| (8, 42, 100, 100 - index, 1, None)));
    let handle = build(&format, &store, m.clone(), &[batch(&m, &rows[..], true)]).await;
    let manifest = manifest(&store, &handle).await;
    assert_eq!(COMPACT_VERSION, manifest.version);
    let reader = format.reader(store, handle).await.unwrap();

    let key_set = |pairs: &[(u32, u64)]| {
        pairs
            .iter()
            .map(|(table_id, tsid)| exact(&m, &["a", "b"], *table_id, *tsid))
            .collect::<BTreeSet<_>>()
    };
    type Case = (
        BTreeSet<ExactSeriesKey>,
        Option<u32>,
        Option<i64>,
        Option<i64>,
    );
    let cases: Vec<Case> = vec![
        (key_set(&[(7, 42)]), None, None, None),
        (key_set(&[(7, 42), (8, 42)]), None, None, None),
        (key_set(&[(7, 42), (7, 43), (8, 42)]), None, None, None),
        (BTreeSet::new(), None, Some(100), Some(101)),
        (BTreeSet::new(), None, Some(50), Some(51)),
        (BTreeSet::new(), Some(8), None, None),
        (key_set(&[(7, 43)]), None, Some(55), Some(58)),
        (key_set(&[(7, 42), (7, 43)]), None, Some(100), Some(101)),
    ];
    for (keys, table_id, start, end) in cases {
        let expect_keys = keys.clone();
        let mut request = full_request();
        request.keys = (!keys.is_empty()).then_some(keys);
        request.table_id = table_id;
        request.start = start;
        request.end = end;
        let expected: Vec<_> = rows
            .iter()
            .copied()
            .filter(|row| {
                (expect_keys.is_empty()
                    || expect_keys.contains(&exact(&m, &["a", "b"], row.0, row.1)))
                    && table_id.is_none_or(|table_id| row.0 == table_id)
                    && start.is_none_or(|start| row.2 >= start)
                    && end.is_none_or(|end| row.2 < end)
            })
            .collect();
        let batches = reader
            .read(request.clone())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            expected,
            collect_rows(&batches),
            "{:?} {table_id:?} {start:?} {end:?}",
            request.keys.as_ref().map(BTreeSet::len)
        );
    }
}

/// A legacy fixed internal page is still read, and names the same children as the compact page
/// the writer now emits.
#[tokio::test]
async fn fixed_internal_root_is_still_readable() {
    let store = store();
    let m = metadata();
    let format = ColumnarSamplesFormat {
        block_rows: 1,
        page_bytes: 288,
        root_capacity: 8,
        ..Default::default()
    };
    let rows: Vec<Sample> = (0..8)
        .map(|index| (7, 42, 100 + index as i64, 1, 1, Some(index)))
        .collect();
    let handle = build(&format, &store, m.clone(), &[batch(&m, &rows[..], false)]).await;
    let before = read_all(&format, &store, handle.clone(), &m).await;
    assert_eq!(rows.len(), before.len());
    let rewritten = rewrite_root(&store, handle, 1, |page| {
        let Page::Compact(page) = page else {
            panic!("the writer must emit a compact root")
        };
        Page::Internal(page.entries().unwrap())
    })
    .await;
    let after = read_all(&format, &store, rewritten, &m).await;
    assert_eq!(before, after);
}

/// A version-1 artifact never holds a compact page, so one that claims version 1 and carries a
/// compact root is refused when the page is loaded: `version` is a promise about the metadata
/// layout, not a hint.
///
/// The manifest itself opens - version 1 is supported - which is what makes this the page gate and
/// not the manifest gate.
#[tokio::test]
async fn compact_page_under_version_one_is_refused() {
    let store = store();
    let m = metadata();
    let format = ColumnarSamplesFormat {
        block_rows: 1,
        page_bytes: 288,
        root_capacity: 4,
        ..Default::default()
    };
    let rows: Vec<Sample> = (0..4)
        .map(|index| (7, 42, 100 + index as i64, 1, 1, Some(index)))
        .collect();
    let handle = build(&format, &store, m.clone(), &[batch(&m, &rows[..], false)]).await;
    let manifest = manifest(&store, &handle).await;
    assert_eq!(COMPACT_VERSION, manifest.version);
    let (_, trailer, page, _) = part_root(&store, &handle).await;
    assert!(matches!(page, Page::Compact(_)));
    assert_eq!(compact::ENCODING, trailer.root.payload.encoding);

    let path = manifest.path();
    let mut downgraded = serde_json::to_value(&manifest).unwrap();
    downgraded["version"] = serde_json::json!(VERSION);
    let json = serde_json::to_vec(&downgraded).unwrap();
    store.write(&path, json.clone()).await.unwrap();
    let mut entry = handle.entry().clone();
    for object in &mut entry.artifact.objects {
        if object.path == path {
            object.file_size = json.len() as u64;
        }
    }
    entry.artifact.file_size = entry.artifact.objects.iter().map(|o| o.file_size).sum();
    let (purger, _receiver) = series_index_channel(store.clone());
    let downgraded = SamplesFileHandle::new(handle.file_id(), entry, purger);

    let reader = format.reader(store.clone(), downgraded).await.unwrap();
    let error = match reader
        .read(full_request())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
    {
        Ok(_) => panic!("a version-1 artifact cannot hold a compact page"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("compact"), "{error}");
}

async fn read_all(
    format: &ColumnarSamplesFormat,
    store: &ObjectStore,
    handle: SamplesFileHandle,
    m: &RegionMetadataRef,
) -> Vec<Sample> {
    let reader = format.reader(store.clone(), handle).await.unwrap();
    let mut request = full_request();
    request.keys = Some(BTreeSet::from([exact(m, &["a", "b"], 7, 42)]));
    let batches = reader
        .read(request)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    collect_rows(&batches)
}

/// The descriptor, trailer, root page, and bytes of a one-part artifact, whatever its depth.
async fn part_root(
    store: &ObjectStore,
    handle: &SamplesFileHandle,
) -> (Manifest, Trailer, Page, Vec<u8>) {
    let manifest = manifest(store, handle).await;
    assert_eq!(
        1,
        manifest.files.len(),
        "one part keeps the root addressable"
    );
    let bytes = store
        .read(&manifest.part_path(0))
        .await
        .unwrap()
        .to_bytes()
        .to_vec();
    let trailer = Trailer::decode(
        &bytes[bytes.len() - TRAILER_SIZE as usize..],
        bytes.len() as u64,
    )
    .unwrap();
    let page = Page::decode(
        &trailer
            .root
            .payload
            .unpack(part_page(&bytes, &trailer))
            .unwrap(),
        &trailer.root,
        trailer.depth,
        trailer.areas(),
        &manifest,
    )
    .unwrap();
    (manifest, trailer, page, bytes)
}

/// Replaces the stored root page of a one-part artifact under `encoding`, with every outer
/// checksum recomputed, so a test can prove the reader still accepts the older layout.
async fn rewrite_root(
    store: &ObjectStore,
    handle: SamplesFileHandle,
    encoding: u16,
    rewrite: impl FnOnce(Page) -> Page,
) -> SamplesFileHandle {
    let (mut manifest, mut trailer, page, mut bytes) = part_root(store, &handle).await;
    let start = trailer.root.payload.offset as usize;
    let encoded = rewrite(page).encode();
    bytes.truncate(start);
    bytes.extend_from_slice(&encoded);
    trailer.root.payload = PayloadReference {
        offset: start as u64,
        stored: encoded.len() as u32,
        decoded: encoded.len() as u32,
        checksum: codec::checksum(&encoded),
        encoding,
        compression: 0,
    };
    trailer.file_length = (bytes.len() + TRAILER_SIZE as usize) as u64;
    bytes.extend_from_slice(&trailer.encode());
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
        }
    }
    entry.artifact.file_size = entry.artifact.objects.iter().map(|o| o.file_size).sum();
    let (purger, _receiver) = series_index_channel(store.clone());
    SamplesFileHandle::new(handle.file_id(), entry, purger)
}

struct RecordingRead {
    inner: oio::Reader,
    path: String,
    reads: Arc<Mutex<Vec<String>>>,
}

impl mock::Read for RecordingRead {
    async fn open(
        &self,
        range: mock::BytesRange,
    ) -> mock::Result<(mock::RpRead, Box<dyn oio::ReadStreamDyn>)> {
        self.reads.lock().unwrap().push(self.path.clone());
        self.inner.open(range).await
    }

    async fn read(&self, range: mock::BytesRange) -> mock::Result<(mock::RpRead, mock::Buffer)> {
        self.reads.lock().unwrap().push(self.path.clone());
        self.inner.read(range).await
    }
}

/// A manifest version above what this reader knows is refused at open, before any part is read -
/// which is how a reader that only knows version 1 refuses a compact artifact.
#[tokio::test]
async fn compact_artifact_is_refused_before_any_part_read() {
    let store = store();
    let m = metadata();
    let format = ColumnarSamplesFormat {
        block_rows: 1,
        page_bytes: 288,
        root_capacity: 4,
        ..Default::default()
    };
    let rows: Vec<Sample> = (0..4)
        .map(|index| (7, 42, 100 + index as i64, 1, 1, Some(index)))
        .collect();
    let handle = build(&format, &store, m.clone(), &[batch(&m, &rows[..], false)]).await;
    let manifest = manifest(&store, &handle).await;
    assert_eq!(COMPACT_VERSION, manifest.version);
    // The predicate a version-1-only reader applies to this manifest, which is what refuses it.
    assert_ne!(VERSION, manifest.version);
    let part = manifest.part_path(0);

    // The same gate refuses any unknown version before a part is read.
    let path = manifest.path();
    let mut future = serde_json::to_value(&manifest).unwrap();
    future["version"] = serde_json::json!(COMPACT_VERSION + 1);
    let json = serde_json::to_vec(&future).unwrap();
    store.write(&path, json.clone()).await.unwrap();
    let mut entry = handle.entry().clone();
    for object in &mut entry.artifact.objects {
        if object.path == path {
            object.file_size = json.len() as u64;
        }
    }
    entry.artifact.file_size = entry.artifact.objects.iter().map(|o| o.file_size).sum();
    let (purger, _receiver) = series_index_channel(store.clone());
    let unknown = SamplesFileHandle::new(handle.file_id(), entry, purger);

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
    let error = format
        .reader(store.layer(layer), unknown)
        .await
        .err()
        .expect("an unknown manifest version must be refused at open");
    assert!(error.to_string().contains("manifest version"), "{error}");
    let reads = reads.lock().unwrap().clone();
    assert!(
        reads.iter().all(|path| path.ends_with("manifest.json")),
        "no part may be read before the refusal: {reads:?}"
    );
    assert!(!reads.iter().any(|path| path.ends_with(&part)));
}

/// Fields the frozen version-2 manifest names, so a test can install it under its own paths.
#[derive(serde::Deserialize)]
struct FrozenV2Manifest {
    region_id: u64,
    source_file_id: FileId,
    generation: FileId,
}

/// Installs the frozen version-2 fixture: a depth-two part whose root is a compact page.
async fn compact_fixture(store: &ObjectStore) -> SamplesFileHandle {
    let hex = include_str!(
        "../../../../../../tests/compatibility/wire/mito_columnar_samples/compact-v2-part.hex"
    )
    .trim();
    let bytes = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect::<Vec<_>>();
    let json = include_str!(
        "../../../../../../tests/compatibility/wire/mito_columnar_samples/compact-v2-manifest.json"
    );
    let frozen: FrozenV2Manifest = serde_json::from_str(json).unwrap();
    let prefix = crate::series_index::samples::samples_prefix(frozen.region_id, frozen.generation);
    let manifest_path = format!("{prefix}manifest.json");
    let part_path = format!("{prefix}part-0.samples");
    store.write(&part_path, bytes.clone()).await.unwrap();
    store.write(&manifest_path, json).await.unwrap();
    let objects = vec![
        crate::series_index::artifact::ArtifactObject {
            path: manifest_path,
            file_size: json.len() as u64,
        },
        crate::series_index::artifact::ArtifactObject {
            path: part_path,
            file_size: bytes.len() as u64,
        },
    ];
    let artifact = crate::series_index::artifact::ArtifactMetadata {
        format: ArtifactFormat::ColumnarSamples,
        generation: frozen.generation,
        file_size: (json.len() + bytes.len()) as u64,
        objects,
    };
    let (purger, _) = series_index_channel(store.clone());
    SamplesFileHandle::new(
        RegionFileId::new(
            store_api::storage::RegionId::from_u64(frozen.region_id),
            frozen.source_file_id,
        ),
        SamplesEntry {
            source_file_ids: Vec::new(),
            window: None,
            source_windows: Default::default(),
            file_id: frozen.source_file_id,
            artifact,
        },
        purger,
    )
}

/// The frozen version-2 fixture reads: its root is a compact page, and both blocks come back with
/// their exact values.
#[tokio::test]
async fn compact_fixture_v2_reads() {
    let store = store();
    let handle = compact_fixture(&store).await;
    let format = ColumnarSamplesFormat::default();
    let reader = format.reader(store.clone(), handle.clone()).await.unwrap();
    let batches = reader
        .read(full_request())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        vec![
            (7, 42, 100, 9, 1, Some((-0.0f64).to_bits())),
            (7, 42, 200, 10, 1, Some(2.5f64.to_bits())),
        ],
        collect_rows(&batches)
    );
    // The frozen bytes are the compact layout: the root names two children in encoding 2, and the
    // manifest is version 2 while still naming the whole-key area the fixture stores.
    let manifest = manifest(&store, &handle).await;
    assert_eq!(COMPACT_VERSION, manifest.version);
    assert_eq!(
        IdentityEncoding::ExactSparseKeyArea,
        manifest.identity_encoding,
        "version 2 permits compact pages but does not change the key-area layout"
    );
    let bytes = store.read(&manifest.part_path(0)).await.unwrap().to_bytes();
    let trailer = Trailer::decode(
        &bytes[bytes.len() - TRAILER_SIZE as usize..],
        bytes.len() as u64,
    )
    .unwrap();
    assert_eq!(2, trailer.depth);
    assert_eq!(compact::ENCODING, trailer.root.payload.encoding);
    let root = Page::decode(
        &trailer
            .root
            .payload
            .unpack(part_page(&bytes, &trailer))
            .unwrap(),
        &trailer.root,
        trailer.depth,
        trailer.areas(),
        &manifest,
    )
    .unwrap();
    let Page::Compact(page) = &root else {
        panic!("the frozen root must be a compact page")
    };
    assert_eq!(2, page.count());
}

/// The suffix key area and the compact internal page meet in one part: the keys are stored as
/// suffixes and the path to their leaves is a compact page.
///
/// The two optimisations are orthogonal, so this is what any part large enough to need an internal
/// page looks like now: the root names its leaves in encoding 2, and each leaf's descriptors carry
/// the identifiers a reader recombines with the stored suffixes. The key area is checked against
/// exactly those suffixes, which is the half a compact page could silently break: a page that
/// misreports which leaf a block belongs to would rebuild the key under the wrong identifier.
#[tokio::test]
async fn suffix_key_area_with_a_compact_root_reads() {
    let store = store();
    let m = metadata();
    // One block per row, so a modest row count fills several leaves and needs an internal page.
    let format = ColumnarSamplesFormat {
        block_rows: 1,
        page_bytes: 4096,
        ..Default::default()
    };
    // Two series share `(7, 42)` and differ only in their labels, which only the stored suffix can
    // tell apart; the third has no labels at all, so its suffix is empty.
    let series: [(&[&str], u32, u64); 3] =
        [(&["host-1"], 7, 42), (&["host-2"], 7, 42), (&[], 7, 43)];
    let mut keys = Vec::new();
    let mut times = Vec::new();
    for (labels, table_id, tsid) in series {
        for round in 0..200i64 {
            keys.push(new_sparse_primary_key(labels, &m, table_id, tsid));
            times.push(100 + round);
        }
    }
    let handle = build(
        &format,
        &store,
        m.clone(),
        &[keyed_batch(&m, &keys, &times, false)],
    )
    .await;

    let (manifest, trailer, root, _bytes) = part_root(&store, &handle).await;
    // Both fields at once: the metadata layout permits a compact page, and the key area still
    // names the suffix layout.
    assert_eq!(COMPACT_VERSION, manifest.version);
    assert_eq!(
        IdentityEncoding::CURRENT_SAMPLES,
        manifest.identity_encoding
    );
    assert_eq!(2, trailer.depth);
    assert_eq!(compact::ENCODING, trailer.root.payload.encoding);
    let Page::Compact(page) = &root else {
        panic!("a depth-two part's root must be a compact page")
    };
    assert!(
        page.count() > 1,
        "an internal page must name several leaves"
    );

    // The key area holds one stored suffix per series, with the checksum of the whole key: no
    // copy repeats the 22-byte prefix the descriptors already carry.
    let suffix_area = trailer.key_end - trailer.payload_end;
    let suffix_bytes: u64 = series
        .iter()
        .map(|(labels, table_id, tsid)| {
            let key = exact(&m, labels, *table_id, *tsid);
            series_key_suffix(key.bytes(), key.id()).unwrap().len() as u64 + KEY_CHECKSUM_BYTES
        })
        .sum();
    assert_eq!(suffix_bytes, suffix_area);
    let whole_bytes: u64 = series
        .iter()
        .map(|(labels, table_id, tsid)| {
            exact(&m, labels, *table_id, *tsid).bytes().len() as u64 + KEY_CHECKSUM_BYTES
        })
        .sum();
    assert!(
        suffix_area < whole_bytes,
        "the stored copies must not repeat the prefix"
    );

    // Reading everything crosses the compact page: every leaf is visited and every block decodes.
    let reader = format.reader(store.clone(), handle).await.unwrap();
    let all = reader
        .read(full_request())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let expected: Vec<Sample> = series
        .iter()
        .flat_map(|(_, table_id, tsid)| {
            (0..200i64).map(move |round| (*table_id, *tsid, 100 + round, 1, 0, Some(0)))
        })
        .collect();
    assert_eq!(expected, collect_rows(&all));

    // A point lookup is answered by one identifier's blocks, and its keys are the reconstructed
    // whole keys: the suffix is recombined with the descriptor's own prefix.
    let target = exact(&m, &["host-1"], 7, 42);
    let mut query = full_request();
    query.keys = Some(BTreeSet::from([target.clone()]));
    let picked = reader
        .read(query)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        vec![target.clone()],
        picked
            .iter()
            .map(|batch| batch.key.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
    );
    assert_eq!(
        (0..200i64)
            .map(|round| (7, 42, 100 + round, 1, 0, Some(0)))
            .collect::<Vec<_>>(),
        collect_rows(&picked)
    );
}
