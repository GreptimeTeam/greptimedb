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

//! Compact internal metadata pages, metadata encoding id `2`.
//!
//! An internal page names its children's identifiers, time envelopes, and payload locations.
//! Siblings are ordered and stored back to back, so almost every field of one entry is either
//! implied by its predecessor or a small number: this encoding stores only what changes, which
//! makes one 64 KiB page name thousands of children instead of hundreds.
//!
//! The decoded page is `[16-byte header][entry stream][restart array]`, little-endian, with no
//! padding. The header is `kind: u32 = 1`, `count: u64 > 0`, `child_metadata_encoding: u16`
//! (`1` leaf children, `2` compact internal children), `restart_interval: u8 = 32`, and one zero
//! reserved byte. Each entry is
//!
//! ```text
//! control u8    bit 0 first table changed, bit 1 last != first, bit 2 last table changed,
//!               bit 3 nonzero offset gap, bit 4 Zstd; bits 5..7 zero
//! first id      at a restart: table u32 + tsid u64
//!               otherwise, against the previous entry's last: same table U(tsid delta),
//!               new table U(positive table delta) + U(absolute tsid)
//! last id       0 bytes when equal to first, otherwise the same pair rule from this first
//! min time      at a restart: i64; otherwise Z(min - previous min), a reversible predictor
//! span          U(max - min), widened so the full signed range is one entry
//! child offset  at a restart: absolute u64; otherwise the previous end plus U(gap) when the
//!               gap flag is set
//! stored length U(stored), positive
//! decoded       absent for raw children, U(decoded) for Zstd children
//! checksum      8 bytes, XXH64 of the stored child bytes
//! ```
//!
//! The restart array holds `ceil(count / 32)` `u16` offsets relative to the entry stream. Entry
//! zero and every 32nd entry use the restart form, so nothing there depends on a predecessor: a
//! search binary-searches the restart first identifiers, starts one group earlier, and scans.
//!
//! The whole page is validated once, when it is loaded and cached; a search then decodes only the
//! entries it visits.

use std::ops::Range;
use std::sync::Arc;

use crate::error::Result;
use crate::series_index::samples::codec::{Cursor, put_u32, put_u64, put_varint};
use crate::series_index::samples::metadata::{
    Areas, Bounds, PAGE_LIMIT, PageReference, PayloadReference,
};
use crate::series_index::samples::{MetricSeriesId, invalid, require, unsupported};

/// Metadata encoding id of a compact internal page.
pub(super) const ENCODING: u16 = 2;
/// Metadata encoding id of the leaf pages a compact internal page names at depth two.
pub(super) const CHILD_LEAF: u16 = 1;
/// Metadata encoding id of compact internal children, for a tree deeper than version 1 allows.
pub(super) const CHILD_COMPACT: u16 = 2;
/// Entries between two restart forms. Fixed by this encoding; the header records it.
pub(super) const RESTART_INTERVAL: u64 = 32;
/// Header bytes, before the entry stream.
pub(super) const HEADER_SIZE: usize = 16;
const KIND_INTERNAL: u32 = 1;

const FIRST_TABLE_CHANGED: u8 = 1;
const LAST_DIFFERS: u8 = 2;
const LAST_TABLE_CHANGED: u8 = 4;
const OFFSET_GAP: u8 = 8;
const COMPRESSED: u8 = 16;
const KNOWN_FLAGS: u8 =
    FIRST_TABLE_CHANGED | LAST_DIFFERS | LAST_TABLE_CHANGED | OFFSET_GAP | COMPRESSED;
/// Flags a restart entry may not carry: they would state a relation to a predecessor.
const DEPENDENT_FLAGS: u8 = FIRST_TABLE_CHANGED | OFFSET_GAP;

/// Smallest possible entry: control, a one-byte identifier delta, a one-byte time delta, a
/// one-byte span, a one-byte stored length, and the checksum.
///
/// Used to bound a page's entry count by its bytes before anything is allocated for it.
const MIN_ENTRY_BYTES: usize = 13;
/// Most children a compact page could name: the header leaves room for
/// [`MIN_ENTRY_BYTES`]-sized entries and nothing more. A writer's configured capacity defaults to
/// this, so the encoded size is what splits a page rather than a fixed child count.
pub(super) const CAPACITY: usize = (PAGE_LIMIT - HEADER_SIZE) / MIN_ENTRY_BYTES;
/// Largest possible entry, which is what a writer reserves for a leaf page whose bounds are
/// still moving: a table change and an absolute tsid in both identifiers (5 + 10 bytes each),
/// a ten-byte time delta, a ten-byte span, an eight-byte restart offset, a three-byte stored
/// length, a three-byte decoded length, and the checksum.
pub(super) const MAX_ENTRY_BYTES: usize = 1 + 15 + 15 + 10 + 10 + 8 + 3 + 3 + 8;

fn zigzag(value: i64) -> u64 {
    ((value as u64) << 1) ^ ((value >> 63) as u64)
}

fn unzigzag(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

/// Reads one unsigned LEB128 value, refusing a form a shorter encoding would produce.
///
/// The legacy decoders accept redundant continuation bytes; this codec fixes one encoding per
/// value, so every page has exactly one byte representation.
fn varint(c: &mut Cursor<'_>) -> Result<u64> {
    let mut value = 0u64;
    for shift in (0..70).step_by(7) {
        let byte = c.u8()?;
        if shift == 63 {
            require(byte <= 1, "sample compact varint overflow")?;
        }
        value |= u64::from(byte & 127) << shift;
        if byte & 128 == 0 {
            require(shift == 0 || byte != 0, "non-minimal sample compact varint")?;
            return Ok(value);
        }
    }
    Err(invalid("sample compact varint overflow"))
}

/// `max - min` widened so the full signed range is representable and no `i64` can overflow.
fn span(min: i64, max: i64) -> Result<u64> {
    require(max >= min, "reversed sample bounds")?;
    u64::try_from(i128::from(max) - i128::from(min)).map_err(|_| invalid("sample compact span"))
}

/// The predecessor state one entry needs to be decoded, whichever form encoded it.
#[derive(Debug, Clone, Copy)]
struct Tail {
    last: MetricSeriesId,
    min: i64,
    end: u64,
}

/// One child of a compact internal page, as the codec reads and writes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ChildEntry {
    pub bounds: Bounds,
    pub offset: u64,
    pub stored: u32,
    pub decoded: u32,
    pub checksum: u64,
    pub compression: u16,
}

impl ChildEntry {
    /// The payload location this entry names, under the page's child encoding.
    pub(super) fn payload(&self, encoding: u16) -> PayloadReference {
        PayloadReference {
            offset: self.offset,
            stored: self.stored,
            decoded: self.decoded,
            checksum: self.checksum,
            encoding,
            compression: self.compression,
        }
    }

    /// The page reference a parent page or the trailer holds for this child.
    pub(super) fn reference(&self, encoding: u16) -> PageReference {
        PageReference {
            bounds: self.bounds,
            payload: self.payload(encoding),
        }
    }

    fn end(&self) -> Result<u64> {
        self.offset
            .checked_add(u64::from(self.stored))
            .ok_or_else(|| invalid("sample payload offset overflow"))
    }
}

impl From<&PageReference> for ChildEntry {
    fn from(reference: &PageReference) -> Self {
        Self {
            bounds: reference.bounds,
            offset: reference.payload.offset,
            stored: reference.payload.stored,
            decoded: reference.payload.decoded,
            checksum: reference.payload.checksum,
            compression: reference.payload.compression,
        }
    }
}

/// What a writer knows about a leaf page before it is encoded.
///
/// Its decoded length is exact (it is the 12-byte prefix plus its descriptors) and its stored
/// length never exceeds that, because the writer keeps the raw bytes whenever compression does not
/// shrink them. Both are therefore bounded before the page exists.
#[derive(Debug, Clone, Copy)]
pub(super) struct LeafPlan {
    pub bounds: Bounds,
    pub decoded: usize,
}

/// Encodes the internal page that references `entries`, in the form a writer emits.
pub(super) fn encode(entries: &[ChildEntry], child_encoding: u16) -> Result<Vec<u8>> {
    require(!entries.is_empty(), "empty sample compact page")?;
    require(
        matches!(child_encoding, CHILD_LEAF | CHILD_COMPACT),
        "unknown sample child encoding",
    )?;
    let count =
        u64::try_from(entries.len()).map_err(|_| invalid("sample compact entry count overflow"))?;
    let groups = usize::try_from(count.div_ceil(RESTART_INTERVAL))
        .map_err(|_| invalid("sample compact entry count overflow"))?;
    let mut out = Vec::with_capacity(HEADER_SIZE + 2 * groups + entries.len() * 24);
    put_u32(&mut out, KIND_INTERNAL);
    put_u64(&mut out, count);
    out.extend_from_slice(&child_encoding.to_le_bytes());
    out.push(RESTART_INTERVAL as u8);
    out.push(0);
    let mut restarts = Vec::with_capacity(groups);
    let mut previous: Option<Tail> = None;
    for (index, entry) in entries.iter().enumerate() {
        let index = index as u64;
        // The page's own limits, applied to every child before it is written.
        entry.payload(child_encoding).validate(0, u64::MAX, true)?;
        if index.is_multiple_of(RESTART_INTERVAL) {
            restarts.push(
                u16::try_from(out.len() - HEADER_SIZE)
                    .map_err(|_| invalid("sample compact restart offset overflow"))?,
            );
        }
        encode_entry(&mut out, index, previous.as_ref(), entry)?;
        previous = Some(Tail {
            last: entry.bounds.last,
            min: entry.bounds.min,
            end: entry.end()?,
        });
    }
    require(
        out.len() + 2 * groups <= PAGE_LIMIT,
        "sample compact page exceeds limit",
    )?;
    for restart in restarts {
        out.extend_from_slice(&restart.to_le_bytes());
    }
    Ok(out)
}

/// The bytes a page that references `leaves` will occupy once their pages are written.
///
/// This is the writer's page-splitting rule: a page is full when the next child would take it past
/// [`PAGE_LIMIT`]. It bounds every field without the pages themselves: identifiers and time
/// envelopes are known, a page's stored length is at most its decoded length, and pages are
/// emitted back to back so no offset gap can appear. `slack` covers a leaf whose bounds can still
/// move, which is at most [`MAX_ENTRY_BYTES`] for one leaf.
pub(super) fn planned_bytes(leaves: &[LeafPlan], slack: usize) -> Result<usize> {
    let mut offset = 0u64;
    let mut entries = Vec::with_capacity(leaves.len());
    for leaf in leaves {
        let stored =
            u32::try_from(leaf.decoded).map_err(|_| invalid("sample leaf page exceeds limit"))?;
        entries.push(ChildEntry {
            bounds: leaf.bounds,
            offset,
            stored,
            decoded: stored,
            // Compression is not decided yet; charging the Zstd form and a decoded length that
            // covers the raw one bounds both outcomes.
            compression: 1,
            checksum: 0,
        });
        offset = offset
            .checked_add(u64::from(stored))
            .ok_or_else(|| invalid("sample payload offset overflow"))?;
    }
    let encoded = if entries.is_empty() {
        0
    } else {
        encode(&entries, CHILD_LEAF)?.len()
    };
    encoded
        .checked_add(slack)
        .ok_or_else(|| invalid("sample compact page size overflow"))
}

/// Writes one entry in the form `index` requires.
fn encode_entry(
    out: &mut Vec<u8>,
    index: u64,
    previous: Option<&Tail>,
    entry: &ChildEntry,
) -> Result<()> {
    let bounds = entry.bounds;
    require(
        bounds.first <= bounds.last && bounds.min <= bounds.max,
        "reversed sample bounds",
    )?;
    require(
        entry.stored > 0 && entry.decoded > 0 && entry.compression <= 1,
        "invalid sample payload reference",
    )?;
    if entry.compression == 0 {
        require(entry.stored == entry.decoded, "raw samples length mismatch")?;
    }
    let restart = index.is_multiple_of(RESTART_INTERVAL);
    let tail = match restart {
        true => None,
        false => {
            Some(previous.ok_or_else(|| invalid("sample compact entry without a predecessor"))?)
        }
    };
    let start = out.len();
    out.push(0);
    let mut control = 0u8;
    if restart {
        put_u32(out, bounds.first.table_id);
        put_u64(out, bounds.first.tsid);
    } else {
        let tail = tail.ok_or_else(|| invalid("missing sample compact predecessor"))?;
        require(tail.last <= bounds.first, "unordered sample child ranges")?;
        if tail.last.table_id == bounds.first.table_id {
            // Zero is allowed: two children may start at the same identifier.
            put_varint(out, bounds.first.tsid - tail.last.tsid);
        } else {
            control |= FIRST_TABLE_CHANGED;
            put_varint(
                out,
                u64::from(bounds.first.table_id) - u64::from(tail.last.table_id),
            );
            put_varint(out, bounds.first.tsid);
        }
    }
    if bounds.last != bounds.first {
        control |= LAST_DIFFERS;
        if bounds.last.table_id == bounds.first.table_id {
            put_varint(out, bounds.last.tsid - bounds.first.tsid);
        } else {
            require(
                bounds.last.table_id > bounds.first.table_id,
                "reversed sample child range",
            )?;
            control |= LAST_TABLE_CHANGED;
            put_varint(
                out,
                u64::from(bounds.last.table_id) - u64::from(bounds.first.table_id),
            );
            put_varint(out, bounds.last.tsid);
        }
    }
    if let Some(tail) = tail {
        put_varint(out, zigzag(bounds.min.wrapping_sub(tail.min)));
    } else {
        put_u64(out, bounds.min as u64);
    }
    put_varint(out, span(bounds.min, bounds.max)?);
    if let Some(tail) = tail {
        if entry.offset != tail.end {
            require(
                entry.offset > tail.end,
                "unordered sample compact child pages",
            )?;
            control |= OFFSET_GAP;
            put_varint(out, entry.offset - tail.end);
        }
    } else {
        put_u64(out, entry.offset);
    }
    put_varint(out, u64::from(entry.stored));
    if entry.compression != 0 {
        control |= COMPRESSED;
        put_varint(out, u64::from(entry.decoded));
    }
    put_u64(out, entry.checksum);
    out[start] = control;
    Ok(())
}

/// Reads one entry, returning it with its successor state and the position of the next entry.
///
/// The rules a single entry can check on its own - the flags it may carry, the sign and limits of
/// every field, and the span - live here, so a page is either decodable or refused; relations
/// between siblings are checked by [`CompactPage::decode`].
fn decode_entry(
    bytes: &[u8],
    start: usize,
    stream: &Range<usize>,
    index: u64,
    previous: Option<Tail>,
) -> Result<(ChildEntry, Tail, usize)> {
    require(
        start >= stream.start && start < stream.end,
        "truncated sample compact entry",
    )?;
    let mut c = Cursor::new(&bytes[start..stream.end]);
    let control = c.u8()?;
    require(
        control & !KNOWN_FLAGS == 0,
        "unknown sample compact entry flag",
    )?;
    let restart = index.is_multiple_of(RESTART_INTERVAL);
    if restart {
        require(
            control & DEPENDENT_FLAGS == 0,
            "dependent sample compact restart entry",
        )?;
    }
    let last_differs = control & LAST_DIFFERS != 0;
    require(
        last_differs || control & LAST_TABLE_CHANGED == 0,
        "dependent sample compact last id",
    )?;
    let first = if restart {
        MetricSeriesId {
            table_id: c.u32()?,
            tsid: c.u64()?,
        }
    } else {
        let tail = previous.ok_or_else(|| invalid("sample compact entry without a predecessor"))?;
        if control & FIRST_TABLE_CHANGED == 0 {
            MetricSeriesId {
                table_id: tail.last.table_id,
                tsid: tsid(tail.last.tsid, varint(&mut c)?)?,
            }
        } else {
            MetricSeriesId {
                table_id: table_id(tail.last.table_id, varint(&mut c)?)?,
                tsid: varint(&mut c)?,
            }
        }
    };
    let last = if !last_differs {
        first
    } else if control & LAST_TABLE_CHANGED == 0 {
        MetricSeriesId {
            table_id: first.table_id,
            tsid: tsid(first.tsid, varint(&mut c)?)?,
        }
    } else {
        MetricSeriesId {
            table_id: table_id(first.table_id, varint(&mut c)?)?,
            tsid: varint(&mut c)?,
        }
    };
    require(
        first <= last && (first != last) == last_differs,
        "reversed or redundantly encoded sample child range",
    )?;
    let min = if restart {
        c.u64()? as i64
    } else {
        previous
            .ok_or_else(|| invalid("sample compact entry without a predecessor"))?
            .min
            .wrapping_add(unzigzag(varint(&mut c)?))
    };
    let max = i64::try_from(i128::from(min) + i128::from(varint(&mut c)?))
        .map_err(|_| invalid("sample compact span overflow"))?;
    let offset = if restart {
        c.u64()?
    } else {
        let tail = previous.ok_or_else(|| invalid("sample compact entry without a predecessor"))?;
        if control & OFFSET_GAP == 0 {
            tail.end
        } else {
            let gap = varint(&mut c)?;
            require(gap > 0, "redundant sample compact offset gap")?;
            tail.end
                .checked_add(gap)
                .ok_or_else(|| invalid("sample compact offset overflow"))?
        }
    };
    if let Some(tail) = previous {
        require(offset >= tail.end, "unordered sample compact child pages")?;
    }
    let stored = varint(&mut c)?;
    require(stored > 0, "empty sample payload")?;
    let stored = u32::try_from(stored).map_err(|_| invalid("sample payload exceeds limits"))?;
    let decoded = if control & COMPRESSED == 0 {
        stored
    } else {
        let decoded = varint(&mut c)?;
        require(decoded > 0, "empty sample payload")?;
        u32::try_from(decoded).map_err(|_| invalid("sample payload exceeds limits"))?
    };
    let checksum = c.u64()?;
    let next = stream.end - c.remaining();
    let entry = ChildEntry {
        bounds: Bounds {
            first,
            last,
            min,
            max,
        },
        offset,
        stored,
        decoded,
        checksum,
        compression: u16::from(control & COMPRESSED != 0),
    };
    Ok((
        entry,
        Tail {
            last,
            min,
            end: entry.end()?,
        },
        next,
    ))
}

/// Adds a checked delta to a table identifier.
fn table_id(base: u32, delta: u64) -> Result<u32> {
    u32::try_from(
        u64::from(base)
            .checked_add(delta)
            .ok_or_else(|| invalid("sample compact table id overflow"))?,
    )
    .map_err(|_| invalid("sample compact table id overflow"))
}

/// Adds a checked delta to a tsid.
fn tsid(base: u64, delta: u64) -> Result<u64> {
    base.checked_add(delta)
        .ok_or_else(|| invalid("sample compact tsid overflow"))
}

/// A validated compact internal page.
///
/// The encoded bytes are the page: they are validated once when the page is loaded and then
/// searched in place, so a page of thousands of children costs its own size and nothing per child.
#[derive(Debug)]
pub(super) struct CompactPage {
    bytes: Vec<u8>,
    count: u64,
    child_encoding: u16,
    stream: Range<usize>,
    /// Offsets of the entries that carry a restart form, relative to the entry stream.
    restarts: Vec<u32>,
    bounds: Bounds,
}

impl CompactPage {
    /// Decodes and fully validates one compact internal page.
    ///
    /// `reference` is the page's own reference in its parent, so its bounds are the aggregate the
    /// entries must derive, and its payload marks the end of the area every child lives in.
    pub(super) fn decode(
        bytes: Vec<u8>,
        reference: &PageReference,
        depth: u32,
        areas: Areas,
    ) -> Result<Self> {
        require(bytes.len() <= PAGE_LIMIT, "sample page exceeds limit")?;
        if depth != 2 {
            return Err(unsupported("samples compact page depth"));
        }
        let mut c = Cursor::new(&bytes);
        require(c.u32()? == KIND_INTERNAL, "samples metadata kind/depth")?;
        let count = c.u64()?;
        require(count > 0, "empty sample page")?;
        let child_encoding = c.u16()?;
        // Version 1 has two levels, so a compact page is the root and its children are leaves.
        require(
            child_encoding == CHILD_LEAF,
            "samples compact child encoding",
        )?;
        require(
            c.u8()? == RESTART_INTERVAL as u8,
            "samples compact restart interval",
        )?;
        require(c.u8()? == 0, "nonzero sample compact reserved field")?;
        let groups = usize::try_from(count.div_ceil(RESTART_INTERVAL))
            .map_err(|_| invalid("sample compact entry count overflow"))?;
        let array = groups
            .checked_mul(2)
            .ok_or_else(|| invalid("sample compact restart array overflow"))?;
        require(
            HEADER_SIZE + array < bytes.len(),
            "truncated sample compact restart array",
        )?;
        let stream = HEADER_SIZE..bytes.len() - array;
        require(
            count <= (stream.len() / MIN_ENTRY_BYTES) as u64,
            "sample compact entry count exceeds page",
        )?;
        let mut restarts = Vec::with_capacity(groups);
        let mut reader = Cursor::new(&bytes[stream.end..]);
        for _ in 0..groups {
            let offset = u32::from(reader.u16()?);
            match restarts.last() {
                None => require(
                    offset == 0,
                    "sample compact restart array must start at zero",
                )?,
                Some(previous) => require(
                    offset > *previous && (offset as usize) < stream.len(),
                    "unordered sample compact restart offset",
                )?,
            }
            restarts.push(offset);
        }
        let bounds = Self::validate(
            &bytes,
            count,
            child_encoding,
            &stream,
            &restarts,
            reference,
            areas,
        )?;
        require(
            bounds == reference.bounds,
            "samples page bounds disagree with parent",
        )?;
        Ok(Self {
            bytes,
            count,
            child_encoding,
            stream,
            restarts,
            bounds,
        })
    }

    /// Checks every entry and the relations between them, once per loaded page.
    #[allow(clippy::too_many_arguments)]
    fn validate(
        bytes: &[u8],
        count: u64,
        child_encoding: u16,
        stream: &Range<usize>,
        restarts: &[u32],
        reference: &PageReference,
        areas: Areas,
    ) -> Result<Bounds> {
        let mut bounds: Option<Bounds> = None;
        let mut previous: Option<Tail> = None;
        let mut position = stream.start;
        let mut end = areas.metadata_start;
        for index in 0..count {
            if index.is_multiple_of(RESTART_INTERVAL) {
                let expected =
                    stream.start + restarts[(index / RESTART_INTERVAL) as usize] as usize;
                require(
                    position == expected,
                    "sample compact restart offset does not name an entry start",
                )?;
            }
            let (entry, tail, next) = decode_entry(bytes, position, stream, index, previous)?;
            entry
                .payload(child_encoding)
                .validate(end, reference.payload.offset, true)?;
            end = entry.end()?;
            match bounds.as_mut() {
                Some(bounds) => {
                    require(
                        bounds.last <= entry.bounds.first,
                        "unordered sample compact child ranges",
                    )?;
                    bounds.extend(entry.bounds);
                }
                None => bounds = Some(entry.bounds),
            }
            previous = Some(tail);
            position = next;
        }
        require(position == stream.end, "trailing sample compact page bytes")?;
        bounds.ok_or_else(|| invalid("empty sample page"))
    }

    pub(super) fn bounds(&self) -> Bounds {
        self.bounds
    }

    pub(super) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The children this page names, for tests that pin its capacity and its search windows.
    #[cfg(test)]
    pub(super) fn count(&self) -> u64 {
        self.count
    }

    pub(super) fn retained_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.bytes.capacity()
            + self.restarts.capacity() * std::mem::size_of::<u32>()
    }

    /// Candidate entry ranges for the identifier intervals in `targets`, in file order.
    ///
    /// Ranges are merged, so a request decodes each entry at most once however many of its target
    /// identifiers a child can hold. An empty result means no child can own any target.
    pub(super) fn windows<I>(&self, targets: I) -> Result<Vec<Range<u64>>>
    where
        I: IntoIterator<Item = (MetricSeriesId, MetricSeriesId)>,
    {
        let mut ranges = Vec::new();
        for (first, last) in targets {
            if let Some(range) = self.search(first, last)? {
                ranges.push(range);
            }
        }
        ranges.sort_unstable_by_key(|range| range.start);
        let mut merged: Vec<Range<u64>> = Vec::with_capacity(ranges.len());
        for range in ranges {
            match merged.last_mut() {
                Some(open) if range.start <= open.end => open.end = open.end.max(range.end),
                _ => merged.push(range),
            }
        }
        Ok(merged)
    }

    /// The entries whose identifier range can hold one of `first..=last`.
    ///
    /// Children are ordered and disjoint, so the candidates are contiguous: every child before the
    /// first one whose range reaches `first` ends too early, and every child after the last one
    /// that starts too late begins past it. The restart first identifiers are binary-searched, one
    /// group is started before the result, and the scan then runs forward - which is also how a run
    /// of equal identifiers longer than a group is found.
    fn search(&self, first: MetricSeriesId, last: MetricSeriesId) -> Result<Option<Range<u64>>> {
        require(first <= last, "reversed sample compact target")?;
        let groups = self.restarts.len() as u64;
        let start = self.lower_bound(first)?.saturating_sub(1).min(groups - 1);
        let mut index = start * RESTART_INTERVAL;
        let mut position = self.stream.start + self.restarts[start as usize] as usize;
        let mut previous = None;
        let mut window: Option<Range<u64>> = None;
        while let Some((entry_index, entry)) = step(self, &mut position, &mut index, &mut previous)?
        {
            if entry.bounds.first > last {
                break;
            }
            if entry.bounds.last < first {
                continue;
            }
            match window.as_mut() {
                Some(window) => window.end = entry_index + 1,
                None => window = Some(entry_index..entry_index + 1),
            }
        }
        Ok(window)
    }

    /// The first restart group whose first identifier is not below `target`.
    fn lower_bound(&self, target: MetricSeriesId) -> Result<u64> {
        let groups = self.restarts.len() as u64;
        let (mut low, mut high) = (0, groups);
        while low < high {
            let middle = low + (high - low) / 2;
            if self.restart_first(middle)? < target {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        Ok(low)
    }

    /// The first identifier of one restart group, read from its fixed-width restart form.
    fn restart_first(&self, group: u64) -> Result<MetricSeriesId> {
        let start = self.stream.start
            + *self
                .restarts
                .get(group as usize)
                .ok_or_else(|| invalid("missing sample compact restart"))? as usize;
        let mut c = Cursor::new(&self.bytes[start..self.stream.end]);
        let control = c.u8()?;
        require(
            control & DEPENDENT_FLAGS == 0,
            "dependent sample compact restart entry",
        )?;
        Ok(MetricSeriesId {
            table_id: c.u32()?,
            tsid: c.u64()?,
        })
    }

    /// Every entry of this page, decoded in file order. Used by tests and by rewrites.
    #[cfg(test)]
    pub(super) fn entries(&self) -> Result<Vec<PageReference>> {
        let page = Arc::new(Self {
            bytes: self.bytes.clone(),
            count: self.count,
            child_encoding: self.child_encoding,
            stream: self.stream.clone(),
            restarts: self.restarts.clone(),
            bounds: self.bounds,
        });
        let mut entries = Entries::start(&page, 0)?;
        let mut out = Vec::new();
        while let Some((_, reference)) = entries.next()? {
            out.push(reference);
        }
        Ok(out)
    }
}

/// Cursor over a compact page's entries, resumed from the restart group that holds `index`.
pub(super) struct Entries {
    page: Arc<CompactPage>,
    position: usize,
    index: u64,
    previous: Option<Tail>,
}

impl Entries {
    /// Positions a cursor at `index` by resuming from its restart group.
    pub(super) fn start(page: &Arc<CompactPage>, index: u64) -> Result<Self> {
        require(index < page.count, "sample compact entry out of range")?;
        let group = index / RESTART_INTERVAL;
        let mut entries = Self {
            page: page.clone(),
            position: page.stream.start + page.restarts[group as usize] as usize,
            index: group * RESTART_INTERVAL,
            previous: None,
        };
        while entries.index < index {
            entries.next()?;
        }
        Ok(entries)
    }

    /// The next entry with its index, in file order.
    pub(super) fn next(&mut self) -> Result<Option<(u64, PageReference)>> {
        let page = self.page.clone();
        let Some((index, entry)) = step(
            &page,
            &mut self.position,
            &mut self.index,
            &mut self.previous,
        )?
        else {
            return Ok(None);
        };
        Ok(Some((index, entry.reference(page.child_encoding))))
    }
}

/// Advances one entry, or reports the end of the page.
fn step(
    page: &CompactPage,
    position: &mut usize,
    index: &mut u64,
    previous: &mut Option<Tail>,
) -> Result<Option<(u64, ChildEntry)>> {
    if *index >= page.count {
        return Ok(None);
    }
    let (entry, tail, next) =
        decode_entry(&page.bytes, *position, &page.stream, *index, *previous)?;
    let current = *index;
    *position = next;
    *previous = Some(tail);
    *index = index
        .checked_add(1)
        .ok_or_else(|| invalid("sample compact entry index overflow"))?;
    Ok(Some((current, entry)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cursor(bytes: &[u8]) -> Cursor<'_> {
        Cursor::new(bytes)
    }

    #[test]
    fn varint_widths_boundaries_and_minimality() {
        // Every 7-bit width transition, including the tenth byte of a `u64`.
        for &(value, width) in &[
            (0u64, 1usize),
            (1, 1),
            (127, 1),
            (128, 2),
            (16_383, 2),
            (16_384, 3),
            ((1 << 21) - 1, 3),
            (1 << 21, 4),
            ((1 << 28) - 1, 4),
            (1 << 28, 5),
            ((1 << 35) - 1, 5),
            (1 << 35, 6),
            ((1 << 42) - 1, 6),
            (1 << 42, 7),
            ((1 << 49) - 1, 7),
            (1 << 49, 8),
            ((1 << 56) - 1, 8),
            (1 << 56, 9),
            ((1 << 63) - 1, 9),
            (1 << 63, 10),
            (u64::MAX - 1, 10),
            (u64::MAX, 10),
        ] {
            let mut out = Vec::new();
            put_varint(&mut out, value);
            assert_eq!(width, out.len(), "width of {value}");
            assert_eq!(value, varint(&mut cursor(&out)).unwrap(), "round trip");
        }
        // A tenth byte above one cannot be a `u64`, and an eleventh byte is not one either.
        let mut overflow = vec![0xFF; 9];
        overflow.push(2);
        assert!(varint(&mut cursor(&overflow)).is_err());
        let mut long = vec![0xFF; 10];
        long.push(0);
        assert!(varint(&mut cursor(&long)).is_err());
        // Truncation at every byte of a maximal value.
        let mut maximal = Vec::new();
        put_varint(&mut maximal, u64::MAX);
        for length in 0..maximal.len() {
            assert!(
                varint(&mut cursor(&maximal[..length])).is_err(),
                "{length} bytes of a maximal varint must be truncated"
            );
        }
        // Non-minimal forms are refused by this codec...
        assert!(varint(&mut cursor(&[0x80, 0x00])).is_err());
        assert!(varint(&mut cursor(&[0xFF, 0x80, 0x00])).is_err());
        // ...without tightening the legacy decoder, which reads them.
        assert_eq!(0, Cursor::new(&[0x80, 0x00]).varint().unwrap());
        assert_eq!(127, Cursor::new(&[0xFF, 0x80, 0x00]).varint().unwrap());
    }

    #[test]
    fn zigzag_predictor_is_reversible_across_the_signed_range() {
        for delta in [0i64, 1, -1, i64::MAX, i64::MIN, i64::MIN + 1] {
            assert_eq!(delta, unzigzag(zigzag(delta)), "round trip of {delta}");
        }
        // The predictor is exact, including across the extremes, because it wraps.
        for previous in [i64::MIN, -1, 0, i64::MAX] {
            for next in [i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX] {
                let delta = next.wrapping_sub(previous);
                assert_eq!(next, previous.wrapping_add(unzigzag(zigzag(delta))));
            }
        }
        assert_eq!(u64::MAX, zigzag(i64::MIN));
        assert_eq!(0, span(i64::MIN, i64::MIN).unwrap());
        assert_eq!(u64::MAX, span(i64::MIN, i64::MAX).unwrap());
        assert!(span(0, -1).is_err());
    }
}
