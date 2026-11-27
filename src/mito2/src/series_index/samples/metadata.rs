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

//! Bounded descriptor leaves and references into the shared index and payload parts.

use crate::error::Result;
use crate::series_index::samples::codec::{self, Cursor, put_u32, put_u64};
use crate::series_index::samples::{
    MetricSeriesId, SamplesMetadata, invalid, require, unsupported, valid_rows,
};

pub(super) const PAGE_LIMIT: usize = 65536;
/// Largest decoded block payload, and therefore the largest frame a decoder has to decompress.
const CHUNK_LIMIT: usize = 1024 * 1024;
/// Fixed part of a block descriptor, before its 44-byte column descriptors.
///
/// The exact series key is not part of it. A descriptor keeps a fixed-size [`KeyRef`] into
/// the leaf's key area instead, so descriptor size is independent of key length and a key
/// longer than a metadata page is representable.
pub(super) const BLOCK_FIXED_BYTES: usize = 60;
/// Smallest possible block descriptor: the fixed part plus its three mandatory columns.
const MIN_BLOCK_BYTES: usize = BLOCK_FIXED_BYTES + 3 * 44;

/// Descriptor bytes for a block with `columns` column descriptors.
///
/// Independent of the key's length: the descriptor references the key, it does not hold it.
/// This is the one place the descriptor size formula lives, for the writer's page planning
/// and for readers that inspect decoded pages.
pub(super) fn descriptor_bytes(columns: usize) -> usize {
    BLOCK_FIXED_BYTES + 44 * columns
}

#[derive(Debug, Clone, Copy)]
pub(super) struct PayloadReference {
    pub offset: u64,
    pub stored: u32,
    pub decoded: u32,
    pub checksum: u64,
    pub encoding: u16,
    pub compression: u16,
}
impl PayloadReference {
    pub fn end(&self) -> Result<u64> {
        self.offset
            .checked_add(u64::from(self.stored))
            .ok_or_else(|| invalid("sample payload offset overflow"))
    }
    pub fn validate(&self, start: u64, end: u64, page: bool) -> Result<()> {
        let limit = if page { PAGE_LIMIT } else { CHUNK_LIMIT };
        require(
            self.stored > 0
                && self.decoded > 0
                && self.decoded as usize <= limit
                && self.stored as usize <= limit + 65536,
            "sample payload exceeds limits",
        )?;
        require(
            self.offset >= start && self.end()? <= end,
            "sample payload outside area",
        )?;
        if self.compression == 0 {
            require(self.stored == self.decoded, "raw samples length mismatch")?;
        }
        Ok(())
    }
    pub(super) fn encode(&self, out: &mut Vec<u8>) {
        put_u64(out, self.offset);
        put_u32(out, self.stored);
        put_u32(out, self.decoded);
        put_u64(out, self.checksum);
        out.extend(self.encoding.to_le_bytes());
        out.extend(self.compression.to_le_bytes());
        put_u32(out, 0);
    }
    pub(super) fn decode(c: &mut Cursor<'_>) -> Result<Self> {
        let result = Self {
            offset: c.u64()?,
            stored: c.u32()?,
            decoded: c.u32()?,
            checksum: c.u64()?,
            encoding: c.u16()?,
            compression: c.u16()?,
        };
        require(c.u32()? == 0, "nonzero sample payload reserved field")?;
        Ok(result)
    }
    fn validate_payload(&self, bytes: &[u8]) -> Result<()> {
        require(
            bytes.len() == self.stored as usize && codec::checksum(bytes) == self.checksum,
            "sample checksum mismatch",
        )
    }
    pub fn unpack(&self, bytes: &[u8]) -> Result<Vec<u8>> {
        self.validate_payload(bytes)?;
        codec::decompress(bytes, self.compression, self.decoded as usize)
    }
    pub fn unpack_with_decoder<'a>(
        &self,
        bytes: &'a [u8],
        decoder: &'a mut codec::PayloadDecoder,
    ) -> Result<&'a [u8]> {
        self.validate_payload(bytes)?;
        decoder.decompress(bytes, self.compression, self.decoded as usize)
    }
}
pub(super) fn pack(
    bytes: &[u8],
    offset: u64,
    encoding: u16,
    page: bool,
) -> Result<(PayloadReference, Vec<u8>)> {
    require(
        !bytes.is_empty() && bytes.len() <= if page { PAGE_LIMIT } else { CHUNK_LIMIT },
        "sample encoded payload exceeds limit",
    )?;
    pack_bytes(bytes, offset, encoding)
}

/// Compresses a size-checked payload and records the checksum of its stored representation.
pub(super) fn pack_bytes(
    bytes: &[u8],
    offset: u64,
    encoding: u16,
) -> Result<(PayloadReference, Vec<u8>)> {
    let compressed =
        zstd::bulk::compress(bytes, 3).map_err(|e| invalid(format!("sample compression: {e}")))?;
    let (compression, stored) = if compressed.len() < bytes.len() {
        (1, compressed)
    } else {
        (0, bytes.to_vec())
    };
    let reference = PayloadReference {
        offset,
        stored: stored.len() as u32,
        decoded: bytes.len() as u32,
        checksum: codec::checksum(&stored),
        encoding,
        compression,
    };

    Ok((reference, stored))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Bounds {
    pub first: MetricSeriesId,
    pub last: MetricSeriesId,
    pub min: i64,
    pub max: i64,
}
impl Bounds {
    pub fn validate(&self) -> Result<()> {
        require(
            self.first <= self.last && self.min <= self.max,
            "reversed sample bounds",
        )
    }
    pub fn extend(&mut self, next: Self) {
        self.last = next.last;
        self.min = self.min.min(next.min);
        self.max = self.max.max(next.max);
    }
    fn encode(&self, out: &mut Vec<u8>) {
        for id in [self.first, self.last] {
            put_u32(out, id.table_id);
            put_u64(out, id.tsid);
        }
        put_u64(out, self.min as u64);
        put_u64(out, self.max as u64);
    }
    fn decode(c: &mut Cursor<'_>) -> Result<Self> {
        let result = Self {
            first: MetricSeriesId {
                table_id: c.u32()?,
                tsid: c.u64()?,
            },
            last: MetricSeriesId {
                table_id: c.u32()?,
                tsid: c.u64()?,
            },
            min: c.u64()? as i64,
            max: c.u64()? as i64,
        };
        result.validate()?;
        Ok(result)
    }
}
#[derive(Debug, Clone)]
pub(super) struct PageReference {
    pub bounds: Bounds,
    pub payload: PayloadReference,
}
impl PageReference {
    pub(super) fn encode(&self, out: &mut Vec<u8>) {
        self.bounds.encode(out);
        self.payload.encode(out);
    }
    pub(super) fn decode(c: &mut Cursor<'_>) -> Result<Self> {
        let reference = Self {
            bounds: Bounds::decode(c)?,
            payload: PayloadReference::decode(c)?,
        };
        if reference.payload.encoding != 1 || reference.payload.compression > 1 {
            return Err(unsupported("samples metadata codec"));
        }
        Ok(reference)
    }
}
#[derive(Debug, Clone)]
pub(super) struct Column {
    pub role: u32,
    pub id: u32,
    pub payload: PayloadReference,
    pub nulls: u32,
}
/// The location of one exact series key inside a leaf's key area.
///
/// This reference is the descriptor's whole identity. The key bytes themselves are stored
/// once per series in the leaf's key area, outside every bounded metadata page, so a key may
/// be longer than a page and the descriptor stays 12 bytes whatever its length is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct KeyRef {
    /// Absolute offset of the first key byte from the start of the part.
    pub offset: u64,
    /// Key length in bytes; nonzero.
    pub length: u32,
}

impl KeyRef {
    pub fn end(&self) -> Result<u64> {
        self.offset
            .checked_add(u64::from(self.length))
            .ok_or_else(|| invalid("sample key offset overflow"))
    }

    /// End of this key's stored entry: its bytes followed by its checksum.
    ///
    /// `length` covers the key only, so the stored checksum is exactly [`KEY_CHECKSUM_BYTES`]
    /// past [`Self::end`] and never part of what the reference itself names.
    pub fn checksum_end(&self) -> Result<u64> {
        self.end()?
            .checked_add(KEY_CHECKSUM_BYTES)
            .ok_or_else(|| invalid("sample key offset overflow"))
    }

    fn encode(&self, out: &mut Vec<u8>) {
        put_u64(out, self.offset);
        put_u32(out, self.length);
    }

    fn decode(c: &mut Cursor<'_>) -> Result<Self> {
        Ok(Self {
            offset: c.u64()?,
            length: c.u32()?,
        })
    }

    /// Requires this reference to name a nonempty key entry inside `area`.
    ///
    /// The entry includes the stored checksum, so a reference whose key ends inside the area but
    /// whose checksum would not is rejected like any other out-of-area reference.
    pub fn validate(&self, area: KeyArea) -> Result<()> {
        require(
            self.length > 0 && self.offset >= area.start && self.checksum_end()? <= area.end,
            "sample key reference outside key area",
        )
    }
}

/// Bytes of the XXH64 stored after each key, seed zero.
///
/// A key copy is `[key bytes][checksum]`; the descriptor's reference names only the key, so a
/// fetched key can be verified before it is used to attribute rows to a series. Without it a
/// flipped label byte would silently move rows to another series, which the old inline-key layout
/// could not suffer: its keys sat in a page whose stored bytes were already checksummed.
pub(super) const KEY_CHECKSUM_BYTES: u64 = std::mem::size_of::<u64>() as u64;

/// A leaf's key area: exact series keys stored back to back, outside every metadata page.
///
/// Keys are appended in block order and are never page-bounded, so their length is bounded by
/// the index, not by `PAGE_LIMIT`. Every copy is followed by its checksum. Because blocks are
/// ordered by exact key, every block of one series is adjacent and shares a single copy of the
/// key, which makes an equal [`KeyRef`] mean "the same exact series".
#[derive(Debug, Clone, Copy)]
pub(super) struct KeyArea {
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Clone)]
pub(super) struct Block {
    pub part_id: u32,
    /// Coarse `(table_id, tsid)` prefix decoded from the block's exact key.
    ///
    /// It prunes candidates but never names the owner: `(table_id, tsid)` is a hash, so two
    /// distinct series can share it. The owner is the key [`Self::key`] references.
    pub id: MetricSeriesId,
    /// Fixed-size reference to the block's exact key in the leaf's key area.
    ///
    /// Equal references mean the same exact series; a different series that collides on the
    /// identifier always carries a different reference.
    pub key: KeyRef,
    pub min: i64,
    pub max: i64,
    pub ordinal: u64,
    pub rows: u32,
    pub columns: Vec<Column>,
}
/// The ordering boundary retained between pages and leaves, without column descriptors.
///
/// A [`KeyRef`] is an offset inside one leaf's key area, so two references are comparable only
/// when they belong to the same leaf; `leaf` carries that scope in the boundary itself so no
/// caller can compare references across a leaf boundary by accident. Two leaves hold unrelated
/// key areas, which is why a series that spans leaves is recognised by [`BlockOrder::continues`]
/// on the exact key bytes instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BlockOrder {
    leaf: u64,
    id: MetricSeriesId,
    key: KeyRef,
    min: i64,
    max: i64,
    ordinal: u64,
}

impl BlockOrder {
    /// The leaf this block belongs to.
    pub(crate) fn leaf(&self) -> u64 {
        self.leaf
    }

    /// The coarse `(table_id, tsid)` prefix, which is ordered across leaves as well.
    pub(crate) fn id(&self) -> MetricSeriesId {
        self.id
    }

    /// The block's key reference, a location inside [`Self::leaf`]'s key area.
    pub(crate) fn key(&self) -> KeyRef {
        self.key
    }

    /// Checks the next block of the *same* leaf against this boundary.
    ///
    /// Ownership and ordering of a leaf are decided on the exact `(table_id, tsid)` and on the
    /// key reference only: two blocks of one leaf that share one reference share one key copy,
    /// so they are one exact series, while a different series always has its own copy, placed
    /// after the previous one and restarting the ordinal. Only descriptor fields are read, so a
    /// block this request prunes is never fetched and its key content is not validated.
    pub(crate) fn precedes(&self, next: &Self, adjacent: bool) -> Result<()> {
        require(
            self.leaf == next.leaf,
            "sample key references compared across leaves",
        )?;
        require(self.id <= next.id, "unordered sample series keys")?;
        if next.key == self.key {
            self.continues(next, adjacent)?;
        } else {
            // A different series must place its one key copy after the previous series' copy.
            // Overlap, or a second copy of a key that is still current, is a corrupt reference
            // rather than a silent second read of the same bytes.
            require(
                self.key.checksum_end()? <= next.key.offset,
                "overlapping sample key references",
            )?;
            if adjacent {
                require(next.ordinal == 0, "first sample block has nonzero ordinal")?;
            }
        }
        Ok(())
    }

    /// Requires `next` to continue one exact series in time and ordinal.
    ///
    /// Equal exact key bytes are what make two blocks the same series across a leaf boundary,
    /// where their references belong to different key areas. A repeated key that restarts its
    /// ordinal or goes back in time is a corrupt leaf, not a second series.
    pub(crate) fn continues(&self, next: &Self, adjacent: bool) -> Result<()> {
        require(
            self.max <= next.min && self.ordinal < next.ordinal,
            "unordered sample blocks",
        )?;
        if adjacent {
            require(
                self.ordinal.checked_add(1) == Some(next.ordinal),
                "nonconsecutive sample ordinal",
            )?;
        }
        Ok(())
    }
}

impl Block {
    pub fn payload_end(&self) -> Result<u64> {
        self.columns
            .last()
            .ok_or_else(|| invalid("sample block has no columns"))?
            .payload
            .end()
    }

    /// The ordering boundary this block contributes, scoped to its leaf.
    pub(crate) fn order(&self, leaf: u64) -> BlockOrder {
        BlockOrder {
            leaf,
            id: self.id,
            key: self.key,
            min: self.min,
            max: self.max,
            ordinal: self.ordinal,
        }
    }

    /// Checks this block against the previous block of the same page.
    ///
    /// A page owns one key area, so both references name one key area and are comparable; a block
    /// in another leaf is compared through [`BlockOrder::continues`] and its exact key instead.
    fn follows_in_page(&self, previous: &Self, adjacent: bool) -> Result<()> {
        // One scope for both sides: a page belongs to exactly one leaf, and no reference built
        // here outlives the page.
        const PAGE_SCOPE: u64 = 0;
        previous
            .order(PAGE_SCOPE)
            .precedes(&self.order(PAGE_SCOPE), adjacent)
    }

    /// The coarse bounds used for pruning. Exact ownership lives in `key`.
    pub fn bounds(&self) -> Bounds {
        Bounds {
            first: self.id,
            last: self.id,
            min: self.min,
            max: self.max,
        }
    }
    pub fn encode(&self, out: &mut Vec<u8>) {
        put_u32(out, self.part_id);
        put_u32(out, self.id.table_id);
        put_u64(out, self.id.tsid);
        self.key.encode(out);
        put_u64(out, self.min as u64);
        put_u64(out, self.max as u64);
        put_u64(out, self.ordinal);
        put_u32(out, self.rows);
        put_u32(out, self.columns.len() as u32);
        for column in &self.columns {
            put_u32(out, column.role);
            put_u32(out, column.id);
            column.payload.encode(out);
            put_u32(out, column.nulls);
        }
    }
    fn decode(c: &mut Cursor<'_>, manifest: &SamplesMetadata, keys: KeyArea) -> Result<Self> {
        let part_id = c.u32()?;
        let part = manifest
            .files
            .get(part_id as usize)
            .ok_or_else(|| invalid("unknown samples payload part"))?;
        let id = MetricSeriesId {
            table_id: c.u32()?,
            tsid: c.u64()?,
        };
        // Only the key's location is decoded here. The bytes stay in the key area, so a
        // descriptor never grows with the key and a request that prunes this block never
        // reads it; the reader validates the reference and its `(table_id, tsid)` prefix
        // when it fetches the key for a block it actually selects.
        let key = KeyRef::decode(c)?;
        key.validate(keys)?;
        let min = c.u64()? as i64;
        let max = c.u64()? as i64;
        let ordinal = c.u64()?;
        let rows = c.u32()?;
        let count = c.u32()? as usize;
        require(
            min <= max && valid_rows(rows as usize, manifest.fields.len()),
            "invalid sample block bounds/count",
        )?;
        require(
            count >= 3 && count <= manifest.fields.len() + 3 && count <= c.remaining() / 44,
            "invalid sample column count",
        )?;
        let mut columns = Vec::with_capacity(count);
        let mut next_field = 0;
        let mut previous_end = 0;
        for index in 0..count {
            let role = c.u32()?;
            let column_id = c.u32()?;
            let payload = PayloadReference::decode(c)?;
            let nulls = c.u32()?;
            if index < 3 {
                require(
                    role == index as u32 + 1
                        && column_id
                            == if index == 0 {
                                manifest.timestamp.column_id
                            } else {
                                0
                            }
                        && nulls == 0,
                    "invalid mandatory sample column",
                )?;
            } else {
                require(
                    role == 4 && nulls < rows,
                    "invalid sample field role/null count",
                )?;
                let position = manifest.fields[next_field..]
                    .iter()
                    .position(|f| f.column_id == column_id)
                    .ok_or_else(|| invalid("unknown or unordered sample field"))?
                    + next_field;
                next_field = position + 1;
            }
            if payload.encoding <= 7 {
                let valid = match role {
                    1 => matches!(payload.encoding, 1 | 2),
                    2 => matches!(payload.encoding, 3 | 4),
                    3 => matches!(payload.encoding, 6 | 7),
                    4 => payload.encoding == 5,
                    _ => false,
                };
                require(valid, "sample codec role mismatch")?;
            }
            payload.validate(previous_end, part.file_size, false)?;
            previous_end = payload.end()?;
            columns.push(Column {
                role,
                id: column_id,
                payload,
                nulls,
            });
        }
        Ok(Self {
            part_id,
            id,
            key,
            min,
            max,
            ordinal,
            rows,
            columns,
        })
    }
}
#[derive(Debug)]
pub(super) struct Page(pub Vec<Block>);

impl Page {
    pub(crate) fn retained_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.0.capacity() * std::mem::size_of::<Block>()
            + self
                .0
                .iter()
                .map(|b| b.columns.capacity() * std::mem::size_of::<Column>())
                .sum::<usize>()
    }
    pub fn bounds(&self) -> Result<Bounds> {
        self.0
            .iter()
            .map(Block::bounds)
            .reduce(|mut bounds, next| {
                bounds.extend(next);
                bounds
            })
            .ok_or_else(|| invalid("empty samples leaf"))
    }
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_u32(&mut out, 0);
        put_u64(&mut out, self.0.len() as u64);
        for block in &self.0 {
            block.encode(&mut out);
        }
        out
    }
    pub fn decode(
        bytes: &[u8],
        reference: &PageReference,
        keys: KeyArea,
        manifest: &SamplesMetadata,
    ) -> Result<Self> {
        require(bytes.len() <= PAGE_LIMIT, "sample leaf exceeds limit")?;
        let mut c = Cursor::new(bytes);
        require(c.u32()? == 0, "unknown samples leaf encoding")?;
        let count = c.length()?;
        require(
            count > 0 && count <= c.remaining() / MIN_BLOCK_BYTES,
            "invalid sample leaf block count",
        )?;
        let mut blocks: Vec<Block> = Vec::with_capacity(count);
        for _ in 0..count {
            let block = Block::decode(&mut c, manifest, keys)?;
            if let Some(previous) = blocks.last() {
                block.follows_in_page(previous, true)?;
                require(
                    previous.part_id <= block.part_id,
                    "unordered sample part references",
                )?;
                if previous.part_id == block.part_id {
                    require(
                        previous.payload_end()? <= block.columns[0].payload.offset,
                        "overlapping sample blocks",
                    )?;
                }
            }
            blocks.push(block);
        }
        c.end()?;
        let page = Self(blocks);
        require(
            page.bounds()? == reference.bounds,
            "sample leaf bounds disagree with directory",
        )?;
        Ok(page)
    }
}
