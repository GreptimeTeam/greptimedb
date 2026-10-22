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

//! Explicit sample framing and bounded two-level metadata pages.

use crate::error::Result;
use crate::series_index::samples::codec::{self, Cursor, put_u32, put_u64};
use crate::series_index::samples::{
    Manifest, MetricSeriesId, VERSION, invalid, require, unsupported, valid_rows,
};

pub(super) const MAGIC: &[u8; 8] = b"GTSAMPLE";
pub(super) const HEADER_SIZE: u64 = 16;
pub(super) const TRAILER_SIZE: u64 = 112;
pub(super) const PAGE_LIMIT: usize = 65536;
pub(super) const ROOT_CAPACITY: usize = (PAGE_LIMIT - 12) / 72;
const CHUNK_LIMIT: usize = 1024 * 1024;

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
    fn encode(&self, out: &mut Vec<u8>) {
        put_u64(out, self.offset);
        put_u32(out, self.stored);
        put_u32(out, self.decoded);
        put_u64(out, self.checksum);
        out.extend(self.encoding.to_le_bytes());
        out.extend(self.compression.to_le_bytes());
        put_u32(out, 0);
    }
    fn decode(c: &mut Cursor<'_>) -> Result<Self> {
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
    pub fn unpack(&self, bytes: &[u8]) -> Result<Vec<u8>> {
        require(
            bytes.len() == self.stored as usize && codec::checksum(bytes) == self.checksum,
            "sample checksum mismatch",
        )?;
        codec::decompress(bytes, self.compression, self.decoded as usize)
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
    reference.validate(offset, reference.end()?, page)?;
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
    fn encode(&self, out: &mut Vec<u8>) {
        self.bounds.encode(out);
        self.payload.encode(out);
    }
    fn decode(c: &mut Cursor<'_>) -> Result<Self> {
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
#[derive(Debug, Clone)]
pub(super) struct Block {
    pub id: MetricSeriesId,
    pub min: i64,
    pub max: i64,
    pub ordinal: u64,
    pub rows: u32,
    pub columns: Vec<Column>,
}
impl Block {
    pub fn bounds(&self) -> Bounds {
        Bounds {
            first: self.id,
            last: self.id,
            min: self.min,
            max: self.max,
        }
    }
    pub fn size(&self) -> usize {
        44 + 44 * self.columns.len()
    }
    pub fn encode(&self, out: &mut Vec<u8>) {
        put_u32(out, self.id.table_id);
        put_u64(out, self.id.tsid);
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
    fn decode(
        c: &mut Cursor<'_>,
        manifest: &Manifest,
        payload_end: u64,
        previous_end: &mut u64,
    ) -> Result<Self> {
        let id = MetricSeriesId {
            table_id: c.u32()?,
            tsid: c.u64()?,
        };
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
            payload.validate(*previous_end, payload_end, false)?;
            *previous_end = payload.end()?;
            columns.push(Column {
                role,
                id: column_id,
                payload,
                nulls,
            });
        }
        Ok(Self {
            id,
            min,
            max,
            ordinal,
            rows,
            columns,
        })
    }
    pub fn follows(&self, previous: &Self, adjacent: bool) -> Result<()> {
        require(previous.id <= self.id, "unordered sample identifiers")?;
        if self.id == previous.id {
            require(
                previous.max <= self.min && previous.ordinal < self.ordinal,
                "unordered sample blocks",
            )?;
            if adjacent {
                require(
                    previous.ordinal.checked_add(1) == Some(self.ordinal),
                    "nonconsecutive sample ordinal",
                )?;
            }
        } else if adjacent {
            require(self.ordinal == 0, "first sample block has nonzero ordinal")?;
        }
        Ok(())
    }
}
#[derive(Debug)]
pub(super) enum Page {
    Leaf(Vec<Block>),
    Internal(Vec<PageReference>),
}
impl Page {
    pub fn bounds(&self) -> Result<Bounds> {
        let bounds: Vec<_> = match self {
            Self::Leaf(v) => v.iter().map(Block::bounds).collect(),
            Self::Internal(v) => v.iter().map(|c| c.bounds).collect(),
        };
        let mut result = *bounds.first().ok_or_else(|| invalid("empty sample page"))?;
        for bound in &bounds[1..] {
            result.extend(*bound);
        }
        Ok(result)
    }
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            Self::Leaf(v) => {
                put_u32(&mut out, 0);
                put_u64(&mut out, v.len() as u64);
                for block in v {
                    block.encode(&mut out);
                }
            }
            Self::Internal(v) => {
                put_u32(&mut out, 1);
                put_u64(&mut out, v.len() as u64);
                for child in v {
                    child.encode(&mut out);
                }
            }
        }
        out
    }
    pub fn decode(
        bytes: &[u8],
        reference: &PageReference,
        depth: u32,
        payload_end: u64,
        manifest: &Manifest,
    ) -> Result<Self> {
        require(bytes.len() <= PAGE_LIMIT, "sample page exceeds limit")?;
        let mut c = Cursor::new(bytes);
        let kind = c.u32()?;
        let count = c.length()?;
        require(count > 0, "empty sample page")?;
        let page = match (kind, depth) {
            (0, 1) => {
                require(
                    count <= c.remaining() / 176,
                    "sample block count exceeds page",
                )?;
                let mut blocks: Vec<Block> = Vec::with_capacity(count);
                let mut end = HEADER_SIZE;
                for _ in 0..count {
                    let block = Block::decode(&mut c, manifest, payload_end, &mut end)?;
                    if let Some(previous) = blocks.last() {
                        block.follows(previous, true)?;
                    }
                    blocks.push(block);
                }
                Self::Leaf(blocks)
            }
            (1, 2) => {
                require(
                    count <= ROOT_CAPACITY && count <= c.remaining() / 72,
                    "sample root exceeds capacity",
                )?;
                let mut children: Vec<PageReference> = Vec::with_capacity(count);
                let mut end = payload_end;
                for _ in 0..count {
                    let child = PageReference::decode(&mut c)?;
                    child
                        .payload
                        .validate(end, reference.payload.offset, true)?;
                    end = child.payload.end()?;
                    if let Some(previous) = children.last() {
                        require(
                            previous.bounds.last <= child.bounds.first,
                            "unordered sample child ranges",
                        )?;
                    }
                    children.push(child);
                }
                Self::Internal(children)
            }
            _ => return Err(unsupported("samples metadata kind/depth")),
        };
        c.end()?;
        require(
            page.bounds()? == reference.bounds,
            "samples page bounds disagree with parent",
        )?;
        Ok(page)
    }
}
pub(super) fn header() -> Vec<u8> {
    let mut out = MAGIC.to_vec();
    put_u32(&mut out, VERSION);
    put_u32(&mut out, 0);
    out
}
pub(super) fn validate_header(bytes: &[u8]) -> Result<()> {
    let mut c = Cursor::new(bytes);
    require(c.take(8)? == MAGIC, "sample header magic")?;
    if c.u32()? != VERSION {
        return Err(unsupported("samples file version"));
    }
    require(c.u32()? == 0, "nonzero sample header reserved field")?;
    c.end()
}
pub(super) struct Trailer {
    pub root: PageReference,
    pub depth: u32,
    pub file_length: u64,
    pub payload_end: u64,
}
impl Trailer {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.root.encode(&mut out);
        put_u32(&mut out, self.depth);
        put_u32(&mut out, VERSION);
        put_u64(&mut out, self.file_length);
        put_u64(&mut out, self.payload_end);
        out.extend(MAGIC);
        let sum = codec::checksum(&out);
        put_u64(&mut out, sum);
        out
    }
    pub fn decode(bytes: &[u8], file_length: u64) -> Result<Self> {
        require(
            bytes.len() == TRAILER_SIZE as usize,
            "truncated sample trailer",
        )?;
        let mut c = Cursor::new(bytes);
        let root = PageReference::decode(&mut c)?;
        let depth = c.u32()?;
        let version = c.u32()?;
        if !(1..=2).contains(&depth) || version != VERSION {
            return Err(unsupported("sample trailer version/depth"));
        }
        let length = c.u64()?;
        let payload_end = c.u64()?;
        require(
            c.take(8)? == MAGIC && c.u64()? == codec::checksum(&bytes[..104]),
            "sample trailer checksum/magic",
        )?;
        require(
            length == file_length
                && payload_end >= HEADER_SIZE
                && file_length >= HEADER_SIZE + TRAILER_SIZE,
            "invalid sample framing",
        )?;
        root.payload
            .validate(payload_end, file_length - TRAILER_SIZE, true)?;
        require(
            root.payload.end()? == file_length - TRAILER_SIZE,
            "sample root must end at trailer",
        )?;
        Ok(Self {
            root,
            depth,
            file_length,
            payload_end,
        })
    }
}
