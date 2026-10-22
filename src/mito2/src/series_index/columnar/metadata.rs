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

//! Explicit little-endian framing and the two-level metadata index.

use crate::error::Result;
use crate::series_index::columnar::codec::{
    self, Compression, Cursor, put_string, put_u32, put_u64,
};
use crate::series_index::columnar::{VERSION, invalid, require, unsupported};
use crate::series_index::{MetricSeriesId, ROW_COUNT_COLUMN, TSID_COLUMN};

pub(crate) const MAGIC: &[u8; 8] = b"GTSERIES";
pub(crate) const HEADER_SIZE: u64 = 16;
pub(crate) const TRAILER_SIZE: u64 = 96;
pub(crate) const PAGE_TARGET: usize = 65536;
pub(crate) const PAGE_LIMIT: usize = 8 * 1024 * 1024;
pub(crate) const PAGE_STORED_LIMIT: usize = PAGE_LIMIT + PAGE_TARGET;
pub(crate) const ROOT_CAPACITY: usize = (PAGE_TARGET - 12) / 56;

#[derive(Debug, Clone, Copy)]
pub(crate) struct PayloadReference {
    pub(crate) offset: u64,
    pub(crate) stored: u32,
    pub(crate) decoded: u32,
    pub(crate) checksum: u64,
    pub(crate) encoding: u16,
    pub(crate) compression: u16,
}
impl PayloadReference {
    pub(crate) fn end(&self) -> Result<u64> {
        self.offset
            .checked_add(u64::from(self.stored))
            .ok_or_else(|| invalid("series offset overflow"))
    }
    pub(crate) fn validate(&self, start: u64, end: u64, page: bool) -> Result<()> {
        let (decoded_limit, stored_limit) = if page {
            (PAGE_LIMIT, PAGE_STORED_LIMIT)
        } else {
            (codec::COLUMN_LIMIT, codec::COLUMN_STORED_LIMIT)
        };
        require(
            self.stored > 0
                && self.decoded > 0
                && self.stored as usize <= stored_limit
                && self.decoded as usize <= decoded_limit,
            "series payload exceeds size limits",
        )?;
        require(
            self.offset >= start && self.end()? <= end,
            "series payload outside its area",
        )
    }
    pub(crate) fn encode(&self, out: &mut Vec<u8>) {
        put_u64(out, self.offset);
        put_u32(out, self.stored);
        put_u32(out, self.decoded);
        put_u64(out, self.checksum);
        out.extend(self.encoding.to_le_bytes());
        out.extend(self.compression.to_le_bytes());
        put_u32(out, 0);
    }
    pub(crate) fn decode(c: &mut Cursor<'_>) -> Result<Self> {
        let reference = Self {
            offset: c.u64()?,
            stored: c.u32()?,
            decoded: c.u32()?,
            checksum: c.u64()?,
            encoding: c.u16()?,
            compression: c.u16()?,
        };
        require(c.u32()? == 0, "nonzero series reference reserved field")?;
        Ok(reference)
    }
    pub(crate) fn unpack(&self, bytes: &[u8]) -> Result<Vec<u8>> {
        require(
            bytes.len() == self.stored as usize && codec::checksum(bytes) == self.checksum,
            "series payload checksum mismatch",
        )?;
        codec::decompress(bytes, self.compression, self.decoded as usize)
    }
}

pub(crate) fn pack(
    bytes: &[u8],
    offset: u64,
    encoding: u16,
    compression: Compression,
    level: i32,
    page: bool,
) -> Result<(PayloadReference, Vec<u8>)> {
    let limit = if page {
        PAGE_LIMIT
    } else {
        codec::COLUMN_LIMIT
    };
    require(
        !bytes.is_empty() && bytes.len() <= limit,
        "series decoded payload exceeds size limit",
    )?;
    let stored = codec::compress(bytes, compression, level)?;
    let reference = PayloadReference {
        offset,
        stored: u32::try_from(stored.len())
            .map_err(|_| invalid("series stored length overflow"))?,
        decoded: bytes.len() as u32,
        checksum: codec::checksum(&stored),
        encoding,
        compression: compression as u16,
    };
    reference.validate(offset, reference.end()?, page)?;
    Ok((reference, stored))
}

#[derive(Debug, Clone)]
pub(crate) struct PageReference {
    pub(crate) first: MetricSeriesId,
    pub(crate) last: MetricSeriesId,
    pub(crate) payload: PayloadReference,
}
impl PageReference {
    pub(crate) fn encode(&self, out: &mut Vec<u8>) {
        put_u32(out, self.first.table_id);
        put_u64(out, self.first.tsid);
        put_u32(out, self.last.table_id);
        put_u64(out, self.last.tsid);
        self.payload.encode(out);
    }
    pub(crate) fn decode(c: &mut Cursor<'_>) -> Result<Self> {
        let first = MetricSeriesId {
            table_id: c.u32()?,
            tsid: c.u64()?,
        };
        let last = MetricSeriesId {
            table_id: c.u32()?,
            tsid: c.u64()?,
        };
        require(first <= last, "reversed series page bounds")?;
        Ok(Self {
            first,
            last,
            payload: PayloadReference::decode(c)?,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Column {
    pub(crate) name: String,
    pub(crate) payload: PayloadReference,
    pub(crate) null_count: u32,
    pub(crate) bounds: Option<(String, String)>,
}
#[derive(Debug, Clone)]
pub(crate) struct Block {
    pub(crate) table_id: u32,
    pub(crate) first: u64,
    pub(crate) last: u64,
    pub(crate) rows: u32,
    pub(crate) columns: Vec<Column>,
}
impl Block {
    pub(crate) fn first_id(&self) -> MetricSeriesId {
        MetricSeriesId {
            table_id: self.table_id,
            tsid: self.first,
        }
    }
    pub(crate) fn last_id(&self) -> MetricSeriesId {
        MetricSeriesId {
            table_id: self.table_id,
            tsid: self.last,
        }
    }
    pub(crate) fn encode(&self, out: &mut Vec<u8>) {
        put_u32(out, self.table_id);
        put_u64(out, self.first);
        put_u64(out, self.last);
        put_u32(out, self.rows);
        put_u32(out, self.columns.len() as u32);
        for column in &self.columns {
            put_string(out, &column.name);
            column.payload.encode(out);
            put_u32(out, column.null_count);
            if let Some((min, max)) = &column.bounds {
                put_u32(out, 1);
                put_string(out, min);
                put_string(out, max);
            } else {
                put_u32(out, 0);
            }
        }
    }
    fn decode(
        c: &mut Cursor<'_>,
        names: &[String],
        payload_end: u64,
        previous_end: &mut u64,
    ) -> Result<Self> {
        let table_id = c.u32()?;
        let first = c.u64()?;
        let last = c.u64()?;
        let rows = c.u32()?;
        let count = c.u32()? as usize;
        require(
            rows > 0
                && rows as usize <= codec::COLUMN_LIMIT / 8
                && first <= last
                && u64::from(rows - 1) <= last - first,
            "invalid series block bounds/count",
        )?;
        require(
            count >= 2 && count <= names.len() && count <= c.remaining() / 48,
            "invalid series column count",
        )?;
        let mut columns = Vec::with_capacity(count);
        let mut next_name = 0;
        for index in 0..count {
            let name = c.string()?;
            let position = names[next_name..]
                .iter()
                .position(|n| *n == name)
                .map(|p| p + next_name)
                .ok_or_else(|| invalid("unknown or unordered series column"))?;
            require(
                index >= 2 || position == index,
                "missing required series column",
            )?;
            next_name = position + 1;
            let payload = PayloadReference::decode(c)?;
            payload.validate(*previous_end, payload_end, false)?;
            *previous_end = payload.end()?;
            let null_count = c.u32()?;
            require(null_count <= rows, "invalid series null count")?;
            let bounds = match c.u32()? {
                0 => None,
                1 => {
                    let min = c.string()?;
                    let max = c.string()?;
                    require(min <= max, "reversed string bounds")?;
                    Some((min, max))
                }
                _ => return Err(unsupported("series statistics kind")),
            };
            if name == TSID_COLUMN || name == ROW_COUNT_COLUMN {
                require(
                    null_count == 0 && bounds.is_none(),
                    "invalid integer column statistics",
                )?;
            } else {
                require(
                    bounds.is_some() == (null_count < rows),
                    "missing or invalid string statistics",
                )?;
            }
            columns.push(Column {
                name,
                payload,
                null_count,
                bounds,
            });
        }
        require(
            columns.iter().take(2).all(|column| {
                column.payload.encoding != codec::INTEGER || rows <= column.payload.decoded
            }),
            "series row count exceeds integer payload lengths",
        )?;
        Ok(Self {
            table_id,
            first,
            last,
            rows,
            columns,
        })
    }
}

#[derive(Debug)]
pub(crate) enum Page {
    Leaf(Vec<Block>),
    Internal(Vec<PageReference>),
}
impl Page {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            Self::Leaf(blocks) => {
                put_u32(&mut out, 0);
                put_u64(&mut out, blocks.len() as u64);
                for block in blocks {
                    block.encode(&mut out);
                }
            }
            Self::Internal(children) => {
                put_u32(&mut out, 1);
                put_u64(&mut out, children.len() as u64);
                for child in children {
                    child.encode(&mut out);
                }
            }
        }
        out
    }
    pub(crate) fn decode(
        bytes: &[u8],
        reference: &PageReference,
        depth: u32,
        payload_end: u64,
        names: &[String],
    ) -> Result<Self> {
        let mut c = Cursor::new(bytes);
        let kind = c.u32()?;
        let count = c.length()?;
        require(count > 0, "empty series metadata page")?;
        let (page, first, last) = match (kind, depth) {
            (0, 1) => {
                require(
                    count <= c.remaining() / 28,
                    "series block count exceeds page",
                )?;
                let mut blocks: Vec<Block> = Vec::new();
                let mut end = HEADER_SIZE;
                for _ in 0..count {
                    let block = Block::decode(&mut c, names, payload_end, &mut end)?;
                    if let Some(previous) = blocks.last() {
                        require(
                            previous.last_id() < block.first_id(),
                            "unordered series blocks",
                        )?;
                    }
                    blocks.push(block);
                }
                let first = blocks[0].first_id();
                let last = blocks[count - 1].last_id();
                (Self::Leaf(blocks), first, last)
            }
            (1, 2) => {
                require(
                    bytes.len() <= PAGE_TARGET
                        && count <= ROOT_CAPACITY
                        && count <= c.remaining() / 56,
                    "series root exceeds capacity",
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
                        require(previous.last < child.first, "unordered series child ranges")?;
                    }
                    children.push(child);
                }
                let first = children[0].first;
                let last = children[count - 1].last;
                (Self::Internal(children), first, last)
            }
            _ => return Err(unsupported("series metadata kind/depth")),
        };
        c.end()?;
        require(
            first == reference.first && last == reference.last,
            "series page bounds disagree with parent",
        )?;
        Ok(page)
    }
}

pub(crate) fn header() -> Vec<u8> {
    let mut out = MAGIC.to_vec();
    put_u32(&mut out, VERSION);
    put_u32(&mut out, 0);
    out
}
pub(crate) fn validate_header(bytes: &[u8]) -> Result<()> {
    let mut c = Cursor::new(bytes);
    require(c.take(8)? == MAGIC, "invalid series header magic")?;
    if c.u32()? != VERSION {
        return Err(unsupported("series file version"));
    }
    require(c.u32()? == 0, "nonzero series header reserved field")?;
    c.end()
}

pub(crate) struct Trailer {
    pub(crate) root: PageReference,
    pub(crate) depth: u32,
    pub(crate) file_length: u64,
    pub(crate) payload_end: u64,
}
impl Trailer {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.root.encode(&mut out);
        put_u32(&mut out, self.depth);
        put_u32(&mut out, VERSION);
        put_u64(&mut out, self.file_length);
        put_u64(&mut out, self.payload_end);
        out.extend(MAGIC);
        let checksum = codec::checksum(&out);
        put_u64(&mut out, checksum);
        out
    }
    pub(crate) fn decode(bytes: &[u8], file_length: u64) -> Result<Self> {
        require(
            bytes.len() == TRAILER_SIZE as usize,
            "truncated series trailer",
        )?;
        let mut c = Cursor::new(bytes);
        let root = PageReference::decode(&mut c)?;
        let depth = c.u32()?;
        let version = c.u32()?;
        if !(1..=2).contains(&depth) || version != VERSION {
            return Err(unsupported("series trailer version/depth"));
        }
        let length = c.u64()?;
        let payload_end = c.u64()?;
        require(
            c.take(8)? == MAGIC && c.u64()? == codec::checksum(&bytes[..88]),
            "series trailer checksum/magic mismatch",
        )?;
        require(
            length == file_length
                && payload_end >= HEADER_SIZE
                && file_length >= HEADER_SIZE + TRAILER_SIZE,
            "invalid series file framing",
        )?;
        root.payload
            .validate(payload_end, file_length - TRAILER_SIZE, true)?;
        require(
            root.payload.end()? == file_length - TRAILER_SIZE,
            "series root does not end at trailer",
        )?;
        Ok(Self {
            root,
            depth,
            file_length,
            payload_end,
        })
    }
}
