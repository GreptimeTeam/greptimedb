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

//! Shared samples index tail. All offsets are relative to the index object.

use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::storage::FileId;

use super::codec::{Cursor, checksum, put_u32, put_u64};
use super::metadata::{PageReference, PayloadReference};
use super::{MANIFEST_LIMIT, Part, TimestampColumn, ValueColumn, invalid, require};
use crate::error::{Result, SerdeJsonSnafu};

const MAGIC: &[u8; 8] = b"GTSMPIDX";
const VERSION: u32 = 2;
pub(super) const FOOTER_SIZE: usize = 100;

/// Leaf directory records are independently checksummed for selective range reads.
pub(super) const ENTRY_SIZE: u64 = 88;

pub(super) struct LeafEntry {
    pub keys_start: u64,
    pub page: PageReference,
}

impl LeafEntry {
    pub fn keys(&self) -> super::metadata::KeyArea {
        super::metadata::KeyArea {
            start: self.keys_start,
            end: self.page.payload.offset,
        }
    }
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(ENTRY_SIZE as usize);
        put_u64(&mut bytes, self.keys_start);
        self.page.encode(&mut bytes);
        let digest = checksum(&bytes);
        put_u64(&mut bytes, digest);
        bytes
    }

    pub fn decode(bytes: &[u8], directory: u64) -> Result<Self> {
        let mut c = checked_record(bytes, ENTRY_SIZE as usize)?;
        let keys_start = c.u64()?;
        let page = PageReference::decode(&mut c)?;
        c.end()?;
        page.payload.validate(keys_start, directory, true)?;
        Ok(Self { keys_start, page })
    }
}

fn checked_record(bytes: &[u8], size: usize) -> Result<Cursor<'_>> {
    require(bytes.len() == size, "invalid samples index record size")?;
    let (body, digest) = bytes.split_at(size - 8);
    require(
        checksum(body) == Cursor::new(digest).u64()?,
        "samples index checksum mismatch",
    )?;
    Ok(Cursor::new(body))
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Footer {
    pub source: FileId,
    pub generation: FileId,
    pub directory: u64,
    pub leaves: u64,
    pub metadata: PayloadReference,
}

impl Footer {
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(FOOTER_SIZE);
        bytes.extend(MAGIC);
        put_u32(&mut bytes, VERSION);
        bytes.extend(self.source.as_bytes());
        bytes.extend(self.generation.as_bytes());
        put_u64(&mut bytes, self.directory);
        put_u64(&mut bytes, self.leaves);
        self.metadata.encode(&mut bytes);
        let digest = checksum(&bytes);
        put_u64(&mut bytes, digest);
        bytes
    }

    pub fn decode(bytes: &[u8], file_size: u64) -> Result<Self> {
        let mut c = checked_record(bytes, FOOTER_SIZE)?;
        require(
            c.take(8)? == MAGIC && c.u32()? == VERSION,
            "unsupported samples index",
        )?;
        let mut id = || -> Result<FileId> {
            let mut id = [0; 16];
            id.copy_from_slice(c.take(16)?);
            Ok(FileId::from_bytes(id))
        };
        let source = id()?;
        let generation = id()?;
        let directory = c.u64()?;
        let leaves = c.u64()?;
        let metadata = PayloadReference::decode(&mut c)?;
        c.end()?;
        require(
            leaves
                .checked_mul(ENTRY_SIZE)
                .and_then(|len| directory.checked_add(len))
                == Some(metadata.offset)
                && metadata.end()?.checked_add(FOOTER_SIZE as u64) == Some(file_size)
                && metadata.encoding == 1
                && metadata.compression <= 1
                && metadata.stored > 0
                && metadata.stored <= metadata.decoded
                && metadata.decoded as usize <= MANIFEST_LIMIT
                && (metadata.compression != 0 || metadata.stored == metadata.decoded),
            "invalid samples index section bounds",
        )?;
        Ok(Self {
            source,
            generation,
            directory,
            leaves,
            metadata,
        })
    }
}

#[derive(Serialize, Deserialize)]
pub(super) struct Schema {
    pub timestamp: TimestampColumn,
    pub fields: Vec<ValueColumn>,
}

pub(super) fn encode_metadata(
    schema: &Schema,
    parts: &[Part],
    offset: u64,
) -> Result<(PayloadReference, Vec<u8>)> {
    let mut bytes = Vec::new();
    for (key, value) in [
        (
            b"schema".as_slice(),
            serde_json::to_vec(schema).context(SerdeJsonSnafu)?,
        ),
        (
            b"parts".as_slice(),
            serde_json::to_vec(parts).context(SerdeJsonSnafu)?,
        ),
    ] {
        require(
            bytes.len() + key.len() + value.len() + 8 <= MANIFEST_LIMIT,
            "samples metadata exceeds limit",
        )?;
        put_u32(&mut bytes, key.len() as u32);
        put_u32(&mut bytes, value.len() as u32);
        bytes.extend(key);
        bytes.extend(value);
    }
    let decoded = bytes.len() as u32;
    let compressed = zstd::bulk::compress(&bytes, 3)
        .map_err(|e| invalid(format!("compressing samples metadata: {e}")))?;
    let (compression, bytes) = if compressed.len() < bytes.len() {
        (1, compressed)
    } else {
        (0, bytes)
    };
    Ok((
        PayloadReference {
            offset,
            stored: bytes.len() as u32,
            decoded,
            checksum: checksum(&bytes),
            encoding: 1,
            compression,
        },
        bytes,
    ))
}

pub(super) fn decode_metadata(
    reference: &PayloadReference,
    stored: &[u8],
) -> Result<(Schema, Vec<Part>)> {
    let bytes = reference.unpack(stored)?;
    let mut c = Cursor::new(&bytes);
    let mut schema = None;
    let mut parts = None;
    while c.remaining() > 0 {
        let key_len = c.u32()? as usize;
        let value_len = c.u32()? as usize;
        let key = c.take(key_len)?;
        let value = c.take(value_len)?;
        match key {
            b"schema" => {
                require(schema.is_none(), "duplicate samples schema")?;
                schema = Some(serde_json::from_slice(value).context(SerdeJsonSnafu)?);
            }
            b"parts" => {
                require(parts.is_none(), "duplicate samples inventory")?;
                parts = Some(serde_json::from_slice(value).context(SerdeJsonSnafu)?);
            }
            _ => {}
        }
    }
    Ok((
        schema.ok_or_else(|| invalid("missing samples schema"))?,
        parts.ok_or_else(|| invalid("missing samples inventory"))?,
    ))
}
