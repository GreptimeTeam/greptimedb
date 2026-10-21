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

//! Bounded, explicit wire codecs. Encoding and compression have independent IDs.

use std::collections::HashMap;
use std::io::Read;

use crate::error::Result;
use crate::series_index::columnar::{invalid, require, unsupported};

pub(crate) const COLUMN_LIMIT: usize = 64 * 1024 * 1024;
pub(crate) const COLUMN_STORED_LIMIT: usize = COLUMN_LIMIT + 1024 * 1024;
pub(crate) const INTEGER: u16 = 1;
pub(crate) const DICTIONARY: u16 = 2;

#[derive(Debug, Clone, Copy)]
#[repr(u16)]
pub(crate) enum Compression {
    None = 0,
    Zstd = 1,
}

pub(crate) fn checksum(bytes: &[u8]) -> u64 {
    twox_hash::XxHash64::oneshot(0, bytes)
}

pub(crate) fn compress(bytes: &[u8], compression: Compression, level: i32) -> Result<Vec<u8>> {
    match compression {
        Compression::None => Ok(bytes.to_vec()),
        Compression::Zstd => zstd::stream::encode_all(bytes, level)
            .map_err(|e| invalid(format!("series compression: {e}"))),
    }
}

pub(crate) fn decompress(bytes: &[u8], compression: u16, decoded: usize) -> Result<Vec<u8>> {
    let output = match compression {
        0 => bytes.to_vec(),
        1 => {
            let decoder = zstd::stream::read::Decoder::new(bytes)
                .map_err(|e| invalid(format!("series zstd header: {e}")))?;
            let mut output = Vec::new();
            // Never let a frame's claimed size control allocation or output growth.
            decoder
                .take(decoded as u64 + 1)
                .read_to_end(&mut output)
                .map_err(|e| invalid(format!("series decompression: {e}")))?;
            output
        }
        id => return Err(unsupported(format!("series compression {id}"))),
    };
    require(output.len() == decoded, "series decoded length mismatch")?;
    Ok(output)
}

pub(crate) fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend(value.to_le_bytes());
}
pub(crate) fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend(value.to_le_bytes());
}
pub(crate) fn put_string(out: &mut Vec<u8>, value: &str) {
    put_u64(out, value.len() as u64);
    out.extend(value.as_bytes());
}
pub(crate) fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 128 {
        out.push(value as u8 | 128);
        value >>= 7;
    }
    out.push(value as u8);
}

pub(crate) struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> Cursor<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
    pub(crate) fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }
    pub(crate) fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(count)
            .ok_or_else(|| invalid("series length overflow"))?;
        let result = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| invalid("truncated series payload"))?;
        self.offset = end;
        Ok(result)
    }
    pub(crate) fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    pub(crate) fn u16(&mut self) -> Result<u16> {
        let b = self.take(2)?;
        Ok(u16::from_le_bytes([b[0], b[1]]))
    }
    pub(crate) fn u32(&mut self) -> Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }
    pub(crate) fn u64(&mut self) -> Result<u64> {
        let b = self.take(8)?;
        Ok(u64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }
    pub(crate) fn length(&mut self) -> Result<usize> {
        usize::try_from(self.u64()?).map_err(|_| invalid("series length exceeds address space"))
    }
    pub(crate) fn string(&mut self) -> Result<String> {
        let len = self.length()?;
        std::str::from_utf8(self.take(len)?)
            .map(str::to_owned)
            .map_err(|_| invalid("invalid series UTF-8"))
    }
    pub(crate) fn varint(&mut self) -> Result<u64> {
        let mut value = 0;
        for shift in (0..70).step_by(7) {
            let byte = self.u8()?;
            require(shift != 63 || byte <= 1, "series varint overflow")?;
            value |= u64::from(byte & 127) << shift;
            if byte & 128 == 0 {
                return Ok(value);
            }
        }
        Err(invalid("series varint overflow"))
    }
    pub(crate) fn end(self) -> Result<()> {
        require(self.remaining() == 0, "trailing series payload bytes")
    }
}

pub(crate) fn encode_integers(values: &[u64]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut previous = 0u64;
    for &value in values {
        let delta = value.wrapping_sub(previous) as i64;
        put_varint(&mut out, ((delta as u64) << 1) ^ ((delta >> 63) as u64));
        previous = value;
    }
    out
}

pub(crate) fn decode_integers(bytes: &[u8], count: usize) -> Result<Vec<u64>> {
    require(count <= bytes.len(), "integer count exceeds payload")?;
    let mut cursor = Cursor::new(bytes);
    let mut out = Vec::with_capacity(count);
    let mut previous = 0u64;
    for _ in 0..count {
        let value = cursor.varint()?;
        let delta = (value >> 1) ^ (0u64.wrapping_sub(value & 1));
        previous = previous.wrapping_add(delta);
        out.push(previous);
    }
    cursor.end()?;
    Ok(out)
}

#[derive(Debug)]
pub(crate) struct Dictionary {
    pub(crate) values: Vec<Option<String>>,
    pub(crate) codes: Vec<u32>,
}

pub(crate) fn encode_dictionary(values: &[Option<String>]) -> Result<Vec<u8>> {
    let mut lookup: HashMap<&str, u32> = HashMap::new();
    let mut dictionary = Vec::new();
    put_u64(&mut dictionary, 0); // patched with the entry count below
    dictionary.push(0); // slot zero is always null
    let mut codes = Vec::new();
    for value in values {
        let code = match value {
            None => 0,
            Some(value) => {
                if let Some(code) = lookup.get(value.as_str()) {
                    *code
                } else {
                    let code = u32::try_from(lookup.len() + 1)
                        .map_err(|_| invalid("too many dictionary entries"))?;
                    lookup.insert(value, code);
                    dictionary.push(1);
                    put_string(&mut dictionary, value);
                    code
                }
            }
        };
        put_varint(&mut codes, u64::from(code));
        require(
            dictionary.len() + codes.len() <= COLUMN_LIMIT,
            "series dictionary exceeds chunk limit",
        )?;
    }
    dictionary[..8].copy_from_slice(&((lookup.len() + 1) as u64).to_le_bytes());
    let mut out = Vec::new();
    put_varint(&mut out, dictionary.len() as u64);
    out.extend(dictionary);
    out.extend(codes);
    Ok(out)
}

pub(crate) fn decode_dictionary(bytes: &[u8], count: usize) -> Result<Dictionary> {
    let mut cursor = Cursor::new(bytes);
    let length =
        usize::try_from(cursor.varint()?).map_err(|_| invalid("dictionary length overflow"))?;
    let mut dictionary = Cursor::new(cursor.take(length)?);
    let entries = dictionary.length()?;
    require(
        entries > 0 && entries <= dictionary.remaining(),
        "invalid dictionary count",
    )?;
    require(
        count <= cursor.remaining(),
        "dictionary code count exceeds payload",
    )?;
    let mut values = Vec::new();
    let mut unique = std::collections::HashSet::new();
    for index in 0..entries {
        let present = dictionary.u8()?;
        if index == 0 {
            require(present == 0, "dictionary slot zero must be null")?;
            values.push(None);
        } else {
            require(present == 1, "nonzero dictionary slot must be present")?;
            let value = dictionary.string()?;
            require(unique.insert(value.clone()), "duplicate dictionary value")?;
            values.push(Some(value));
        }
    }
    dictionary.end()?;
    let mut codes = Vec::with_capacity(count);
    for _ in 0..count {
        let code = cursor.varint()?;
        require(
            code < entries as u64 && code <= u32::MAX as u64,
            "invalid dictionary code",
        )?;
        codes.push(code as u32);
    }
    cursor.end()?;
    Ok(Dictionary { values, codes })
}
