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

//! Lossless, bounded sample column encodings. Nulls never become NaNs.

use crate::error::Result;
pub(super) use crate::series_index::columnar::codec::{
    Cursor, checksum, decompress, put_u32, put_u64, put_varint,
};
use crate::series_index::samples::{MAX_ROWS, require, unsupported};

fn zig(x: i64) -> u64 {
    ((x as u64) << 1) ^ ((x >> 63) as u64)
}
fn unzig(x: u64) -> u64 {
    (x >> 1) ^ (0u64.wrapping_sub(x & 1))
}

pub(super) fn encode_time(values: &[i64]) -> (u16, Vec<u8>) {
    let mut out = values[0].to_le_bytes().to_vec();
    let step = if values.len() == 1 {
        Some(0)
    } else {
        values[1].checked_sub(values[0])
    };
    if let Some(step) = step.filter(|&step| {
        step >= 0
            && values
                .windows(2)
                .all(|v| v[1].checked_sub(v[0]) == Some(step))
    }) {
        put_u64(&mut out, step as u64);
        (1, out)
    } else {
        let mut prior = 0u64;
        for (i, pair) in values.windows(2).enumerate() {
            let delta = (pair[1] as u64).wrapping_sub(pair[0] as u64);
            put_varint(
                &mut out,
                zig(if i == 0 {
                    delta
                } else {
                    delta.wrapping_sub(prior)
                } as i64),
            );
            prior = delta;
        }
        (2, out)
    }
}
pub(super) fn encode_sequence(values: &[u64]) -> (u16, Vec<u8>) {
    let mut out = values[0].to_le_bytes().to_vec();
    if values.iter().all(|v| *v == values[0]) {
        return (3, out);
    }
    for pair in values.windows(2) {
        put_varint(&mut out, zig(pair[1].wrapping_sub(pair[0]) as i64));
    }
    (4, out)
}
pub(super) fn decode_numbers(bytes: &[u8], encoding: u16, rows: usize) -> Result<Vec<u64>> {
    require(
        (1..=MAX_ROWS).contains(&rows),
        "invalid samples number count",
    )?;
    if !(1..=4).contains(&encoding) {
        return Err(unsupported("samples number encoding"));
    }
    let mut c = Cursor::new(bytes);
    let first = c.u64()?;
    let mut values = Vec::with_capacity(rows);
    values.push(first);
    if encoding == 1 {
        let step = c.u64()? as i64;
        require(
            step >= 0 && (rows != 1 || step == 0),
            "invalid sample timestamp step",
        )?;
        let mut last = first as i64;
        for _ in 1..rows {
            last = last
                .checked_add(step)
                .ok_or_else(|| super::invalid("sample timestamp overflow"))?;
            values.push(last as u64);
        }
    } else if encoding == 3 {
        values.resize(rows, first);
    } else {
        require(
            rows - 1 <= c.remaining(),
            "sample number count exceeds bytes",
        )?;
        let mut previous = first;
        let mut delta = 0u64;
        for i in 1..rows {
            let next = unzig(c.varint()?);
            delta = if encoding == 4 || i == 1 {
                next
            } else {
                delta.wrapping_add(next)
            };
            previous = previous.wrapping_add(delta);
            values.push(previous);
        }
    }
    c.end()?;
    if encoding <= 2 {
        require(
            values.windows(2).all(|v| (v[0] as i64) <= v[1] as i64),
            "unordered sample timestamps",
        )?;
    }
    Ok(values)
}

pub(super) fn encode_float(values: &[Option<u64>]) -> Vec<u8> {
    let nullable = values.iter().any(Option::is_none);
    let mut out = vec![u8::from(nullable)];
    if nullable {
        out.resize(1 + values.len().div_ceil(8), 0);
        for (i, value) in values.iter().enumerate() {
            if value.is_some() {
                out[1 + i / 8] |= 1 << (i % 8);
            }
        }
    }
    let mut previous = None;
    for &value in values.iter().flatten() {
        if let Some(last) = previous {
            put_varint(&mut out, value ^ last);
        } else {
            put_u64(&mut out, value);
        }
        previous = Some(value);
    }
    out
}
pub(super) fn decode_float(bytes: &[u8], rows: usize, nulls: u32) -> Result<Vec<Option<u64>>> {
    require(
        (1..=MAX_ROWS).contains(&rows) && (nulls as usize) < rows,
        "invalid samples float count",
    )?;
    let mut c = Cursor::new(bytes);
    let bitmap = match c.u8()? {
        0 => {
            require(nulls == 0, "samples float null count mismatch")?;
            None
        }
        1 => {
            require(nulls > 0, "redundant samples validity bitmap")?;
            let bitmap = c.take(rows.div_ceil(8))?;
            if !rows.is_multiple_of(8) {
                require(
                    bitmap[bitmap.len() - 1] >> (rows % 8) == 0,
                    "nonzero sample validity padding",
                )?;
            }
            let present: u32 = bitmap.iter().map(|b| b.count_ones()).sum();
            require(
                present as usize + nulls as usize == rows,
                "samples validity count mismatch",
            )?;
            Some(bitmap)
        }
        _ => return Err(unsupported("samples validity kind")),
    };
    let first = c.u64()?;
    require(
        rows - nulls as usize - 1 <= c.remaining(),
        "sample float count exceeds bytes",
    )?;
    let mut previous = None;
    let mut out = Vec::with_capacity(rows);
    for i in 0..rows {
        if bitmap.is_none_or(|b| b[i / 8] & (1 << (i % 8)) != 0) {
            let value = match previous {
                None => first,
                Some(last) => last ^ c.varint()?,
            };
            out.push(Some(value));
            previous = Some(value);
        } else {
            out.push(None);
        }
    }
    c.end()?;
    Ok(out)
}
pub(super) fn encode_operations(values: &[u8]) -> (u16, Vec<u8>) {
    if values.iter().all(|v| *v == values[0]) {
        (6, vec![values[0]])
    } else {
        (7, values.to_vec())
    }
}
pub(super) fn decode_operations(bytes: &[u8], encoding: u16, rows: usize) -> Result<Vec<u8>> {
    require(
        (1..=MAX_ROWS).contains(&rows),
        "invalid samples operation count",
    )?;
    let values = match encoding {
        6 => {
            require(bytes.len() == 1, "invalid constant sample operation")?;
            vec![bytes[0]; rows]
        }
        7 => {
            require(bytes.len() == rows, "invalid sample operation length")?;
            bytes.to_vec()
        }
        _ => return Err(unsupported("samples operation encoding")),
    };
    require(values.iter().all(|v| *v <= 1), "unknown sample operation")?;
    Ok(values)
}
