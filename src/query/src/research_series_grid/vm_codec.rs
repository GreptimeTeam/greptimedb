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

//! Decoder for the experiment's format-2 envelope, not VM's decimal value format.

pub struct Decoder {
    zstd: zstd::bulk::Decompressor<'static>,
    scratch: Vec<u8>,
    pub timestamps: Vec<i64>,
    pub value_bits: Vec<i64>,
}

impl Decoder {
    pub fn new() -> Result<Self, String> {
        let mut zstd = zstd::bulk::Decompressor::new().map_err(|e| e.to_string())?;
        zstd.set_parameter(zstd::zstd_safe::DParameter::WindowLogMax(23))
            .map_err(|e| e.to_string())?;
        Ok(Self {
            zstd,
            scratch: vec![0; 8192 * 10],
            timestamps: Vec::new(),
            value_bits: Vec::new(),
        })
    }

    pub fn decode(&mut self, data: &[u8], count: usize) -> Result<(), String> {
        self.timestamps.clear();
        self.value_bits.clear();
        if !(1..=8192).contains(&count) || data.len() < 22 || data.len() > 22 + 20 * 8192 {
            return Err("Invalid format-2 envelope".into());
        }
        let first_time =
            i64::from_le_bytes(data[2..10].try_into().map_err(|_| "Invalid time header")?);
        let first_value = i64::from_le_bytes(
            data[10..18]
                .try_into()
                .map_err(|_| "Invalid value header")?,
        );
        let time_size =
            u32::from_le_bytes(data[18..22].try_into().map_err(|_| "Invalid size header")?)
                as usize;
        if time_size > data.len() - 22 {
            return Err("Invalid timestamp payload size".into());
        }
        decode_column(
            &mut self.zstd,
            &mut self.scratch,
            &data[22..22 + time_size],
            data[0],
            first_time,
            count,
            &mut self.timestamps,
        )?;
        decode_column(
            &mut self.zstd,
            &mut self.scratch,
            &data[22 + time_size..],
            data[1],
            first_value,
            count,
            &mut self.value_bits,
        )?;
        Ok(())
    }
}

fn decode_column(
    decoder: &mut zstd::bulk::Decompressor<'_>,
    scratch: &mut [u8],
    data: &[u8],
    kind: u8,
    first: i64,
    count: usize,
    output: &mut Vec<i64>,
) -> Result<(), String> {
    let (kind, data) = if kind == 1 || kind == 4 {
        let size = decoder
            .decompress_to_buffer(data, &mut scratch[..count * 10])
            .map_err(|e| e.to_string())?;
        (if kind == 1 { 5 } else { 6 }, &scratch[..size])
    } else {
        (kind, data)
    };
    decode_integers(data, kind, first, count, output).map_err(str::to_string)
}

fn varint(data: &mut &[u8]) -> Result<i64, &'static str> {
    let mut value = 0_u64;
    for shift in (0..70).step_by(7) {
        let (&byte, tail) = data.split_first().ok_or("Truncated varint")?;
        *data = tail;
        if shift == 63 && byte > 1 {
            return Err("Overflowing varint");
        }
        value |= u64::from(byte & 127) << shift;
        if byte < 128 {
            return Ok(((value >> 1) as i64) ^ -((value & 1) as i64));
        }
    }
    Err("Overflowing varint")
}

fn decode_integers(
    mut data: &[u8],
    kind: u8,
    first: i64,
    count: usize,
    output: &mut Vec<i64>,
) -> Result<(), &'static str> {
    if count == 0 || count > 8192 || !matches!(kind, 2 | 3 | 5 | 6) {
        return Err("Invalid integer stream");
    }
    output.clear();
    output.reserve(count);
    let mut value = first;
    match kind {
        3 => output.resize(count, first),
        2 => {
            let delta = varint(&mut data)?;
            for _ in 0..count {
                output.push(value);
                value = value.wrapping_add(delta);
            }
        }
        5 | 6 => {
            if kind == 5 && count < 2 {
                return Err("Delta2 requires two samples");
            }
            output.push(value);
            let mut delta = 0_i64;
            for index in 1..count {
                let next = varint(&mut data)?;
                delta = if kind == 6 || index == 1 {
                    next
                } else {
                    delta.wrapping_add(next)
                };
                // The codec carries IEEE bits through signed integers; wrapping is intentional.
                value = value.wrapping_add(delta);
                output.push(value);
            }
        }
        _ => return Err("Invalid integer stream"),
    }
    if !data.is_empty() {
        return Err("Trailing integer data");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_and_delta2_match_known_wire_values() {
        let mut output = Vec::new();
        decode_integers(&[2, 1, 4], 6, 10, 4, &mut output).unwrap();
        assert_eq!(output, [10, 11, 10, 12]);
        decode_integers(&[2, 2, 1], 5, 10, 4, &mut output).unwrap();
        assert_eq!(output, [10, 11, 13, 14]);
        decode_integers(&[2], 2, i64::MAX, 3, &mut output).unwrap();
        assert_eq!(output, [i64::MAX, i64::MIN, i64::MIN + 1]);
        decode_integers(&[], 3, 0x7ff0000000000002, 2, &mut output).unwrap();
        assert_eq!(output, [0x7ff0000000000002; 2]);
    }

    #[test]
    fn rejects_truncation_overflow_and_trailing_bytes() {
        let mut output = Vec::new();
        for data in [&[128][..], &[255; 10][..], &[2, 0][..]] {
            assert!(decode_integers(data, 6, 0, 2, &mut output).is_err());
        }
        assert!(decode_integers(&[0], 3, 0, 1, &mut output).is_err());
        assert!(decode_integers(&[], 5, 0, 1, &mut output).is_err());
        let mut minus_one = &[1][..];
        assert_eq!(varint(&mut minus_one), Ok(-1));
        let mut minimum = &[255, 255, 255, 255, 255, 255, 255, 255, 255, 1][..];
        assert_eq!(varint(&mut minimum), Ok(i64::MIN));
    }

    #[test]
    fn envelope_preserves_signed_zero_and_bounds_decompression() {
        let mut decoder = Decoder::new().unwrap();
        let mut envelope = vec![2, 3];
        envelope.extend_from_slice(&10_i64.to_le_bytes());
        envelope.extend_from_slice(&(-0.0_f64).to_bits().to_le_bytes());
        envelope.extend_from_slice(&1_u32.to_le_bytes());
        envelope.push(2);
        decoder.decode(&envelope, 3).unwrap();
        assert_eq!(decoder.timestamps, [10, 11, 12]);
        assert_eq!(decoder.value_bits, [i64::MIN; 3]);
        assert!(decoder.decode(&envelope[..21], 3).is_err());
        envelope[0] = 4;
        let compressed = zstd::bulk::compress(&[0; 1024], 1).unwrap();
        envelope[18..22].copy_from_slice(&(compressed.len() as u32).to_le_bytes());
        envelope.truncate(22);
        envelope.extend_from_slice(&compressed);
        assert!(decoder.decode(&envelope, 3).is_err());
        let compressed = zstd::bulk::compress(&[2, 2], 1).unwrap();
        envelope[18..22].copy_from_slice(&(compressed.len() as u32).to_le_bytes());
        envelope.truncate(22);
        envelope.extend_from_slice(&compressed);
        decoder.decode(&envelope, 3).unwrap();
        assert_eq!(decoder.timestamps, [10, 11, 12]);
    }
}
