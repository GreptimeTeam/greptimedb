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

//! Compatibility decoder for UDDSketch states written before the canonical v1 format.

use std::collections::{HashMap, HashSet};

use bincode::Options;
use serde::Deserialize;
use uddsketch::{UddSketch, UddSketchRef};

const MAX_BYTES: usize = 64 * 1024 * 1024;
const MAX_BUCKETS: usize = 1_000_000;
const HEADER_LEN: usize = 48;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq)]
enum LegacyBucketKey {
    Negative(i64),
    Zero,
    Positive(i64),
    Invalid,
}

#[derive(Debug, Deserialize)]
struct LegacyBucket {
    count: u64,
    next: LegacyBucketKey,
}

#[derive(Debug, Deserialize)]
struct LegacyBucketStore {
    map: HashMap<LegacyBucketKey, LegacyBucket>,
    head: LegacyBucketKey,
}

#[derive(Debug, Deserialize)]
struct LegacyUddSketch {
    buckets: LegacyBucketStore,
    alpha: f64,
    gamma: f64,
    compactions: u32,
    max_buckets: u64,
    count: u64,
    sum: f64,
}

#[derive(Debug, Deserialize)]
struct LegacyUddSketchState {
    uddsketch: LegacyUddSketch,
    initial_error: f64,
}

pub(crate) fn decode(raw: &[u8]) -> Result<UddSketch, String> {
    match UddSketch::decode(raw) {
        Ok(sketch) => Ok(sketch),
        Err(current_error) => decode_legacy_state(raw).map_err(|legacy_error| {
            format!(
                "canonical decode failed: {current_error}; legacy decode failed: {legacy_error}"
            )
        }),
    }
}

pub(crate) fn quantile(raw: &[u8], quantile: f64) -> Result<Option<f64>, String> {
    match UddSketchRef::parse(raw) {
        Ok(sketch) => sketch.quantile(quantile).map_err(|error| error.to_string()),
        Err(current_error) => decode_legacy_sketch(raw)
            .and_then(|sketch| sketch.quantile(quantile))
            .map_err(|legacy_error| {
                format!(
                    "canonical decode failed: {current_error}; legacy decode failed: {legacy_error}"
                )
            }),
    }
}

fn validate_legacy_input(raw: &[u8]) -> Result<(), String> {
    if raw.len() > MAX_BYTES {
        return Err("input exceeds the legacy decode byte limit".to_string());
    }
    let map_len = raw
        .get(..8)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u64::from_le_bytes)
        .ok_or_else(|| "legacy input is truncated before the bucket count".to_string())?;
    if map_len > MAX_BUCKETS as u64 {
        return Err("legacy populated bucket count exceeds decode limit".to_string());
    }
    Ok(())
}

fn legacy_options() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(MAX_BYTES as u64)
        .reject_trailing_bytes()
}

fn decode_legacy_state(raw: &[u8]) -> Result<UddSketch, String> {
    validate_legacy_input(raw)?;
    let state = legacy_options()
        .deserialize::<LegacyUddSketchState>(raw)
        .map_err(|error| error.to_string())?;
    let encoded = state.into_canonical()?;
    UddSketch::decode(&encoded).map_err(|error| error.to_string())
}

fn decode_legacy_sketch(raw: &[u8]) -> Result<LegacyUddSketch, String> {
    validate_legacy_input(raw)?;
    legacy_options()
        .deserialize::<LegacyUddSketchState>(raw)
        .map(|state| state.uddsketch)
        .or_else(|state_error| {
            legacy_options()
                .deserialize::<LegacyUddSketch>(raw)
                .map_err(|sketch_error| {
                    format!(
                        "legacy state decode failed: {state_error}; legacy sketch decode failed: {sketch_error}"
                    )
                })
        })
}

impl LegacyUddSketchState {
    fn into_canonical(self) -> Result<Vec<u8>, String> {
        let sketch = self.uddsketch;
        let max_buckets = u32::try_from(sketch.max_buckets)
            .map_err(|_| "legacy maximum bucket count exceeds u32".to_string())?;
        if !(7..=MAX_BUCKETS as u32).contains(&max_buckets) {
            return Err("legacy maximum bucket count is outside supported limits".to_string());
        }
        let compactions = u8::try_from(sketch.compactions)
            .map_err(|_| "legacy compaction count exceeds u8".to_string())?;
        if compactions > 63 {
            return Err("legacy compaction count exceeds 63".to_string());
        }

        let (expected_alpha, expected_gamma) = mapping(self.initial_error, compactions)?;
        if sketch.alpha.to_bits() != expected_alpha.to_bits()
            || sketch.gamma.to_bits() != expected_gamma.to_bits()
        {
            return Err("legacy mapping metadata is inconsistent".to_string());
        }

        let buckets = sketch.buckets.ordered()?;
        if buckets.len() > max_buckets as usize {
            return Err("legacy populated bucket count exceeds its configured limit".to_string());
        }
        let decoded_count = buckets.iter().try_fold(0_u64, |total, (_, count)| {
            total
                .checked_add(*count)
                .ok_or_else(|| "legacy bucket count sum overflows u64".to_string())
        })?;
        if decoded_count != sketch.count {
            return Err("legacy bucket counts do not match the value count".to_string());
        }
        if sketch.count == 0 && sketch.sum.to_bits() != 0.0_f64.to_bits() {
            return Err("legacy empty sketch sum is not positive zero".to_string());
        }

        encode_canonical(
            max_buckets,
            self.initial_error,
            compactions,
            sketch.count,
            sketch.sum,
            &buckets,
        )
    }
}

impl LegacyUddSketch {
    fn quantile(self, quantile: f64) -> Result<Option<f64>, String> {
        if !quantile.is_finite() || !(0.0..=1.0).contains(&quantile) {
            return Err("invalid quantile".to_string());
        }
        if self.compactions >= 64 {
            return Err("legacy compaction count must be below 64".to_string());
        }
        validate_current_mapping(self.alpha, self.gamma, self.compactions)?;
        if self.max_buckets == 0 || self.max_buckets > MAX_BUCKETS as u64 {
            return Err("legacy maximum bucket count is outside supported limits".to_string());
        }

        let count = self.count;
        let alpha = self.alpha;
        let gamma = self.gamma;
        let buckets = self.buckets.ordered()?;
        if buckets.len() > self.max_buckets as usize {
            return Err("legacy populated bucket count exceeds its configured limit".to_string());
        }
        for (key, _) in &buckets {
            if !legacy_bucket_key_is_attainable(self.gamma, *key) {
                return Err("legacy bucket index cannot represent an f64".to_string());
            }
        }
        let decoded_count = buckets.iter().try_fold(0_u64, |total, (_, count)| {
            total
                .checked_add(*count)
                .ok_or_else(|| "legacy bucket count sum overflows u64".to_string())
        })?;
        if decoded_count != count {
            return Err("legacy bucket counts do not match the value count".to_string());
        }
        if count == 0 {
            return Ok(None);
        }

        let target = if quantile == 1.0 {
            count
        } else {
            ((count as f64 * quantile) as u64)
                .saturating_add(1)
                .min(count)
        };
        let mut seen = 0_u64;
        for (key, bucket_count) in buckets {
            seen += bucket_count;
            if seen >= target {
                return Ok(Some(legacy_bucket_value(alpha, gamma, key)?));
            }
        }
        Err("legacy bucket counts do not cover the quantile rank".to_string())
    }
}

fn validate_current_mapping(
    mut alpha: f64,
    mut gamma: f64,
    compactions: u32,
) -> Result<(), String> {
    if !alpha.is_finite() || !(0.0..=1.0).contains(&alpha) {
        return Err("legacy current error is outside [0, 1]".to_string());
    }
    if !gamma.is_finite() || gamma <= 1.0 {
        return (alpha == 1.0 && gamma == f64::INFINITY && compactions >= 5)
            .then_some(())
            .ok_or_else(|| "legacy gamma must be greater than one".to_string());
    }
    if alpha == 1.0 {
        return (compactions > 0 && 1.0 - 2.0 / (gamma + 1.0) == 1.0)
            .then_some(())
            .ok_or_else(|| "legacy saturated mapping metadata is inconsistent".to_string());
    }

    for _ in 0..compactions {
        alpha /= 1.0 + (1.0 - alpha * alpha).sqrt();
        gamma = gamma.sqrt();
    }
    let expected_gamma = (1.0 + alpha) / (1.0 - alpha);
    let relative_difference = (gamma - expected_gamma).abs() / expected_gamma;
    if relative_difference > 1e-10 {
        return Err("legacy mapping metadata is inconsistent".to_string());
    }
    Ok(())
}

fn legacy_bucket_key_is_attainable(gamma: f64, key: LegacyBucketKey) -> bool {
    let index = match key {
        LegacyBucketKey::Zero => return true,
        LegacyBucketKey::Negative(index) | LegacyBucketKey::Positive(index) => index,
        LegacyBucketKey::Invalid => return false,
    };
    if index == i64::MAX {
        return true;
    }

    let minimum = f64::from_bits(1).log(gamma).ceil() as i64;
    let maximum = f64::MAX.log(gamma).ceil() as i64;
    (minimum..=maximum).contains(&index)
}

fn legacy_bucket_value(alpha: f64, gamma: f64, key: LegacyBucketKey) -> Result<f64, String> {
    let magnitude = |index: i64| gamma.powf(index as f64 - 1.0) * (1.0 + alpha);
    match key {
        LegacyBucketKey::Negative(index) => Ok(-magnitude(index)),
        LegacyBucketKey::Zero => Ok(0.0),
        LegacyBucketKey::Positive(index) => Ok(magnitude(index)),
        LegacyBucketKey::Invalid => Err("legacy bucket chain contains the end marker".to_string()),
    }
}

impl LegacyBucketStore {
    fn ordered(self) -> Result<Vec<(LegacyBucketKey, u64)>, String> {
        if self.map.len() > MAX_BUCKETS {
            return Err("legacy populated bucket count exceeds decode limit".to_string());
        }
        if self.map.is_empty() {
            if self.head != LegacyBucketKey::Invalid {
                return Err("legacy empty bucket store has a nonempty head".to_string());
            }
            return Ok(Vec::new());
        }

        let mut buckets = Vec::with_capacity(self.map.len());
        let mut visited = HashSet::with_capacity(self.map.len());
        let mut key = self.head;
        while key != LegacyBucketKey::Invalid {
            if !visited.insert(key) {
                return Err("legacy bucket chain contains a cycle".to_string());
            }
            let bucket = self
                .map
                .get(&key)
                .ok_or_else(|| "legacy bucket chain references a missing bucket".to_string())?;
            if bucket.count == 0 {
                return Err("legacy bucket has a zero count".to_string());
            }
            buckets.push((key, bucket.count));
            key = bucket.next;
        }
        if buckets.len() != self.map.len() || self.map.contains_key(&LegacyBucketKey::Invalid) {
            return Err("legacy bucket store contains unreachable buckets".to_string());
        }
        if !buckets.windows(2).all(|pair| key_lt(pair[0].0, pair[1].0)) {
            return Err("legacy bucket chain is not strictly ordered".to_string());
        }
        Ok(buckets)
    }
}

fn mapping(initial_error: f64, compactions: u8) -> Result<(f64, f64), String> {
    if !initial_error.is_finite() || !(1e-12..1.0).contains(&initial_error) {
        return Err("legacy initial error is outside [1e-12, 1)".to_string());
    }
    let mut alpha = initial_error;
    let mut gamma = (1.0 + initial_error) / (1.0 - initial_error);
    for _ in 0..compactions {
        gamma *= gamma;
        alpha = 2.0 * alpha / (1.0 + alpha.powi(2));
    }
    Ok((alpha, gamma))
}

fn encode_canonical(
    max_buckets: u32,
    initial_error: f64,
    compactions: u8,
    count: u64,
    sum: f64,
    buckets: &[(LegacyBucketKey, u64)],
) -> Result<Vec<u8>, String> {
    let negative = buckets
        .iter()
        .filter_map(|(key, count)| match key {
            LegacyBucketKey::Negative(index) => Some((*index, *count)),
            _ => None,
        })
        .collect::<Vec<_>>();
    let zero_count = buckets
        .iter()
        .find_map(|(key, count)| (*key == LegacyBucketKey::Zero).then_some(*count))
        .unwrap_or(0);
    let positive = buckets
        .iter()
        .filter_map(|(key, count)| match key {
            LegacyBucketKey::Positive(index) => Some((*index, *count)),
            _ => None,
        })
        .collect::<Vec<_>>();

    let mut encoded = vec![0; HEADER_LEN];
    put_varint(&mut encoded, negative.len() as u64);
    put_varint(&mut encoded, zero_count);
    put_varint(&mut encoded, positive.len() as u64);
    encode_section(&mut encoded, &negative, true)?;
    encode_section(&mut encoded, &positive, false)?;

    let payload_len = u32::try_from(encoded.len() - HEADER_LEN)
        .map_err(|_| "legacy canonical payload exceeds u32".to_string())?;
    encoded[0..4].copy_from_slice(b"UDDS");
    encoded[4] = 1;
    encoded[6] = compactions;
    encoded[8..12].copy_from_slice(&max_buckets.to_le_bytes());
    encoded[12..16].copy_from_slice(&(buckets.len() as u32).to_le_bytes());
    encoded[16..24].copy_from_slice(&initial_error.to_bits().to_le_bytes());
    encoded[24..32].copy_from_slice(&count.to_le_bytes());
    encoded[32..40].copy_from_slice(&sum.to_bits().to_le_bytes());
    encoded[40..44].copy_from_slice(&payload_len.to_le_bytes());
    Ok(encoded)
}

fn encode_section(
    output: &mut Vec<u8>,
    buckets: &[(i64, u64)],
    descending: bool,
) -> Result<(), String> {
    let mut previous = None;
    for &(index, count) in buckets {
        let encoded_index = match previous {
            None => zigzag(index),
            Some((previous_index, _)) => {
                let delta = if descending {
                    previous_index as i128 - index as i128
                } else {
                    index as i128 - previous_index as i128
                };
                u64::try_from(delta)
                    .ok()
                    .filter(|delta| *delta != 0)
                    .ok_or_else(|| "legacy bucket indices are not strictly ordered".to_string())?
            }
        };
        let encoded_count = match previous {
            None => count,
            Some((_, previous_count)) => zigzag(count.wrapping_sub(previous_count) as i64),
        };
        put_varint(output, encoded_index);
        put_varint(output, encoded_count);
        previous = Some((index, count));
    }
    Ok(())
}

fn put_varint(output: &mut Vec<u8>, value: u64) {
    let mut buffer = [0; 9];
    let len = vu128::encode_u64(&mut buffer, value);
    output.extend_from_slice(&buffer[..len]);
}

const fn zigzag(value: i64) -> u64 {
    (value.wrapping_shl(1) ^ (value >> 63)) as u64
}

fn key_lt(left: LegacyBucketKey, right: LegacyBucketKey) -> bool {
    use LegacyBucketKey::*;
    match (left, right) {
        (Negative(left), Negative(right)) => left > right,
        (Negative(_), Zero | Positive(_)) | (Zero, Positive(_)) => true,
        (Positive(left), Positive(right)) => left < right,
        _ => false,
    }
}

#[cfg(test)]
pub(crate) const LEGACY_STATE: &[u8] = &[
    4, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 116, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 116, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 116,
    0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 116, 0, 0, 0, 0, 0, 0, 0,
    123, 20, 174, 71, 225, 122, 132, 63, 253, 74, 129, 90, 191, 82, 240, 63, 0, 0, 0, 0, 128, 0, 0,
    0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 123, 20, 174, 71, 225, 122,
    132, 63,
];

#[cfg(test)]
pub(crate) const COMPACTED_LEGACY_SKETCH: &[u8] = &[
    6, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 36, 0, 0, 0, 0, 0,
    0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 36, 0, 0, 0, 0,
    0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 254, 255, 255, 255, 255, 255, 255,
    255, 29, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 2, 0, 0, 0,
    2, 0, 0, 0, 0, 0, 0, 0, 36, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0,
    0, 3, 0, 0, 0, 0, 0, 0, 0, 29, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 0, 0,
    0, 0, 35, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 254, 255, 255,
    255, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 240, 63, 169, 137, 186, 120, 1, 63, 82, 71, 12, 0,
    0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 201, 0, 0, 0, 0, 0, 0, 0, 112, 103, 108, 212, 220, 81, 180, 84,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_format_is_decoded_without_legacy_fallback() {
        let mut sketch = UddSketch::new(128, 0.01).unwrap();
        sketch.add(42.0).unwrap();
        let expected = sketch.quantile(0.5).unwrap();
        let encoded = sketch.encode().unwrap();

        assert!(decode_legacy_state(&encoded).is_err());
        assert_eq!(decode(&encoded).unwrap(), sketch);
        assert_eq!(quantile(&encoded, 0.5).unwrap(), expected);
    }

    #[test]
    fn legacy_decoder_rejects_oversized_bucket_count_before_deserializing() {
        let mut encoded = LEGACY_STATE.to_vec();
        encoded[..8].copy_from_slice(&((MAX_BUCKETS as u64) + 1).to_le_bytes());

        assert_eq!(
            decode_legacy_state(&encoded).unwrap_err(),
            "legacy populated bucket count exceeds decode limit"
        );
    }

    #[test]
    fn legacy_decoder_reads_bare_sketch_from_scalar_callers() {
        let bare_sketch = &LEGACY_STATE[..LEGACY_STATE.len() - std::mem::size_of::<f64>()];

        assert_eq!(
            quantile(bare_sketch, 0.5).unwrap(),
            Some(0.9900000000000001)
        );
    }

    #[test]
    fn legacy_decoder_reads_compacted_bare_sketch() {
        assert!(decode_legacy_state(COMPACTED_LEGACY_SKETCH).is_err());
        assert!(quantile(COMPACTED_LEGACY_SKETCH, 0.5).unwrap().is_some());
    }

    #[test]
    fn legacy_quantile_rejects_invalid_mapping() {
        let bare_len = LEGACY_STATE.len() - std::mem::size_of::<f64>();
        let mut invalid = LEGACY_STATE[..bare_len].to_vec();
        let alpha_offset = bare_len - 44;
        invalid[alpha_offset..alpha_offset + 8].copy_from_slice(&f64::NAN.to_le_bytes());

        assert!(quantile(&invalid, 0.5).is_err());

        let mut sketch = decode_legacy_sketch(&LEGACY_STATE[..bare_len]).unwrap();
        sketch.gamma = 2.0;
        assert!(sketch.quantile(0.5).is_err());

        let mut sketch = decode_legacy_sketch(&LEGACY_STATE[..bare_len]).unwrap();
        sketch.alpha = 1.0;
        sketch.gamma = 2.0;
        assert!(sketch.quantile(0.5).is_err());

        let mut sketch = decode_legacy_sketch(&LEGACY_STATE[..bare_len]).unwrap();
        sketch.alpha = 1e-12;
        sketch.gamma = 1.0 + 1e-9;
        sketch.compactions = 0;
        assert!(sketch.quantile(0.5).is_err());

        let mut sketch = decode_legacy_sketch(&LEGACY_STATE[..bare_len]).unwrap();
        sketch.alpha = 1.0;
        sketch.gamma = 1e20;
        sketch.compactions = 0;
        assert!(sketch.quantile(0.5).is_err());

        let mut sketch = decode_legacy_sketch(&LEGACY_STATE[..bare_len]).unwrap();
        sketch.alpha = 1.0;
        sketch.gamma = f64::INFINITY;
        sketch.compactions = 1;
        assert!(validate_current_mapping(sketch.alpha, sketch.gamma, sketch.compactions).is_err());

        let mut sketch = decode_legacy_sketch(&LEGACY_STATE[..bare_len]).unwrap();
        sketch.alpha = 0.99999999;
        sketch.gamma = 2.0;
        sketch.compactions = 0;
        assert!(sketch.quantile(0.5).is_err());

        let mut sketch = decode_legacy_sketch(&LEGACY_STATE[..bare_len]).unwrap();
        let LegacyBucketKey::Negative(index) = sketch.buckets.head else {
            panic!("Expected negative head bucket");
        };
        let bucket = sketch
            .buckets
            .map
            .remove(&LegacyBucketKey::Negative(index))
            .unwrap();
        sketch
            .buckets
            .map
            .insert(LegacyBucketKey::Negative(i64::MIN), bucket);
        sketch.buckets.head = LegacyBucketKey::Negative(i64::MIN);
        assert!(sketch.quantile(0.5).is_err());
    }

    #[test]
    fn legacy_quantile_accepts_saturated_mapping() {
        let sketch = LegacyUddSketch {
            buckets: LegacyBucketStore {
                map: HashMap::from([(
                    LegacyBucketKey::Positive(0),
                    LegacyBucket {
                        count: 1,
                        next: LegacyBucketKey::Invalid,
                    },
                )]),
                head: LegacyBucketKey::Positive(0),
            },
            alpha: 1.0,
            gamma: f64::INFINITY,
            compactions: 63,
            max_buckets: 7,
            count: 1,
            sum: f64::INFINITY,
        };

        assert_eq!(sketch.quantile(0.5).unwrap(), Some(0.0));
    }

    #[test]
    fn legacy_quantile_handles_maximum_count_at_one() {
        let sketch = LegacyUddSketch {
            buckets: LegacyBucketStore {
                map: HashMap::from([(
                    LegacyBucketKey::Zero,
                    LegacyBucket {
                        count: u64::MAX,
                        next: LegacyBucketKey::Invalid,
                    },
                )]),
                head: LegacyBucketKey::Zero,
            },
            alpha: 0.01,
            gamma: (1.0 + 0.01) / (1.0 - 0.01),
            compactions: 0,
            max_buckets: 128,
            count: u64::MAX,
            sum: 0.0,
        };

        assert_eq!(sketch.quantile(1.0).unwrap(), Some(0.0));
    }
}
