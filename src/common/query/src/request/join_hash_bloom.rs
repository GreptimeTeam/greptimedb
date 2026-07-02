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

//! Join-hash Bloom filter payload model for remote dynamic filter updates.
//!
//! The Bloom payload carries a bitset over DataFusion join-hash `u64` values
//! along with the hash seeds, join key child indices, and a residual
//! DataFusion expression where runtime `HashTableLookupExpr` nodes have been
//! replaced by `lit(true)`. Receivers combine the residual expression with a
//! Bloom probe expression via `AND` to obtain the final predicate.

use std::collections::HashSet;

use datafusion_common::{DataFusionError, Result as DataFusionResult};
use serde::{Deserialize, Serialize};

use super::base64_serde;

/// Current wire-format version for join-hash Bloom payloads.
pub const JOIN_HASH_BLOOM_VERSION: u32 = 1;

/// Upper bound on the number of Bloom hash probes per lookup key.
/// Keeps probe cost bounded and prevents obviously misconfigured payloads.
pub const MAX_BLOOM_NUM_PROBES: u32 = 64;

/// Upper bound on the residual DataFusion expression bytes carried in a
/// Bloom payload. The residual is an optional safety net; oversized
/// residuals indicate a producer-side budget violation.
pub const MAX_BLOOM_RESIDUAL_BYTES: usize = 8192;

/// Conservative estimate of proto envelope overhead for a Bloom payload
/// when wrapped inside `RemoteDynFilterPayload`.  Used during encoding to
/// avoid overshooting the payload budget.
pub const BLOOM_PROTO_ENVELOPE_OVERHEAD_ESTIMATE: usize = 256;

/// Default number of Bloom hash probes used by the encoder.
pub const BLOOM_ENCODER_NUM_PROBES: u32 = 5;

/// Bits allocated per distinct hash in the Bloom filter.
pub const BLOOM_ENCODER_BITS_PER_HASH: u64 = 10;

/// Minimum Bloom size in bits.
pub const BLOOM_ENCODER_MIN_NUM_BITS: u64 = 64;

/// Largest `num_bits` the encoder will ever allocate (hard OOM guard).
pub const BLOOM_ENCODER_MAX_NUM_BITS_BUDGET: u64 = 256 * 1024; // 256 KiB = 32 KiB bytes

/// Serialized Bloom filter payload for remote dynamic filter updates.
///
/// Carries a Bloom bitset over DataFusion join-hash `u64` values together
/// with the parameters needed to reconstruct the probe-side hash computation
/// and a residual DataFusion expression with `HashTableLookupExpr` replaced
/// by `lit(true)`.
///
/// Byte fields are base64-encoded in JSON to keep the wire representation
/// compact and safe.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JoinHashBloomPayload {
    pub version: u32,
    pub df_seed0: u64,
    pub df_seed1: u64,
    pub df_seed2: u64,
    pub df_seed3: u64,
    pub num_bits: u64,
    pub num_probes: u32,
    #[serde(with = "base64_serde::bytes")]
    pub bitset: Vec<u8>,
    pub join_key_child_indices: Vec<u32>,
    #[serde(with = "base64_serde::bytes")]
    pub residual_datafusion_physical_expr: Vec<u8>,
    /// Cross-platform hash-compatibility fingerprint.
    ///
    /// Computed deterministically by running a `HashExpr` over a canonical
    /// `RecordBatch` built from `input_schema`.  Zero means "not computed".
    pub hash_compat_fingerprint: u64,
}

impl JoinHashBloomPayload {
    /// Fail-closed validation of payload invariants.
    ///
    /// Rejects:
    /// - unsupported version
    /// - `num_bits == 0`
    /// - `num_bits` not representable as `usize`
    /// - `num_probes == 0` or exceeding [`MAX_BLOOM_NUM_PROBES`]
    /// - bitset length not equal to `ceil(num_bits / 8)`
    /// - empty or duplicate join key child indices
    /// - empty residual expression or residual expression exceeding
    ///   [`MAX_BLOOM_RESIDUAL_BYTES`]
    pub fn validate(&self) -> DataFusionResult<()> {
        if self.version != JOIN_HASH_BLOOM_VERSION {
            return Err(DataFusionError::Plan(format!(
                "JoinHashBloomPayload: unsupported version {} (expected {})",
                self.version, JOIN_HASH_BLOOM_VERSION
            )));
        }

        if self.num_bits == 0 {
            return Err(DataFusionError::Plan(
                "JoinHashBloomPayload: num_bits is zero".to_string(),
            ));
        }

        if self.num_bits > usize::MAX as u64 {
            return Err(DataFusionError::Plan(format!(
                "JoinHashBloomPayload: num_bits {} exceeds usize::MAX",
                self.num_bits
            )));
        }

        if self.num_probes == 0 {
            return Err(DataFusionError::Plan(
                "JoinHashBloomPayload: num_probes is zero".to_string(),
            ));
        }

        if self.num_probes > MAX_BLOOM_NUM_PROBES {
            return Err(DataFusionError::Plan(format!(
                "JoinHashBloomPayload: num_probes {} exceeds limit {}",
                self.num_probes, MAX_BLOOM_NUM_PROBES
            )));
        }

        let expected_bitset_len = num_bits_ceil_bytes(self.num_bits as usize);
        if self.bitset.len() != expected_bitset_len {
            return Err(DataFusionError::Plan(format!(
                "JoinHashBloomPayload: bitset length {} does not match ceil(num_bits {}/8) = {}",
                self.bitset.len(),
                self.num_bits,
                expected_bitset_len
            )));
        }

        if self.join_key_child_indices.is_empty() {
            return Err(DataFusionError::Plan(
                "JoinHashBloomPayload: join_key_child_indices is empty".to_string(),
            ));
        }

        let mut seen = HashSet::with_capacity(self.join_key_child_indices.len());
        for &child_index in &self.join_key_child_indices {
            if !seen.insert(child_index) {
                return Err(DataFusionError::Plan(format!(
                    "JoinHashBloomPayload: duplicate join key child index {}",
                    child_index
                )));
            }
        }

        if self.residual_datafusion_physical_expr.is_empty() {
            return Err(DataFusionError::Plan(
                "JoinHashBloomPayload: residual expression is empty".to_string(),
            ));
        }

        if self.residual_datafusion_physical_expr.len() > MAX_BLOOM_RESIDUAL_BYTES {
            return Err(DataFusionError::Plan(format!(
                "JoinHashBloomPayload: residual expression {} bytes exceeds budget {}",
                self.residual_datafusion_physical_expr.len(),
                MAX_BLOOM_RESIDUAL_BYTES
            )));
        }

        Ok(())
    }

    /// Converts this model to the corresponding proto `JoinHashBloomPayload`.
    pub fn to_proto(&self) -> api::v1::region::JoinHashBloomPayload {
        api::v1::region::JoinHashBloomPayload {
            version: self.version,
            df_seed0: self.df_seed0,
            df_seed1: self.df_seed1,
            df_seed2: self.df_seed2,
            df_seed3: self.df_seed3,
            num_bits: self.num_bits,
            num_probes: self.num_probes,
            bitset: self.bitset.clone(),
            join_key_child_indices: self.join_key_child_indices.clone(),
            residual_datafusion_physical_expr: self.residual_datafusion_physical_expr.clone(),
            hash_compat_fingerprint: self.hash_compat_fingerprint,
        }
    }

    /// Constructs this model from a proto `JoinHashBloomPayload`.
    ///
    /// Does *not* validate — call [`Self::validate`] separately.
    pub fn from_proto(payload: &api::v1::region::JoinHashBloomPayload) -> DataFusionResult<Self> {
        Ok(Self {
            version: payload.version,
            df_seed0: payload.df_seed0,
            df_seed1: payload.df_seed1,
            df_seed2: payload.df_seed2,
            df_seed3: payload.df_seed3,
            num_bits: payload.num_bits,
            num_probes: payload.num_probes,
            bitset: payload.bitset.clone(),
            join_key_child_indices: payload.join_key_child_indices.clone(),
            residual_datafusion_physical_expr: payload.residual_datafusion_physical_expr.clone(),
            hash_compat_fingerprint: payload.hash_compat_fingerprint,
        })
    }

    /// Returns the expected bitset byte length for `num_bits`, i.e.
    /// `ceil(num_bits / 8)`.
    pub fn expected_bitset_bytes(&self) -> usize {
        num_bits_ceil_bytes(self.num_bits as usize)
    }

    /// Tests whether the given join hash is a member of this Bloom filter.
    ///
    /// Uses SplitMix64 double-hashing to derive the probe sequence.
    /// This is deterministic and false-negative-free for any hash that was
    /// previously inserted into the bitset with the same parameters.
    #[inline]
    pub fn contains_join_hash(&self, hash: u64) -> bool {
        bloom_contains(hash, &self.bitset, self.num_bits, self.num_probes)
    }

    /// Inserts a join hash into this Bloom filter's bitset in-place.
    ///
    /// This is used during encoding to build the bitset from distinct join hashes.
    #[inline]
    pub fn insert_join_hash(&mut self, hash: u64) {
        bloom_insert(hash, &mut self.bitset, self.num_bits, self.num_probes)
    }

    /// Builds a new Bloom payload from distinct join hashes.
    ///
    /// Allocates a fresh zeroed bitset of the correct size and inserts every
    /// hash. The returned payload passes [`Self::validate`] when `num_bits`,
    /// `num_probes`, and the other required fields are set correctly.
    ///
    /// The `hash_compat_fingerprint` is left at zero; the encoder should set it
    /// after construction.
    pub fn build_from_hashes(
        hashes: impl IntoIterator<Item = u64>,
        num_bits: u64,
        num_probes: u32,
        seeds: (u64, u64, u64, u64),
        join_key_child_indices: Vec<u32>,
        residual_datafusion_physical_expr: Vec<u8>,
    ) -> Self {
        let bitset_len = num_bits_ceil_bytes(num_bits as usize);
        let mut bitset = vec![0u8; bitset_len];
        bloom_insert_many(hashes, &mut bitset, num_bits, num_probes);
        Self {
            version: JOIN_HASH_BLOOM_VERSION,
            df_seed0: seeds.0,
            df_seed1: seeds.1,
            df_seed2: seeds.2,
            df_seed3: seeds.3,
            num_bits,
            num_probes,
            bitset,
            join_key_child_indices,
            residual_datafusion_physical_expr,
            hash_compat_fingerprint: 0,
        }
    }

    /// Fallible builder: validates parameters before allocation and insertion.
    ///
    /// Checks `num_bits`, `num_probes`, join key child indices, and residual before
    /// allocating the bitset.  The returned payload is already validated via
    /// [`Self::validate`].
    pub fn try_build_from_hashes(
        hashes: impl IntoIterator<Item = u64>,
        num_bits: u64,
        num_probes: u32,
        seeds: (u64, u64, u64, u64),
        join_key_child_indices: Vec<u32>,
        residual_datafusion_physical_expr: Vec<u8>,
    ) -> DataFusionResult<Self> {
        // --- validate before allocating ---

        if num_bits == 0 {
            return Err(DataFusionError::Plan(
                "JoinHashBloomPayload: try_build_from_hashes num_bits is zero".to_string(),
            ));
        }
        if num_bits > BLOOM_ENCODER_MAX_NUM_BITS_BUDGET {
            return Err(DataFusionError::Plan(format!(
                "JoinHashBloomPayload: try_build_from_hashes num_bits {} exceeds budget {}",
                num_bits, BLOOM_ENCODER_MAX_NUM_BITS_BUDGET
            )));
        }
        if num_bits > usize::MAX as u64 {
            return Err(DataFusionError::Plan(format!(
                "JoinHashBloomPayload: try_build_from_hashes num_bits {} exceeds usize::MAX",
                num_bits
            )));
        }
        if num_probes == 0 {
            return Err(DataFusionError::Plan(
                "JoinHashBloomPayload: try_build_from_hashes num_probes is zero".to_string(),
            ));
        }
        if num_probes > MAX_BLOOM_NUM_PROBES {
            return Err(DataFusionError::Plan(format!(
                "JoinHashBloomPayload: try_build_from_hashes num_probes {} exceeds limit {}",
                num_probes, MAX_BLOOM_NUM_PROBES
            )));
        }
        if join_key_child_indices.is_empty() {
            return Err(DataFusionError::Plan(
                "JoinHashBloomPayload: try_build_from_hashes join_key_child_indices is empty"
                    .to_string(),
            ));
        }
        if residual_datafusion_physical_expr.is_empty() {
            return Err(DataFusionError::Plan(
                "JoinHashBloomPayload: try_build_from_hashes residual expression is empty"
                    .to_string(),
            ));
        }

        // --- allocate and build ---

        let payload = Self::build_from_hashes(
            hashes,
            num_bits,
            num_probes,
            seeds,
            join_key_child_indices,
            residual_datafusion_physical_expr,
        );

        payload.validate()?;

        Ok(payload)
    }

    /// Compute a Bloom size recommendation given a number of distinct hashes
    /// and a payload byte budget.
    ///
    /// Returns `None` if the budget is too small to accommodate even the
    /// minimum Bloom, the residual, and proto envelope overhead.
    pub fn compute_bloom_sizing(
        distinct_hash_count: u64,
        residual_bytes_len: usize,
        max_payload_bytes: usize,
    ) -> Option<(u64, u32)> {
        let num_bits = std::cmp::max(
            BLOOM_ENCODER_MIN_NUM_BITS,
            distinct_hash_count.saturating_mul(BLOOM_ENCODER_BITS_PER_HASH),
        );
        let num_probes = BLOOM_ENCODER_NUM_PROBES;

        let bitset_bytes = num_bits_ceil_bytes(num_bits as usize);

        // Quick budget check: residual + bitset + envelope overhead must fit
        let estimated_total = residual_bytes_len
            .saturating_add(bitset_bytes)
            .saturating_add(BLOOM_PROTO_ENVELOPE_OVERHEAD_ESTIMATE);

        if estimated_total > max_payload_bytes {
            // Shrink num_bits to fit, trading off false-positive rate
            let available_for_bitset = max_payload_bytes
                .saturating_sub(residual_bytes_len)
                .saturating_sub(BLOOM_PROTO_ENVELOPE_OVERHEAD_ESTIMATE);
            let max_bits = (available_for_bitset as u64)
                .saturating_mul(8)
                .min(BLOOM_ENCODER_MAX_NUM_BITS_BUDGET);
            if max_bits < BLOOM_ENCODER_MIN_NUM_BITS {
                return None;
            }
            Some((max_bits, num_probes))
        } else if num_bits > BLOOM_ENCODER_MAX_NUM_BITS_BUDGET {
            // Cap at hard budget even if there is room
            Some((BLOOM_ENCODER_MAX_NUM_BITS_BUDGET, num_probes))
        } else {
            Some((num_bits, num_probes))
        }
    }
}

/// `ceil(n / 8)` without floating point.
#[inline]
fn num_bits_ceil_bytes(num_bits: usize) -> usize {
    (num_bits / 8) + usize::from(!num_bits.is_multiple_of(8))
}

// ---------------------------------------------------------------------------
// SplitMix64 double-hash Bloom helpers
// ---------------------------------------------------------------------------

/// SplitMix64 — a fast, high-quality 64-bit mixing function.
///
/// Each round avalanches all input bits. This is the same SplitMix64 used by
/// many hash-table and Bloom filter implementations.
#[inline]
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

/// Returns the bit position for the k-th probe of the given hash using
/// SplitMix64 double-hashing.
///
/// - `h1 = splitmix64(hash)`
/// - `delta = splitmix64(h1)`
/// - `bit = (h1 + k * delta) % num_bits`
#[inline]
fn bloom_bit_index(hash: u64, k: u32, num_bits: u64) -> u64 {
    let h1 = splitmix64(hash);
    let delta = splitmix64(h1);
    h1.wrapping_add((k as u64).wrapping_mul(delta)) % num_bits
}

/// Returns `true` if `hash` is present in the Bloom filter.
///
/// `bitset` is a byte slice of length `ceil(num_bits / 8)`. Bit 0 of each
/// byte is the least-significant bit.
fn bloom_contains(hash: u64, bitset: &[u8], num_bits: u64, num_probes: u32) -> bool {
    for k in 0..num_probes {
        let bit = bloom_bit_index(hash, k, num_bits);
        let byte_idx = (bit >> 3) as usize;
        let bit_mask = 1u8 << (bit & 7);
        if bitset[byte_idx] & bit_mask == 0 {
            return false;
        }
    }
    true
}

/// Sets the bits for `hash` in the Bloom filter.
fn bloom_insert(hash: u64, bitset: &mut [u8], num_bits: u64, num_probes: u32) {
    for k in 0..num_probes {
        let bit = bloom_bit_index(hash, k, num_bits);
        let byte_idx = (bit >> 3) as usize;
        let bit_mask = 1u8 << (bit & 7);
        bitset[byte_idx] |= bit_mask;
    }
}

/// Inserts many hashes and returns the number of *distinct* hashes inserted.
fn bloom_insert_many(
    hashes: impl IntoIterator<Item = u64>,
    bitset: &mut [u8],
    num_bits: u64,
    num_probes: u32,
) -> u64 {
    let mut distinct = 0u64;
    let mut seen = std::collections::HashSet::new();
    for hash in hashes {
        if seen.insert(hash) {
            distinct += 1;
            bloom_insert(hash, bitset, num_bits, num_probes);
        }
    }
    distinct
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_valid_payload() -> JoinHashBloomPayload {
        JoinHashBloomPayload {
            version: JOIN_HASH_BLOOM_VERSION,
            df_seed0: 4,
            df_seed1: 5,
            df_seed2: 6,
            df_seed3: 7,
            num_bits: 1024,
            num_probes: 3,
            bitset: vec![0u8; 128],
            join_key_child_indices: vec![0, 1],
            residual_datafusion_physical_expr: vec![1, 2, 3],
            hash_compat_fingerprint: 0,
        }
    }

    #[test]
    fn validate_accepts_valid_payload() {
        minimal_valid_payload().validate().unwrap();
    }

    #[test]
    fn validate_rejections_table_driven() {
        #[track_caller]
        fn case(mutate: impl FnOnce(&mut JoinHashBloomPayload), expected_substr: &str) {
            let mut p = minimal_valid_payload();
            mutate(&mut p);
            let err = p.validate().unwrap_err();
            assert!(
                err.to_string().contains(expected_substr),
                "expected error containing '{expected_substr}', got: {}",
                err
            );
        }

        case(|p| p.version = 99, "unsupported version 99");
        case(|p| p.num_bits = 0, "num_bits is zero");
        case(|p| p.num_probes = 0, "num_probes is zero");
        case(|p| p.num_probes = MAX_BLOOM_NUM_PROBES + 1, "exceeds limit");
        case(|p| p.bitset = vec![0u8; 127], "bitset length");
        case(
            |p| p.join_key_child_indices = vec![0, 1, 0],
            "duplicate join key child index 0",
        );
        case(
            |p| p.residual_datafusion_physical_expr = vec![0u8; MAX_BLOOM_RESIDUAL_BYTES + 1],
            "exceeds budget",
        );
        case(
            |p| p.join_key_child_indices = vec![],
            "join_key_child_indices is empty",
        );
        case(
            |p| p.residual_datafusion_physical_expr = vec![],
            "residual expression is empty",
        );
    }

    #[test]
    fn validate_num_bits_not_divisible_by_8() {
        let mut p = minimal_valid_payload();
        p.num_bits = 1025;
        p.bitset = vec![0u8; 129]; // ceil(1025/8) = 129
        p.validate().unwrap();

        p.bitset = vec![0u8; 128];
        let err = p.validate().unwrap_err();
        assert!(err.to_string().contains("bitset length"));
    }

    #[test]
    fn compute_bloom_sizing_shrink_path_respects_hard_cap() {
        let hard_cap_bytes = num_bits_ceil_bytes(BLOOM_ENCODER_MAX_NUM_BITS_BUDGET as usize);
        let max_payload_bytes = hard_cap_bytes + BLOOM_PROTO_ENVELOPE_OVERHEAD_ESTIMATE + 1024;

        let (num_bits, num_probes) = JoinHashBloomPayload::compute_bloom_sizing(
            BLOOM_ENCODER_MAX_NUM_BITS_BUDGET,
            0,
            max_payload_bytes,
        )
        .unwrap();

        assert_eq!(num_bits, BLOOM_ENCODER_MAX_NUM_BITS_BUDGET);
        assert_eq!(num_probes, BLOOM_ENCODER_NUM_PROBES);

        JoinHashBloomPayload::try_build_from_hashes(
            [1, 2, 3],
            num_bits,
            num_probes,
            (0, 1, 2, 3),
            vec![0],
            vec![1],
        )
        .unwrap();
    }

    #[test]
    fn proto_roundtrip_preserves_fields() {
        let original = minimal_valid_payload();
        let proto = original.to_proto();
        let decoded = JoinHashBloomPayload::from_proto(&proto).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn json_serde_roundtrip_uses_base64() {
        let original = minimal_valid_payload();
        let json = serde_json::to_string(&original).unwrap();
        let decoded: JoinHashBloomPayload = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, original);

        // Verify bitset and residual are base64-encoded (not raw bytes)
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        let bitset_str = value["bitset"].as_str().unwrap();
        let residual_str = value["residual_datafusion_physical_expr"].as_str().unwrap();
        // Should be valid base64
        use base64::Engine;
        use base64::prelude::BASE64_STANDARD;
        BASE64_STANDARD.decode(bitset_str).unwrap();
        BASE64_STANDARD.decode(residual_str).unwrap();
        // And should not be raw byte arrays
        assert!(!bitset_str.starts_with('['));
    }

    #[test]
    fn json_rejects_invalid_base64_in_bitset() {
        let err = serde_json::from_str::<JoinHashBloomPayload>(
            r#"{"version":1,"df_seed0":0,"df_seed1":0,"df_seed2":0,"df_seed3":0,"num_bits":8,"num_probes":1,"bitset":"!!!not.base64!!!","join_key_child_indices":[],"residual_datafusion_physical_expr":"","hash_compat_fingerprint":0}"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("invalid base64"));
    }

    #[test]
    fn bloom_insert_contains_no_false_negatives() {
        let hashes: Vec<u64> = (0..100).map(|i| i * 7 + 13).collect();
        let payload = JoinHashBloomPayload::build_from_hashes(
            hashes.clone(),
            1024,
            3,
            (1, 2, 3, 4),
            vec![0],
            vec![1],
        );

        // Every inserted hash must be found
        for &h in &hashes {
            assert!(
                payload.contains_join_hash(h),
                "hash {} should be in the Bloom filter",
                h
            );
        }

        // Hash not inserted should usually be absent (may have false positives)
        // but we can at least verify the method doesn't crash
        let _ = payload.contains_join_hash(u64::MAX);
    }

    #[test]
    fn bloom_roundtrip_no_false_negatives_table_driven() {
        type Roundtrip = fn(&JoinHashBloomPayload) -> JoinHashBloomPayload;

        fn json_roundtrip(p: &JoinHashBloomPayload) -> JoinHashBloomPayload {
            serde_json::from_str(&serde_json::to_string(p).unwrap()).unwrap()
        }

        fn proto_roundtrip(p: &JoinHashBloomPayload) -> JoinHashBloomPayload {
            JoinHashBloomPayload::from_proto(&p.to_proto()).unwrap()
        }

        let hashes = vec![42u64, 12345, 98765];
        let original = JoinHashBloomPayload::build_from_hashes(
            hashes.clone(),
            256,
            2,
            (0, 0, 0, 0),
            vec![0],
            vec![1],
        );

        // Test both JSON and proto roundtrip preserve all inserted hashes
        let test_cases: &[(&str, Roundtrip)] =
            &[("json", json_roundtrip), ("proto", proto_roundtrip)];

        for (name, decode) in test_cases {
            let decoded = decode(&original);
            for &h in &hashes {
                assert!(
                    decoded.contains_join_hash(h),
                    "[{name}] hash {h} should be in the Bloom filter after roundtrip"
                );
            }
        }
    }
}
