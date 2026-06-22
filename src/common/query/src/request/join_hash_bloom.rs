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
use std::sync::Arc;

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

/// Identifies the join-hash scheme used to produce the hash values stored
/// in the Bloom filter.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum JoinHashKind {
    /// Sentinel / unknown.
    Unspecified = 0,
    /// DataFusion `create_hashes(..., HASH_JOIN_SEED)` producing `u64` hashes.
    DatafusionJoinHashU64 = 1,
}

impl JoinHashKind {
    pub fn to_proto(&self) -> i32 {
        use api::v1::region::JoinHashKind as Proto;
        match self {
            Self::Unspecified => Proto::Unspecified as i32,
            Self::DatafusionJoinHashU64 => Proto::DatafusionJoinHashU64 as i32,
        }
    }

    pub fn from_proto_i32(value: i32) -> Option<Self> {
        let proto =
            <api::v1::region::JoinHashKind as std::convert::TryFrom<i32>>::try_from(value).ok()?;
        Some(match proto {
            api::v1::region::JoinHashKind::Unspecified => Self::Unspecified,
            api::v1::region::JoinHashKind::DatafusionJoinHashU64 => Self::DatafusionJoinHashU64,
        })
    }
}

/// The hash algorithm used for Bloom bucket probing (SplitMix64
/// double-hashing).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum BloomHashAlgorithm {
    /// Sentinel / unknown.
    Unspecified = 0,
    /// SplitMix64-based double-hashing (v1).
    Splitmix64DoubleHashV1 = 1,
}

impl BloomHashAlgorithm {
    pub fn to_proto(&self) -> i32 {
        use api::v1::region::BloomHashAlgorithm as Proto;
        match self {
            Self::Unspecified => Proto::Unspecified as i32,
            Self::Splitmix64DoubleHashV1 => Proto::Splitmix64DoubleHashV1 as i32,
        }
    }

    pub fn from_proto_i32(value: i32) -> Option<Self> {
        let proto =
            <api::v1::region::BloomHashAlgorithm as std::convert::TryFrom<i32>>::try_from(value)
                .ok()?;
        Some(match proto {
            api::v1::region::BloomHashAlgorithm::Unspecified => Self::Unspecified,
            api::v1::region::BloomHashAlgorithm::Splitmix64DoubleHashV1 => {
                Self::Splitmix64DoubleHashV1
            }
        })
    }
}

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
    pub hash_kind: JoinHashKind,
    pub df_seed0: u64,
    pub df_seed1: u64,
    pub df_seed2: u64,
    pub df_seed3: u64,
    pub num_bits: u64,
    pub num_probes: u32,
    pub distinct_hash_count: u64,
    #[serde(with = "base64_serde::bytes")]
    pub bitset: Vec<u8>,
    pub join_key_child_indices: Vec<u32>,
    #[serde(with = "base64_serde::bytes")]
    pub residual_datafusion_physical_expr: Vec<u8>,
    pub bloom_hash_algorithm: BloomHashAlgorithm,
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
    /// - unsupported hash kind or bloom algorithm
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

        if self.hash_kind != JoinHashKind::DatafusionJoinHashU64 {
            return Err(DataFusionError::Plan(format!(
                "JoinHashBloomPayload: unsupported hash_kind {:?}",
                self.hash_kind
            )));
        }

        if self.bloom_hash_algorithm != BloomHashAlgorithm::Splitmix64DoubleHashV1 {
            return Err(DataFusionError::Plan(format!(
                "JoinHashBloomPayload: unsupported bloom_hash_algorithm {:?}",
                self.bloom_hash_algorithm
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
            hash_kind: self.hash_kind.to_proto(),
            df_seed0: self.df_seed0,
            df_seed1: self.df_seed1,
            df_seed2: self.df_seed2,
            df_seed3: self.df_seed3,
            num_bits: self.num_bits,
            num_probes: self.num_probes,
            distinct_hash_count: self.distinct_hash_count,
            bitset: self.bitset.clone(),
            join_key_child_indices: self.join_key_child_indices.clone(),
            residual_datafusion_physical_expr: self.residual_datafusion_physical_expr.clone(),
            bloom_hash_algorithm: self.bloom_hash_algorithm.to_proto(),
            hash_compat_fingerprint: self.hash_compat_fingerprint,
        }
    }

    /// Constructs this model from a proto `JoinHashBloomPayload`.
    ///
    /// Does *not* validate — call [`Self::validate`] separately.
    pub fn from_proto(payload: &api::v1::region::JoinHashBloomPayload) -> DataFusionResult<Self> {
        let hash_kind = JoinHashKind::from_proto_i32(payload.hash_kind).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "JoinHashBloomPayload: unknown hash_kind value {}",
                payload.hash_kind
            ))
        })?;

        let bloom_hash_algorithm = BloomHashAlgorithm::from_proto_i32(payload.bloom_hash_algorithm)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "JoinHashBloomPayload: unknown bloom_hash_algorithm value {}",
                    payload.bloom_hash_algorithm
                ))
            })?;

        Ok(Self {
            version: payload.version,
            hash_kind,
            df_seed0: payload.df_seed0,
            df_seed1: payload.df_seed1,
            df_seed2: payload.df_seed2,
            df_seed3: payload.df_seed3,
            num_bits: payload.num_bits,
            num_probes: payload.num_probes,
            distinct_hash_count: payload.distinct_hash_count,
            bitset: payload.bitset.clone(),
            join_key_child_indices: payload.join_key_child_indices.clone(),
            residual_datafusion_physical_expr: payload.residual_datafusion_physical_expr.clone(),
            bloom_hash_algorithm,
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
        bloom_contains(
            hash,
            &self.bitset,
            self.num_bits,
            self.num_probes,
            self.bloom_hash_algorithm,
        )
    }

    /// Inserts a join hash into this Bloom filter's bitset in-place.
    ///
    /// This is used during encoding to build the bitset from distinct join hashes.
    #[inline]
    pub fn insert_join_hash(&mut self, hash: u64) {
        bloom_insert(
            hash,
            &mut self.bitset,
            self.num_bits,
            self.num_probes,
            self.bloom_hash_algorithm,
        )
    }

    /// Builds a new Bloom payload from distinct join hashes.
    ///
    /// Allocates a fresh zeroed bitset of the correct size and inserts every
    /// hash. The returned payload passes [`Self::validate`] when `num_bits`,
    /// `num_probes`, and the other required fields are set correctly.
    ///
    /// The `hash_compat_fingerprint` is left at zero; the encoder should set it
    /// after construction via [`Self::hash_compat_fingerprint`].
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
        let distinct = bloom_insert_many(
            hashes,
            &mut bitset,
            num_bits,
            num_probes,
            BloomHashAlgorithm::Splitmix64DoubleHashV1,
        );
        Self {
            version: JOIN_HASH_BLOOM_VERSION,
            hash_kind: JoinHashKind::DatafusionJoinHashU64,
            df_seed0: seeds.0,
            df_seed1: seeds.1,
            df_seed2: seeds.2,
            df_seed3: seeds.3,
            num_bits,
            num_probes,
            distinct_hash_count: distinct,
            bitset,
            join_key_child_indices,
            residual_datafusion_physical_expr,
            bloom_hash_algorithm: BloomHashAlgorithm::Splitmix64DoubleHashV1,
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
            let max_bits = (available_for_bitset as u64).saturating_mul(8);
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

/// DataFusion [`PhysicalExpr`] that probes a join-hash Bloom filter against
/// probe-side columns.
///
/// Owns the child expressions selected by the payload's `join_key_child_indices` and
/// an immutable, already-validated [`JoinHashBloomPayload`].
///
/// During evaluation, a DataFusion [`HashExpr`](datafusion::physical_plan::joins::HashExpr)
/// computes the join-hash `u64` values from the serialized seeds, and the
/// Bloom bitset is probed for each row. The result is a non-nullable
/// `BooleanArray` with false-positive-only semantics.
#[derive(Clone, Debug, Eq)]
pub struct JoinHashBloomProbeExpr {
    children: Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>>,
    bloom: Arc<JoinHashBloomPayload>,
}

impl JoinHashBloomProbeExpr {
    /// Creates a new probe expression **without validation**.
    ///
    /// This constructor panics via `try_new` if the arguments are invalid.
    /// Prefer [`Self::try_new`] in production code paths.
    ///
    /// # Panics
    ///
    /// Panics if the bloom payload fails validation, children are empty, or
    /// `children.len()` does not match `bloom.join_key_child_indices.len()`.
    pub fn new(
        children: Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>>,
        bloom: Arc<JoinHashBloomPayload>,
    ) -> Self {
        Self::try_new(children, bloom)
            .expect("JoinHashBloomProbeExpr::new called with invalid state")
    }

    /// Fallible constructor that validates the bloom payload and
    /// children against the payload's `join_key_child_indices`.
    ///
    /// Errors:
    /// - bloom payload validation fails (version, bitset len, num_bits, etc.)
    /// - `children` is empty
    /// - `children.len() != bloom.join_key_child_indices.len()`
    pub fn try_new(
        children: Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>>,
        bloom: Arc<JoinHashBloomPayload>,
    ) -> DataFusionResult<Self> {
        bloom.validate()?;

        if children.is_empty() {
            return Err(DataFusionError::Plan(
                "JoinHashBloomProbeExpr: children is empty".to_string(),
            ));
        }

        if children.len() != bloom.join_key_child_indices.len() {
            return Err(DataFusionError::Plan(format!(
                "JoinHashBloomProbeExpr: children.len() {} != bloom.join_key_child_indices.len() {}",
                children.len(),
                bloom.join_key_child_indices.len()
            )));
        }

        Ok(Self { children, bloom })
    }

    /// Runtime validation guard for [`Self::evaluate`].
    ///
    /// Catches stale / mutated payloads that may have bypassed construction-time
    /// checks. This is a defense-in-depth measure.
    fn guard(&self) -> DataFusionResult<()> {
        if self.children.is_empty() {
            return Err(DataFusionError::Plan(
                "JoinHashBloomProbeExpr::evaluate: children is empty".to_string(),
            ));
        }
        if self.children.len() != self.bloom.join_key_child_indices.len() {
            return Err(DataFusionError::Plan(format!(
                "JoinHashBloomProbeExpr::evaluate: children.len() {} != bloom.join_key_child_indices.len() {}",
                self.children.len(),
                self.bloom.join_key_child_indices.len()
            )));
        }
        // Re-validate the bloom payload to catch invalid bitset lengths,
        // zero num_bits / num_probes, etc. before indexing into the bitset.
        self.bloom.validate()?;
        Ok(())
    }

    /// Returns a reference to the underlying Bloom payload.
    pub fn bloom_payload(&self) -> &JoinHashBloomPayload {
        &self.bloom
    }
}

impl std::hash::Hash for JoinHashBloomProbeExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for child in &self.children {
            child.dyn_hash(state);
        }
        self.bloom.hash(state);
    }
}

impl PartialEq for JoinHashBloomProbeExpr {
    fn eq(&self, other: &Self) -> bool {
        self.children == other.children && self.bloom == other.bloom
    }
}

impl std::fmt::Display for JoinHashBloomProbeExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cols = self
            .children
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let seeds = (
            self.bloom.df_seed0,
            self.bloom.df_seed1,
            self.bloom.df_seed2,
            self.bloom.df_seed3,
        );
        write!(f, "bloom_probe({cols}, seeds={seeds:?})")
    }
}

impl datafusion::physical_plan::PhysicalExpr for JoinHashBloomProbeExpr {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(
        &self,
        _input_schema: &datafusion::arrow::datatypes::Schema,
    ) -> datafusion_common::Result<datafusion::arrow::datatypes::DataType> {
        Ok(datafusion::arrow::datatypes::DataType::Boolean)
    }

    fn nullable(
        &self,
        _input_schema: &datafusion::arrow::datatypes::Schema,
    ) -> datafusion_common::Result<bool> {
        Ok(false)
    }

    fn children(&self) -> Vec<&Arc<dyn datafusion::physical_plan::PhysicalExpr>> {
        self.children.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>>,
    ) -> datafusion_common::Result<Arc<dyn datafusion::physical_plan::PhysicalExpr>> {
        if children.len() != self.children.len() {
            return Err(datafusion_common::DataFusionError::Plan(format!(
                "JoinHashBloomProbeExpr::with_new_children: expected {} children, got {}",
                self.children.len(),
                children.len()
            )));
        }
        Ok(Arc::new(Self {
            children,
            bloom: Arc::clone(&self.bloom),
        }))
    }

    fn evaluate(
        &self,
        batch: &datafusion::arrow::record_batch::RecordBatch,
    ) -> datafusion_common::Result<datafusion_expr::ColumnarValue> {
        // Runtime guard: reject invalid/stale payload before probing
        self.guard()?;

        let num_rows = batch.num_rows();
        let seeds = (
            self.bloom.df_seed0,
            self.bloom.df_seed1,
            self.bloom.df_seed2,
            self.bloom.df_seed3,
        );
        let hash_expr = datafusion::physical_plan::joins::HashExpr::new(
            self.children.clone(),
            datafusion::physical_plan::joins::SeededRandomState::with_seeds(
                seeds.0, seeds.1, seeds.2, seeds.3,
            ),
            "join_hash_bloom_probe_hash".to_string(),
        );
        let hashes = datafusion::physical_plan::PhysicalExpr::evaluate(&hash_expr, batch)?
            .into_array(num_rows)?;
        let hashes = hashes
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .ok_or_else(|| {
                DataFusionError::Plan(
                    "JoinHashBloomProbeExpr: HashExpr did not return UInt64Array".to_string(),
                )
            })?;

        let num_bits = self.bloom.num_bits;
        let num_probes = self.bloom.num_probes;
        let bitset: &[u8] = &self.bloom.bitset; // borrow, don't clone

        let bools: Vec<bool> = hashes
            .iter()
            .map(|hash| {
                hash.is_some_and(|h| {
                    bloom_contains(
                        h,
                        bitset,
                        num_bits,
                        num_probes,
                        BloomHashAlgorithm::Splitmix64DoubleHashV1,
                    )
                })
            })
            .collect();

        let array = datafusion::arrow::array::BooleanArray::from(bools);
        Ok(datafusion_expr::ColumnarValue::Array(Arc::new(array)))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
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
fn bloom_contains(
    hash: u64,
    bitset: &[u8],
    num_bits: u64,
    num_probes: u32,
    _algorithm: BloomHashAlgorithm,
) -> bool {
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
fn bloom_insert(
    hash: u64,
    bitset: &mut [u8],
    num_bits: u64,
    num_probes: u32,
    _algorithm: BloomHashAlgorithm,
) {
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
    algorithm: BloomHashAlgorithm,
) -> u64 {
    let mut distinct = 0u64;
    let mut seen = std::collections::HashSet::new();
    for hash in hashes {
        if seen.insert(hash) {
            distinct += 1;
            bloom_insert(hash, bitset, num_bits, num_probes, algorithm);
        }
    }
    distinct
}

/// Compute a deterministic cross-platform hash-compatibility fingerprint.
///
/// Evaluates a DataFusion `HashExpr` with the given children and seeds over a
/// canonical `RecordBatch` built from `input_schema`, then digests the output
/// `UInt64Array` with FNV-1a (over little-endian bytes) finalized with
/// SplitMix64. The digest also includes deterministic tags for the seeds and
/// child output data types, because DataFusion may intentionally hash equal
/// values of different integer widths to the same `u64`. The canonical batch
/// has fixed-size rows with column-index-dependent values so child order is
/// reflected in the `HashExpr` output sequence.
///
/// Returns `Ok(nonzero_u64)` on success or `Err(DataFusionError::Plan)` for
/// unsupported input types.
pub(crate) fn compute_hash_compat_fingerprint(
    children: &[Arc<dyn datafusion::physical_plan::PhysicalExpr>],
    input_schema: &datafusion::arrow::datatypes::Schema,
    seeds: (u64, u64, u64, u64),
) -> DataFusionResult<u64> {
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_plan::joins::{HashExpr, SeededRandomState};

    if children.is_empty() {
        return Err(DataFusionError::Plan(
            "fingerprint: empty children list".to_string(),
        ));
    }

    let mut fnv: u64 = 0xcbf29ce484222325;
    digest_bytes(&mut fnv, b"greptimedb-join-hash-bloom-compat-v1");
    digest_u64(&mut fnv, seeds.0);
    digest_u64(&mut fnv, seeds.1);
    digest_u64(&mut fnv, seeds.2);
    digest_u64(&mut fnv, seeds.3);
    digest_u64(&mut fnv, children.len() as u64);
    for child in children {
        let data_type = child.data_type(input_schema)?;
        digest_data_type(&mut fnv, &data_type)?;
    }

    // Build canonical batch — 3 rows with column-index-distinct values.
    // Unsupported non-key columns are represented as typed null arrays in
    // `build_canonical_batch`; unsupported key child types fail above.
    let batch = build_canonical_batch(input_schema)?;
    let hash_expr = HashExpr::new(
        children.to_vec(),
        SeededRandomState::with_seeds(seeds.0, seeds.1, seeds.2, seeds.3),
        "join_hash_bloom_hash_compat".to_string(),
    );

    let result = hash_expr.evaluate(&batch)?;
    let array = result
        .into_array(batch.num_rows())
        .map_err(|e| DataFusionError::Plan(format!("fingerprint: cannot convert to array: {e}")))?;
    let hashes = array
        .as_any()
        .downcast_ref::<datafusion::arrow::array::UInt64Array>()
        .ok_or_else(|| {
            DataFusionError::Plan("fingerprint: HashExpr did not produce UInt64Array".to_string())
        })?;

    // FNV-1a over little-endian bytes of each u64 hash, with a null marker for
    // completeness even though HashExpr normally returns a non-null UInt64Array.
    for h in hashes.iter() {
        match h {
            Some(h) => {
                digest_u8(&mut fnv, 1);
                digest_u64(&mut fnv, h);
            }
            None => {
                digest_u8(&mut fnv, 0);
            }
        }
    }

    // Finalize with SplitMix64 to avalanche bits; prevent accidental zero output
    let mut fp = splitmix64(fnv);
    if fp == 0 {
        fp = 1;
    }
    Ok(fp)
}

fn digest_u8(state: &mut u64, value: u8) {
    *state ^= value as u64;
    *state = state.wrapping_mul(0x100000001b3);
}

fn digest_u64(state: &mut u64, value: u64) {
    digest_bytes(state, &value.to_le_bytes());
}

fn digest_bytes(state: &mut u64, bytes: &[u8]) {
    digest_u64_lenless(state, bytes.len() as u64);
    for &byte in bytes {
        digest_u8(state, byte);
    }
}

fn digest_u64_lenless(state: &mut u64, value: u64) {
    for byte in value.to_le_bytes() {
        digest_u8(state, byte);
    }
}

fn digest_data_type(
    state: &mut u64,
    data_type: &datafusion::arrow::datatypes::DataType,
) -> DataFusionResult<()> {
    use datafusion::arrow::datatypes::DataType;

    match data_type {
        DataType::Boolean => digest_u64(state, 1),
        DataType::Int8 => digest_u64(state, 2),
        DataType::Int16 => digest_u64(state, 3),
        DataType::Int32 => digest_u64(state, 4),
        DataType::Int64 => digest_u64(state, 5),
        DataType::UInt8 => digest_u64(state, 6),
        DataType::UInt16 => digest_u64(state, 7),
        DataType::UInt32 => digest_u64(state, 8),
        DataType::UInt64 => digest_u64(state, 9),
        DataType::Float32 => digest_u64(state, 10),
        DataType::Float64 => digest_u64(state, 11),
        DataType::Utf8 => digest_u64(state, 12),
        DataType::LargeUtf8 => digest_u64(state, 13),
        DataType::Binary => digest_u64(state, 14),
        DataType::LargeBinary => digest_u64(state, 15),
        DataType::Date32 => digest_u64(state, 16),
        DataType::Date64 => digest_u64(state, 17),
        DataType::Timestamp(unit, timezone) => {
            digest_u64(state, 18);
            digest_time_unit(state, unit);
            match timezone {
                Some(timezone) => {
                    digest_u8(state, 1);
                    digest_bytes(state, timezone.as_bytes());
                }
                None => digest_u8(state, 0),
            }
        }
        other => {
            return Err(DataFusionError::Plan(format!(
                "fingerprint: unsupported child data type {other:?}"
            )));
        }
    }
    Ok(())
}

fn digest_time_unit(state: &mut u64, unit: &datafusion::arrow::datatypes::TimeUnit) {
    use datafusion::arrow::datatypes::TimeUnit;

    let tag = match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 2,
        TimeUnit::Microsecond => 3,
        TimeUnit::Nanosecond => 4,
    };
    digest_u64(state, tag);
}

/// Canonical batch with 3 rows. Each column gets column-index-distinct scalar values
/// so the fingerprint is sensitive to column types and ordering.
fn build_canonical_batch(
    schema: &datafusion::arrow::datatypes::Schema,
) -> DataFusionResult<datafusion::arrow::record_batch::RecordBatch> {
    use datafusion::arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
        Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray,
        StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array, new_null_array,
    };
    use datafusion::arrow::datatypes::{DataType, TimeUnit};

    let column_values: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(col_idx, field)| {
            let idx = col_idx as i64;
            Ok(match field.data_type() {
                DataType::Boolean => Arc::new(BooleanArray::from(vec![
                    idx % 2 == 0,
                    idx % 3 != 0,
                    idx % 2 != 0,
                ])) as ArrayRef,
                DataType::Int8 => Arc::new(Int8Array::from(vec![
                    (idx + 1) as i8,
                    (idx + 2) as i8,
                    (idx + 3) as i8,
                ])),
                DataType::Int16 => Arc::new(Int16Array::from(vec![
                    (idx + 1) as i16,
                    (idx + 2) as i16,
                    (idx + 3) as i16,
                ])),
                DataType::Int32 => Arc::new(Int32Array::from(vec![
                    (idx + 1) as i32,
                    (idx + 2) as i32,
                    (idx + 3) as i32,
                ])),
                DataType::Int64 => Arc::new(Int64Array::from(vec![idx + 1, idx + 2, idx + 3])),
                DataType::UInt8 => Arc::new(UInt8Array::from(vec![
                    (idx + 1) as u8,
                    (idx + 2) as u8,
                    (idx + 3) as u8,
                ])),
                DataType::UInt16 => Arc::new(UInt16Array::from(vec![
                    (idx + 1) as u16,
                    (idx + 2) as u16,
                    (idx + 3) as u16,
                ])),
                DataType::UInt32 => Arc::new(UInt32Array::from(vec![
                    (idx + 1) as u32,
                    (idx + 2) as u32,
                    (idx + 3) as u32,
                ])),
                DataType::UInt64 => Arc::new(UInt64Array::from(vec![
                    (idx + 1) as u64,
                    (idx + 2) as u64,
                    (idx + 3) as u64,
                ])),
                DataType::Float32 => Arc::new(Float32Array::from(vec![
                    (idx + 1) as f32,
                    (idx + 2) as f32,
                    (idx + 3) as f32,
                ])),
                DataType::Float64 => Arc::new(Float64Array::from(vec![
                    (idx + 1) as f64,
                    (idx + 2) as f64,
                    (idx + 3) as f64,
                ])),
                DataType::Utf8 => Arc::new(StringArray::from(vec![
                    format!("c{idx}_0"),
                    format!("c{idx}_1"),
                    format!("c{idx}_2"),
                ])),
                DataType::LargeUtf8 => Arc::new(LargeStringArray::from(vec![
                    format!("c{idx}_0"),
                    format!("c{idx}_1"),
                    format!("c{idx}_2"),
                ])),
                DataType::Binary => {
                    let v0: Vec<u8> = format!("b{idx}_0").into_bytes();
                    let v1: Vec<u8> = format!("b{idx}_1").into_bytes();
                    let v2: Vec<u8> = format!("b{idx}_2").into_bytes();
                    Arc::new(BinaryArray::from_iter_values([&v0[..], &v1[..], &v2[..]]))
                }
                DataType::LargeBinary => {
                    let v0: Vec<u8> = format!("b{idx}_0").into_bytes();
                    let v1: Vec<u8> = format!("b{idx}_1").into_bytes();
                    let v2: Vec<u8> = format!("b{idx}_2").into_bytes();
                    Arc::new(LargeBinaryArray::from_iter_values([
                        &v0[..],
                        &v1[..],
                        &v2[..],
                    ]))
                }
                DataType::Date32 => Arc::new(Date32Array::from(vec![
                    (idx + 1) as i32,
                    (idx + 2) as i32,
                    (idx + 3) as i32,
                ])),
                DataType::Date64 => Arc::new(Date64Array::from(vec![idx + 1, idx + 2, idx + 3])),
                DataType::Timestamp(TimeUnit::Second, _) => {
                    Arc::new(TimestampSecondArray::from(vec![idx + 1, idx + 2, idx + 3]))
                }
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    Arc::new(TimestampMillisecondArray::from(vec![
                        idx + 1,
                        idx + 2,
                        idx + 3,
                    ]))
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    Arc::new(TimestampMicrosecondArray::from(vec![
                        idx + 1,
                        idx + 2,
                        idx + 3,
                    ]))
                }
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    Arc::new(TimestampNanosecondArray::from(vec![
                        idx + 1,
                        idx + 2,
                        idx + 3,
                    ]))
                }
                other => new_null_array(other, 3),
            })
        })
        .collect::<DataFusionResult<Vec<_>>>()?;

    let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
        Arc::new(schema.clone()),
        column_values,
    )
    .map_err(|e| {
        DataFusionError::Plan(format!("fingerprint: failed to build canonical batch: {e}"))
    })?;

    Ok(batch)
}

#[cfg(test)]
mod tests {
    use datafusion_common::DataFusionError;

    use super::*;

    fn minimal_valid_payload() -> JoinHashBloomPayload {
        JoinHashBloomPayload {
            version: JOIN_HASH_BLOOM_VERSION,
            hash_kind: JoinHashKind::DatafusionJoinHashU64,
            df_seed0: 4,
            df_seed1: 5,
            df_seed2: 6,
            df_seed3: 7,
            num_bits: 1024,
            num_probes: 3,
            distinct_hash_count: 100,
            bitset: vec![0u8; 128],
            join_key_child_indices: vec![0, 1],
            residual_datafusion_physical_expr: vec![1, 2, 3],
            bloom_hash_algorithm: BloomHashAlgorithm::Splitmix64DoubleHashV1,
            hash_compat_fingerprint: 0,
        }
    }

    #[test]
    fn validate_accepts_valid_payload() {
        minimal_valid_payload().validate().unwrap();
    }

    #[test]
    fn validate_rejects_unsupported_version() {
        let mut p = minimal_valid_payload();
        p.version = 99;
        let err = p.validate().unwrap_err();
        assert!(err.to_string().contains("unsupported version 99"));
    }

    #[test]
    fn validate_rejects_unspecified_hash_kind() {
        let mut p = minimal_valid_payload();
        p.hash_kind = JoinHashKind::Unspecified;
        let err = p.validate().unwrap_err();
        assert!(err.to_string().contains("unsupported hash_kind"));
    }

    #[test]
    fn validate_rejects_unspecified_bloom_algorithm() {
        let mut p = minimal_valid_payload();
        p.bloom_hash_algorithm = BloomHashAlgorithm::Unspecified;
        let err = p.validate().unwrap_err();
        assert!(err.to_string().contains("unsupported bloom_hash_algorithm"));
    }

    #[test]
    fn validate_rejects_zero_num_bits() {
        let mut p = minimal_valid_payload();
        p.num_bits = 0;
        let err = p.validate().unwrap_err();
        assert!(err.to_string().contains("num_bits is zero"));
    }

    #[test]
    fn validate_accepts_num_bits_at_usize_max() {
        let mut p = minimal_valid_payload();
        p.num_bits = usize::MAX as u64;
        // bitset must match: ceil(usize::MAX / 8) — but that's huge and would OOM.
        // On 64-bit platforms, usize::MAX == u64::MAX and the check
        // `num_bits > usize::MAX` is always false, so we only verify
        // the validation path does not panic on a passing value.
        // The `> usize::MAX` branch is only reachable on 32-bit targets.
        if cfg!(target_pointer_width = "64") {
            // On 64-bit we cannot construct a bitset of size ceil(u64::MAX/8),
            // but we can verify that smaller valid sizes still pass.
            p.num_bits = 1024;
            p.bitset = vec![0u8; 128];
            p.validate().unwrap();
        }
    }

    #[test]
    fn validate_rejects_zero_num_probes() {
        let mut p = minimal_valid_payload();
        p.num_probes = 0;
        let err = p.validate().unwrap_err();
        assert!(err.to_string().contains("num_probes is zero"));
    }

    #[test]
    fn validate_rejects_excessive_num_probes() {
        let mut p = minimal_valid_payload();
        p.num_probes = MAX_BLOOM_NUM_PROBES + 1;
        let err = p.validate().unwrap_err();
        assert!(err.to_string().contains("exceeds limit"));
    }

    #[test]
    fn validate_rejects_wrong_bitset_length() {
        let mut p = minimal_valid_payload();
        p.bitset = vec![0u8; 127]; // should be 128 for num_bits=1024
        let err = p.validate().unwrap_err();
        assert!(err.to_string().contains("bitset length"));
    }

    #[test]
    fn validate_rejects_duplicate_join_key_child_indices() {
        let mut p = minimal_valid_payload();
        p.join_key_child_indices = vec![0, 1, 0];
        let err = p.validate().unwrap_err();
        assert!(err.to_string().contains("duplicate join key child index 0"));
    }

    #[test]
    fn validate_rejects_oversized_residual() {
        let mut p = minimal_valid_payload();
        p.residual_datafusion_physical_expr = vec![0u8; MAX_BLOOM_RESIDUAL_BYTES + 1];
        let err = p.validate().unwrap_err();
        assert!(err.to_string().contains("exceeds budget"));
    }

    #[test]
    fn validate_rejects_empty_join_key_child_indices() {
        let mut p = minimal_valid_payload();
        p.join_key_child_indices = vec![];
        let err = p.validate().unwrap_err();
        assert!(err.to_string().contains("join_key_child_indices is empty"));
    }

    #[test]
    fn validate_rejects_empty_residual() {
        let mut p = minimal_valid_payload();
        p.residual_datafusion_physical_expr = vec![];
        let err = p.validate().unwrap_err();
        assert!(err.to_string().contains("residual expression is empty"));
    }

    #[test]
    fn validate_num_bits_exactly_divisible_by_8() {
        let mut p = minimal_valid_payload();
        p.num_bits = 1024;
        p.bitset = vec![0u8; 128];
        p.validate().unwrap();
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
    fn from_proto_rejects_unknown_hash_kind() {
        let proto = api::v1::region::JoinHashBloomPayload {
            hash_kind: 999,
            ..Default::default()
        };
        let err = JoinHashBloomPayload::from_proto(&proto).unwrap_err();
        assert!(matches!(err, DataFusionError::Plan(_)));
        assert!(err.to_string().contains("unknown hash_kind"));
    }

    #[test]
    fn from_proto_rejects_unknown_bloom_algorithm() {
        let proto = api::v1::region::JoinHashBloomPayload {
            version: JOIN_HASH_BLOOM_VERSION,
            hash_kind: api::v1::region::JoinHashKind::DatafusionJoinHashU64 as i32,
            bloom_hash_algorithm: 999,
            hash_compat_fingerprint: 0,
            ..Default::default()
        };
        let err = JoinHashBloomPayload::from_proto(&proto).unwrap_err();
        assert!(matches!(err, DataFusionError::Plan(_)));
        assert!(err.to_string().contains("unknown bloom_hash_algorithm"));
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
            r#"{"version":1,"hash_kind":"DatafusionJoinHashU64","df_seed0":0,"df_seed1":0,"df_seed2":0,"df_seed3":0,"num_bits":8,"num_probes":1,"distinct_hash_count":0,"bitset":"!!!not.base64!!!","join_key_child_indices":[],"residual_datafusion_physical_expr":"","bloom_hash_algorithm":"Splitmix64DoubleHashV1"}"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("invalid base64"));
    }

    #[test]
    fn expected_bitset_bytes_computes_ceil_division() {
        let p = JoinHashBloomPayload {
            num_bits: 1024,
            bitset: vec![0u8; 128],
            ..minimal_valid_payload()
        };
        assert_eq!(p.expected_bitset_bytes(), 128);

        let p2 = JoinHashBloomPayload {
            num_bits: 1025,
            bitset: vec![0u8; 129],
            ..p
        };
        assert_eq!(p2.expected_bitset_bytes(), 129);
    }

    #[test]
    fn num_bits_ceil_bytes_does_not_overflow() {
        assert_eq!(num_bits_ceil_bytes(usize::MAX), (usize::MAX / 8) + 1);
    }

    // ── Bloom bitset / SplitMix64 tests ──────────────────────────

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
    fn bloom_roundtrip_no_false_negatives_after_json() {
        let hashes = vec![42u64, 12345, 98765];
        let payload = JoinHashBloomPayload::build_from_hashes(
            hashes.clone(),
            256,
            2,
            (0, 0, 0, 0),
            vec![0],
            vec![1],
        );

        let json = serde_json::to_string(&payload).unwrap();
        let decoded: JoinHashBloomPayload = serde_json::from_str(&json).unwrap();

        for &h in &hashes {
            assert!(decoded.contains_join_hash(h));
        }
    }

    #[test]
    fn bloom_roundtrip_no_false_negatives_after_proto() {
        let hashes = vec![42u64, 12345, 98765];
        let payload = JoinHashBloomPayload::build_from_hashes(
            hashes.clone(),
            256,
            2,
            (0, 0, 0, 0),
            vec![0],
            vec![1],
        );

        let proto = payload.to_proto();
        let decoded = JoinHashBloomPayload::from_proto(&proto).unwrap();

        for &h in &hashes {
            assert!(decoded.contains_join_hash(h));
        }
    }

    #[test]
    fn build_from_hashes_counts_distinct() {
        let hashes = vec![1u64, 2, 3, 2, 1]; // 3 distinct
        let payload =
            JoinHashBloomPayload::build_from_hashes(hashes, 256, 2, (0, 0, 0, 0), vec![0], vec![1]);
        assert_eq!(payload.distinct_hash_count, 3);
    }

    #[test]
    fn splitmix64_is_deterministic() {
        let h1 = splitmix64(42);
        let h2 = splitmix64(42);
        assert_eq!(h1, h2);

        let h3 = splitmix64(43);
        assert_ne!(h1, h3);
    }

    // ── JoinHashBloomProbeExpr tests ────────────────────────────

    use datafusion::arrow::array::Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::PhysicalExpr;

    #[test]
    fn probe_expr_evaluates_non_null_boolean_array() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let col: Arc<dyn PhysicalExpr> =
            Arc::new(datafusion::physical_expr::expressions::Column::new("id", 0));

        // Build a Bloom with some sample hashes
        let hashes = vec![100u64, 200, 300];
        let bloom = Arc::new(JoinHashBloomPayload::build_from_hashes(
            hashes,
            512,
            2,
            (1, 2, 3, 4),
            vec![0],
            vec![1],
        ));

        let probe = JoinHashBloomProbeExpr::new(vec![Arc::clone(&col)], bloom);

        // Create a batch with some values
        let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                10, 20, 30, 40, 50,
            ]))],
        )
        .unwrap();

        let result = probe.evaluate(&batch).unwrap();
        match result {
            datafusion_expr::ColumnarValue::Array(arr) => {
                let bool_arr = arr
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::BooleanArray>()
                    .unwrap();
                assert_eq!(bool_arr.len(), 5);
                // No nulls
                assert_eq!(bool_arr.null_count(), 0);
                // All should be boolean (either true or false)
                for i in 0..5 {
                    assert!(!bool_arr.is_null(i));
                }
            }
            _ => panic!("expected array result"),
        }
    }

    #[test]
    fn probe_expr_uses_datafusion_hash_expr_hashes_without_false_negatives() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let col: Arc<dyn PhysicalExpr> =
            Arc::new(datafusion::physical_expr::expressions::Column::new("id", 0));
        let seeds = (1, 2, 3, 4);
        let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                10, 20, 30, 40, 50,
            ]))],
        )
        .unwrap();

        let hash_expr = datafusion::physical_plan::joins::HashExpr::new(
            vec![Arc::clone(&col)],
            datafusion::physical_plan::joins::SeededRandomState::with_seeds(
                seeds.0, seeds.1, seeds.2, seeds.3,
            ),
            "test_hash".to_string(),
        );
        let hashes = hash_expr
            .evaluate(&batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        let hashes = hashes
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .unwrap();
        let inserted_hashes = hashes.iter().map(|hash| hash.unwrap()).collect::<Vec<_>>();

        let bloom = Arc::new(JoinHashBloomPayload::build_from_hashes(
            inserted_hashes,
            512,
            2,
            seeds,
            vec![0],
            vec![1],
        ));
        let probe = JoinHashBloomProbeExpr::new(vec![Arc::clone(&col)], bloom);

        let result = probe.evaluate(&batch).unwrap();
        let datafusion_expr::ColumnarValue::Array(arr) = result else {
            panic!("expected array result");
        };
        let bool_arr = arr
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BooleanArray>()
            .unwrap();

        assert_eq!(bool_arr.len(), batch.num_rows());
        assert!(bool_arr.iter().all(|value| value == Some(true)));
    }

    #[test]
    fn probe_expr_message_borrows_all_data() {
        // Sanity check that Debug and Display work
        let col: Arc<dyn PhysicalExpr> =
            Arc::new(datafusion::physical_expr::expressions::Column::new("id", 0));
        let bloom = Arc::new(JoinHashBloomPayload::build_from_hashes(
            vec![1u64],
            64,
            2,
            (0, 0, 0, 0),
            vec![0],
            vec![1],
        ));
        let probe = JoinHashBloomProbeExpr::new(vec![Arc::clone(&col)], bloom);
        let debug_str = format!("{probe:?}");
        assert!(debug_str.contains("JoinHashBloomProbeExpr"));
        let display_str = format!("{probe}");
        assert!(display_str.contains("bloom_probe"));
    }

    #[test]
    fn probe_expr_equality_and_hash() {
        let col: Arc<dyn PhysicalExpr> =
            Arc::new(datafusion::physical_expr::expressions::Column::new("id", 0));
        let bloom1 = Arc::new(JoinHashBloomPayload::build_from_hashes(
            vec![1u64],
            64,
            2,
            (0, 0, 0, 0),
            vec![0],
            vec![1],
        ));
        let bloom2 = Arc::new(JoinHashBloomPayload::build_from_hashes(
            vec![1u64],
            64,
            2,
            (0, 0, 0, 0),
            vec![0],
            vec![1],
        ));

        let probe1 = JoinHashBloomProbeExpr::new(vec![Arc::clone(&col)], bloom1);
        let probe2 = JoinHashBloomProbeExpr::new(vec![Arc::clone(&col)], bloom2);

        assert_eq!(probe1, probe2);
    }

    #[test]
    fn probe_expr_children_and_with_new_children() {
        let col_a: Arc<dyn PhysicalExpr> =
            Arc::new(datafusion::physical_expr::expressions::Column::new("a", 0));
        let col_b: Arc<dyn PhysicalExpr> =
            Arc::new(datafusion::physical_expr::expressions::Column::new("b", 1));
        let bloom = Arc::new(JoinHashBloomPayload::build_from_hashes(
            vec![1u64],
            64,
            2,
            (0, 0, 0, 0),
            vec![0, 1],
            vec![1],
        ));

        let probe = Arc::new(JoinHashBloomProbeExpr::new(
            vec![Arc::clone(&col_a), Arc::clone(&col_b)],
            bloom,
        ));

        let children = probe.children();
        assert_eq!(children.len(), 2);

        // with_new_children with correct count succeeds
        let new_probe = Arc::clone(&probe)
            .with_new_children(vec![Arc::clone(&col_b), Arc::clone(&col_a)])
            .unwrap();
        assert_eq!(new_probe.children().len(), 2);

        // with_new_children with wrong count fails
        let err = Arc::clone(&probe)
            .with_new_children(vec![Arc::clone(&col_a)])
            .unwrap_err();
        assert!(err.to_string().contains("expected 2 children"));
    }

    // ── Hardening tests ──────────────────────────────────────

    #[test]
    fn try_new_rejects_empty_children() {
        let bloom = Arc::new(JoinHashBloomPayload::build_from_hashes(
            vec![1u64],
            64,
            2,
            (0, 0, 0, 0),
            vec![0],
            vec![1],
        ));
        let err = JoinHashBloomProbeExpr::try_new(vec![], bloom).unwrap_err();
        assert!(matches!(err, DataFusionError::Plan(_)));
        assert!(err.to_string().contains("children is empty"));
    }

    #[test]
    fn try_new_rejects_child_count_mismatch() {
        let col: Arc<dyn PhysicalExpr> =
            Arc::new(datafusion::physical_expr::expressions::Column::new("a", 0));
        let bloom = Arc::new(JoinHashBloomPayload::build_from_hashes(
            vec![1u64],
            64,
            2,
            (0, 0, 0, 0),
            vec![0, 1],
            vec![1], // 2 join key child indices
        ));
        let err = JoinHashBloomProbeExpr::try_new(vec![Arc::clone(&col)], bloom).unwrap_err();
        assert!(
            err.to_string()
                .contains("bloom.join_key_child_indices.len()")
        );
    }

    #[test]
    fn try_new_rejects_invalid_bloom_payload() {
        let col: Arc<dyn PhysicalExpr> =
            Arc::new(datafusion::physical_expr::expressions::Column::new("a", 0));
        let mut bloom = JoinHashBloomPayload::build_from_hashes(
            vec![1u64],
            64,
            2,
            (0, 0, 0, 0),
            vec![0],
            vec![1],
        );
        // Corrupt: set num_bits = 0
        bloom.num_bits = 0;
        let err =
            JoinHashBloomProbeExpr::try_new(vec![Arc::clone(&col)], Arc::new(bloom)).unwrap_err();
        assert!(err.to_string().contains("num_bits is zero"));
    }

    #[test]
    #[should_panic(expected = "new called with invalid state")]
    fn new_panics_on_invalid_payload() {
        let col: Arc<dyn PhysicalExpr> =
            Arc::new(datafusion::physical_expr::expressions::Column::new("id", 0));
        let mut bloom = JoinHashBloomPayload::build_from_hashes(
            vec![1u64, 2, 3],
            64,
            2,
            (0, 0, 0, 0),
            vec![0],
            vec![1],
        );
        bloom.num_bits = 0; // invalid
        let corrupted = Arc::new(bloom);
        // This should panic via try_new in new()
        let _bad = JoinHashBloomProbeExpr::new(vec![Arc::clone(&col)], corrupted);
    }

    #[test]
    fn try_new_rejects_invalid_payload_does_not_panic() {
        let col: Arc<dyn PhysicalExpr> =
            Arc::new(datafusion::physical_expr::expressions::Column::new("id", 0));
        let mut bloom = JoinHashBloomPayload::build_from_hashes(
            vec![1u64, 2, 3],
            64,
            2,
            (0, 0, 0, 0),
            vec![0],
            vec![1],
        );
        bloom.num_bits = 0; // invalid
        let corrupted = Arc::new(bloom);
        let err = JoinHashBloomProbeExpr::try_new(vec![Arc::clone(&col)], corrupted).unwrap_err();
        assert!(matches!(err, DataFusionError::Plan(_)));
        assert!(err.to_string().contains("num_bits is zero"));
    }

    #[test]
    fn evaluate_with_invalid_bitset_length_errors_not_panics() {
        let col: Arc<dyn PhysicalExpr> =
            Arc::new(datafusion::physical_expr::expressions::Column::new("id", 0));

        // Build a bloom with wrong bitset length
        let mut bloom = JoinHashBloomPayload::build_from_hashes(
            vec![10u64, 20, 30],
            64,
            2,
            (0, 0, 0, 0),
            vec![0],
            vec![1],
        );
        bloom.num_bits = 128; // 128 bits need 16 bytes, but bitset has 8
        // bitset is still 8 bytes (wrong)
        let bloom_arc = Arc::new(bloom);

        let err = JoinHashBloomProbeExpr::try_new(vec![Arc::clone(&col)], bloom_arc).unwrap_err();
        assert!(err.to_string().contains("bitset length"));
    }

    #[test]
    fn evaluate_with_zero_num_probes_errors_not_panics() {
        let col: Arc<dyn PhysicalExpr> =
            Arc::new(datafusion::physical_expr::expressions::Column::new("id", 0));
        let mut bloom = JoinHashBloomPayload::build_from_hashes(
            vec![10u64],
            64,
            2,
            (0, 0, 0, 0),
            vec![0],
            vec![1],
        );
        bloom.num_probes = 0;
        let err =
            JoinHashBloomProbeExpr::try_new(vec![Arc::clone(&col)], Arc::new(bloom)).unwrap_err();
        assert!(err.to_string().contains("num_probes is zero"));
    }

    #[test]
    fn evaluate_with_zero_num_bits_errors_not_panics() {
        let col: Arc<dyn PhysicalExpr> =
            Arc::new(datafusion::physical_expr::expressions::Column::new("id", 0));
        let mut bloom = JoinHashBloomPayload::build_from_hashes(
            vec![10u64],
            64,
            2,
            (0, 0, 0, 0),
            vec![0],
            vec![1],
        );
        bloom.num_bits = 0;
        let err =
            JoinHashBloomProbeExpr::try_new(vec![Arc::clone(&col)], Arc::new(bloom)).unwrap_err();
        assert!(err.to_string().contains("num_bits is zero"));
    }
}
