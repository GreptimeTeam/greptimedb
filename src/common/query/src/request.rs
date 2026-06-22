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

mod base64_serde;
mod initial_remote_dyn_filter_reg;
mod join_hash_bloom;

use std::sync::Arc;

use api::v1::region::RegionRequestHeader;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, InListExpr, lit};
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::joins::HashTableLookupExpr;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_expr::{LogicalPlan, Operator};
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use datafusion_proto::protobuf::PhysicalExprNode;
use prost::Message;
use serde::{Deserialize, Serialize};
use snafu::ensure;
use store_api::storage::RegionId;

/// Current wire-format version for remote dynamic filter payload updates.
pub use self::initial_remote_dyn_filter_reg::{
    INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY,
    INITIAL_REMOTE_DYN_FILTER_REGS_MAX_TOTAL_PROTO_BYTES, InitialDynFilterReg,
    InitialDynFilterRegs, InitialDynFilterSnapshot,
};
pub use self::join_hash_bloom::{
    BLOOM_ENCODER_BITS_PER_HASH, BLOOM_ENCODER_MAX_NUM_BITS_BUDGET, BLOOM_ENCODER_MIN_NUM_BITS,
    BLOOM_ENCODER_NUM_PROBES, BLOOM_PROTO_ENVELOPE_OVERHEAD_ESTIMATE, BloomHashAlgorithm,
    JOIN_HASH_BLOOM_VERSION, JoinHashBloomPayload, JoinHashBloomProbeExpr, JoinHashKind,
    MAX_BLOOM_NUM_PROBES, MAX_BLOOM_RESIDUAL_BYTES,
};
use crate::error::{DynFilterPayloadTooLargeSnafu, Error as CommonQueryError};

pub const DYN_FILTER_PROTOCOL_VERSION: u32 = 1;

/// Serialized predicate payload for remote dynamic filter updates.
///
/// The payload is tagged in JSON so receivers can reject unsupported encodings
/// before decoding engine-specific bytes. For DataFusion expressions the
/// `payload` bytes are serialized by `serde_json` as a base64 string, for example:
///
/// ```json
/// { "kind": "datafusion", "payload": "CQgH" }
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
pub enum DynFilterPayload {
    /// A serialized DataFusion [`PhysicalExpr`] encoded as a protobuf
    /// [`PhysicalExprNode`].
    Datafusion(#[serde(with = "base64_serde::bytes")] Vec<u8>),
    /// A join-hash Bloom filter over DataFusion `u64` join hashes.
    JoinHashBloom(JoinHashBloomPayload),
}

impl DynFilterPayload {
    /// Validates payload-level invariants that can be checked without a receiver schema.
    pub fn validate(&self) -> DataFusionResult<()> {
        match self {
            Self::Datafusion(_) => Ok(()),
            Self::JoinHashBloom(bloom) => bloom.validate(),
        }
    }

    /// Encodes a DataFusion physical expression into a bounded dynamic filter payload.
    ///
    /// Runtime-only hash lookup predicates are degraded to `true` before encoding so
    /// serializable min/max bounds around them can still be shipped to remote scans.
    /// If the full serializable predicate is still larger than `max_payload_bytes`, large
    /// membership predicates (`IN (...)`) are also degraded to `true` as a bounds-only fallback.
    pub fn from_datafusion_expr(
        expr: &Arc<dyn PhysicalExpr>,
        max_payload_bytes: usize,
    ) -> DataFusionResult<Self> {
        match encode_remote_dyn_filter_expr(expr, max_payload_bytes, false) {
            Ok(bytes) => Ok(Self::Datafusion(bytes)),
            Err(CommonQueryError::DynFilterPayloadTooLarge { .. }) => {
                encode_remote_dyn_filter_expr(expr, max_payload_bytes, true)
                    .map(Self::Datafusion)
                    .map_err(DataFusionError::from)
            }
            Err(error) => Err(DataFusionError::from(error)),
        }
    }

    /// Encodes a DataFusion physical expression into a bounded dynamic filter payload
    /// with registered children context, attempting Bloom conversion for eligible
    /// `HashTableLookupExpr` conjuncts.
    ///
    /// When the expression is a flattened AND conjunction containing exactly one
    /// `HashTableLookupExpr` as a positive conjunct, the encoder attempts to build a
    /// Bloom payload with a hash-compat fingerprint.  If Bloom conversion is infeasible
    /// (ArrayMap, child mismatch, budget exceeded, unsupported structure, fingerprint error),
    /// it falls back to a fail-open `Datafusion` payload that drops unsafe lookup-containing
    /// conjuncts rather than replacing nested lookups in-place.
    pub fn from_datafusion_expr_with_registered_children(
        expr: &Arc<dyn PhysicalExpr>,
        registered_children: &[Arc<dyn PhysicalExpr>],
        max_payload_bytes: usize,
        input_schema: &datafusion::arrow::datatypes::Schema,
    ) -> DataFusionResult<Self> {
        match try_encode_bloom_payload(expr, registered_children, max_payload_bytes, input_schema) {
            Ok(bloom) => Ok(Self::JoinHashBloom(bloom)),
            Err(_fallback) => {
                encode_fail_open_remote_dyn_filter_expr(expr, max_payload_bytes, false)
                    .map(Self::Datafusion)
                    .or_else(|error| match error {
                        CommonQueryError::DynFilterPayloadTooLarge { .. } => {
                            encode_fail_open_remote_dyn_filter_expr(expr, max_payload_bytes, true)
                                .map(Self::Datafusion)
                        }
                        error => Err(error),
                    })
                    .map_err(DataFusionError::from)
            }
        }
    }

    /// Decodes a DataFusion dynamic filter payload against the provided input schema.
    ///
    /// The decoded expression is validated to ensure column indexes stay within the receiver-side
    /// schema, column names are consistent (defensive check), and the payload stays within
    /// `max_payload_bytes`.
    pub fn decode_datafusion_expr(
        &self,
        task_ctx: &TaskContext,
        input_schema: &datafusion::arrow::datatypes::Schema,
        max_payload_bytes: usize,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        let Self::Datafusion(bytes) = self else {
            return Err(DataFusionError::Plan(
                "DynFilterPayload::decode_datafusion_expr called on non-Datafusion payload"
                    .to_string(),
            ));
        };
        validate_payload_size(bytes.len(), max_payload_bytes).map_err(DataFusionError::from)?;
        let codec = DefaultPhysicalExtensionCodec {};
        let proto = PhysicalExprNode::decode(bytes.as_slice()).map_err(|e| {
            DataFusionError::Internal(format!("Failed to decode PhysicalExprNode: {e}"))
        })?;

        let expr = parse_physical_expr(&proto, task_ctx, input_schema, &codec)?;
        validate_supported_payload_expr(&expr)?;
        validate_decoded_payload_expr(&expr, input_schema)?;
        Ok(expr)
    }

    /// Decodes this payload into a single DataFusion physical expression using
    /// the registered dynamic-filter children for context.
    ///
    /// This is a convenience wrapper around [`Self::decode_placement_aware`] that
    /// combines pushdown and exact expressions with AND for callers that do not
    /// need placement-aware splitting (e.g. legacy tests or single-filter usage).
    pub fn decode_expr_with_registered_children(
        &self,
        task_ctx: &TaskContext,
        input_schema: &datafusion::arrow::datatypes::Schema,
        registered_children: &[Arc<dyn PhysicalExpr>],
        max_payload_bytes: usize,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        let decoded = self.decode_placement_aware(
            task_ctx,
            input_schema,
            registered_children,
            max_payload_bytes,
        )?;
        match decoded.exact_expr {
            Some(exact) => Ok(Arc::new(BinaryExpr::new(
                decoded.pushdown_expr,
                Operator::And,
                exact,
            ))),
            None => Ok(decoded.pushdown_expr),
        }
    }

    /// Decodes this payload into placement-aware parts.
    ///
    /// See [`DecodedDynFilterExprs`] for semantics.
    pub fn decode_placement_aware(
        &self,
        task_ctx: &TaskContext,
        input_schema: &datafusion::arrow::datatypes::Schema,
        registered_children: &[Arc<dyn PhysicalExpr>],
        max_payload_bytes: usize,
    ) -> DataFusionResult<DecodedDynFilterExprs> {
        match self {
            Self::Datafusion(_) => {
                let pushdown_expr =
                    self.decode_datafusion_expr(task_ctx, input_schema, max_payload_bytes)?;
                Ok(DecodedDynFilterExprs {
                    pushdown_expr,
                    exact_expr: None,
                })
            }
            Self::JoinHashBloom(bloom) => {
                bloom.validate()?;

                let encoded_bytes = self.encoded_payload_bytes();
                validate_payload_size(encoded_bytes, max_payload_bytes)
                    .map_err(DataFusionError::from)?;

                let num_registered = registered_children.len();
                for &child_index in &bloom.join_key_child_indices {
                    if child_index as usize >= num_registered {
                        return Err(DataFusionError::Plan(format!(
                            "JoinHashBloomPayload: join key child index {} out of range (registered children: {})",
                            child_index, num_registered
                        )));
                    }
                }

                // Decode residual DataFusion expression
                let residual_expr = decode_physical_expr_from_bytes(
                    &bloom.residual_datafusion_physical_expr,
                    task_ctx,
                    input_schema,
                    max_payload_bytes,
                )?;

                // Ensure residual is Boolean-typed
                let residual_dt = residual_expr.data_type(input_schema)?;
                if residual_dt != datafusion::arrow::datatypes::DataType::Boolean {
                    return Err(DataFusionError::Plan(format!(
                        "JoinHashBloomPayload: residual expression has type {:?}, expected Boolean",
                        residual_dt
                    )));
                }

                // Select probe children from registered_children by join-key child index
                let probe_children: Vec<Arc<dyn PhysicalExpr>> = bloom
                    .join_key_child_indices
                    .iter()
                    .map(|&child_index| Arc::clone(&registered_children[child_index as usize]))
                    .collect();

                // Recompute fingerprint; zero/mismatch/recompute-error yields lit(true) exact.
                let exact_expr: Arc<dyn PhysicalExpr> = if bloom.hash_compat_fingerprint == 0 {
                    lit(true)
                } else {
                    match crate::request::join_hash_bloom::compute_hash_compat_fingerprint(
                        &probe_children,
                        input_schema,
                        (
                            bloom.df_seed0,
                            bloom.df_seed1,
                            bloom.df_seed2,
                            bloom.df_seed3,
                        ),
                    ) {
                        Ok(local_fp) if local_fp == bloom.hash_compat_fingerprint => {
                            Arc::new(JoinHashBloomProbeExpr::try_new(
                                probe_children,
                                Arc::new(bloom.clone()),
                            )?) as Arc<dyn PhysicalExpr>
                        }
                        _ => lit(true),
                    }
                };

                Ok(DecodedDynFilterExprs {
                    pushdown_expr: residual_expr,
                    exact_expr: Some(exact_expr),
                })
            }
        }
    }

    /// Returns the proto-encoded byte size of this payload for budget tracking.
    ///
    /// - `Datafusion`: length of the raw protobuf bytes.
    /// - `JoinHashBloom`: `encoded_len()` of the whole typed
    ///   `RemoteDynFilterPayload` proto that carries the Bloom payload,
    ///   conservatively computed by converting to proto and measuring.
    pub fn encoded_payload_bytes(&self) -> usize {
        match self {
            Self::Datafusion(bytes) => bytes.len(),
            Self::JoinHashBloom(_bloom) => {
                use prost::Message;
                let proto = self.to_region_proto_payload();
                proto.encoded_len()
            }
        }
    }

    /// Converts this payload to the corresponding gRPC
    /// `RemoteDynFilterPayload` proto.
    pub fn to_region_proto_payload(&self) -> api::v1::region::RemoteDynFilterPayload {
        match self {
            Self::Datafusion(bytes) => api::v1::region::RemoteDynFilterPayload {
                kind: Some(
                    api::v1::region::remote_dyn_filter_payload::Kind::DatafusionPhysicalExpr(
                        bytes.clone(),
                    ),
                ),
            },
            Self::JoinHashBloom(bloom) => api::v1::region::RemoteDynFilterPayload {
                kind: Some(
                    api::v1::region::remote_dyn_filter_payload::Kind::JoinHashBloom(
                        bloom.to_proto(),
                    ),
                ),
            },
        }
    }

    /// Constructs a `DynFilterPayload` from a gRPC
    /// `RemoteDynFilterPayload` proto.
    ///
    /// Rejects missing `kind` and unknown / unspecified enum values
    /// (hash kind / bloom algorithm) via the payload's own validation.
    pub fn from_region_proto_payload(
        payload: api::v1::region::RemoteDynFilterPayload,
    ) -> DataFusionResult<Self> {
        let kind = payload.kind.ok_or_else(|| {
            DataFusionError::Plan("RemoteDynFilterPayload::kind is missing".to_string())
        })?;

        match kind {
            api::v1::region::remote_dyn_filter_payload::Kind::DatafusionPhysicalExpr(bytes) => {
                Ok(Self::Datafusion(bytes))
            }
            api::v1::region::remote_dyn_filter_payload::Kind::JoinHashBloom(bloom) => {
                let model = JoinHashBloomPayload::from_proto(&bloom)?;
                model.validate()?;
                Ok(Self::JoinHashBloom(model))
            }
        }
    }
}

fn encode_physical_expr_to_bytes(expr: &Arc<dyn PhysicalExpr>) -> DataFusionResult<Vec<u8>> {
    let codec = DefaultPhysicalExtensionCodec {};
    let proto = serialize_physical_expr(expr, &codec)?;
    let mut bytes = Vec::new();
    proto.encode(&mut bytes).map_err(|e| {
        DataFusionError::Internal(format!("Failed to encode PhysicalExprNode: {e}"))
    })?;
    Ok(bytes)
}

fn encode_remote_dyn_filter_expr(
    expr: &Arc<dyn PhysicalExpr>,
    max_payload_bytes: usize,
    bounds_only: bool,
) -> Result<Vec<u8>, CommonQueryError> {
    let expr = portable_remote_dyn_filter_expr(Arc::clone(expr), bounds_only)
        .map_err(CommonQueryError::from)?;
    let bytes = encode_physical_expr_to_bytes(&expr).map_err(CommonQueryError::from)?;
    validate_payload_size(bytes.len(), max_payload_bytes)?;
    Ok(bytes)
}

fn encode_fail_open_remote_dyn_filter_expr(
    expr: &Arc<dyn PhysicalExpr>,
    max_payload_bytes: usize,
    bounds_only: bool,
) -> Result<Vec<u8>, CommonQueryError> {
    let expr = fail_open_remote_dyn_filter_expr(Arc::clone(expr), bounds_only)
        .map_err(CommonQueryError::from)?;
    let bytes = encode_physical_expr_to_bytes(&expr).map_err(CommonQueryError::from)?;
    validate_payload_size(bytes.len(), max_payload_bytes)?;
    Ok(bytes)
}

fn portable_remote_dyn_filter_expr(
    expr: Arc<dyn PhysicalExpr>,
    bounds_only: bool,
) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
    expr.transform_up(|node| {
        if node.as_any().is::<HashTableLookupExpr>()
            || (bounds_only && node.as_any().is::<InListExpr>())
        {
            Ok(Transformed::yes(lit(true)))
        } else {
            Ok(Transformed::no(node))
        }
    })
    .map(|transformed| transformed.data)
}

/// Builds a fail-open DataFusion fallback expression for the registered-children
/// encoder path.
///
/// Unlike [`portable_remote_dyn_filter_expr`], this does **not** replace nested
/// `HashTableLookupExpr` nodes in-place. Replacing a lookup under non-monotone
/// contexts such as `NOT(lookup)` would turn the subtree into `false` and could
/// create false negatives. Instead, any top-level AND conjunct containing a
/// lookup is dropped entirely; if the whole expression is unsafe, the fallback
/// is `lit(true)`.
fn fail_open_remote_dyn_filter_expr(
    expr: Arc<dyn PhysicalExpr>,
    bounds_only: bool,
) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
    let conjuncts = flatten_and_conjuncts(&expr);
    let mut saw_lookup = false;
    let mut safe_conjuncts = Vec::with_capacity(conjuncts.len());

    for conjunct in conjuncts {
        if contains_hashtable_lookup(&conjunct) {
            saw_lookup = true;
            continue;
        }

        let conjunct = if bounds_only {
            portable_remote_dyn_filter_expr(conjunct, true)?
        } else {
            conjunct
        };
        safe_conjuncts.push(conjunct);
    }

    if !saw_lookup {
        return portable_remote_dyn_filter_expr(expr, bounds_only);
    }

    Ok(rebuild_and_conjuncts(safe_conjuncts))
}

fn rebuild_and_conjuncts(mut conjuncts: Vec<Arc<dyn PhysicalExpr>>) -> Arc<dyn PhysicalExpr> {
    let Some(first) = conjuncts.pop() else {
        return lit(true);
    };

    conjuncts.into_iter().rev().fold(first, |right, left| {
        Arc::new(BinaryExpr::new(left, Operator::And, right)) as Arc<dyn PhysicalExpr>
    })
}

// ---------------------------------------------------------------------------
// Bloom encoder helpers
// ---------------------------------------------------------------------------

/// Flattens a physical expression that is a top-level chain of AND
/// (`BinaryExpr` with `Operator::And`) into a vector of conjuncts.
fn flatten_and_conjuncts(expr: &Arc<dyn PhysicalExpr>) -> Vec<Arc<dyn PhysicalExpr>> {
    let mut conjuncts = Vec::new();
    let mut stack = vec![Arc::clone(expr)];
    while let Some(node) = stack.pop() {
        if let Some(and) = node.as_any().downcast_ref::<BinaryExpr>()
            && *and.op() == Operator::And
        {
            stack.push(Arc::clone(and.right()));
            stack.push(Arc::clone(and.left()));
            continue;
        }
        conjuncts.push(node);
    }
    conjuncts
}

/// Returns the single `HashTableLookupExpr` that is a direct AND conjunct, if
/// exactly one exists and every other conjunct is lookup-free.
fn extract_safe_single_lookup(conjuncts: &[Arc<dyn PhysicalExpr>]) -> Option<usize> {
    let mut lookup_idx: Option<usize> = None;
    for (i, conj) in conjuncts.iter().enumerate() {
        if conj.as_any().is::<HashTableLookupExpr>() {
            if lookup_idx.is_some() {
                return None;
            }
            lookup_idx = Some(i);
            continue;
        }
        if contains_hashtable_lookup(conj) {
            return None;
        }
    }
    lookup_idx
}

fn contains_hashtable_lookup(expr: &Arc<dyn PhysicalExpr>) -> bool {
    let mut found = false;
    let apply_result = expr.apply(|node| {
        if node.as_any().is::<HashTableLookupExpr>() {
            found = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    });
    apply_result.is_err() || found
}

fn map_on_columns_to_join_key_child_indices(
    lookup: &HashTableLookupExpr,
    registered_children: &[Arc<dyn PhysicalExpr>],
) -> Option<Vec<u32>> {
    let on_columns = lookup.on_columns();
    let mut join_key_child_indices = Vec::with_capacity(on_columns.len());
    for on_col in on_columns {
        let mut found = None;
        for (idx, reg_child) in registered_children.iter().enumerate() {
            if on_col == reg_child {
                if found.is_some() {
                    return None;
                }
                found = Some(idx as u32);
            }
        }
        match found {
            Some(child_index) => join_key_child_indices.push(child_index),
            None => return None,
        }
    }
    let mut seen = std::collections::HashSet::with_capacity(join_key_child_indices.len());
    for &child_index in &join_key_child_indices {
        if !seen.insert(child_index) {
            return None;
        }
    }
    Some(join_key_child_indices)
}

fn build_residual_expr(
    original_expr: &Arc<dyn PhysicalExpr>,
    lookup_conjunct: &Arc<dyn PhysicalExpr>,
) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
    if Arc::ptr_eq(original_expr, lookup_conjunct) {
        return Ok(lit(true));
    }
    original_expr
        .clone()
        .transform_up(|node| {
            if Arc::ptr_eq(&node, lookup_conjunct) || node.as_any().is::<HashTableLookupExpr>() {
                Ok(Transformed::yes(lit(true)))
            } else {
                Ok(Transformed::no(node))
            }
        })
        .map(|t| t.data)
}

fn try_encode_bloom_payload(
    expr: &Arc<dyn PhysicalExpr>,
    registered_children: &[Arc<dyn PhysicalExpr>],
    max_payload_bytes: usize,
    input_schema: &datafusion::arrow::datatypes::Schema,
) -> DataFusionResult<JoinHashBloomPayload> {
    // 1. Flatten AND conjuncts
    let conjuncts = flatten_and_conjuncts(expr);

    // 2. Extract exactly one safe HashTableLookupExpr conjunct index
    let Some(lookup_idx) = extract_safe_single_lookup(&conjuncts) else {
        return Err(DataFusionError::Plan(
            "Bloom encoder: no single safe HashTableLookupExpr conjunct".to_string(),
        ));
    };

    let lookup_dyn = Arc::clone(&conjuncts[lookup_idx]);
    let lookup = lookup_dyn
        .as_any()
        .downcast_ref::<HashTableLookupExpr>()
        .expect("already checked above");

    // 3. Map on_columns to join key child indices
    let Some(join_key_child_indices) =
        map_on_columns_to_join_key_child_indices(lookup, registered_children)
    else {
        return Err(DataFusionError::Plan(
            "Bloom encoder: cannot map on_columns to registered children".to_string(),
        ));
    };

    // 3.5 Compute hash compat fingerprint using the selected probe children
    let seeds = lookup.seeds();
    let probe_children: Vec<Arc<dyn PhysicalExpr>> = join_key_child_indices
        .iter()
        .map(|&child_index| Arc::clone(&registered_children[child_index as usize]))
        .collect();
    let hash_compat_fingerprint = crate::request::join_hash_bloom::compute_hash_compat_fingerprint(
        &probe_children,
        input_schema,
        seeds,
    )
    .map_err(|e| {
        DataFusionError::Plan(format!(
            "Bloom encoder: cannot compute hash compat fingerprint: {e}"
        ))
    })?;

    // 4. Get distinct hash count
    let distinct_count = lookup.distinct_join_hash_count().ok_or_else(|| {
        DataFusionError::Plan(
            "Bloom encoder: HashTableLookupExpr uses ArrayMap, falling back".to_string(),
        )
    })? as u64;

    // 5. Build residual expression
    let residual_expr = build_residual_expr(expr, &lookup_dyn)?;
    let residual_bytes = encode_physical_expr_to_bytes(&residual_expr)?;

    // 6. Compute Bloom sizing
    let Some((num_bits, num_probes)) = JoinHashBloomPayload::compute_bloom_sizing(
        distinct_count,
        residual_bytes.len(),
        max_payload_bytes,
    ) else {
        return Err(DataFusionError::Plan(
            "Bloom encoder: payload budget too small for Bloom".to_string(),
        ));
    };

    // 7. Build the Bloom payload
    let mut payload = JoinHashBloomPayload::try_build_from_hashes(
        std::iter::empty(),
        num_bits,
        num_probes,
        seeds,
        join_key_child_indices,
        residual_bytes,
    )?;

    let mut inserted_count = 0u64;
    let ok = lookup.visit_distinct_join_hashes(&mut |h| {
        payload.insert_join_hash(h);
        inserted_count = inserted_count.saturating_add(1);
    })?;
    if !ok {
        return Err(DataFusionError::Plan(
            "Bloom encoder: HashTableLookupExpr uses ArrayMap, falling back".to_string(),
        ));
    }
    if inserted_count != distinct_count {
        return Err(DataFusionError::Plan(format!(
            "Bloom encoder: distinct hash count changed during encoding (counted {}, inserted {})",
            distinct_count, inserted_count
        )));
    }
    payload.distinct_hash_count = inserted_count;
    payload.hash_compat_fingerprint = hash_compat_fingerprint;
    payload.validate()?;

    // 8. Final budget check
    let encoded_len = DynFilterPayload::JoinHashBloom(payload.clone()).encoded_payload_bytes();
    if encoded_len > max_payload_bytes {
        return Err(DataFusionError::Plan(format!(
            "Bloom encoder: final proto encoded payload {} bytes exceeds budget {}",
            encoded_len, max_payload_bytes
        )));
    }

    Ok(payload)
}

pub(crate) fn decode_physical_expr_from_bytes(
    bytes: &[u8],
    task_ctx: &TaskContext,
    input_schema: &datafusion::arrow::datatypes::Schema,
    max_payload_bytes: usize,
) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
    validate_payload_size(bytes.len(), max_payload_bytes).map_err(DataFusionError::from)?;
    let codec = DefaultPhysicalExtensionCodec {};
    let proto = PhysicalExprNode::decode(bytes).map_err(|e| {
        DataFusionError::Internal(format!("Failed to decode PhysicalExprNode: {e}"))
    })?;

    let expr = parse_physical_expr(&proto, task_ctx, input_schema, &codec)?;
    validate_supported_payload_expr(&expr)?;
    validate_decoded_payload_expr(&expr, input_schema)?;
    Ok(expr)
}

fn validate_payload_size(
    payload_size_bytes: usize,
    max_payload_bytes: usize,
) -> Result<(), CommonQueryError> {
    ensure!(
        payload_size_bytes <= max_payload_bytes,
        DynFilterPayloadTooLargeSnafu {
            payload_size_bytes,
            max_payload_bytes,
        }
    );

    Ok(())
}

fn validate_supported_payload_expr(expr: &Arc<dyn PhysicalExpr>) -> DataFusionResult<()> {
    expr.apply(|node| {
        if node.as_any().is::<HashTableLookupExpr>() {
            return Err(DataFusionError::Plan(
                "HashTableLookupExpr cannot be encoded into DynFilterPayload::Datafusion"
                    .to_string(),
            ));
        }

        Ok(TreeNodeRecursion::Continue)
    })?;

    Ok(())
}

/// Validates decoded dynamic filter physical expressions against the receiver-side schema.
fn validate_decoded_payload_expr(
    expr: &Arc<dyn PhysicalExpr>,
    input_schema: &datafusion::arrow::datatypes::Schema,
) -> DataFusionResult<()> {
    expr.apply(|node| {
        if let Some(column) = node.as_any().downcast_ref::<Column>() {
            let Some(field) = input_schema.fields().get(column.index()) else {
                return Err(DataFusionError::Plan(format!(
                    "Decoded Column '{}' references out-of-bounds index {} for input schema of size {}",
                    column.name(),
                    column.index(),
                    input_schema.fields().len()
                )));
            };

            if field.name() != column.name() {
                return Err(DataFusionError::Plan(format!(
                    "Decoded Column name/index mismatch: payload has '{}' at index {}, but schema field is '{}'",
                    column.name(),
                    column.index(),
                    field.name()
                )));
            }
        }

        Ok(TreeNodeRecursion::Continue)
    })?;

    Ok(())
}

/// Placement-aware decoded dynamic filter expressions.
///
/// Splits a decoded payload so DataNode can install the `pushdown_expr` into a
/// normal [`DynamicFilterPhysicalExpr`] that Mito scan can push down, while the
/// optional `exact_expr` is wrapped in a non-pushdown marker that stays in
/// [`FilterExec`] for row-level evaluation only.
///
/// - `Datafusion` payload: `pushdown_expr = decode_datafusion_expr(...)`,
///   `exact_expr = None`.
/// - `JoinHashBloom` payload: `pushdown_expr = residual_expr`,
///   `exact_expr = Some(bloom_probe_expr)` when fingerprint matches;
///   `exact_expr = Some(lit(true))` on zero/mismatch/recompute error
///   (valid-payload degradation that disables exact Bloom safely).
///
/// [`DynamicFilterPhysicalExpr`]: datafusion::physical_plan::expressions::DynamicFilterPhysicalExpr
#[derive(Debug, Clone)]
pub struct DecodedDynFilterExprs {
    pub pushdown_expr: Arc<dyn PhysicalExpr>,
    pub exact_expr: Option<Arc<dyn PhysicalExpr>>,
}

/// A remote dynamic filter update sent from a query coordinator to region servers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DynFilterUpdate {
    pub protocol_version: u32,
    pub query_id: String,
    pub filter_id: String,
    pub generation: u64,
    pub is_complete: bool,
    pub payload: DynFilterPayload,
}

impl DynFilterUpdate {
    pub fn new(
        query_id: String,
        filter_id: String,
        generation: u64,
        is_complete: bool,
        payload: DynFilterPayload,
    ) -> Self {
        Self {
            protocol_version: DYN_FILTER_PROTOCOL_VERSION,
            query_id,
            filter_id,
            generation,
            is_complete,
            payload,
        }
    }
}

/// The query request to be handled by the RegionServer (Datanode).
#[derive(Clone, Debug)]
pub struct QueryRequest {
    pub header: Option<RegionRequestHeader>,
    pub region_id: RegionId,
    pub plan: LogicalPlan,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use base64::Engine;
    use base64::prelude::BASE64_STANDARD;
    use datafusion::arrow::array::{BooleanArray, Int32Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, InListExpr, NotExpr, lit};
    use datafusion::physical_plan::expressions::col;
    use datafusion::physical_plan::joins::join_hash_map::{JoinHashMapType, JoinHashMapU32};
    use datafusion::physical_plan::joins::{HashTableLookupExpr, Map, SeededRandomState};
    use datafusion_common::DataFusionError;
    use datafusion_expr::Operator;

    use super::*;

    fn make_lookup_expr(col: Arc<dyn PhysicalExpr>, hashes: Vec<u64>) -> Arc<dyn PhysicalExpr> {
        let mut hash_map = JoinHashMapU32::with_capacity(hashes.len().max(1));
        hash_map.update_from_iter(Box::new(hashes.iter().enumerate()), 0);
        let map = Arc::new(Map::HashMap(Box::new(hash_map)));
        Arc::new(HashTableLookupExpr::new(
            vec![col],
            SeededRandomState::with_seeds(1, 2, 3, 4),
            map,
            "lookup".to_string(),
        ))
    }

    // ── Basic tests ──────────────────────────────────────────

    #[test]
    fn dyn_filter_update_sets_protocol_version() {
        let update = DynFilterUpdate::new(
            "query-1".to_string(),
            "filter-1".to_string(),
            3,
            false,
            DynFilterPayload::Datafusion(vec![1, 2, 3]),
        );
        assert_eq!(update.protocol_version, DYN_FILTER_PROTOCOL_VERSION);
        assert!(!update.is_complete);
        assert!(
            matches!(update.payload, DynFilterPayload::Datafusion(ref bytes) if bytes == &vec![1, 2, 3])
        );
    }

    #[test]
    fn dyn_filter_update_json_round_trip_preserves_payload_shape() {
        let update = DynFilterUpdate::new(
            "query-2".to_string(),
            "filter-9".to_string(),
            9,
            true,
            DynFilterPayload::Datafusion(vec![9, 8, 7]),
        );

        let json = serde_json::to_string(&update).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        let decoded: DynFilterUpdate = serde_json::from_str(&json).unwrap();

        assert_eq!(value["generation"], serde_json::json!(9));
        assert!(value.get("epoch").is_none());
        assert_eq!(
            value["payload"],
            serde_json::json!({ "kind": "datafusion", "payload": BASE64_STANDARD.encode([9, 8, 7]) })
        );
        assert_eq!(decoded, update);
        assert!(decoded.is_complete);
        assert!(
            matches!(decoded.payload, DynFilterPayload::Datafusion(ref bytes) if bytes == &vec![9, 8, 7])
        );
    }

    #[test]
    fn dyn_filter_payload_json_uses_base64_for_empty_and_padded_payloads() {
        let empty = serde_json::to_value(DynFilterPayload::Datafusion(vec![])).unwrap();
        let one = serde_json::to_value(DynFilterPayload::Datafusion(vec![1])).unwrap();
        let two = serde_json::to_value(DynFilterPayload::Datafusion(vec![1, 2])).unwrap();

        assert_eq!(
            empty,
            serde_json::json!({"kind": "datafusion", "payload": ""})
        );
        assert_eq!(
            one,
            serde_json::json!({"kind": "datafusion", "payload": BASE64_STANDARD.encode([1])})
        );
        assert_eq!(
            two,
            serde_json::json!({"kind": "datafusion", "payload": BASE64_STANDARD.encode([1, 2])})
        );
    }

    #[test]
    fn dyn_filter_payload_json_rejects_invalid_base64() {
        let err = serde_json::from_value::<DynFilterPayload>(serde_json::json!({
            "kind": "datafusion",
            "payload": "not base64!",
        }))
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("invalid base64 dynamic filter payload")
        );
    }

    #[test]
    fn dyn_filter_payload_round_trips_physical_column_expr() {
        let schema = Schema::new(vec![Field::new("host", DataType::Utf8, false)]);
        let expr: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new_with_schema("host", &schema).unwrap());

        let payload = DynFilterPayload::from_datafusion_expr(&expr, 1024).unwrap();
        let decoded = payload
            .decode_datafusion_expr(&TaskContext::default(), &schema, 1024)
            .unwrap();

        let original = expr.as_any().downcast_ref::<Column>().unwrap();
        let decoded = decoded.as_any().downcast_ref::<Column>().unwrap();

        assert_eq!(decoded.name(), original.name());
        assert_eq!(decoded.index(), original.index());
    }

    #[test]
    fn dyn_filter_payload_decode_rejects_invalid_bytes() {
        let schema = Schema::new(vec![Field::new("host", DataType::Utf8, false)]);
        let payload = DynFilterPayload::Datafusion(vec![1, 2, 3]);

        let err = payload
            .decode_datafusion_expr(&TaskContext::default(), &schema, 1024)
            .unwrap_err();

        assert!(matches!(err, DataFusionError::Internal(_)));
    }

    #[test]
    fn dyn_filter_payload_decode_rejects_column_name_index_mismatch() {
        let schema = Schema::new(vec![Field::new("host", DataType::Utf8, false)]);
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("service", 0));

        let payload = DynFilterPayload::from_datafusion_expr(&expr, 1024).unwrap();
        let err = payload
            .decode_datafusion_expr(&TaskContext::default(), &schema, 1024)
            .unwrap_err();

        let msg = err.to_string();
        assert!(
            msg.contains("name/index mismatch"),
            "expected name/index mismatch error, got: {msg}"
        );
        assert!(msg.contains("service"));
        assert!(msg.contains("host"));
    }

    #[test]
    fn dyn_filter_payload_decode_rejects_out_of_bounds_column_index() {
        let schema = Schema::new(vec![Field::new("host", DataType::Utf8, false)]);
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("host", 1));

        let payload = DynFilterPayload::from_datafusion_expr(&expr, 1024).unwrap();
        let err = payload
            .decode_datafusion_expr(&TaskContext::default(), &schema, 1024)
            .unwrap_err();

        assert!(matches!(err, DataFusionError::Plan(_)));
    }

    #[test]
    fn dyn_filter_payload_hash_lookup_fallback_preserves_bounds() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "device_id",
            DataType::Int32,
            false,
        )]));
        let device_id = col("device_id", &schema).unwrap();
        let lower_bound = Arc::new(BinaryExpr::new(
            Arc::clone(&device_id),
            Operator::GtEq,
            lit(10i32),
        )) as Arc<dyn PhysicalExpr>;
        let lookup = Arc::new(HashTableLookupExpr::new(
            vec![Arc::clone(&device_id)],
            SeededRandomState::with_seeds(0, 0, 0, 0),
            Arc::new(Map::HashMap(Box::new(JoinHashMapU32::with_capacity(0)))),
            "hash_lookup".to_string(),
        )) as Arc<dyn PhysicalExpr>;
        let expr =
            Arc::new(BinaryExpr::new(lower_bound, Operator::And, lookup)) as Arc<dyn PhysicalExpr>;

        let payload = DynFilterPayload::from_datafusion_expr(&expr, 1024).unwrap();
        let decoded = payload
            .decode_datafusion_expr(&TaskContext::default(), &schema, 1024)
            .unwrap();

        assert!(!contains_expr::<HashTableLookupExpr>(&decoded));
        let decoded_display = decoded.to_string();
        assert!(decoded_display.contains("device_id"));
        assert!(decoded_display.contains(">="));
        assert!(!decoded_display.contains("hash_lookup"));
    }

    #[test]
    fn dyn_filter_payload_oversized_inlist_falls_back_to_bounds() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "device_id",
            DataType::Int32,
            false,
        )]));
        let device_id = col("device_id", &schema).unwrap();
        let lower_bound = Arc::new(BinaryExpr::new(
            Arc::clone(&device_id),
            Operator::GtEq,
            lit(8192i32),
        )) as Arc<dyn PhysicalExpr>;
        let upper_bound = Arc::new(BinaryExpr::new(
            Arc::clone(&device_id),
            Operator::LtEq,
            lit(8255i32),
        )) as Arc<dyn PhysicalExpr>;
        let bounds = Arc::new(BinaryExpr::new(lower_bound, Operator::And, upper_bound))
            as Arc<dyn PhysicalExpr>;
        let in_list = Arc::new(
            InListExpr::try_new(
                Arc::clone(&device_id),
                (8192..8256).map(lit).collect(),
                false,
                &schema,
            )
            .unwrap(),
        ) as Arc<dyn PhysicalExpr>;
        let expr = Arc::new(BinaryExpr::new(Arc::clone(&bounds), Operator::And, in_list))
            as Arc<dyn PhysicalExpr>;
        let bounds_only = portable_remote_dyn_filter_expr(Arc::clone(&expr), true).unwrap();
        let bounds_only_size = encode_physical_expr_to_bytes(&bounds_only).unwrap().len();
        let full_size = encode_physical_expr_to_bytes(&expr).unwrap().len();
        assert!(full_size > bounds_only_size);

        let payload = DynFilterPayload::from_datafusion_expr(&expr, bounds_only_size).unwrap();
        let decoded = payload
            .decode_datafusion_expr(&TaskContext::default(), &schema, bounds_only_size)
            .unwrap();

        assert!(!contains_expr::<InListExpr>(&decoded));
        let decoded_display = decoded.to_string();
        assert!(decoded_display.contains("device_id"));
        assert!(decoded_display.contains(">="));
        assert!(decoded_display.contains("<="));
    }

    #[test]
    fn dyn_filter_payload_rejects_oversized_payload() {
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("host", 0));

        let err = DynFilterPayload::from_datafusion_expr(&expr, 1).unwrap_err();

        let DataFusionError::External(error) = err else {
            panic!("expected external common query error, got: {err:?}");
        };
        assert!(matches!(
            error.downcast_ref::<CommonQueryError>(),
            Some(CommonQueryError::DynFilterPayloadTooLarge { .. })
        ));
    }

    // ── Encoder + decoder with fingerprint ───────────────────

    #[test]
    fn encoder_bounds_and_lookup_creates_bloom_with_nonzero_fingerprint() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Float64, false),
        ]));
        let id_col = col("id", &schema).unwrap();
        let val_col = col("val", &schema).unwrap();

        let lower_bound: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::clone(&id_col),
            Operator::GtEq,
            lit(10i32),
        ));
        let lookup = make_lookup_expr(Arc::clone(&id_col), vec![10, 20, 30]);
        let expr: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(lower_bound, Operator::And, lookup));

        let registered: Vec<Arc<dyn PhysicalExpr>> =
            vec![Arc::clone(&id_col), Arc::clone(&val_col)];

        let payload = DynFilterPayload::from_datafusion_expr_with_registered_children(
            &expr,
            &registered,
            4096,
            &schema,
        )
        .unwrap();

        match &payload {
            DynFilterPayload::JoinHashBloom(bloom) => {
                assert!(!bloom.residual_datafusion_physical_expr.is_empty());
                assert_eq!(bloom.join_key_child_indices, vec![0]);
                assert_eq!(bloom.distinct_hash_count, 3);
                assert!(
                    bloom.hash_compat_fingerprint != 0,
                    "expected nonzero fingerprint, got {}",
                    bloom.hash_compat_fingerprint
                );
                for &h in &[10u64, 20, 30] {
                    assert!(bloom.contains_join_hash(h));
                }
            }
            _ => panic!("expected JoinHashBloom, got {:?}", payload),
        }
    }

    #[test]
    fn encoder_lookup_only_creates_bloom_with_lit_true_residual() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let id_col = col("id", &schema).unwrap();

        let lookup = make_lookup_expr(Arc::clone(&id_col), vec![42, 99]);
        let expr: Arc<dyn PhysicalExpr> = lookup;

        let registered: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::clone(&id_col)];

        let payload = DynFilterPayload::from_datafusion_expr_with_registered_children(
            &expr,
            &registered,
            4096,
            &schema,
        )
        .unwrap();

        match payload {
            DynFilterPayload::JoinHashBloom(bloom) => {
                assert!(!bloom.residual_datafusion_physical_expr.is_empty());
                assert_eq!(bloom.distinct_hash_count, 2);
                assert_ne!(bloom.hash_compat_fingerprint, 0);
            }
            DynFilterPayload::Datafusion(_) => panic!("expected JoinHashBloom payload"),
        }
    }

    #[test]
    fn encoder_preserves_on_columns_order() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));
        let col_a = col("a", &schema).unwrap();
        let col_b = col("b", &schema).unwrap();
        let col_c = col("c", &schema).unwrap();

        let mut hash_map = JoinHashMapU32::with_capacity(10);
        let hashes: Vec<u64> = vec![1, 2, 3];
        hash_map.update_from_iter(Box::new(hashes.iter().enumerate()), 0);
        let map = Arc::new(Map::HashMap(Box::new(hash_map)));
        let lookup: Arc<dyn PhysicalExpr> = Arc::new(HashTableLookupExpr::new(
            vec![Arc::clone(&col_a), Arc::clone(&col_c)],
            SeededRandomState::with_seeds(0, 0, 0, 0),
            map,
            "lookup".to_string(),
        ));

        let residual_bound: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::clone(&col_b),
            Operator::GtEq,
            lit(0i32),
        ));
        let expr: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(residual_bound, Operator::And, lookup));

        let registered: Vec<Arc<dyn PhysicalExpr>> =
            vec![Arc::clone(&col_a), Arc::clone(&col_b), Arc::clone(&col_c)];

        let payload = DynFilterPayload::from_datafusion_expr_with_registered_children(
            &expr,
            &registered,
            4096,
            &schema,
        )
        .unwrap();

        match payload {
            DynFilterPayload::JoinHashBloom(bloom) => {
                assert_eq!(bloom.join_key_child_indices, vec![0, 2]);
            }
            DynFilterPayload::Datafusion(_) => panic!("expected JoinHashBloom payload"),
        }
    }

    #[test]
    fn decode_with_matching_fingerprint_uses_probe_expr() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let id_col = col("id", &schema).unwrap();
        let lookup = make_lookup_expr(Arc::clone(&id_col), vec![10, 20, 30]);
        let expr: Arc<dyn PhysicalExpr> = lookup;

        let registered: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::clone(&id_col)];

        let payload = DynFilterPayload::from_datafusion_expr_with_registered_children(
            &expr,
            &registered,
            4096,
            &schema,
        )
        .unwrap();

        let decoded = payload
            .decode_expr_with_registered_children(
                &TaskContext::default(),
                &schema,
                &registered,
                4096,
            )
            .unwrap();

        assert!(
            contains_expr::<JoinHashBloomProbeExpr>(&decoded),
            "expected bloom_probe in decoded expression, got: {}",
            decoded
        );
    }

    #[test]
    fn decode_with_corrupted_fingerprint_returns_residual_only() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let id_col = col("id", &schema).unwrap();
        let lookup = make_lookup_expr(Arc::clone(&id_col), vec![10, 20, 30]);
        let expr: Arc<dyn PhysicalExpr> = lookup;

        let registered: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::clone(&id_col)];

        let payload = DynFilterPayload::from_datafusion_expr_with_registered_children(
            &expr,
            &registered,
            4096,
            &schema,
        )
        .unwrap();

        // Corrupt the fingerprint
        let corrupted = match &payload {
            DynFilterPayload::JoinHashBloom(bloom) => {
                let mut b = bloom.clone();
                b.hash_compat_fingerprint = b.hash_compat_fingerprint.wrapping_add(1);
                DynFilterPayload::JoinHashBloom(b)
            }
            _ => panic!(),
        };

        let decoded = corrupted
            .decode_expr_with_registered_children(
                &TaskContext::default(),
                &schema,
                &registered,
                4096,
            )
            .unwrap();

        assert!(
            !contains_expr::<JoinHashBloomProbeExpr>(&decoded),
            "expected no bloom_probe after fingerprint corruption, got: {}",
            decoded
        );
    }

    #[test]
    fn decode_with_zero_fingerprint_returns_residual_only() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let id_col: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new_with_schema("id", &schema).unwrap());
        let registered: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::clone(&id_col)];

        // Manually construct a Bloom payload with zero fingerprint (old-format compatible)
        let residual_bytes =
            encode_physical_expr_to_bytes(&(lit(true) as Arc<dyn PhysicalExpr>)).unwrap();
        let bloom = JoinHashBloomPayload::build_from_hashes(
            vec![10u64, 20],
            64,
            2,
            (0, 0, 0, 0),
            vec![0],
            residual_bytes,
        );
        let payload = DynFilterPayload::JoinHashBloom(bloom);
        let decoded = payload
            .decode_expr_with_registered_children(
                &TaskContext::default(),
                &schema,
                &registered,
                4096,
            )
            .unwrap();
        assert!(
            !contains_expr::<JoinHashBloomProbeExpr>(&decoded),
            "zero fingerprint should fail open to residual-only, got: {}",
            decoded
        );
    }

    #[test]
    fn decode_fingerprint_recompute_error_returns_residual_only() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(10, 2),
            false,
        )]));
        let amount_col: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new_with_schema("amount", &schema).unwrap());
        let registered: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::clone(&amount_col)];

        let residual_bytes =
            encode_physical_expr_to_bytes(&(lit(true) as Arc<dyn PhysicalExpr>)).unwrap();
        let mut bloom = JoinHashBloomPayload::build_from_hashes(
            vec![10u64, 20],
            64,
            2,
            (0, 0, 0, 0),
            vec![0],
            residual_bytes,
        );
        bloom.hash_compat_fingerprint = 42;

        let decoded = DynFilterPayload::JoinHashBloom(bloom)
            .decode_expr_with_registered_children(
                &TaskContext::default(),
                &schema,
                &registered,
                4096,
            )
            .unwrap();

        assert!(
            !contains_expr::<JoinHashBloomProbeExpr>(&decoded),
            "fingerprint recompute error should fail open to residual-only, got: {}",
            decoded
        );
    }

    #[test]
    fn encoder_allows_unsupported_non_key_schema_column_for_fingerprint() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("unused_decimal", DataType::Decimal128(10, 2), true),
        ]));
        let id_col = col("id", &schema).unwrap();
        let lookup = make_lookup_expr(Arc::clone(&id_col), vec![10, 20]);
        let expr: Arc<dyn PhysicalExpr> = lookup;
        let registered: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::clone(&id_col)];

        let payload = DynFilterPayload::from_datafusion_expr_with_registered_children(
            &expr,
            &registered,
            4096,
            &schema,
        )
        .unwrap();

        match payload {
            DynFilterPayload::JoinHashBloom(bloom) => assert_ne!(bloom.hash_compat_fingerprint, 0),
            DynFilterPayload::Datafusion(_) => {
                panic!("unused unsupported schema column should not disable Bloom")
            }
        }
    }

    #[test]
    fn join_hash_bloom_region_proto_roundtrip_preserves_fingerprint() {
        let mut bloom = JoinHashBloomPayload::build_from_hashes(
            vec![1u64, 2, 3],
            128,
            3,
            (1, 2, 3, 4),
            vec![0],
            vec![1, 2, 3],
        );
        bloom.hash_compat_fingerprint = 0xfeed_beef_dead_beef;
        let payload = DynFilterPayload::JoinHashBloom(bloom.clone());

        let proto = payload.to_region_proto_payload();
        let decoded = DynFilterPayload::from_region_proto_payload(proto).unwrap();

        assert_eq!(decoded, DynFilterPayload::JoinHashBloom(bloom));
    }

    #[test]
    fn encoder_falls_back_on_child_mismatch() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let id_col = col("id", &schema).unwrap();

        let lookup = make_lookup_expr(Arc::clone(&id_col), vec![1, 2]);
        let expr: Arc<dyn PhysicalExpr> = lookup;

        let other_col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("other", 0));
        let registered: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::clone(&other_col)];

        let payload = DynFilterPayload::from_datafusion_expr_with_registered_children(
            &expr,
            &registered,
            4096,
            &schema,
        )
        .unwrap();

        match payload {
            DynFilterPayload::Datafusion(_) => {}
            DynFilterPayload::JoinHashBloom(_) => panic!("expected Datafusion fallback"),
        }
    }

    #[test]
    fn encoder_falls_back_on_multiple_lookups() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let id_col = col("id", &schema).unwrap();

        let lookup1 = make_lookup_expr(Arc::clone(&id_col), vec![1]);
        let lookup2 = make_lookup_expr(Arc::clone(&id_col), vec![2]);

        let expr: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(lookup1, Operator::And, lookup2));

        let registered: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::clone(&id_col)];

        let payload = DynFilterPayload::from_datafusion_expr_with_registered_children(
            &expr,
            &registered,
            4096,
            &schema,
        )
        .unwrap();

        match payload {
            DynFilterPayload::Datafusion(_) => {}
            DynFilterPayload::JoinHashBloom(_) => {
                panic!("expected Datafusion fallback for multiple lookups")
            }
        }
    }

    #[test]
    fn encoder_falls_back_on_lookup_under_or() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let id_col = col("id", &schema).unwrap();

        let lower_bound: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::clone(&id_col),
            Operator::GtEq,
            lit(10i32),
        ));
        let lookup = make_lookup_expr(Arc::clone(&id_col), vec![1, 2]);

        let expr: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(lower_bound, Operator::Or, lookup));

        let registered: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::clone(&id_col)];

        let payload = DynFilterPayload::from_datafusion_expr_with_registered_children(
            &expr,
            &registered,
            4096,
            &schema,
        )
        .unwrap();

        match payload {
            DynFilterPayload::Datafusion(_) => {}
            DynFilterPayload::JoinHashBloom(_) => {
                panic!("expected Datafusion fallback for OR context")
            }
        }
    }

    #[test]
    fn encoder_fail_open_fallback_for_not_lookup_is_true() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let id_col = col("id", &schema).unwrap();

        let lookup = make_lookup_expr(Arc::clone(&id_col), vec![1, 2]);
        let expr: Arc<dyn PhysicalExpr> = Arc::new(NotExpr::new(lookup));

        let registered: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::clone(&id_col)];
        let payload = DynFilterPayload::from_datafusion_expr_with_registered_children(
            &expr,
            &registered,
            4096,
            &schema,
        )
        .unwrap();

        let DynFilterPayload::Datafusion(bytes) = payload else {
            panic!("expected Datafusion fail-open fallback for NOT lookup");
        };
        let decoded = DynFilterPayload::Datafusion(bytes)
            .decode_datafusion_expr(&TaskContext::default(), &schema, 4096)
            .unwrap();

        assert!(!contains_expr::<HashTableLookupExpr>(&decoded));
        assert!(!decoded.to_string().contains("NOT"));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let result = decoded
            .evaluate(&batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        let bools = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bools.iter().all(|value| value == Some(true)));
    }

    #[test]
    fn encoder_fail_open_fallback_preserves_bounds_around_not_lookup() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let id_col = col("id", &schema).unwrap();

        let lower_bound: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::clone(&id_col),
            Operator::GtEq,
            lit(10i32),
        ));
        let lookup = make_lookup_expr(Arc::clone(&id_col), vec![1, 2]);
        let not_lookup: Arc<dyn PhysicalExpr> = Arc::new(NotExpr::new(lookup));
        let expr: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(lower_bound, Operator::And, not_lookup));

        let registered: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::clone(&id_col)];
        let payload = DynFilterPayload::from_datafusion_expr_with_registered_children(
            &expr,
            &registered,
            4096,
            &schema,
        )
        .unwrap();

        let DynFilterPayload::Datafusion(bytes) = payload else {
            panic!("expected Datafusion fail-open fallback for bounds AND NOT lookup");
        };
        let decoded = DynFilterPayload::Datafusion(bytes)
            .decode_datafusion_expr(&TaskContext::default(), &schema, 4096)
            .unwrap();

        assert!(!contains_expr::<HashTableLookupExpr>(&decoded));
        assert!(!decoded.to_string().contains("NOT"));
        assert!(decoded.to_string().contains(">="));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![5, 10, 15]))],
        )
        .unwrap();
        let result = decoded
            .evaluate(&batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        let bools = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(!bools.value(0));
        assert!(bools.value(1));
        assert!(bools.value(2));
    }

    #[test]
    fn encoder_flattens_nested_and_lookup() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let id_col = col("id", &schema).unwrap();

        let inner_lookup = make_lookup_expr(Arc::clone(&id_col), vec![1]);
        let upper_bound: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::clone(&id_col),
            Operator::LtEq,
            lit(100i32),
        ));
        let inner_expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::clone(&upper_bound),
            Operator::And,
            inner_lookup,
        ));
        let lower_bound: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::clone(&id_col),
            Operator::GtEq,
            lit(10i32),
        ));
        let expr: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(lower_bound, Operator::And, inner_expr));

        let registered: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::clone(&id_col)];

        let payload = DynFilterPayload::from_datafusion_expr_with_registered_children(
            &expr,
            &registered,
            4096,
            &schema,
        )
        .unwrap();

        match payload {
            DynFilterPayload::JoinHashBloom(bloom) => assert_eq!(bloom.distinct_hash_count, 1),
            DynFilterPayload::Datafusion(_) => panic!("expected JoinHashBloom"),
        }
    }

    #[test]
    fn encoder_no_false_negatives_for_lookup_hashes() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let id_col = col("id", &schema).unwrap();

        let inserted_hashes = vec![10u64, 20, 30, 10, 30];
        let lookup = make_lookup_expr(Arc::clone(&id_col), inserted_hashes);
        let expr: Arc<dyn PhysicalExpr> = lookup;

        let registered: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::clone(&id_col)];

        let payload = DynFilterPayload::from_datafusion_expr_with_registered_children(
            &expr,
            &registered,
            4096,
            &schema,
        )
        .unwrap();

        match payload {
            DynFilterPayload::JoinHashBloom(bloom) => {
                assert_eq!(bloom.distinct_hash_count, 3);
                for &h in &[10u64, 20, 30] {
                    assert!(
                        bloom.contains_join_hash(h),
                        "hash {h} should be in the Bloom filter"
                    );
                }
            }
            DynFilterPayload::Datafusion(_) => panic!("expected JoinHashBloom"),
        }
    }

    #[test]
    fn encoder_falls_back_on_oversized_bloom_budget() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let id_col = col("id", &schema).unwrap();

        let lookup = make_lookup_expr(Arc::clone(&id_col), vec![1, 2, 3]);
        let expr: Arc<dyn PhysicalExpr> = lookup;

        let registered: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::clone(&id_col)];

        let payload = DynFilterPayload::from_datafusion_expr_with_registered_children(
            &expr,
            &registered,
            50,
            &schema,
        )
        .unwrap();

        match payload {
            DynFilterPayload::Datafusion(bytes) => assert!(!bytes.is_empty()),
            DynFilterPayload::JoinHashBloom(_) => {
                panic!("expected Datafusion fallback for tiny budget")
            }
        }
    }

    #[test]
    fn fingerprint_sensitive_to_seeds() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let id_col = col("id", &schema).unwrap();
        let children: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::clone(&id_col)];

        let fp1 =
            join_hash_bloom::compute_hash_compat_fingerprint(&children, &schema, (0, 0, 0, 0))
                .unwrap();
        let fp2 =
            join_hash_bloom::compute_hash_compat_fingerprint(&children, &schema, (99, 99, 99, 99))
                .unwrap();
        assert_ne!(fp1, fp2, "fingerprints should differ for different seeds");
    }

    #[test]
    fn fingerprint_sensitive_to_child_order() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let col_a = col("a", &schema).unwrap();
        let col_b = col("b", &schema).unwrap();

        let fp_ab = join_hash_bloom::compute_hash_compat_fingerprint(
            &[Arc::clone(&col_a), Arc::clone(&col_b)],
            &schema,
            (1, 2, 3, 4),
        )
        .unwrap();
        let fp_reversed = join_hash_bloom::compute_hash_compat_fingerprint(
            &[Arc::clone(&col_b), Arc::clone(&col_a)],
            &schema,
            (1, 2, 3, 4),
        )
        .unwrap();

        assert_ne!(
            fp_ab, fp_reversed,
            "fingerprint should differ for child order"
        );
    }

    #[test]
    fn fingerprint_sensitive_to_key_type() {
        let schema_i32 = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let schema_i64 = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let col_i32 = col("id", &schema_i32).unwrap();
        let col_i64 = col("id", &schema_i64).unwrap();

        let fp_i32 =
            join_hash_bloom::compute_hash_compat_fingerprint(&[col_i32], &schema_i32, (1, 2, 3, 4))
                .unwrap();
        let fp_i64 =
            join_hash_bloom::compute_hash_compat_fingerprint(&[col_i64], &schema_i64, (1, 2, 3, 4))
                .unwrap();

        assert_ne!(fp_i32, fp_i64, "fingerprint should differ for key type");
    }

    fn contains_expr<T: 'static>(expr: &Arc<dyn PhysicalExpr>) -> bool {
        let mut found = false;
        expr.apply(|node| {
            if node.as_any().is::<T>() {
                found = true;
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })
        .unwrap();
        found
    }
}
