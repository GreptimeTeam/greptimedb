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
#[cfg(any(test, feature = "testing"))]
pub mod remote_membership_test_support;

use std::cell::Cell;
use std::collections::HashSet;
use std::sync::Arc;

use api::v1::region::RegionRequestHeader;
use datafusion::arrow::array::{Array, ArrayRef, StringArray};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::{
    BinaryExpr, CaseExpr, Column, InListExpr, Literal, lit,
};
use datafusion::physical_plan::joins::HashTableLookupExpr;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion_expr::{LogicalPlan, Operator};
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::physical_plan::to_proto::{
    serialize_physical_expr, serialize_physical_expr_with_converter,
};
use datafusion_proto::physical_plan::{
    DefaultPhysicalExtensionCodec, DefaultPhysicalProtoConverter, PhysicalExtensionCodec,
    PhysicalPlanDecodeContext, PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf::physical_expr_node::ExprType as PhysicalExprNodeType;
use datafusion_proto::protobuf::{PhysicalExprNode, PhysicalInListNode, PhysicalPlanNode};
use prost::Message;
use serde::{Deserialize, Serialize};
use snafu::ensure;
use store_api::storage::RegionId;

/// Current wire-format version for remote dynamic filter payload updates.
pub use self::initial_remote_dyn_filter_reg::{
    INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY, InitialDynFilterReg,
    InitialDynFilterRegs, InitialDynFilterSnapshot,
};
use crate::error::{DynFilterPayloadTooLargeSnafu, Error as CommonQueryError};

pub const DYN_FILTER_PROTOCOL_VERSION: u32 = 1;
/// Shared raw protobuf byte budget for remote dynamic filter payloads.
pub const REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES: usize = 512 * 1024;
/// Upper bound on the number of build key values serialized into one remote
/// dynamic filter membership predicate.
const REMOTE_MEMBERSHIP_MAX_VALUES: usize = 2048;
/// Upper bound on the raw Arrow bytes of the build key values serialized into one
/// remote dynamic filter membership predicate. The encoded payload as a whole is
/// still charged against [`REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES`].
const REMOTE_MEMBERSHIP_MAX_BYTES: usize = 128 * 1024;

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
}

impl DynFilterPayload {
    /// Encodes a DataFusion physical expression into a bounded dynamic filter payload.
    ///
    /// Runtime-only hash lookups are recovered as exact build-side membership when
    /// DataFusion published the lookup with a real build lease, the join key is a
    /// single non-dictionary UTF-8 column, and the lookup provably sits in a row-filter
    /// position (the root predicate, an operand of `AND`, or a `CASE` result branch).
    /// The membership is written as the existing `PhysicalInListNode`, so no new wire
    /// variant is needed. Every other lookup is degraded to `true` so the serializable
    /// min/max bounds around it can still be shipped to remote scans.
    /// If the full serializable predicate is still larger than `max_payload_bytes`,
    /// large membership predicates (`IN (...)`) and recovered lookups are also degraded
    /// to `true` as a bounds-only fallback.
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
        let Self::Datafusion(bytes) = self;
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

struct BoundedPhysicalExprEncoder<'a> {
    max_payload_bytes: usize,
    encoded_bytes: Cell<usize>,
    /// Lookups proven to sit in a positive row-filter position, by `Arc` identity.
    ///
    /// [`portable_remote_dyn_filter_expr`] records the exact published nodes here
    /// while it builds the wire form, so the encoder serializes membership only for
    /// the nodes it proved safe and every other lookup keeps the `true` fallback.
    encodable_lookups: &'a HashSet<usize>,
}

impl BoundedPhysicalExprEncoder<'_> {
    /// Charges `proto` (including its encoded children) against the payload budget.
    ///
    /// The accounting is additive over siblings and exact for the top-level node, so
    /// no branch can push the payload over budget unnoticed.
    fn finish(
        &self,
        before_children: usize,
        proto: PhysicalExprNode,
    ) -> DataFusionResult<PhysicalExprNode> {
        let encoded_bytes = before_children.saturating_add(proto.encoded_len());
        self.encoded_bytes.set(encoded_bytes);
        validate_payload_size(encoded_bytes, self.max_payload_bytes)
            .map_err(DataFusionError::from)?;
        Ok(proto)
    }

    /// Encodes a lookup as exact build-side membership, charging every branch literal
    /// and node against the payload budget.
    ///
    /// Returns `None` when the membership cannot be represented, so the caller keeps
    /// the existing `true` fallback. Returning `Err` (including the payload budget
    /// error) aborts the whole payload, which the caller retries as bounds only.
    fn membership_to_proto(
        &self,
        lookup: &HashTableLookupExpr,
        codec: &dyn PhysicalExtensionCodec,
    ) -> DataFusionResult<Option<PhysicalExprNode>> {
        let Some(membership) = RemoteMembership::from_lookup(lookup) else {
            return Ok(None);
        };

        let before_children = self.encoded_bytes.get();
        let proto = match &membership {
            RemoteMembership::ExactInList { key, values } => {
                let Some(literals) = utf8_list_literals(values) else {
                    return Ok(None);
                };

                let mut list = Vec::with_capacity(literals.len());
                for literal in literals {
                    list.push(self.physical_expr_to_proto(&literal, codec)?);
                }

                PhysicalExprNode {
                    expr_id: None,
                    expr_type: Some(PhysicalExprNodeType::InList(Box::new(PhysicalInListNode {
                        expr: Some(Box::new(self.physical_expr_to_proto(key, codec)?)),
                        list,
                        negated: false,
                    }))),
                }
            }
        };

        self.finish(before_children, proto).map(Some)
    }

    /// Encodes the `true` fallback used for every lookup that cannot be recovered.
    fn encode_true(
        &self,
        codec: &dyn PhysicalExtensionCodec,
    ) -> DataFusionResult<PhysicalExprNode> {
        let before_children = self.encoded_bytes.get();
        let literal = lit(true);
        let proto = serialize_physical_expr_with_converter(&literal, codec, self)?;
        self.finish(before_children, proto)
    }
}

impl PhysicalProtoConverterExtension for BoundedPhysicalExprEncoder<'_> {
    fn proto_to_execution_plan(
        &self,
        proto: &PhysicalPlanNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        DefaultPhysicalProtoConverter {}.proto_to_execution_plan(proto, ctx)
    }

    fn execution_plan_to_proto(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        codec: &dyn PhysicalExtensionCodec,
    ) -> DataFusionResult<PhysicalPlanNode> {
        DefaultPhysicalProtoConverter {}.execution_plan_to_proto(plan, codec)
    }

    fn proto_to_physical_expr(
        &self,
        proto: &PhysicalExprNode,
        input_schema: &datafusion::arrow::datatypes::Schema,
        ctx: &PhysicalPlanDecodeContext<'_>,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        DefaultPhysicalProtoConverter {}.proto_to_physical_expr(proto, input_schema, ctx)
    }

    fn physical_expr_to_proto(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        codec: &dyn PhysicalExtensionCodec,
    ) -> DataFusionResult<PhysicalExprNode> {
        // A real `HashTableLookupExpr` has no wire form: DataFusion's own
        // `try_to_proto` degrades it to `true`. Intercept it first so the proven
        // build-side membership can be written with the existing `InList` node
        // instead, without touching the generic fallback or constructing a local
        // `InListExpr` before the budget is known.
        if let Some(lookup) = expr.downcast_ref::<HashTableLookupExpr>() {
            if self
                .encodable_lookups
                .contains(&physical_expr_identity(expr))
                && let Some(proto) = self.membership_to_proto(lookup, codec)?
            {
                return Ok(proto);
            }
            return self.encode_true(codec);
        }

        let before_children = self.encoded_bytes.get();
        let proto = serialize_physical_expr_with_converter(expr, codec, self)?;
        // Replace the children's contribution with the whole subtree, including protobuf framing.
        // Checking each child stops large IN lists before materializing all their Arrow IPC literals.
        self.finish(before_children, proto)
    }
}

fn encode_bounded_physical_expr_to_bytes(
    expr: &Arc<dyn PhysicalExpr>,
    max_payload_bytes: usize,
    encodable_lookups: &HashSet<usize>,
) -> Result<Vec<u8>, CommonQueryError> {
    let codec = DefaultPhysicalExtensionCodec {};
    let encoder = BoundedPhysicalExprEncoder {
        max_payload_bytes,
        encoded_bytes: Cell::new(0),
        encodable_lookups,
    };
    let proto = encoder.physical_expr_to_proto(expr, &codec);
    // Preserve the size error so the caller can retry with bounds only.
    validate_payload_size(encoder.encoded_bytes.get(), max_payload_bytes)?;
    Ok(proto.map_err(CommonQueryError::from)?.encode_to_vec())
}

fn encode_remote_dyn_filter_expr(
    expr: &Arc<dyn PhysicalExpr>,
    max_payload_bytes: usize,
    bounds_only: bool,
) -> Result<Vec<u8>, CommonQueryError> {
    let mut encodable_lookups = HashSet::new();
    let expr = portable_remote_dyn_filter_expr(expr, bounds_only, &mut encodable_lookups);
    encode_bounded_physical_expr_to_bytes(&expr, max_payload_bytes, &encodable_lookups)
}

/// Rewrites the producer predicate into a form the remote wire can carry.
///
/// `HashTableLookupExpr` has no wire form, and the bounds-only retry additionally has
/// to drop `InListExpr`, so both are removed before encoding. The rewrite is allowed
/// to do exactly one thing to a subtree that cannot be represented: **widen it to
/// `true`**, and only at a boolean position where `true` can never remove a row.
///
/// * `AND` and `OR` are monotone, so a widened operand keeps every row the original
///   operator kept.
/// * `CASE` `THEN`/`ELSE` results may widen independently; the branch routing (the
///   `CASE` operand and every `WHEN` key) is never rewritten. If a routing input
///   needs widening, the whole `CASE` widens to `true`, because substituting there
///   could select a *different* branch and therefore a different predicate.
/// * Any other shape (`NOT`, comparisons such as `lookup = false`, `OR`, unknown
///   functions) widens as a whole. Replacing a lookup with `true` *inside* such an
///   operator would flip its value (`NOT true` is `false`) or narrow it, which could
///   discard rows that must still reach the join.
///
/// The single exception is a lookup whose build-side membership is serializable: it is
/// kept (and its identity recorded in `encodable_lookups`) only where the rules above
/// place it in a monotone, row-filtering position. Every result is therefore either
/// the exact producer predicate over the recovered memberships, or a strictly wider
/// one; the encoded predicate can never be `false` or narrower than the producer's.
fn portable_remote_dyn_filter_expr(
    expr: &Arc<dyn PhysicalExpr>,
    bounds_only: bool,
    encodable_lookups: &mut HashSet<usize>,
) -> Arc<dyn PhysicalExpr> {
    rewrite_for_remote_wire(Arc::clone(expr), bounds_only, encodable_lookups)
        .unwrap_or_else(|| lit(true))
}

/// Rewrites one subtree, returning `None` when the whole subtree must widen to `true`.
fn rewrite_for_remote_wire(
    expr: Arc<dyn PhysicalExpr>,
    bounds_only: bool,
    encodable_lookups: &mut HashSet<usize>,
) -> Option<Arc<dyn PhysicalExpr>> {
    if let Some(lookup) = expr.downcast_ref::<HashTableLookupExpr>() {
        if !bounds_only && RemoteMembership::from_lookup(lookup).is_some() {
            encodable_lookups.insert(physical_expr_identity(&expr));
            return Some(expr);
        }
        return None;
    }

    if bounds_only && expr.is::<InListExpr>() {
        return None;
    }

    if let Some(binary) = expr.downcast_ref::<BinaryExpr>() {
        // `AND` and `OR` are monotone in both operands: replacing an operand by
        // `true` can only make the result true for more rows. That keeps the rest of
        // the predicate (typically the bounds) usable when only one operand has to be
        // dropped, e.g. inside the `key IS NULL OR <filter>` wrapper of a null-aware
        // join. Every other operator is treated as opaque.
        let monotone = matches!(binary.op(), Operator::And | Operator::Or);
        if monotone {
            let left =
                rewrite_for_remote_wire(Arc::clone(binary.left()), bounds_only, encodable_lookups)
                    .unwrap_or_else(|| lit(true));
            let right =
                rewrite_for_remote_wire(Arc::clone(binary.right()), bounds_only, encodable_lookups)
                    .unwrap_or_else(|| lit(true));
            return expr.with_new_children(vec![left, right]).ok();
        }
        // `NOT`, comparisons (`lookup = false`) and unknown operators are not
        // monotone: substituting `true` for a child could flip the result to `false`
        // or otherwise narrow it, so the whole node widens instead.
        return (!contains_runtime_membership(&expr, bounds_only)).then_some(expr);
    }

    if let Some(case) = expr.downcast_ref::<CaseExpr>() {
        let mut children = Vec::with_capacity(case.children().len());
        if let Some(operand) = case.expr() {
            // Routing inputs are never rewritten: a widened operand or `WHEN` key
            // would route the same row to a different branch.
            if contains_runtime_membership(operand, bounds_only) {
                return None;
            }
            children.push(Arc::clone(operand));
        }
        for (when, then) in case.when_then_expr() {
            if contains_runtime_membership(when, bounds_only) {
                return None;
            }
            children.push(Arc::clone(when));
            children.push(
                rewrite_for_remote_wire(Arc::clone(then), bounds_only, encodable_lookups)
                    .unwrap_or_else(|| lit(true)),
            );
        }
        if let Some(else_expr) = case.else_expr() {
            children.push(
                rewrite_for_remote_wire(Arc::clone(else_expr), bounds_only, encodable_lookups)
                    .unwrap_or_else(|| lit(true)),
            );
        }
        return expr.with_new_children(children).ok();
    }

    // Every other expression is left untouched unless it holds a runtime membership
    // node, in which case the whole node widens instead of a child being replaced.
    (!contains_runtime_membership(&expr, bounds_only)).then_some(expr)
}

/// Whether the subtree still holds a node that has no wire representation.
///
/// `bounds_only` additionally treats `InListExpr` as runtime-only, because that retry
/// exists solely to drop membership lists that do not fit the payload budget.
fn contains_runtime_membership(expr: &Arc<dyn PhysicalExpr>, bounds_only: bool) -> bool {
    let mut found = false;
    let _ = expr.apply(|node| {
        if node.is::<HashTableLookupExpr>() || (bounds_only && node.is::<InListExpr>()) {
            found = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    });
    found
}

/// Identity of a physical expression node inside one expression tree.
///
/// The rewrite keeps approved lookup nodes by value (they are moved, not cloned), so
/// the encoder receives exactly these `Arc` allocations and the identity is stable.
fn physical_expr_identity(expr: &Arc<dyn PhysicalExpr>) -> usize {
    Arc::as_ptr(expr) as *const () as usize
}

/// Build-side membership that can be serialized into a remote dynamic filter.
///
/// This is the wire backend boundary. Only exact, bounded `key IN (...)` membership
/// is implemented: it reuses the existing `PhysicalInListNode`, needs no in-payload
/// version tag, and is charged against the normal payload budget. A future compact
/// backend (e.g. a serialized build-side lookup table or Bloom filter) is added as
/// another variant plus one encoder arm; it must carry its own version tag and the
/// key type/key semantics (single column, NULL-free, equality/hash domain, including
/// any signed-zero or dictionary normalization), estimate its encoded size *before*
/// writing, and be charged against this same budget. Branch routing, budget
/// accounting and the fail-open contract stay in the encoder and do not depend on
/// which backend is chosen.
enum RemoteMembership {
    /// Exact membership of the published build keys: `probe_key IN (value, ...)`.
    ExactInList {
        /// Probe-side key expression, reused as the `InList` operand.
        key: Arc<dyn PhysicalExpr>,
        /// Published build key values, tied to the lookup's build lease.
        values: ArrayRef,
    },
}

impl RemoteMembership {
    /// Smallest supported membership of `lookup`, or `None` to fail open.
    ///
    /// DataFusion rejects test-only lookups (no build lease), composite keys,
    /// non-`Utf8` keys (float/integer/decimal/dictionary/unknown), NULL-containing
    /// or empty build keys, and values outside the size caps.
    fn from_lookup(lookup: &HashTableLookupExpr) -> Option<Self> {
        let membership = lookup
            .build_key_membership(REMOTE_MEMBERSHIP_MAX_VALUES, REMOTE_MEMBERSHIP_MAX_BYTES)?;
        let values = Arc::clone(membership.values());
        // Only exact `Utf8` membership is in scope; anything else fails open.
        values.as_any().downcast_ref::<StringArray>()?;
        Some(Self::ExactInList {
            key: Arc::clone(membership.key()),
            values,
        })
    }
}

/// Materializes the `IN` list literals for an exact UTF-8 membership.
///
/// Returns `None` if the values are not exactly a non-empty, NULL-free `Utf8` array
/// (defensive: [`RemoteMembership::from_lookup`] already checked it).
fn utf8_list_literals(values: &ArrayRef) -> Option<Vec<Arc<dyn PhysicalExpr>>> {
    let values = values.as_any().downcast_ref::<StringArray>()?;
    if values.is_empty() || values.null_count() != 0 {
        return None;
    }
    values
        .iter()
        .map(|value| {
            value.map(|value| {
                Arc::new(Literal::new(ScalarValue::Utf8(Some(value.to_owned()))))
                    as Arc<dyn PhysicalExpr>
            })
        })
        .collect()
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
        if node.is::<HashTableLookupExpr>() {
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
///
/// Rejects out-of-bounds column indexes and, as a defensive check, rejects columns whose
/// name disagrees with the corresponding schema field. DataFusion physical `Column` is
/// index-authoritative, so a name mismatch usually indicates a coordinator/receiver
/// schema inconsistency that should be surfaced loudly.
fn validate_decoded_payload_expr(
    expr: &Arc<dyn PhysicalExpr>,
    input_schema: &datafusion::arrow::datatypes::Schema,
) -> DataFusionResult<()> {
    expr.apply(|node| {
        if let Some(column) = node.downcast_ref::<Column>() {
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

/// A remote dynamic filter update sent from a query coordinator to region servers.
///
/// `generation` is monotonic within a `query_id`/`filter_id` pair and matches the
/// gRPC field name used by `RemoteDynFilterUpdate`. Receivers use it to ignore
/// stale updates while `is_complete` marks the final payload for the filter.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DynFilterUpdate {
    /// Protocol version used by this update payload.
    pub protocol_version: u32,
    /// Internal query identifier that owns this dynamic filter lifecycle.
    pub query_id: String,
    /// Identifier of the dynamic filter within the query.
    pub filter_id: String,
    /// Monotonic update generation for this filter.
    pub generation: u64,
    /// Whether this update completes the dynamic filter stream.
    pub is_complete: bool,
    /// Serialized predicate payload carried by this update.
    pub payload: DynFilterPayload,
}

impl DynFilterUpdate {
    /// Creates a dynamic filter update with the current protocol version.
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
    /// The header of this request. Often to store some context of the query. None means all to defaults.
    pub header: Option<RegionRequestHeader>,

    /// The id of the region to be queried.
    pub region_id: RegionId,

    /// The form of the query: a logical plan.
    pub plan: LogicalPlan,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use base64::Engine;
    use base64::prelude::BASE64_STANDARD;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, InListExpr, lit};
    use datafusion::physical_plan::expressions::col;
    use datafusion::physical_plan::joins::join_hash_map::JoinHashMapU32;
    use datafusion::physical_plan::joins::{HashTableLookupExpr, Map, SeededRandomState};
    use datafusion_expr::Operator;

    use super::*;

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

        let original = expr.downcast_ref::<Column>().unwrap();
        let decoded = decoded.downcast_ref::<Column>().unwrap();

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
            SeededRandomState::with_seed(0),
            Arc::new(Map::HashMap(Box::new(JoinHashMapU32::with_capacity(0)))),
            "hash_lookup".to_string(),
            None,
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
        let bounds_only = portable_remote_dyn_filter_expr(&expr, true, &mut HashSet::new());
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
    fn bounded_encoding_preserves_bytes_and_exact_limit() {
        let schema = Schema::new(vec![Field::new("device_id", DataType::Int32, false)]);
        let device_id = Arc::new(Column::new("device_id", 0)) as Arc<dyn PhysicalExpr>;
        let in_list = Arc::new(
            InListExpr::try_new(
                Arc::clone(&device_id),
                (0..64).map(lit).collect(),
                false,
                &schema,
            )
            .unwrap(),
        ) as Arc<dyn PhysicalExpr>;
        let lower_bound = Arc::new(BinaryExpr::new(device_id, Operator::GtEq, lit(10i32)))
            as Arc<dyn PhysicalExpr>;
        let expr =
            Arc::new(BinaryExpr::new(lower_bound, Operator::And, in_list)) as Arc<dyn PhysicalExpr>;
        let expected = serialize_physical_expr(&expr, &DefaultPhysicalExtensionCodec {})
            .unwrap()
            .encode_to_vec();

        assert_eq!(
            encode_bounded_physical_expr_to_bytes(&expr, expected.len(), &HashSet::new()).unwrap(),
            expected
        );
        assert!(matches!(
            encode_bounded_physical_expr_to_bytes(&expr, expected.len() - 1, &HashSet::new()),
            Err(CommonQueryError::DynFilterPayloadTooLarge { .. })
        ));
    }

    #[test]
    fn bounded_encoding_stops_before_later_children() {
        let schema = Schema::new(vec![Field::new("host", DataType::Utf8, false)]);
        let expr = Arc::new(
            InListExpr::try_new(
                Arc::new(Column::new("host", 0)),
                (0..8).map(|i| lit(i.to_string().repeat(1024))).collect(),
                false,
                &schema,
            )
            .unwrap(),
        ) as Arc<dyn PhysicalExpr>;
        let full_size = encode_physical_expr_to_bytes(&expr).unwrap().len();
        assert!(full_size > 8192);

        let error =
            encode_bounded_physical_expr_to_bytes(&expr, 2048, &HashSet::new()).unwrap_err();
        let CommonQueryError::DynFilterPayloadTooLarge {
            payload_size_bytes, ..
        } = error
        else {
            panic!("expected payload size error, got {error:?}");
        };
        assert!((2048..4096).contains(&payload_size_bytes));
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

    fn contains_expr<T: PhysicalExpr>(expr: &Arc<dyn PhysicalExpr>) -> bool {
        let mut found = false;
        expr.apply(|node| {
            if node.is::<T>() {
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

/// Remote dynamic filter membership recovered from *real* DataFusion build leases.
///
/// The tests above build `HashTableLookupExpr` by hand, which always has
/// `build_lease = None`. These tests run an actual `HashJoinExec` so DataFusion
/// publishes lookups backed by a real build lease, which is the only way to prove that
/// membership recovery (and its fail-open paths) actually works.
///
/// Every remote predicate is checked twice: structurally (what the payload contains)
/// and by **evaluating** the producer predicate and the decoded predicate on the same
/// rows. Evaluation is what pins the fail-open contract: whatever the encoder does, the
/// decoded predicate must keep every row the producer kept.
#[cfg(test)]
mod remote_membership_tests {
    use std::collections::{BTreeMap, HashSet};

    use datafusion::arrow::array::{ArrayRef, BooleanArray, Float64Array, StringDictionaryBuilder};
    use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::expressions::{
        BinaryExpr, CaseExpr, Column, InListExpr, IsNullExpr, Literal, NotExpr,
    };
    use datafusion::physical_plan::PhysicalExpr;
    use datafusion::physical_plan::joins::{HashExpr, HashTableLookupExpr};
    use datafusion_expr::Operator;

    use super::remote_membership_test_support::{
        key, one_column_schema, partitions, run_real_hash_join, utf8, utf8_join,
    };
    use super::*;

    fn has_expr<T: PhysicalExpr>(expr: &Arc<dyn PhysicalExpr>) -> bool {
        find_expr::<T>(expr).is_some()
    }

    fn find_expr<T: PhysicalExpr>(expr: &Arc<dyn PhysicalExpr>) -> Option<Arc<dyn PhysicalExpr>> {
        let mut found = None;
        expr.apply(|node| {
            if node.is::<T>() {
                found = Some(Arc::clone(node));
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })
        .unwrap();
        found
    }

    fn in_lists(expr: &Arc<dyn PhysicalExpr>) -> Vec<Arc<dyn PhysicalExpr>> {
        let mut found = Vec::new();
        expr.apply(|node| {
            if node.is::<InListExpr>() {
                found.push(Arc::clone(node));
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        found
    }

    fn in_list_values(expr: &Arc<dyn PhysicalExpr>) -> Vec<String> {
        let in_list = expr.downcast_ref::<InListExpr>().unwrap();
        assert!(!in_list.negated());
        in_list
            .list()
            .iter()
            .map(|literal| {
                literal
                    .downcast_ref::<Literal>()
                    .unwrap()
                    .value()
                    .to_string()
                    .trim_matches('\'')
                    .to_string()
            })
            .collect()
    }

    fn assert_has_bounds(expr: &Arc<dyn PhysicalExpr>) {
        let display = expr.to_string();
        assert!(display.contains(">="), "missing lower bound: {display}");
        assert!(display.contains("<="), "missing upper bound: {display}");
    }

    fn decode(
        expr: &Arc<dyn PhysicalExpr>,
        schema: &Arc<Schema>,
        max_payload_bytes: usize,
    ) -> Arc<dyn PhysicalExpr> {
        DynFilterPayload::from_datafusion_expr(expr, max_payload_bytes)
            .unwrap()
            .decode_datafusion_expr(&TaskContext::default(), schema, max_payload_bytes)
            .unwrap()
    }

    fn utf8_batch(schema: &Arc<Schema>, values: &[&str]) -> RecordBatch {
        RecordBatch::try_new(Arc::clone(schema), vec![utf8(values)]).unwrap()
    }

    fn selected(expr: &Arc<dyn PhysicalExpr>, batch: &RecordBatch) -> Vec<bool> {
        let array = expr
            .evaluate(batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        let booleans = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        (0..batch.num_rows())
            .map(|index| booleans.value(index))
            .collect()
    }

    /// Fail-open contract: the decoded predicate keeps every row the producer kept.
    fn assert_fail_open(
        original: &Arc<dyn PhysicalExpr>,
        decoded: &Arc<dyn PhysicalExpr>,
        batch: &RecordBatch,
    ) -> (Vec<bool>, Vec<bool>) {
        let original_selected = selected(original, batch);
        let decoded_selected = selected(decoded, batch);
        for (row, (kept, kept_after)) in original_selected
            .iter()
            .zip(decoded_selected.iter())
            .enumerate()
        {
            assert!(
                kept_after >= kept,
                "decoded predicate dropped row {row}: producer={kept} decoded={kept_after} \
                 (decoded predicate: {decoded})"
            );
        }
        (original_selected, decoded_selected)
    }

    /// Branch chosen by the CASE routing expression for every row.
    fn routed_branch(case: &CaseExpr, batch: &RecordBatch) -> Vec<u64> {
        let routing = case.expr().expect("hash routing operand");
        let array = routing
            .evaluate(batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        let branches = array
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .unwrap();
        (0..batch.num_rows())
            .map(|index| branches.value(index))
            .collect()
    }

    fn hash_seed(expr: &Arc<dyn PhysicalExpr>) -> u64 {
        let mut seed = None;
        expr.apply(|node| {
            if let Some(hash) = node.downcast_ref::<HashExpr>() {
                seed = Some(hash.seed());
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })
        .unwrap();
        seed.expect("hash routing expression")
    }

    /// `trace_id` values used to evaluate recovered filters.
    fn evaluation_batch(schema: &Arc<Schema>, values: &[String]) -> RecordBatch {
        let borrowed = values.iter().map(String::as_str).collect::<Vec<_>>();
        utf8_batch(schema, &borrowed)
    }

    #[tokio::test]
    async fn membership_round_trips_real_utf8_build_lease() {
        let schema = one_column_schema("trace_id", DataType::Utf8);
        let (build, probe, values) = utf8_join(1);
        let trace_id = key("trace_id", 0);
        let filter = run_real_hash_join(
            1,
            Arc::clone(&schema),
            Arc::clone(&schema),
            build,
            probe,
            vec![(Arc::clone(&trace_id), Arc::clone(&trace_id))],
        )
        .await;

        // The producer predicate really holds a runtime-only lookup...
        assert!(has_expr::<HashTableLookupExpr>(&filter.expr));

        // ...and it is serialized as exact build membership, not as `true`.
        let decoded = decode(
            &filter.expr,
            &filter.probe_schema,
            REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES,
        );
        assert!(!has_expr::<HashTableLookupExpr>(&decoded));
        let lists = in_lists(&decoded);
        assert_eq!(lists.len(), 1, "expected one recovered InList: {decoded}");
        assert_eq!(in_list_values(&lists[0]), values);
        assert_has_bounds(&decoded);

        // Members are selected, non-members are not, nothing else changes.
        let mut rows = values.clone();
        rows.push("miss-not-a-member".to_string());
        let batch = evaluation_batch(&schema, &rows);
        let (original_selected, decoded_selected) =
            assert_fail_open(&filter.expr, &decoded, &batch);
        for row in 0..values.len() {
            assert!(original_selected[row], "member row {row} must be selected");
            assert!(decoded_selected[row], "member row {row} must stay selected");
        }
        assert!(
            !decoded_selected[values.len()],
            "non-member must not be selected"
        );
    }

    #[tokio::test]
    async fn membership_preserves_three_branch_case_structure() {
        let schema = one_column_schema("trace_id", DataType::Utf8);
        let (build, probe, values) = utf8_join(3);
        let trace_id = key("trace_id", 0);
        let filter = run_real_hash_join(
            3,
            Arc::clone(&schema),
            Arc::clone(&schema),
            build,
            probe,
            vec![(Arc::clone(&trace_id), Arc::clone(&trace_id))],
        )
        .await;

        // Three hash-routed partitions produce a three-branch CASE.
        let original_case = filter
            .expr
            .downcast_ref::<CaseExpr>()
            .expect("real three-branch CASE");
        assert_eq!(original_case.when_then_expr().len(), 3);

        let decoded = decode(
            &filter.expr,
            &filter.probe_schema,
            REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES,
        );
        let case = decoded.downcast_ref::<CaseExpr>().expect("CASE preserved");
        assert_eq!(case.when_then_expr().len(), 3);
        assert!(case.else_expr().is_some());

        // Routing is untouched: same operand expression and same `HashExpr` seed, so
        // the receiver selects the same branch for every row.
        assert_eq!(
            original_case.expr().unwrap().to_string(),
            case.expr().unwrap().to_string()
        );
        assert_eq!(
            hash_seed(original_case.expr().unwrap()),
            hash_seed(case.expr().unwrap())
        );

        // Branch-specific membership: the WHEN literal names the branch, so the values
        // recovered for each branch can be checked individually.
        let mut branch_values: BTreeMap<u64, Vec<String>> = BTreeMap::new();
        for (when, then) in case.when_then_expr() {
            let branch = match when.downcast_ref::<Literal>().unwrap().value() {
                ScalarValue::UInt64(Some(branch)) => *branch,
                other => panic!("unexpected CASE routing literal: {other:?}"),
            };
            let lists = in_lists(then);
            assert_eq!(lists.len(), 1, "each branch keeps its membership: {then}");
            let mut branch_list = in_list_values(&lists[0]);
            branch_list.sort();
            assert!(
                branch_values.insert(branch, branch_list).is_none(),
                "branch {branch} appears twice"
            );
            assert_has_bounds(then);
        }
        assert_eq!(branch_values.len(), 3, "three distinct branches");

        // Every build key is recovered exactly once, in its own branch.
        let mut recovered = branch_values
            .values()
            .flatten()
            .cloned()
            .collect::<Vec<_>>();
        recovered.sort();
        assert_eq!(recovered, values);

        // Evaluation proves branch-specific routing: a row is selected iff its key is in
        // the membership of the branch the routing sends it to.
        let mut rows = values.clone();
        rows.extend((0..4).map(|i| format!("miss-{i:02}")));
        let batch = evaluation_batch(&schema, &rows);
        let (original_selected, decoded_selected) =
            assert_fail_open(&filter.expr, &decoded, &batch);
        let branches = routed_branch(case, &batch);
        for (row, value) in rows.iter().enumerate() {
            let branch = branches[row];
            let expected = branch_values
                .get(&branch)
                .is_some_and(|members| members.binary_search(value).is_ok());
            assert_eq!(
                decoded_selected[row], expected,
                "row {value} routed to branch {branch} must be selected iff it is a \
                 member of that branch's list"
            );
            // Build members must keep their decision; misses only ever widen.
            if expected {
                assert!(
                    original_selected[row],
                    "producer must select its own build member {value}"
                );
                assert!(decoded_selected[row]);
            }
        }
        assert!(
            decoded_selected
                .iter()
                .filter(|selected| **selected)
                .count()
                >= values.len(),
            "every build member must still be selected"
        );
    }

    #[tokio::test]
    async fn non_monotone_lookups_widen_to_true() {
        let schema = one_column_schema("trace_id", DataType::Utf8);
        let (build, probe, values) = utf8_join(1);
        let trace_id = key("trace_id", 0);
        let filter = run_real_hash_join(
            1,
            Arc::clone(&schema),
            Arc::clone(&schema),
            build,
            probe,
            vec![(Arc::clone(&trace_id), Arc::clone(&trace_id))],
        )
        .await;
        let lookup = find_expr::<HashTableLookupExpr>(&filter.expr).expect("real lookup");

        let mut rows = values.clone();
        rows.extend((0..3).map(|i| format!("miss-{i:02}")));
        let batch = evaluation_batch(&schema, &rows);
        let max = REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES;

        // `NOT lookup`: replacing the lookup with `true` in place would produce
        // `NOT true` = false and drop every row. The whole node must widen instead.
        let not_lookup = Arc::new(NotExpr::new(Arc::clone(&lookup))) as Arc<dyn PhysicalExpr>;
        let decoded = decode(&not_lookup, &filter.probe_schema, max);
        assert_eq!(decoded.to_string(), "true");
        let (original_selected, decoded_selected) = assert_fail_open(&not_lookup, &decoded, &batch);
        assert!(
            decoded_selected.iter().all(|selected| *selected),
            "widened predicate must keep all rows"
        );
        assert!(
            original_selected[values.len()..]
                .iter()
                .all(|selected| *selected),
            "producer `NOT lookup` keeps non-members"
        );
        assert!(
            original_selected[..values.len()]
                .iter()
                .all(|selected| !*selected),
            "producer `NOT lookup` drops members"
        );

        // `lookup = false`: in-place replacement would evaluate `true = false` = false.
        let lookup_is_false = Arc::new(BinaryExpr::new(
            Arc::clone(&lookup),
            Operator::Eq,
            lit(false),
        )) as Arc<dyn PhysicalExpr>;
        let decoded = decode(&lookup_is_false, &filter.probe_schema, max);
        assert_eq!(decoded.to_string(), "true");
        assert_fail_open(&lookup_is_false, &decoded, &batch);

        // `CASE` routing operand: replacing the lookup with `true` would route every row
        // to the first branch. The whole CASE must widen instead.
        let member_probe = Arc::new(BinaryExpr::new(
            Arc::clone(&trace_id),
            Operator::Eq,
            lit(values[0].clone()),
        )) as Arc<dyn PhysicalExpr>;
        let other_probe = Arc::new(BinaryExpr::new(
            Arc::clone(&trace_id),
            Operator::Eq,
            lit("miss-never"),
        )) as Arc<dyn PhysicalExpr>;
        let operand_case = Arc::new(
            CaseExpr::try_new(
                Some(Arc::clone(&lookup)),
                vec![
                    (lit(true), Arc::clone(&member_probe)),
                    (lit(false), Arc::clone(&other_probe)),
                ],
                Some(lit(false)),
            )
            .unwrap(),
        ) as Arc<dyn PhysicalExpr>;
        let decoded = decode(&operand_case, &filter.probe_schema, max);
        assert_eq!(decoded.to_string(), "true");
        let (original_selected, decoded_selected) =
            assert_fail_open(&operand_case, &decoded, &batch);
        assert!(
            original_selected[0],
            "producer selects the member branch row"
        );
        assert!(decoded_selected.iter().all(|selected| *selected));

        // `CASE` WHEN key: a lookup used as a branch condition selects between branches.
        let when_case = Arc::new(
            CaseExpr::try_new(
                None,
                vec![(Arc::clone(&lookup), member_probe)],
                Some(other_probe),
            )
            .unwrap(),
        ) as Arc<dyn PhysicalExpr>;
        let decoded = decode(&when_case, &filter.probe_schema, max);
        assert_eq!(decoded.to_string(), "true");
        let (original_selected, decoded_selected) = assert_fail_open(&when_case, &decoded, &batch);
        assert!(
            original_selected[0],
            "producer selects the member branch row"
        );
        assert!(decoded_selected.iter().all(|selected| *selected));
    }

    #[tokio::test]
    async fn membership_fails_open_for_float_composite_and_dictionary_keys() {
        // Float64 keys: out of the supported type scope.
        let float_schema = one_column_schema("k", DataType::Float64);
        let float_on = key("k", 0);
        let float_filter = run_real_hash_join(
            2,
            Arc::clone(&float_schema),
            Arc::clone(&float_schema),
            partitions(
                &float_schema,
                vec![
                    vec![Arc::new(Float64Array::from(vec![0.0, -0.0])) as ArrayRef],
                    vec![Arc::new(Float64Array::from(vec![1.5])) as ArrayRef],
                ],
            ),
            partitions(
                &float_schema,
                vec![
                    vec![Arc::new(Float64Array::from(vec![0.0, 2.5])) as ArrayRef],
                    vec![Arc::new(Float64Array::from(vec![1.5])) as ArrayRef],
                ],
            ),
            vec![(Arc::clone(&float_on), Arc::clone(&float_on))],
        )
        .await;
        assert!(has_expr::<HashTableLookupExpr>(&float_filter.expr));
        let decoded = decode(
            &float_filter.expr,
            &float_filter.probe_schema,
            REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES,
        );
        assert!(!has_expr::<HashTableLookupExpr>(&decoded));
        assert!(
            in_lists(&decoded).is_empty(),
            "float membership must fail open: {decoded}"
        );
        assert_has_bounds(&decoded);
        let float_batch = RecordBatch::try_new(
            Arc::clone(&float_schema),
            vec![Arc::new(Float64Array::from(vec![0.0, -0.0, 1.5, 2.5])) as ArrayRef],
        )
        .unwrap();
        assert_fail_open(&float_filter.expr, &decoded, &float_batch);

        // Composite (two-column) keys: out of the supported key scope. Both columns take
        // part in the hash partitioning, exactly like a real two-key join.
        let composite_schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, true),
            Field::new("host", DataType::Utf8, true),
        ]));
        let composite_on = vec![
            (key("trace_id", 0), key("trace_id", 0)),
            (key("host", 1), key("host", 1)),
        ];
        let composite_filter = run_real_hash_join(
            2,
            Arc::clone(&composite_schema),
            Arc::clone(&composite_schema),
            partitions(
                &composite_schema,
                vec![
                    vec![utf8(&["trace-0", "trace-1"]), utf8(&["host-a", "host-a"])],
                    vec![utf8(&["trace-2"]), utf8(&["host-b"])],
                ],
            ),
            partitions(
                &composite_schema,
                vec![
                    vec![utf8(&["trace-0", "trace-9"]), utf8(&["host-a", "host-z"])],
                    vec![utf8(&["trace-2"]), utf8(&["host-b"])],
                ],
            ),
            composite_on,
        )
        .await;
        assert!(has_expr::<HashTableLookupExpr>(&composite_filter.expr));
        let decoded = decode(
            &composite_filter.expr,
            &composite_filter.probe_schema,
            REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES,
        );
        assert!(!has_expr::<HashTableLookupExpr>(&decoded));
        assert!(
            in_lists(&decoded).is_empty(),
            "composite membership must fail open: {decoded}"
        );
        assert_has_bounds(&decoded);
        let composite_batch = RecordBatch::try_new(
            Arc::clone(&composite_schema),
            vec![
                utf8(&["trace-0", "trace-2", "trace-9"]),
                utf8(&["host-a", "host-b", "host-z"]),
            ],
        )
        .unwrap();
        assert_fail_open(&composite_filter.expr, &decoded, &composite_batch);

        // Dictionary keys: out of the supported type scope.
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let dict_schema = one_column_schema("trace_id", dict_type);
        let dict_on = key("trace_id", 0);
        let mut dict_builder = StringDictionaryBuilder::<Int32Type>::new();
        dict_builder.append_value("trace-0");
        dict_builder.append_value("trace-1");
        let dict_values: ArrayRef = Arc::new(dict_builder.finish());
        let dict_filter = run_real_hash_join(
            2,
            Arc::clone(&dict_schema),
            Arc::clone(&dict_schema),
            partitions(&dict_schema, vec![vec![Arc::clone(&dict_values)]]),
            partitions(&dict_schema, vec![vec![dict_values]]),
            vec![(Arc::clone(&dict_on), Arc::clone(&dict_on))],
        )
        .await;
        assert!(has_expr::<HashTableLookupExpr>(&dict_filter.expr));
        let decoded = decode(
            &dict_filter.expr,
            &dict_filter.probe_schema,
            REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES,
        );
        assert!(!has_expr::<HashTableLookupExpr>(&decoded));
        assert!(
            in_lists(&decoded).is_empty(),
            "dictionary membership must fail open: {decoded}"
        );
    }

    #[tokio::test]
    async fn membership_over_total_branch_budget_falls_back_to_bounds_only() {
        let schema = one_column_schema("trace_id", DataType::Utf8);
        // Long values so the membership lists dominate the payload.
        let long = "t".repeat(256);
        let values = (0..24)
            .map(|i| format!("{long}-{i:02}"))
            .collect::<Vec<_>>();
        let borrowed = values.iter().map(String::as_str).collect::<Vec<_>>();
        let build = partitions(&schema, vec![vec![utf8(&borrowed)]]);
        let probe = partitions(&schema, vec![vec![utf8(&borrowed)]]);
        let trace_id = key("trace_id", 0);
        let filter = run_real_hash_join(
            3,
            Arc::clone(&schema),
            Arc::clone(&schema),
            build,
            probe,
            vec![(Arc::clone(&trace_id), Arc::clone(&trace_id))],
        )
        .await;
        assert!(has_expr::<HashTableLookupExpr>(&filter.expr));

        // Budget the payload at exactly the bounds-only size: every branch must lose its
        // membership, not just the branch that happened to overflow first.
        let bounds_only = portable_remote_dyn_filter_expr(&filter.expr, true, &mut HashSet::new());
        let bounds_only_size = encode_physical_expr_to_bytes(&bounds_only).unwrap().len();
        let full_size =
            match DynFilterPayload::from_datafusion_expr(&filter.expr, usize::MAX).unwrap() {
                DynFilterPayload::Datafusion(bytes) => bytes.len(),
            };
        assert!(
            full_size > bounds_only_size,
            "test fixture must exceed the bounds-only payload"
        );
        assert!(
            bounds_only_size < REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES,
            "bounds-only fallback must fit the budget"
        );

        let decoded = decode(&filter.expr, &filter.probe_schema, bounds_only_size);
        assert!(!has_expr::<HashTableLookupExpr>(&decoded));
        assert!(
            in_lists(&decoded).is_empty(),
            "over-budget payload must fall back to bounds only: {decoded}"
        );
        let case = decoded.downcast_ref::<CaseExpr>().expect("CASE preserved");
        assert_eq!(case.when_then_expr().len(), 3);
        for (_, branch) in case.when_then_expr() {
            assert_has_bounds(branch);
            assert!(
                !branch.to_string().contains("false"),
                "bounds-only branch must not become false: {branch}"
            );
        }

        // Bounds-only is a strict widening: it keeps every row the producer kept, and
        // every build member is inside its branch's bounds.
        let mut rows = values.clone();
        rows.push("miss-not-a-member".to_string());
        let batch = evaluation_batch(&schema, &rows);
        let (original_selected, decoded_selected) =
            assert_fail_open(&filter.expr, &decoded, &batch);
        for row in 0..values.len() {
            assert!(original_selected[row]);
            assert!(
                decoded_selected[row],
                "bounds-only fallback must keep build member row {row}"
            );
        }

        // The real 512KiB budget still carries all three exact branch memberships.
        let decoded = decode(
            &filter.expr,
            &filter.probe_schema,
            REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES,
        );
        assert_eq!(in_lists(&decoded).len(), 3);
        assert_fail_open(&filter.expr, &decoded, &batch);
    }

    #[test]
    fn forced_overflow_in_under_not_cannot_become_false() {
        let schema = Arc::new(Schema::new(vec![Field::new("host", DataType::Utf8, false)]));
        let host = Arc::new(Column::new("host", 0)) as Arc<dyn PhysicalExpr>;
        let long = "h".repeat(256);
        let in_list = Arc::new(
            InListExpr::try_new(
                Arc::clone(&host),
                (0..16).map(|i| lit(format!("{long}-{i:02}"))).collect(),
                false,
                &schema,
            )
            .unwrap(),
        ) as Arc<dyn PhysicalExpr>;
        // `NOT (host IN (...))`: replacing the list with `true` in place would produce
        // `NOT true` = false, which would drop every row.
        let not_in = Arc::new(NotExpr::new(Arc::clone(&in_list))) as Arc<dyn PhysicalExpr>;

        let full_size = encode_physical_expr_to_bytes(&not_in).unwrap().len();
        assert!(full_size > 2048);
        let decoded = decode(&not_in, &schema, 512);
        assert_eq!(decoded.to_string(), "true");
        assert!(in_lists(&decoded).is_empty());

        let batch = utf8_batch(&schema, &["h-other", &format!("{long}-00")]);
        let (original_selected, decoded_selected) = assert_fail_open(&not_in, &decoded, &batch);
        assert_eq!(original_selected, vec![true, false]);
        assert_eq!(decoded_selected, vec![true, true]);
    }

    #[test]
    fn budget_fallback_keeps_bounds_for_a_positive_over_budget_conjunction() {
        let schema = Arc::new(Schema::new(vec![Field::new("host", DataType::Utf8, false)]));
        let host = Arc::new(Column::new("host", 0)) as Arc<dyn PhysicalExpr>;
        let long = "h".repeat(256);
        let in_list = Arc::new(
            InListExpr::try_new(
                Arc::clone(&host),
                (0..16).map(|i| lit(format!("{long}-{i:02}"))).collect(),
                false,
                &schema,
            )
            .unwrap(),
        ) as Arc<dyn PhysicalExpr>;
        let lower = Arc::new(BinaryExpr::new(
            Arc::clone(&host),
            Operator::GtEq,
            lit(format!("{long}-00")),
        )) as Arc<dyn PhysicalExpr>;
        let expr =
            Arc::new(BinaryExpr::new(lower, Operator::And, in_list)) as Arc<dyn PhysicalExpr>;

        let bounds_only = portable_remote_dyn_filter_expr(&expr, true, &mut HashSet::new());
        let bounds_only_size = encode_physical_expr_to_bytes(&bounds_only).unwrap().len();
        let decoded = decode(&expr, &schema, bounds_only_size);

        // Only the membership list is dropped: the bounds survive and nothing is false.
        assert!(in_lists(&decoded).is_empty());
        assert!(decoded.to_string().contains(">="));
        assert!(!decoded.to_string().contains("false"));

        let batch = utf8_batch(
            &schema,
            &[&format!("{long}-00"), &format!("{long}-99"), "h-other"],
        );
        let (original_selected, decoded_selected) = assert_fail_open(&expr, &decoded, &batch);
        assert_eq!(original_selected, vec![true, false, false]);
        assert_eq!(decoded_selected, vec![true, true, false]);
    }

    #[test]
    fn bounds_survive_a_null_wrapper_or_around_an_over_budget_membership() {
        // `key IS NULL OR <filter>` is how DataFusion keeps probe NULLs for null-aware
        // joins. `OR` is monotone, so the bounds inside must survive even when the
        // membership list is dropped by the budget fallback.
        let schema = Arc::new(Schema::new(vec![Field::new("host", DataType::Utf8, true)]));
        let host = Arc::new(Column::new("host", 0)) as Arc<dyn PhysicalExpr>;
        let long = "h".repeat(256);
        let lower = Arc::new(BinaryExpr::new(
            Arc::clone(&host),
            Operator::GtEq,
            lit(format!("{long}-00")),
        )) as Arc<dyn PhysicalExpr>;
        let in_list = Arc::new(
            InListExpr::try_new(
                Arc::clone(&host),
                (0..16).map(|i| lit(format!("{long}-{i:02}"))).collect(),
                false,
                &schema,
            )
            .unwrap(),
        ) as Arc<dyn PhysicalExpr>;
        let membership =
            Arc::new(BinaryExpr::new(lower, Operator::And, in_list)) as Arc<dyn PhysicalExpr>;
        let is_null = Arc::new(IsNullExpr::new(Arc::clone(&host))) as Arc<dyn PhysicalExpr>;
        let expr =
            Arc::new(BinaryExpr::new(is_null, Operator::Or, membership)) as Arc<dyn PhysicalExpr>;

        let bounds_only = portable_remote_dyn_filter_expr(&expr, true, &mut HashSet::new());
        let bounds_only_size = encode_physical_expr_to_bytes(&bounds_only).unwrap().len();
        let decoded = decode(&expr, &schema, bounds_only_size);

        assert!(in_lists(&decoded).is_empty());
        assert!(decoded.to_string().contains("IS NULL"), "{decoded}");
        assert!(decoded.to_string().contains(">="), "{decoded}");
        assert!(!decoded.to_string().contains("false"), "{decoded}");

        let batch = utf8_batch(
            &schema,
            &[&format!("{long}-00"), &format!("{long}-99"), "h-other"],
        );
        let (original_selected, decoded_selected) = assert_fail_open(&expr, &decoded, &batch);
        assert_eq!(original_selected, vec![true, false, false]);
        assert_eq!(decoded_selected, vec![true, true, false]);
    }

    #[tokio::test]
    async fn recovered_payload_uses_only_existing_wire_variants() {
        let schema = one_column_schema("trace_id", DataType::Utf8);
        let (build, probe, values) = utf8_join(3);
        let trace_id = key("trace_id", 0);
        let filter = run_real_hash_join(
            3,
            Arc::clone(&schema),
            Arc::clone(&schema),
            build,
            probe,
            vec![(Arc::clone(&trace_id), Arc::clone(&trace_id))],
        )
        .await;
        let max = REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES;

        // The recovered payload decodes with the default extension codec, which rejects
        // every expression variant it does not know: a new wire variant would fail here.
        let payload = DynFilterPayload::from_datafusion_expr(&filter.expr, max).unwrap();
        let bytes = match &payload {
            DynFilterPayload::Datafusion(bytes) => bytes.clone(),
        };
        let decoded = payload
            .decode_datafusion_expr(&TaskContext::default(), &filter.probe_schema, max)
            .unwrap();
        assert_eq!(in_lists(&decoded).len(), 3);
        assert_eq!(
            hash_seed(
                filter
                    .expr
                    .downcast_ref::<CaseExpr>()
                    .unwrap()
                    .expr()
                    .unwrap()
            ),
            hash_seed(decoded.downcast_ref::<CaseExpr>().unwrap().expr().unwrap()),
            "HashExpr routing seed must survive the wire"
        );

        // A payload produced by the previous encoder (every lookup degraded to `true`,
        // bounds kept) is still readable and stays a widening of the producer predicate.
        let legacy = portable_remote_dyn_filter_expr(&filter.expr, true, &mut HashSet::new());
        let legacy_bytes = encode_physical_expr_to_bytes(&legacy).unwrap();
        assert!(legacy_bytes.len() < bytes.len());
        let legacy_decoded = DynFilterPayload::Datafusion(legacy_bytes)
            .decode_datafusion_expr(&TaskContext::default(), &filter.probe_schema, max)
            .unwrap();
        let mut rows = values.clone();
        rows.push("miss-not-a-member".to_string());
        let batch = evaluation_batch(&schema, &rows);
        assert_fail_open(&filter.expr, &legacy_decoded, &batch);
    }
}
