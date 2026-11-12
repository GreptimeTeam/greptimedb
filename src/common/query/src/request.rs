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
use std::sync::Arc;

use api::v1::region::RegionRequestHeader;
use datafusion::arrow::array::{Array, ArrayRef, StringArray};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::{Column, InListExpr, Literal};
use datafusion::physical_expr::{conjunction, split_conjunction};
use datafusion::physical_plan::joins::HashTableLookupExpr;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion_expr::LogicalPlan;
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
use crate::error::{
    DynFilterPayloadTooLargeSnafu, DynFilterUnsupportedSnafu, Error as CommonQueryError,
};

pub const DYN_FILTER_PROTOCOL_VERSION: u32 = 1;
/// Shared raw protobuf byte budget for remote dynamic filter payloads.
pub const REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES: usize = 512 * 1024;

/// Smallest number of encoded bytes a single `IN` list literal can take.
///
/// Every list entry is a `PhysicalExprNode` whose `literal` arm holds the
/// [`ScalarValue`] message itself, so the oneof field tag plus its length prefix are
/// already 2 bytes even for an empty `Utf8` value (whose `ScalarValue` body may encode to
/// nothing at all). Protobuf sizes add up across nesting, so a payload of `B` bytes can
/// never hold more than `B / 2` literals. This is the floor the value bound is derived
/// from; it is not an encoder policy constant.
const MIN_ENCODED_LITERAL_BYTES: usize = 2;

/// Upper bound on how many `IN` list literals `max_payload_bytes` can hold.
///
/// The count bound is derived from the transport budget with
/// [`MIN_ENCODED_LITERAL_BYTES`] rather than being an arbitrary cap: a larger budget
/// admits proportionally more build keys, and a small budget refuses the membership
/// before a single literal is built.
fn max_membership_values(max_payload_bytes: usize) -> usize {
    max_payload_bytes / MIN_ENCODED_LITERAL_BYTES
}

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
    /// # Narrow contract: a key list used as a positive pruning hint
    ///
    /// A runtime [`HashTableLookupExpr`] is written as the existing
    /// `PhysicalInListNode` — `key IN (build key, ...)` — built from the lookup's
    /// published build-key list, so no new wire variant is needed. The expression tree
    /// around it is preserved node for node: `IS NULL`, `NOT`, `CASE`, `AND`, `OR` and
    /// every other parent are serialized as-is, and a `NOT` over a lookup therefore
    /// becomes `NOT IN`.
    ///
    /// The membership is a **hint for pruning only**, not a re-statement of the join
    /// semantics. For supported positive pruning, the list preserves actual build-key
    /// matches; it is not universal Boolean equivalence. `IN`/`NOT IN` propagate
    /// `NULL` while a hash lookup can be definite on a `NULL` probe key, and hash
    /// presence can admit collisions, so a predicate that inspects the *negation* of a
    /// lookup is not universally equivalent.
    ///
    /// The conversion is decided by the lookup alone (single-column `Utf8`, non-empty,
    /// `NULL`-free build keys — see [`HashTableLookupExpr::build_key_membership`]).
    /// Exactly one node changes: every [`HashTableLookupExpr`] is written as its key
    /// list, wherever it sits in the tree (`NOT lookup` included, which is why it
    /// becomes `NOT IN`). No node is ever replaced by a *weaker stand-in* — in
    /// particular never by `true` — and no membership is dropped from inside a
    /// predicate: when a lookup cannot be written at all, the whole top-level `AND`
    /// conjunct that holds it is dropped (see below) or the payload is rejected.
    ///
    /// # Top-level conjunctive retention
    ///
    /// If a lookup has no such key list (accessor limitation) or the serialized predicate
    /// does not fit `max_payload_bytes`, [`split_conjunction`] splits the original
    /// expression and the whole conjuncts that cannot be shipped are dropped. Dropping an
    /// `AND` term can only widen a predicate, so the result never drops a row the producer
    /// would keep. Which conjuncts go depends on why the payload was refused:
    ///
    /// - a lookup has no wire form: only conjuncts holding a [`HashTableLookupExpr`] are
    ///   dropped. A static [`InListExpr`] is representable, so `lookup AND id IN (...)`
    ///   keeps the list and stays `id IN (...)`;
    /// - the payload is over budget: conjuncts holding a [`HashTableLookupExpr`] *or* an
    ///   [`InListExpr`] are dropped, because a static `IN` list is the node that usually
    ///   blows the budget. `bounds AND oversized IN (...)` becomes `bounds`.
    ///
    /// Either way a conjunct is kept or dropped as a whole: a membership nested inside
    /// `NOT`, `CASE` or `OR` drops that entire conjunct instead of being rewritten.
    /// Retained conjuncts are encoded again under the same budget, and when the first two
    /// rules still leave an over-budget payload the second one is applied as well, so a
    /// static `IN` list that is too large for the budget is dropped last.
    ///
    /// # No `true` fallback
    ///
    /// Transport never rewrites a dynamic predicate to `true`, because doing so would make
    /// a region believe the producer published a filter it never published. When no
    /// conjunct can be retained (`NOT lookup`, `CASE` / `OR` over a membership) the
    /// predicate is reported as [`CommonQueryError::DynFilterUnsupported`], and when the
    /// retained conjuncts still do not fit `max_payload_bytes` the payload is reported as
    /// [`CommonQueryError::DynFilterPayloadTooLarge`]. Producers answer both errors by not
    /// publishing the filter, which leaves the query running with local dynamic filtering.
    pub fn from_datafusion_expr(
        expr: &Arc<dyn PhysicalExpr>,
        max_payload_bytes: usize,
    ) -> DataFusionResult<Self> {
        match encode_bounded_physical_expr_to_bytes(expr, max_payload_bytes) {
            Ok(bytes) => Ok(Self::Datafusion(bytes)),
            // The predicate cannot be shipped as it stands: either a lookup has no key
            // list, or the frame does not fit the budget. Keep the widest *safe* part of
            // the predicate instead of publishing an always-true filter.
            Err(CommonQueryError::DynFilterUnsupported { .. }) => retain_membership_free_conjuncts(
                expr,
                max_payload_bytes,
                ConjunctRetention::UnrepresentableLookup,
            ),
            Err(CommonQueryError::DynFilterPayloadTooLarge { .. }) => {
                retain_membership_free_conjuncts(
                    expr,
                    max_payload_bytes,
                    ConjunctRetention::OverBudget,
                )
            }
            Err(error) => Err(DataFusionError::from(error)),
        }
    }

    /// Decodes a DataFusion dynamic filter payload against the provided input schema.
    ///
    /// The decoded expression is validated to ensure column indexes stay within the receiver-side
    /// schema, column names are consistent (defensive check), and the payload stays within
    /// `max_payload_bytes`.
    ///
    /// # Precondition: pruning only, never row filtering
    ///
    /// The decoded predicate is only a **pruning hint**. The sole production consumer —
    /// `Predicate::dyn_filters` (in the `table` crate) and its pruning path, reached by
    /// mito2's row-group pruner — uses it to skip containers whose statistics prove that
    /// no row can match, so a predicate that is merely *wider* than the producer's costs
    /// nothing but a wasted scan.
    ///
    /// The payload is not a re-statement of the producer predicate: the membership is the
    /// join's build-key list, so `IN`/`NOT IN` `NULL` propagation and hash collisions can
    /// make it disagree with a runtime lookup, and the encoder drops whole top-level
    /// conjuncts it cannot ship (see [`DynFilterPayload::from_datafusion_expr`]). A
    /// consumer that *filters rows* would turn every such widening into lost data, so row
    /// filtering must not consume this payload unconditionally: it would need to prove the
    /// predicate is equivalent for the rows it drops, which this format does not carry.
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

struct BoundedPhysicalExprEncoder {
    max_payload_bytes: usize,
    encoded_bytes: Cell<usize>,
    /// Set when the encoding aborted on a runtime lookup that has no serializable key
    /// list.
    ///
    /// The abort travels out as an ordinary [`DataFusionError`], which would lose the
    /// typed dynamic filter error on the way, so the flag is what lets
    /// `encode_bounded_physical_expr_to_bytes` rebuild
    /// [`CommonQueryError::DynFilterUnsupported`]. No placeholder is ever encoded in its
    /// place.
    unsupported: Cell<bool>,
}

impl BoundedPhysicalExprEncoder {
    fn new(max_payload_bytes: usize) -> Self {
        Self {
            max_payload_bytes,
            encoded_bytes: Cell::new(0),
            unsupported: Cell::new(false),
        }
    }

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

    /// Encodes a lookup as its build-key membership, charging every list literal and
    /// node against the payload budget.
    ///
    /// The decision only looks at the lookup itself: if DataFusion can hand out the
    /// single-column `Utf8` build-key list it is written as `key IN (...)`; otherwise the
    /// whole payload aborts as unsupported. A lookup is a real predicate that DataFusion
    /// only degrades to `true` in its own proto encoder, so this transport must not write
    /// any substitute for it.
    fn membership_to_proto(
        &self,
        lookup: &HashTableLookupExpr,
        codec: &dyn PhysicalExtensionCodec,
    ) -> DataFusionResult<PhysicalExprNode> {
        let Some(membership) = RemoteMembership::from_lookup(lookup, self.max_payload_bytes) else {
            return Err(self.abort_unsupported_lookup());
        };
        // The count bound is enforced once more before any literal exists, so a build side
        // larger than the budget can hold is never materialized.
        let Some(literals) = utf8_list_literals(
            &membership.values,
            max_membership_values(self.max_payload_bytes),
        ) else {
            return Err(self.abort_unsupported_lookup());
        };

        let before_children = self.encoded_bytes.get();
        let mut list = Vec::with_capacity(literals.len());
        for literal in literals {
            list.push(self.physical_expr_to_proto(&literal, codec)?);
        }

        // The `InList` proto is built by hand instead of going through the generic
        // serializer: that path needs an `InListExpr`, whose only constructors
        // (`InListExpr::try_new` / `try_new_from_array`) require an input `Schema` to
        // resolve the key expression's data type, and this encoder converts a bare
        // expression without one. The key is known to be `Utf8` (see
        // `HashTableLookupExpr::build_key_membership`), which is exactly what the list
        // below writes, so no type check is lost.
        let proto = PhysicalExprNode {
            expr_id: None,
            expr_type: Some(PhysicalExprNodeType::InList(Box::new(PhysicalInListNode {
                expr: Some(Box::new(
                    self.physical_expr_to_proto(&membership.key, codec)?,
                )),
                list,
                negated: false,
            }))),
        };

        self.finish(before_children, proto)
    }

    /// Aborts the payload because the wire has no form for `lookup`.
    ///
    /// The returned error only carries the abort; [`Self::unsupported`] is what the caller
    /// reads to rebuild the typed dynamic filter error.
    fn abort_unsupported_lookup(&self) -> DataFusionError {
        self.unsupported.set(true);
        DataFusionError::NotImplemented(
            "hash table lookup has no UTF-8 build-key membership to encode".to_string(),
        )
    }
}

impl PhysicalProtoConverterExtension for BoundedPhysicalExprEncoder {
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
        // `try_to_proto` degrades it to `true`. Intercept it so the build-key list can
        // be written with the existing `InList` node instead. The decision is made by
        // the lookup alone; every other node, including `IS NULL`, `NOT`, `CASE`,
        // `AND` and `OR`, is serialized exactly as it is.
        if let Some(lookup) = expr.downcast_ref::<HashTableLookupExpr>() {
            return self.membership_to_proto(lookup, codec);
        }

        let before_children = self.encoded_bytes.get();
        let proto = serialize_physical_expr_with_converter(expr, codec, self)?;
        // Replace the children's contribution with the whole subtree, including protobuf framing.
        // Checking each child stops large IN lists before materializing all their Arrow IPC literals.
        self.finish(before_children, proto)
    }
}

/// Encodes `expr` for the remote wire under `max_payload_bytes`.
///
/// Returns the typed [`CommonQueryError::DynFilterUnsupported`] when the predicate holds a
/// lookup whose key list cannot be serialized, and
/// [`CommonQueryError::DynFilterPayloadTooLarge`] when the frame does not fit the budget.
/// The caller answers both with [`retain_membership_free_conjuncts`]; neither is ever
/// replaced with `true`.
fn encode_bounded_physical_expr_to_bytes(
    expr: &Arc<dyn PhysicalExpr>,
    max_payload_bytes: usize,
) -> Result<Vec<u8>, CommonQueryError> {
    let codec = DefaultPhysicalExtensionCodec {};
    let encoder = BoundedPhysicalExprEncoder::new(max_payload_bytes);
    let proto = encoder.physical_expr_to_proto(expr, &codec);
    let unsupported = encoder.unsupported.get();
    // The encoder stops at the node that went over budget, so this reports the size
    // error unwrapped instead of the wrapped DataFusion error.
    validate_payload_size(encoder.encoded_bytes.get(), max_payload_bytes)?;
    if unsupported {
        return Err(DynFilterUnsupportedSnafu {
            reason: "the predicate holds a hash table lookup without a UTF-8 build-key \
                     membership",
        }
        .build());
    }
    Ok(proto.map_err(CommonQueryError::from)?.encode_to_vec())
}

/// Which top-level conjuncts [`retain_membership_free_conjuncts`] has to drop.
#[derive(Clone, Copy, Debug)]
enum ConjunctRetention {
    /// The payload holds a [`HashTableLookupExpr`] without a wire form, so such a lookup
    /// is the only unshippable node: a static [`InListExpr`] is representable and stays.
    UnrepresentableLookup,
    /// The payload exceeds its budget. A static [`InListExpr`] is the node most likely to
    /// be the oversized one, so it is dropped with the lookups.
    OverBudget,
}

/// Keeps the top-level conjuncts of a predicate that cannot be shipped as it stands.
///
/// Dropping an `AND` term can only widen a predicate, so the retained payload still never
/// drops a row the producer would keep. `retention` decides which conjuncts have to go. It
/// only widens as far as it has to: when the narrow retention leaves conjuncts that still do
/// not fit `max_payload_bytes`, the [`InListExpr`]-bearing ones are dropped as well, because
/// a static `IN` list is the node most likely to be the oversized one. When no conjunct can
/// be retained the predicate is reported as
/// [`CommonQueryError::DynFilterUnsupported`], and when even the widest retention does not
/// fit as [`CommonQueryError::DynFilterPayloadTooLarge`]: transport never substitutes
/// `true`.
fn retain_membership_free_conjuncts(
    expr: &Arc<dyn PhysicalExpr>,
    max_payload_bytes: usize,
    retention: ConjunctRetention,
) -> DataFusionResult<DynFilterPayload> {
    match encode_retained_conjuncts_to_bytes(expr, max_payload_bytes, retention) {
        Ok(Some(bytes)) => Ok(DynFilterPayload::Datafusion(bytes)),
        Ok(None) => Err(DynFilterUnsupportedSnafu {
            reason: "the predicate carries a dynamic filter membership that has no wire \
                     form and no membership-free top-level conjunct to keep",
        }
        .build()
        .into()),
        Err(CommonQueryError::DynFilterPayloadTooLarge { .. })
            if matches!(retention, ConjunctRetention::UnrepresentableLookup) =>
        {
            retain_membership_free_conjuncts(expr, max_payload_bytes, ConjunctRetention::OverBudget)
        }
        Err(error) => Err(error.into()),
    }
}

/// Encodes the top-level conjuncts of `expr` that `retention` allows to keep.
///
/// [`split_conjunction`] hands out the top-level `AND` terms of the original
/// expression; a term is dropped as a whole when its subtree holds a node the retention
/// cannot ship (see [`ConjunctRetention`]). Membership nested inside `NOT`, `CASE` or `OR`
/// therefore drops that whole conjunct instead of being rewritten in place, so the payload
/// only ever loses top-level `AND` terms, which can only widen a predicate.
///
/// The retained terms are encoded again under the same `max_payload_bytes`, so a caller
/// supplied budget still bounds the frame. Returns `Ok(None)` when no conjunct can be
/// retained, which the caller reports as unsupported.
fn encode_retained_conjuncts_to_bytes(
    expr: &Arc<dyn PhysicalExpr>,
    max_payload_bytes: usize,
    retention: ConjunctRetention,
) -> Result<Option<Vec<u8>>, CommonQueryError> {
    let mut retained = Vec::new();
    for conjunct in split_conjunction(expr) {
        if !retention.drops_conjunct(conjunct)? {
            retained.push(Arc::clone(conjunct));
        }
    }
    if retained.is_empty() {
        return Ok(None);
    }
    encode_bounded_physical_expr_to_bytes(&conjunction(retained), max_payload_bytes).map(Some)
}

impl ConjunctRetention {
    /// Whether the whole subtree of `expr` holds a node this retention cannot ship.
    ///
    /// This is a "whole conjunct" test on purpose: a conjunct that reaches such a node
    /// anywhere is dropped by the retention instead of being rewritten around it.
    fn drops_conjunct(self, expr: &Arc<dyn PhysicalExpr>) -> DataFusionResult<bool> {
        let mut found = false;
        expr.apply(|node| {
            let unshippable = match self {
                Self::UnrepresentableLookup => node.is::<HashTableLookupExpr>(),
                Self::OverBudget => node.is::<HashTableLookupExpr>() || node.is::<InListExpr>(),
            };
            if unshippable {
                found = true;
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })?;
        Ok(found)
    }
}

/// Build-key list of one hash lookup, written as `probe_key IN (value, ...)`.
///
/// The membership reuses the existing `PhysicalInListNode`, so no new wire variant
/// and no in-payload version tag are needed, and it is charged against the normal
/// payload budget. Only the build-key list itself is consulted here; the wire form
/// never depends on where the lookup sits in the predicate tree.
struct RemoteMembership {
    /// Probe-side key expression, reused as the `InList` operand.
    key: Arc<dyn PhysicalExpr>,
    /// Published build key values, tied to the lookup's build lease.
    values: ArrayRef,
}

impl RemoteMembership {
    /// Key-list membership of `lookup`, or `None` when it has no such form.
    ///
    /// The count bound handed to DataFusion is derived from the byte budget (see
    /// [`max_membership_values`]) instead of being a policy constant, and the byte bound
    /// is the raw payload budget itself, which every build-key buffer has to fit before it
    /// can be written as literals. Both are conservative early guards, not the exact
    /// serialized frame length: the encoder still charges every literal against the budget
    /// node by node.
    ///
    /// The accessor keeps the value-level limitations — it returns `None` for lookups
    /// without a build lease (e.g. a test-only lookup), composite keys, non-`Utf8` keys
    /// (integer/float/decimal/dictionary/unknown), and `NULL`-containing or empty build
    /// keys — and [`utf8_list_literals`] re-checks everything it materializes. Those are
    /// accessor and materialization limitations, not encoder policy.
    fn from_lookup(lookup: &HashTableLookupExpr, max_payload_bytes: usize) -> Option<Self> {
        let membership = lookup
            .build_key_membership(max_membership_values(max_payload_bytes), max_payload_bytes)?;
        Some(Self {
            key: Arc::clone(membership.key()),
            values: Arc::clone(membership.values()),
        })
    }
}

/// Materializes the `IN` list literals for a build-key membership.
///
/// This is the authoritative value check: the list is only built for a non-empty,
/// `NULL`-free, non-dictionary `Utf8` array of at most `max_values` entries. `max_values`
/// is checked *before* a [`Literal`] is created for every build key, so an over-large
/// build side is refused instead of being materialized and then measured.
fn utf8_list_literals(values: &ArrayRef, max_values: usize) -> Option<Vec<Arc<dyn PhysicalExpr>>> {
    let values = values.as_any().downcast_ref::<StringArray>()?;
    if values.is_empty() || values.null_count() != 0 || values.len() > max_values {
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

/// Asserts the predicate is reported as having no wire form instead of becoming `true`.
#[cfg(test)]
fn assert_unsupported(error: DataFusionError) {
    assert_typed_dyn_filter_error(error, "DynFilterUnsupported", |error| {
        matches!(error, CommonQueryError::DynFilterUnsupported { .. })
    });
}

/// Asserts the retained payload still exceeds the caller-supplied budget.
#[cfg(test)]
fn assert_too_large(error: DataFusionError) {
    assert_typed_dyn_filter_error(error, "DynFilterPayloadTooLarge", |error| {
        matches!(error, CommonQueryError::DynFilterPayloadTooLarge { .. })
    });
}

#[cfg(test)]
fn assert_typed_dyn_filter_error(
    error: DataFusionError,
    expected: &str,
    matches_error: impl Fn(&CommonQueryError) -> bool,
) {
    let DataFusionError::External(error) = error else {
        panic!("expected external common query error, got: {error:?}");
    };
    let typed = error
        .downcast_ref::<CommonQueryError>()
        .unwrap_or_else(|| panic!("expected external {expected}, got: {error:?}"));
    assert!(matches_error(typed), "expected {expected}, got: {error:?}");
}

/// Whether `error` reports a dynamic filter predicate the wire cannot represent.
///
/// Transport never rewrites a predicate to `true`, so every producer answers this error by
/// not publishing the filter (or its initial snapshot) and continuing the query.
pub fn is_dyn_filter_unsupported(error: &DataFusionError) -> bool {
    matches!(
        error,
        DataFusionError::External(source)
            if source
                .downcast_ref::<CommonQueryError>()
                .is_some_and(CommonQueryError::is_dyn_filter_unsupported)
    )
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

/// First node of type `T` anywhere in `expr`.
#[cfg(test)]
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

/// Whether `expr` holds a node of type `T` anywhere.
#[cfg(test)]
fn has_expr<T: PhysicalExpr>(expr: &Arc<dyn PhysicalExpr>) -> bool {
    find_expr::<T>(expr).is_some()
}

/// Encodes `expr` at `max_payload_bytes` and decodes it against `schema`. This is the
/// round trip a receiver performs, so the tests never assert on the encoder alone.
#[cfg(test)]
fn decode(
    expr: &Arc<dyn PhysicalExpr>,
    schema: &Arc<datafusion::arrow::datatypes::Schema>,
    max_payload_bytes: usize,
) -> Arc<dyn PhysicalExpr> {
    DynFilterPayload::from_datafusion_expr(expr, max_payload_bytes)
        .unwrap()
        .decode_datafusion_expr(&TaskContext::default(), schema, max_payload_bytes)
        .unwrap()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use base64::Engine;
    use base64::prelude::BASE64_STANDARD;
    use datafusion::arrow::array::{ArrayRef, Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, InListExpr, NotExpr, lit};
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

    /// A hand-built lookup has no build lease, hence no key list: the wire cannot
    /// represent it, so the encoder aborts unless a membership-free conjunct can be kept.
    fn unrepresentable_lookup(key: &Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        Arc::new(HashTableLookupExpr::new(
            vec![Arc::clone(key)],
            SeededRandomState::with_seed(0),
            Arc::new(Map::HashMap(Box::new(JoinHashMapU32::with_capacity(0)))),
            "hash_lookup".to_string(),
            None,
        )) as Arc<dyn PhysicalExpr>
    }

    #[test]
    fn dyn_filter_payload_unconvertible_lookup_keeps_integer_bounds() {
        // `bounds AND lookup` where the lookup has no key list: only the
        // membership-bearing conjunct is dropped, so the integer bounds still travel as a
        // regular payload instead of an always-true filter or an error.
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
        let upper_bound = Arc::new(BinaryExpr::new(
            Arc::clone(&device_id),
            Operator::LtEq,
            lit(20i32),
        )) as Arc<dyn PhysicalExpr>;
        let bounds = Arc::new(BinaryExpr::new(lower_bound, Operator::And, upper_bound))
            as Arc<dyn PhysicalExpr>;
        let expr = Arc::new(BinaryExpr::new(
            Arc::clone(&bounds),
            Operator::And,
            unrepresentable_lookup(&device_id),
        )) as Arc<dyn PhysicalExpr>;

        // The predicate itself has no wire form: the lookup has no key list.
        assert!(matches!(
            encode_bounded_physical_expr_to_bytes(&expr, 1024),
            Err(CommonQueryError::DynFilterUnsupported { .. })
        ));

        // Keeping the membership-free top-level conjuncts turns it into a normal payload.
        let decoded = decode(&expr, &schema, 1024);
        let display = decoded.to_string();
        assert!(
            display.contains(">= 10"),
            "lower bound must survive: {display}"
        );
        assert!(
            display.contains("<= 20"),
            "upper bound must survive: {display}"
        );
        assert!(!has_expr::<HashTableLookupExpr>(&decoded), "{display}");
        assert!(!has_expr::<InListExpr>(&decoded), "{display}");
        // Both bounds are copied verbatim: no `AND` term was rewritten in place.
        assert_eq!(
            encode_physical_expr_to_bytes(&decoded).unwrap(),
            encode_physical_expr_to_bytes(&bounds).unwrap()
        );
    }

    #[test]
    fn unrepresentable_lookup_drops_only_the_lookup_conjunct() {
        // One unsupported lookup (no key list) between the bounds and a static `IN` list
        // that is itself perfectly representable: only the lookup-bearing conjunct is
        // dropped, so the list survives as a whole and the payload stays as narrow as the
        // wire allows.
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
        let upper_bound = Arc::new(BinaryExpr::new(
            Arc::clone(&device_id),
            Operator::LtEq,
            lit(20i32),
        )) as Arc<dyn PhysicalExpr>;
        let bounds = Arc::new(BinaryExpr::new(
            Arc::clone(&lower_bound),
            Operator::And,
            Arc::clone(&upper_bound),
        )) as Arc<dyn PhysicalExpr>;
        let in_list = Arc::new(
            InListExpr::try_new(
                Arc::clone(&device_id),
                (10..20).map(lit).collect(),
                false,
                &schema,
            )
            .unwrap(),
        ) as Arc<dyn PhysicalExpr>;
        let expr = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::clone(&bounds),
                Operator::And,
                unrepresentable_lookup(&device_id),
            )) as Arc<dyn PhysicalExpr>,
            Operator::And,
            Arc::clone(&in_list),
        )) as Arc<dyn PhysicalExpr>;

        let decoded = decode(&expr, &schema, REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES);
        let display = decoded.to_string();
        assert!(display.contains(">= 10"), "{display}");
        assert!(display.contains("<= 20"), "{display}");
        assert!(!has_expr::<HashTableLookupExpr>(&decoded), "{display}");
        // The static `IN` list is representable, so it is kept verbatim.
        assert!(has_expr::<InListExpr>(&decoded), "{display}");
        assert_eq!(
            split_conjunction(&decoded).len(),
            3,
            "the two bounds and the static `IN` list survive: {display}"
        );
        // The retained conjuncts are the original terms verbatim: the list was not
        // rewritten, truncated or moved.
        let retained_in_list = find_expr::<InListExpr>(&decoded).expect("static IN survives");
        let retained_in_list = retained_in_list.downcast_ref::<InListExpr>().unwrap();
        assert!(!retained_in_list.negated());
        assert_eq!(retained_in_list.list().len(), 10);
        let retained = conjunction(vec![
            Arc::clone(&lower_bound),
            Arc::clone(&upper_bound),
            in_list,
        ]);
        assert_eq!(
            encode_physical_expr_to_bytes(&decoded).unwrap(),
            encode_physical_expr_to_bytes(&retained).unwrap()
        );
    }

    #[test]
    fn over_budget_membership_drops_the_oversized_in_list_too() {
        // The same shape under a budget that only fits the bounds: keeping the
        // representative-but-large `IN` list is not enough, so the retention widens once
        // more and drops the `InListExpr`-bearing conjuncts as well.
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
        let upper_bound = Arc::new(BinaryExpr::new(
            Arc::clone(&device_id),
            Operator::LtEq,
            lit(20i32),
        )) as Arc<dyn PhysicalExpr>;
        let bounds = Arc::new(BinaryExpr::new(
            Arc::clone(&lower_bound),
            Operator::And,
            Arc::clone(&upper_bound),
        )) as Arc<dyn PhysicalExpr>;
        let in_list = Arc::new(
            InListExpr::try_new(
                Arc::clone(&device_id),
                (10..20).map(lit).collect(),
                false,
                &schema,
            )
            .unwrap(),
        ) as Arc<dyn PhysicalExpr>;
        let expr = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::clone(&bounds),
                Operator::And,
                unrepresentable_lookup(&device_id),
            )) as Arc<dyn PhysicalExpr>,
            Operator::And,
            in_list,
        )) as Arc<dyn PhysicalExpr>;

        let budget = encode_physical_expr_to_bytes(&bounds).unwrap().len();
        assert!(encode_bounded_physical_expr_to_bytes(&expr, budget).is_err());

        let decoded = decode(&expr, &schema, budget);
        let display = decoded.to_string();
        assert!(display.contains(">= 10"), "{display}");
        assert!(display.contains("<= 20"), "{display}");
        assert!(!has_expr::<HashTableLookupExpr>(&decoded), "{display}");
        assert!(!has_expr::<InListExpr>(&decoded), "{display}");
        assert_eq!(
            encode_physical_expr_to_bytes(&decoded).unwrap(),
            encode_physical_expr_to_bytes(&bounds).unwrap()
        );
    }

    #[test]
    fn dyn_filter_payload_negated_unconvertible_lookup_is_unsupported() {
        // `NOT lookup` is a single top-level conjunct that carries the membership, so
        // there is nothing safe to keep. Transport must not encode `NOT true` (or `true`)
        // in its place: the predicate is reported as unsupported.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "device_id",
            DataType::Int32,
            false,
        )]));
        let device_id = col("device_id", &schema).unwrap();
        let not_lookup =
            Arc::new(NotExpr::new(unrepresentable_lookup(&device_id))) as Arc<dyn PhysicalExpr>;

        assert_unsupported(DynFilterPayload::from_datafusion_expr(&not_lookup, 1024).unwrap_err());
    }

    #[test]
    fn dyn_filter_payload_membership_under_or_is_unsupported() {
        // `bound OR lookup` is a single top-level conjunct that carries the membership:
        // rewriting it to `bound OR true` would be nothing but `true`, and keeping only
        // the bound would *narrow* the predicate, so it is reported as unsupported.
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
        let expr = Arc::new(BinaryExpr::new(
            lower_bound,
            Operator::Or,
            unrepresentable_lookup(&device_id),
        )) as Arc<dyn PhysicalExpr>;

        assert_unsupported(DynFilterPayload::from_datafusion_expr(&expr, 1024).unwrap_err());
    }

    #[test]
    fn dyn_filter_payload_over_budget_keeps_bounds() {
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
        let bounds = Arc::new(BinaryExpr::new(
            Arc::clone(&lower_bound),
            Operator::And,
            Arc::clone(&upper_bound),
        )) as Arc<dyn PhysicalExpr>;
        let oversized_in = Arc::new(
            InListExpr::try_new(
                Arc::clone(&device_id),
                (8192..8256).map(lit).collect(),
                false,
                &schema,
            )
            .unwrap(),
        ) as Arc<dyn PhysicalExpr>;
        let expr = Arc::new(BinaryExpr::new(
            Arc::clone(&bounds),
            Operator::And,
            oversized_in,
        )) as Arc<dyn PhysicalExpr>;

        // A caller-supplied limit that fits the bounds but not the oversized `IN` list:
        // the membership conjunct is dropped and the bounds are encoded under the same
        // budget.
        let bounds_size = encode_physical_expr_to_bytes(&bounds).unwrap().len();
        assert!(encode_bounded_physical_expr_to_bytes(&expr, bounds_size).is_err());

        let decoded = decode(&expr, &schema, bounds_size);
        let display = decoded.to_string();
        assert!(display.contains(">= 8192"), "{display}");
        assert!(display.contains("<= 8255"), "{display}");
        assert!(!has_expr::<InListExpr>(&decoded), "{display}");
        assert_eq!(
            encode_physical_expr_to_bytes(&decoded).unwrap(),
            encode_physical_expr_to_bytes(&bounds).unwrap()
        );

        // A budget that does not even fit the retained bounds is reported as too large:
        // nothing that cannot be shipped may be replaced with `true`.
        let true_size = encode_physical_expr_to_bytes(&lit(true)).unwrap().len();
        assert!(true_size < bounds_size);
        assert_too_large(DynFilterPayload::from_datafusion_expr(&expr, true_size).unwrap_err());
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
            encode_bounded_physical_expr_to_bytes(&expr, expected.len()).unwrap(),
            expected
        );
        assert!(matches!(
            encode_bounded_physical_expr_to_bytes(&expr, expected.len() - 1),
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

        let error = encode_bounded_physical_expr_to_bytes(&expr, 2048).unwrap_err();
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

    #[test]
    fn membership_value_bound_is_derived_from_the_byte_budget() {
        // One list entry costs at least `MIN_ENCODED_LITERAL_BYTES` on the wire, so the
        // count bound is the budget divided by that floor: never the raw byte count (the
        // old behaviour) and never an encoder-side policy cap.
        let cheapest = encode_physical_expr_to_bytes(&lit("")).unwrap().len();
        assert!(
            cheapest >= MIN_ENCODED_LITERAL_BYTES,
            "floor must hold for the cheapest literal, got {cheapest} bytes"
        );
        assert_eq!(
            max_membership_values(REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES),
            REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES / MIN_ENCODED_LITERAL_BYTES
        );
        assert!(
            max_membership_values(REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES)
                < REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES,
            "the count bound must be tighter than the raw byte budget"
        );
        assert_eq!(max_membership_values(64), 64 / MIN_ENCODED_LITERAL_BYTES);
    }

    #[test]
    fn utf8_list_literals_is_the_authoritative_value_check() {
        let values: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        assert!(utf8_list_literals(&values, 3).is_some());
        // The count is bounded before a single `Literal` is built.
        assert!(utf8_list_literals(&values, 2).is_none());
        assert!(utf8_list_literals(&values, 0).is_none());
        // NULL-containing, empty and non-`Utf8` arrays are refused here, not only by the
        // `HashTableLookupExpr` accessor.
        let with_null: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None]));
        assert!(utf8_list_literals(&with_null, 8).is_none());
        let empty: ArrayRef = Arc::new(StringArray::from(Vec::<&str>::new()));
        assert!(utf8_list_literals(&empty, 8).is_none());
        let not_utf8: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        assert!(utf8_list_literals(&not_utf8, 8).is_none());
    }
}

/// Remote dynamic filter membership recovered from *real* DataFusion build leases.
///
/// The tests above build `HashTableLookupExpr` by hand, which always has
/// `build_lease = None`. These tests run an actual `HashJoinExec` so DataFusion
/// publishes lookups backed by a real build lease, which is the only way to prove that
/// the key list (and the membership-free conjunct retention) actually works.
///
/// Most remote predicates are checked twice: structurally (what the payload contains)
/// and by **evaluating** the producer predicate and the decoded predicate on the same
/// `NULL`-free rows. Evaluation pins the contract that applies here: the converted
/// membership is the build key list, so on `NULL`-free keys only, the decoded predicate
/// keeps exactly the rows the producer kept. It is a pruning hint, not a universal
/// equivalence — see `not_lookup_becomes_not_in` for the `NULL` caveat.
#[cfg(test)]
mod remote_membership_tests {
    use std::collections::BTreeMap;

    use datafusion::arrow::array::{
        ArrayRef, BooleanArray, Float64Array, Int32Array, StringArray, StringDictionaryBuilder,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::expressions::{
        BinaryExpr, CaseExpr, Column, InListExpr, IsNullExpr, Literal, NotExpr, lit,
    };
    use datafusion::physical_plan::PhysicalExpr;
    use datafusion::physical_plan::joins::{HashExpr, HashTableLookupExpr};
    use datafusion_expr::Operator;

    use super::remote_membership_test_support::{
        RealDynFilter, key, one_column_schema, partitions, run_real_hash_join, utf8, utf8_join,
    };
    use super::*;

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

    /// Widening check: the decoded predicate keeps every row the producer kept.
    ///
    /// Only used on `NULL`-free rows, where the build-key membership is exact for the
    /// converted node.
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

    /// UTF-8 batch that can contain SQL NULLs, for wrapper semantics.
    fn nullable_evaluation_batch(schema: &Arc<Schema>, values: &[Option<String>]) -> RecordBatch {
        let array = StringArray::from(values.to_vec());
        RecordBatch::try_new(Arc::clone(schema), vec![Arc::new(array) as ArrayRef]).unwrap()
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

    /// Runs a real one-partition `Utf8` hash join over `key_count` distinct keys.
    async fn real_join_with_keys(key_count: usize) -> (Arc<Schema>, RealDynFilter, Vec<String>) {
        let schema = one_column_schema("trace_id", DataType::Utf8);
        let trace_id = key("trace_id", 0);
        let values = (0..key_count)
            .map(|i| format!("trace-{i:04}"))
            .collect::<Vec<_>>();
        let borrowed = values.iter().map(String::as_str).collect::<Vec<_>>();
        let build = partitions(&schema, vec![vec![utf8(&borrowed)]]);
        let probe = partitions(&schema, vec![vec![utf8(&borrowed)]]);
        let filter = run_real_hash_join(
            1,
            Arc::clone(&schema),
            Arc::clone(&schema),
            build,
            probe,
            vec![(Arc::clone(&trace_id), Arc::clone(&trace_id))],
        )
        .await;
        (schema, filter, values)
    }

    #[tokio::test]
    async fn bounds_survive_unrepresentable_integer_lookup() {
        // Integer keys are not a single-column `Utf8` membership, so this real lookup has
        // no wire form. Retention keeps the top-level bounds and drops the lookup
        // conjunct, so the decoded predicate only ever gets wider.
        let schema = one_column_schema("k", DataType::Int32);
        let k = key("k", 0);
        let filter = run_real_hash_join(
            1,
            Arc::clone(&schema),
            Arc::clone(&schema),
            partitions(
                &schema,
                vec![vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef]],
            ),
            partitions(
                &schema,
                vec![vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 10])) as ArrayRef
                ]],
            ),
            vec![(Arc::clone(&k), Arc::clone(&k))],
        )
        .await;
        let lookup = find_expr::<HashTableLookupExpr>(&filter.expr).expect("real lookup");

        let lower_bound = Arc::new(BinaryExpr::new(Arc::clone(&k), Operator::GtEq, lit(2i32)))
            as Arc<dyn PhysicalExpr>;
        let upper_bound = Arc::new(BinaryExpr::new(Arc::clone(&k), Operator::LtEq, lit(4i32)))
            as Arc<dyn PhysicalExpr>;
        let bounds = Arc::new(BinaryExpr::new(lower_bound, Operator::And, upper_bound))
            as Arc<dyn PhysicalExpr>;
        let expr = Arc::new(BinaryExpr::new(Arc::clone(&bounds), Operator::And, lookup))
            as Arc<dyn PhysicalExpr>;

        let decoded = decode(
            &expr,
            &filter.probe_schema,
            REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES,
        );
        let display = decoded.to_string();
        assert!(
            display.contains(">= 2"),
            "lower bound must survive: {display}"
        );
        assert!(
            display.contains("<= 4"),
            "upper bound must survive: {display}"
        );
        assert!(!has_expr::<HashTableLookupExpr>(&decoded), "{display}");
        assert!(!has_expr::<InListExpr>(&decoded), "{display}");

        // The producer and the decoded predicate agree on every row the producer kept and
        // only diverge on the non-member `4`, where the retained payload widens.
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 10])) as ArrayRef],
        )
        .unwrap();
        let (original_selected, decoded_selected) = assert_fail_open(&expr, &decoded, &batch);
        assert_eq!(
            original_selected,
            vec![false, true, true, false, false, false]
        );
        assert_eq!(
            decoded_selected,
            vec![false, true, true, true, false, false]
        );
    }

    #[tokio::test]
    async fn small_budget_keeps_bounds_and_drops_real_membership() {
        // A real `Utf8` lookup next to integer bounds, shipped under a caller-supplied
        // limit that fits only the bounds: retention drops the membership conjunct and
        // still sends the bounds, which can only widen the producer predicate.
        let probe_schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, true),
            Field::new("k", DataType::Int32, true),
        ]));
        let build_schema = one_column_schema("trace_id", DataType::Utf8);
        let long = "t".repeat(256);
        let values = (0..24)
            .map(|i| format!("{long}-{i:02}"))
            .collect::<Vec<_>>();
        let borrowed = values.iter().map(String::as_str).collect::<Vec<_>>();
        let build_key = key("trace_id", 0);
        let probe_key = key("trace_id", 0);
        let filter = run_real_hash_join(
            1,
            Arc::clone(&build_schema),
            Arc::clone(&probe_schema),
            partitions(&build_schema, vec![vec![utf8(&borrowed)]]),
            partitions(
                &probe_schema,
                vec![vec![
                    utf8(&borrowed),
                    Arc::new(Int32Array::from(vec![0i32; values.len()])) as ArrayRef,
                ]],
            ),
            vec![(Arc::clone(&build_key), Arc::clone(&probe_key))],
        )
        .await;
        let lookup = find_expr::<HashTableLookupExpr>(&filter.expr).expect("real lookup");

        let k = key("k", 1);
        let lower_bound = Arc::new(BinaryExpr::new(Arc::clone(&k), Operator::GtEq, lit(2i32)))
            as Arc<dyn PhysicalExpr>;
        let upper_bound = Arc::new(BinaryExpr::new(Arc::clone(&k), Operator::LtEq, lit(5i32)))
            as Arc<dyn PhysicalExpr>;
        let bounds = Arc::new(BinaryExpr::new(lower_bound, Operator::And, upper_bound))
            as Arc<dyn PhysicalExpr>;
        let expr = Arc::new(BinaryExpr::new(Arc::clone(&bounds), Operator::And, lookup))
            as Arc<dyn PhysicalExpr>;

        // Only the bounds fit: the membership of 24 long keys is far beyond this budget.
        let bounds_size = encode_physical_expr_to_bytes(&bounds).unwrap().len();
        let decoded = decode(&expr, &filter.probe_schema, bounds_size);
        let display = decoded.to_string();
        assert!(
            display.contains(">= 2"),
            "lower bound must survive: {display}"
        );
        assert!(
            display.contains("<= 5"),
            "upper bound must survive: {display}"
        );
        assert!(!has_expr::<HashTableLookupExpr>(&decoded), "{display}");
        assert!(!has_expr::<InListExpr>(&decoded), "{display}");

        // Evaluation: a member inside the bounds is kept by both, a member outside the
        // bounds is dropped by both, and only the non-member is widened.
        let trace_ids = vec![
            values[0].as_str(),
            values[1].as_str(),
            "miss-not-a-member",
            values[2].as_str(),
        ];
        let batch = RecordBatch::try_new(
            Arc::clone(&probe_schema),
            vec![
                utf8(&trace_ids),
                Arc::new(Int32Array::from(vec![2i32, 6, 3, 5])) as ArrayRef,
            ],
        )
        .unwrap();
        let (original_selected, decoded_selected) = assert_fail_open(&expr, &decoded, &batch);
        assert_eq!(original_selected, vec![true, false, false, true]);
        assert_eq!(decoded_selected, vec![true, false, true, true]);
    }

    #[tokio::test]
    async fn real_642_key_hash_join_becomes_exact_642_literal_in() {
        let (schema, filter, values) = real_join_with_keys(642).await;
        assert_eq!(values.len(), 642);
        // The producer predicate really holds a runtime-only lookup...
        assert!(has_expr::<HashTableLookupExpr>(&filter.expr));

        // ...and it is written as one literal per build key within the payload budget.
        let decoded = decode(
            &filter.expr,
            &filter.probe_schema,
            REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES,
        );
        assert!(!has_expr::<HashTableLookupExpr>(&decoded));
        let lists = in_lists(&decoded);
        assert_eq!(lists.len(), 1, "expected one converted InList: {decoded}");
        let mut recovered = in_list_values(&lists[0]);
        assert_eq!(recovered.len(), 642, "one literal per build key");
        recovered.sort();
        assert_eq!(recovered, values);

        // The membership selects exactly the build keys.
        let mut rows = values.clone();
        rows.push("miss-not-a-member".to_string());
        let batch = evaluation_batch(&schema, &rows);
        let (original_selected, decoded_selected) =
            assert_fail_open(&filter.expr, &decoded, &batch);
        for row in 0..values.len() {
            assert!(
                original_selected[row],
                "build key row {row} must be selected"
            );
            assert!(
                decoded_selected[row],
                "build key row {row} must stay selected"
            );
        }
        assert!(
            !decoded_selected[values.len()],
            "non-member must not be selected"
        );
    }

    #[tokio::test]
    async fn large_build_side_is_refused_before_materializing_literals() {
        // A build side far larger than a small budget can hold must be refused by the
        // derived count bound *before* the encoder creates one `Literal` per build key.
        let (_, filter, values) = real_join_with_keys(1024).await;
        let lookup = find_expr::<HashTableLookupExpr>(&filter.expr).expect("real lookup");
        let lookup = lookup.downcast_ref::<HashTableLookupExpr>().unwrap();

        // DataFusion itself would hand out every build key ...
        let unbounded = lookup
            .build_key_membership(usize::MAX, usize::MAX)
            .expect("the build lease holds the keys");
        assert_eq!(unbounded.values().len(), values.len());

        // ... but a budget that only fits the producer's own bounds leaves room for a few
        // literals, so the count bound derived from it refuses the list outright.
        let bounds = conjunction(
            split_conjunction(&filter.expr)
                .into_iter()
                .filter(|conjunct| !has_expr::<HashTableLookupExpr>(conjunct))
                .cloned()
                .collect::<Vec<_>>(),
        );
        let budget = encode_physical_expr_to_bytes(&bounds).unwrap().len();
        assert!(
            max_membership_values(budget) < values.len(),
            "the derived count bound must stay below the build side size"
        );
        assert!(RemoteMembership::from_lookup(lookup, budget).is_none());

        // Nothing of the 1024-key membership reaches the payload.
        let decoded = decode(&filter.expr, &filter.probe_schema, budget);
        assert!(!has_expr::<HashTableLookupExpr>(&decoded), "{decoded}");
        assert!(!has_expr::<InListExpr>(&decoded), "{decoded}");
        assert_has_bounds(&decoded);
    }

    #[tokio::test]
    async fn lookup_or_key_is_null_preserves_wrapper_and_becomes_in_list() {
        // The `key IS NULL OR <filter>` wrapper of a null-aware join is an ordinary
        // node: it stays in the payload and only the lookup inside it is converted.
        let (schema, filter, values) = real_join_with_keys(8).await;
        let lookup = find_expr::<HashTableLookupExpr>(&filter.expr).expect("real lookup");
        let trace_id = key("trace_id", 0);
        let wrapped = Arc::new(BinaryExpr::new(
            Arc::clone(&lookup),
            Operator::Or,
            Arc::new(IsNullExpr::new(Arc::clone(&trace_id))),
        )) as Arc<dyn PhysicalExpr>;

        let decoded = decode(
            &wrapped,
            &filter.probe_schema,
            REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES,
        );
        let display = decoded.to_string();
        assert!(
            display.contains("IS NULL"),
            "IS NULL wrapper must survive: {display}"
        );
        assert!(!display.contains("hash_lookup"), "{display}");
        let lists = in_lists(&decoded);
        assert_eq!(lists.len(), 1, "expected one converted InList: {display}");
        assert_eq!(in_list_values(&lists[0]), values);

        let mut rows = values
            .iter()
            .map(|value| Some(value.clone()))
            .collect::<Vec<_>>();
        rows.push(Some("miss-not-a-member".to_string()));
        rows.push(None);
        let batch = nullable_evaluation_batch(&schema, &rows);
        let (original_selected, decoded_selected) = assert_fail_open(&wrapped, &decoded, &batch);
        for row in 0..values.len() {
            assert!(original_selected[row]);
            assert!(decoded_selected[row]);
        }
        assert!(
            original_selected[rows.len() - 1],
            "producer `lookup OR key IS NULL` keeps a NULL probe"
        );
        assert!(
            decoded_selected[rows.len() - 1],
            "decoded `IN ... OR key IS NULL` must also keep a NULL probe"
        );
    }

    #[tokio::test]
    async fn null_build_keys_keep_only_the_safe_bounds() {
        // The key-list accessor requires `NULL`-free build keys, so a build side with a
        // `NULL` key has no membership. This producer shape is `bounds AND lookup`: the
        // bounds DataFusion derived from the non-`NULL` keys are retained, and the lookup
        // is dropped instead of the predicate being replaced with `true`.
        let schema = one_column_schema("trace_id", DataType::Utf8);
        let trace_id = key("trace_id", 0);
        let build_values: ArrayRef = Arc::new(StringArray::from(vec![
            Some("trace-a"),
            None,
            Some("trace-b"),
        ]));
        let filter = run_real_hash_join(
            1,
            Arc::clone(&schema),
            Arc::clone(&schema),
            partitions(&schema, vec![vec![build_values]]),
            partitions(&schema, vec![vec![utf8(&["trace-a", "trace-b", "miss"])]]),
            vec![(Arc::clone(&trace_id), Arc::clone(&trace_id))],
        )
        .await;
        assert!(has_expr::<HashTableLookupExpr>(&filter.expr));
        let producer = filter.expr.to_string();
        assert!(producer.contains(">= trace-a"), "{producer}");
        assert!(producer.contains("<= trace-b"), "{producer}");

        let decoded = decode(
            &filter.expr,
            &filter.probe_schema,
            REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES,
        );
        let display = decoded.to_string();
        assert!(!has_expr::<HashTableLookupExpr>(&decoded), "{display}");
        assert!(!has_expr::<InListExpr>(&decoded), "{display}");
        assert!(display.contains(">= trace-a"), "{display}");
        assert!(display.contains("<= trace-b"), "{display}");
        assert_ne!(display, "true", "transport must not widen to `true`");

        // The retained conjuncts are the producer bounds verbatim.
        let bounds = split_conjunction(&filter.expr)
            .into_iter()
            .filter(|conjunct| !has_expr::<HashTableLookupExpr>(conjunct))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(
            encode_physical_expr_to_bytes(&decoded).unwrap(),
            encode_physical_expr_to_bytes(&conjunction(bounds)).unwrap()
        );

        // Widening only: every build key row the producer kept is still kept.
        let rows = ["trace-a", "trace-b", "miss"]
            .map(ToString::to_string)
            .to_vec();
        let batch = evaluation_batch(&schema, &rows);
        let (original_selected, decoded_selected) =
            assert_fail_open(&filter.expr, &decoded, &batch);
        assert_eq!(original_selected, vec![true, true, false]);
        assert!(decoded_selected[0], "build key row must stay selected");
        assert!(decoded_selected[1], "build key row must stay selected");
    }

    /// `NOT lookup` becomes `NOT (key IN (...))`.
    ///
    /// This narrow contract is about **positive pruning**: the membership is the build
    /// key list of the join, so converting the node in place structurally preserves the
    /// negation instead of substituting `true` into it. It is *not* a claim of
    /// universal equivalence with the join: `IN`/`NOT IN` are `NULL`-propagating while
    /// a hash lookup can be definite on a `NULL` probe key, so a `NULL` key that the
    /// lookup keeps can still be dropped by `NOT IN`. The rows below are non-`NULL`,
    /// which is the shape the hint is meant for.
    #[tokio::test]
    async fn not_lookup_becomes_not_in() {
        let (schema, filter, values) = real_join_with_keys(8).await;
        let lookup = find_expr::<HashTableLookupExpr>(&filter.expr).expect("real lookup");
        let not_lookup = Arc::new(NotExpr::new(Arc::clone(&lookup))) as Arc<dyn PhysicalExpr>;

        let decoded = decode(
            &not_lookup,
            &filter.probe_schema,
            REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES,
        );
        let display = decoded.to_string();
        assert!(
            display.starts_with("NOT "),
            "NOT must survive the wire: {display}"
        );
        let lists = in_lists(&decoded);
        assert_eq!(lists.len(), 1, "expected one converted InList: {display}");
        assert_eq!(in_list_values(&lists[0]), values);

        let mut rows = values.clone();
        rows.push("miss-not-a-member".to_string());
        let batch = evaluation_batch(&schema, &rows);
        let (original_selected, decoded_selected) = assert_fail_open(&not_lookup, &decoded, &batch);
        assert!(original_selected[..values.len()].iter().all(|kept| !*kept));
        assert!(
            original_selected[values.len()],
            "producer `NOT lookup` keeps the non-member"
        );
        assert!(decoded_selected[..values.len()].iter().all(|kept| !*kept));
        assert!(
            decoded_selected[values.len()],
            "decoded `NOT IN` keeps the non-member"
        );
    }

    #[tokio::test]
    async fn unsupported_float_composite_and_dictionary_keys_keep_only_safe_conjuncts() {
        // Float64 keys: the key-list accessor only exposes a single `Utf8` column.
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
        // The partitioned producer publishes a per-partition `CASE`, so the only
        // top-level conjunct carries the membership and there is nothing safe to keep:
        // the predicate is reported as unsupported instead of becoming `true`.
        assert_unsupported(
            DynFilterPayload::from_datafusion_expr(
                &float_filter.expr,
                REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES,
            )
            .unwrap_err(),
        );

        // Composite (two-column) keys: not a single-column membership. Both columns
        // take part in the hash partitioning, exactly like a real two-key join.
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
        // Same per-partition `CASE` shape as the float case: unsupported, not `true`.
        assert_unsupported(
            DynFilterPayload::from_datafusion_expr(
                &composite_filter.expr,
                REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES,
            )
            .unwrap_err(),
        );

        // Dictionary keys: not a plain `Utf8` key column.
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
        // Dictionary keys are not a plain `Utf8` key column either. This producer shape
        // is a flat `bounds AND lookup`, so retention keeps the bounds conjunct that
        // DataFusion itself published and drops only the membership.
        let display = decoded.to_string();
        assert!(!has_expr::<HashTableLookupExpr>(&decoded), "{display}");
        assert!(!has_expr::<InListExpr>(&decoded), "{display}");
        assert!(
            display.contains(">="),
            "lower bound must survive: {display}"
        );
        assert!(
            display.contains("<="),
            "upper bound must survive: {display}"
        );
    }

    #[tokio::test]
    async fn producer_paths_encode_150_and_1024_keys() {
        // The two producer shapes that can carry a build key list: the static `IN` list
        // pushdown (150 and 1024 are the pushdown limits of the runtime experiment) and
        // the hash lookup. Neither is capped by an encoder-side value/byte policy: the
        // only bound is the payload budget itself.
        for key_count in [150, 1024] {
            let (schema, filter, values) = real_join_with_keys(key_count).await;
            assert!(has_expr::<HashTableLookupExpr>(&filter.expr));
            let decoded = decode(
                &filter.expr,
                &filter.probe_schema,
                REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES,
            );
            let lists = in_lists(&decoded);
            assert_eq!(lists.len(), 1, "{key_count} keys: {decoded}");
            assert_eq!(in_list_values(&lists[0]), values);

            // The static `IN` list pushdown of the same size encodes as well.
            let in_list = Arc::new(
                InListExpr::try_new(
                    key("trace_id", 0),
                    values.iter().map(|value| lit(value.clone())).collect(),
                    false,
                    &schema,
                )
                .unwrap(),
            ) as Arc<dyn PhysicalExpr>;
            let decoded = decode(
                &in_list,
                &filter.probe_schema,
                REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES,
            );
            let lists = in_lists(&decoded);
            assert_eq!(lists.len(), 1, "{key_count} static keys: {decoded}");
            assert_eq!(in_list_values(&lists[0]), values);
        }
    }

    #[tokio::test]
    async fn over_budget_membership_case_is_unsupported() {
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

        // The real payload budget carries all three branch memberships exactly.
        let decoded = decode(
            &filter.expr,
            &filter.probe_schema,
            REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES,
        );
        assert_eq!(in_lists(&decoded).len(), 3);
        let case = decoded.downcast_ref::<CaseExpr>().expect("CASE preserved");
        assert_eq!(case.when_then_expr().len(), 3);

        // The whole predicate is one top-level `CASE` conjunct that carries the
        // membership, so a budget that only fits `true` has nothing safe to keep:
        // membership is never rewritten inside `CASE` and the payload is an explicit
        // error instead of an always-true filter.
        let true_size = encode_physical_expr_to_bytes(&lit(true)).unwrap().len();
        assert!(true_size < REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES);
        assert_unsupported(
            DynFilterPayload::from_datafusion_expr(&filter.expr, true_size).unwrap_err(),
        );
    }

    #[test]
    fn over_budget_negation_is_unsupported() {
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
        // `NOT (host IN (...))`: dropping the list in place would evaluate `NOT true`
        // = false and drop every row, so the whole predicate is unsupported.
        let not_in = Arc::new(NotExpr::new(Arc::clone(&in_list))) as Arc<dyn PhysicalExpr>;
        let true_size = encode_physical_expr_to_bytes(&lit(true)).unwrap().len();

        assert_unsupported(DynFilterPayload::from_datafusion_expr(&not_in, true_size).unwrap_err());
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

        // Receivers still accept a plain `true` payload: an older sender could publish one,
        // and a producer may legitimately publish `true` itself. This transport no longer
        // synthesizes `true` as an encoding fallback, so the decode-side compatibility stays
        // while the encode-side fallback is gone.
        let legacy_bytes = encode_physical_expr_to_bytes(&lit(true)).unwrap();
        assert!(legacy_bytes.len() < bytes.len());
        let legacy_decoded = DynFilterPayload::Datafusion(legacy_bytes)
            .decode_datafusion_expr(&TaskContext::default(), &filter.probe_schema, max)
            .unwrap();
        assert_eq!(legacy_decoded.to_string(), "true");
        let mut rows = values.clone();
        rows.push("miss-not-a-member".to_string());
        let batch = evaluation_batch(&schema, &rows);
        assert_fail_open(&filter.expr, &legacy_decoded, &batch);
    }
}
