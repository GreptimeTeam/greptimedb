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

use std::sync::Arc;

use api::v1::region::RegionRequestHeader;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::joins::HashTableLookupExpr;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_expr::LogicalPlan;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use datafusion_proto::protobuf::PhysicalExprNode;
use prost::Message;
use serde::{Deserialize, Serialize};
use store_api::storage::RegionId;

/// Current wire-format version for remote dynamic filter payload updates.
pub use self::initial_remote_dyn_filter_reg::{
    INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY,
    INITIAL_REMOTE_DYN_FILTER_REGS_MAX_TOTAL_PROTO_BYTES, InitialDynFilterReg,
    InitialDynFilterRegs, InitialDynFilterSnapshot,
};

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
}

impl DynFilterPayload {
    /// Encodes a DataFusion physical expression into a bounded dynamic filter payload.
    ///
    /// This rejects expressions that cannot be safely shipped as dynamic filter
    /// predicates and fails if the serialized payload exceeds `max_payload_bytes`.
    pub fn from_datafusion_expr(
        expr: &Arc<dyn PhysicalExpr>,
        max_payload_bytes: usize,
    ) -> DataFusionResult<Self> {
        validate_supported_payload_expr(expr)?;

        let codec = DefaultPhysicalExtensionCodec {};
        let proto = serialize_physical_expr(expr, &codec)?;
        let mut bytes = Vec::new();
        proto.encode(&mut bytes).map_err(|e| {
            DataFusionError::Internal(format!("Failed to encode PhysicalExprNode: {e}"))
        })?;

        validate_payload_size(bytes.len(), max_payload_bytes)?;

        Ok(Self::Datafusion(bytes))
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
        validate_payload_size(bytes.len(), max_payload_bytes)?;
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

pub(crate) fn decode_physical_expr_from_bytes(
    bytes: &[u8],
    task_ctx: &TaskContext,
    input_schema: &datafusion::arrow::datatypes::Schema,
    max_payload_bytes: usize,
) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
    validate_payload_size(bytes.len(), max_payload_bytes)?;
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
) -> DataFusionResult<()> {
    if payload_size_bytes > max_payload_bytes {
        return Err(DataFusionError::Plan(format!(
            "DynFilterPayload::Datafusion is {} bytes, which exceeds the configured limit of {} bytes",
            payload_size_bytes, max_payload_bytes
        )));
    }

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
    use datafusion::physical_expr::expressions::Column;

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
    fn dyn_filter_payload_rejects_oversized_payload() {
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("host", 0));

        let err = DynFilterPayload::from_datafusion_expr(&expr, 1).unwrap_err();

        assert!(matches!(err, DataFusionError::Plan(_)));
    }
}
