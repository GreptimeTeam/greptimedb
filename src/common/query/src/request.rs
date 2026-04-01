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

use std::sync::Arc;

use api::v1::region::RegionRequestHeader;
use datafusion::arrow::datatypes::Schema;
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

pub const DYN_FILTER_PROTOCOL_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
pub enum DynFilterPayload {
    Datafusion(Vec<u8>),
}

impl DynFilterPayload {
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

    pub fn decode_datafusion_expr(
        &self,
        task_ctx: &TaskContext,
        input_schema: &Schema,
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

fn validate_decoded_payload_expr(
    expr: &Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DynFilterUpdate {
    pub protocol_version: u32,
    pub query_id: String,
    pub filter_id: String,
    pub epoch: u64,
    pub is_complete: bool,
    pub payload: DynFilterPayload,
}

impl DynFilterUpdate {
    pub fn new(
        query_id: String,
        filter_id: String,
        epoch: u64,
        is_complete: bool,
        payload: DynFilterPayload,
    ) -> Self {
        Self {
            protocol_version: DYN_FILTER_PROTOCOL_VERSION,
            query_id,
            filter_id,
            epoch,
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
        let decoded: DynFilterUpdate = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded, update);
        assert!(decoded.is_complete);
        assert!(
            matches!(decoded.payload, DynFilterPayload::Datafusion(ref bytes) if bytes == &vec![9, 8, 7])
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
        let mismatched_expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("service", 0));

        let payload = DynFilterPayload::from_datafusion_expr(&mismatched_expr, 1024).unwrap();
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
