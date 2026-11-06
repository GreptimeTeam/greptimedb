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

//! Versioned envelope for one grouped read of several regions by one datanode.
//!
//! A grouped read is request batching only: `concat(execute(plan, r1), execute(plan, r2), ...)`.
//! The frontend selects several regions of one table that live on the same datanode, wraps the
//! plain per-region plan in this envelope and sends one request. The datanode decodes the nested
//! plan once per region against that region's own catalog and executes the region plans in order,
//! concatenating their batches. Nothing is merged or reduced across regions, so the upper layers
//! of the frontend plan do not change.
//!
//! The envelope is the *root relation* of the serialized request. It is a Substrait
//! [`ExtensionLeafRel`](substrait::substrait_proto_df::proto::ExtensionLeafRel) whose extension
//! name is [`DATANODE_GROUPED_READ_NAME`] and whose payload is JSON. A datanode peeks at the root
//! relation before it builds a region catalog, so a request that is not a grouped envelope is
//! decoded exactly as before.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::DefaultTableSource;
use datafusion_common::{DFSchemaRef, DataFusionError};
use datafusion_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use prost::Message;
use serde::{Deserialize, Serialize};
use store_api::storage::RegionId;
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use table::metadata::TableType;
use table::table::adapter::DfTableProviderAdapter;
use table::table_name::TableName;

use crate::region_query::RegionQueryTarget;

use crate::query_engine::DefaultSerializer;

/// The version of the grouped read envelope this build produces and accepts.
pub const DATANODE_GROUPED_READ_VERSION: u32 = 1;

/// The extension name of the envelope. It is the Substrait extension `type_url` and the name of
/// the logical node, so the serializer registry finds the same node on both sides.
pub const DATANODE_GROUPED_READ_NAME: &str = "DatanodeGroupedRead";

/// Upper bound on the number of regions one grouped read may name.
pub const MAX_GROUPED_READ_REGIONS: usize = 1024;

/// Upper bound on the serialized envelope payload.
pub const MAX_GROUPED_READ_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;

/// The JSON payload of a grouped read envelope.
///
/// The nested plan is one plan shared by every region: the datanode decodes it once per region
/// against that region's catalog. `table_id` and `table_name` describe the one ordinary logical
/// table the nested plan reads; a datanode checks every named region against them before it runs
/// anything.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct GroupedReadPayload {
    version: u32,
    region_ids: Vec<u64>,
    table_id: u32,
    table_name: String,
    nested_plan: Vec<u8>,
}

impl GroupedReadPayload {
    /// Builds a payload and validates it.
    pub fn try_new(
        region_ids: Vec<u64>,
        table_id: u32,
        table_name: String,
        nested_plan: Vec<u8>,
    ) -> std::result::Result<Self, String> {
        let payload = Self {
            version: DATANODE_GROUPED_READ_VERSION,
            region_ids,
            table_id,
            table_name,
            nested_plan,
        };
        payload.validate_shape()?;
        Ok(payload)
    }

    pub fn version(&self) -> u32 {
        self.version
    }

    pub fn region_ids(&self) -> &[u64] {
        &self.region_ids
    }

    pub fn table_id(&self) -> u32 {
        self.table_id
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn nested_plan(&self) -> &[u8] {
        &self.nested_plan
    }

    /// The anchor region of the request: the first region of the group.
    pub fn anchor_region_id(&self) -> Option<RegionId> {
        self.region_ids.first().copied().map(RegionId::from_u64)
    }

    /// Serializes the payload as the envelope's JSON.
    pub fn encode(&self) -> std::result::Result<Vec<u8>, String> {
        self.validate_shape()?;
        let encoded = serde_json::to_vec(self)
            .map_err(|error| format!("cannot serialize grouped read payload: {error}"))?;
        if encoded.len() > MAX_GROUPED_READ_PAYLOAD_BYTES {
            return Err(format!(
                "grouped read payload is {} bytes, above the {MAX_GROUPED_READ_PAYLOAD_BYTES} \
                 byte bound",
                encoded.len()
            ));
        }
        Ok(encoded)
    }

    /// Parses an envelope payload. The version is checked here, so an older datanode can answer
    /// "not supported" instead of running the request against the anchor region alone.
    pub fn decode(bytes: &[u8]) -> std::result::Result<Self, GroupedReadEnvelopeError> {
        if bytes.len() > MAX_GROUPED_READ_PAYLOAD_BYTES {
            return Err(GroupedReadEnvelopeError::Malformed(format!(
                "grouped read payload is {} bytes, above the {MAX_GROUPED_READ_PAYLOAD_BYTES} \
                 byte bound",
                bytes.len()
            )));
        }
        let payload: Self = serde_json::from_slice(bytes).map_err(|error| {
            GroupedReadEnvelopeError::Malformed(format!(
                "cannot parse grouped read payload: {error}"
            ))
        })?;
        if payload.version != DATANODE_GROUPED_READ_VERSION {
            return Err(GroupedReadEnvelopeError::UnsupportedVersion(
                payload.version,
            ));
        }
        payload
            .validate_shape()
            .map_err(GroupedReadEnvelopeError::Malformed)?;
        Ok(payload)
    }

    /// Checks the parts of the payload that do not depend on the plan.
    fn validate_shape(&self) -> std::result::Result<(), String> {
        if self.version != DATANODE_GROUPED_READ_VERSION {
            return Err(format!(
                "unsupported grouped read version {}, this build reads version {}",
                self.version, DATANODE_GROUPED_READ_VERSION
            ));
        }
        if self.region_ids.is_empty() {
            return Err("a grouped read names no region".to_string());
        }
        if self.region_ids.len() > MAX_GROUPED_READ_REGIONS {
            return Err(format!(
                "a grouped read names {} regions, above the {MAX_GROUPED_READ_REGIONS} bound",
                self.region_ids.len()
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        for region_id in &self.region_ids {
            if !seen.insert(*region_id) {
                return Err(format!("region {region_id} is named twice"));
            }
        }
        if self.table_name.is_empty() {
            return Err("a grouped read names no table".to_string());
        }
        if self.nested_plan.is_empty() {
            return Err("a grouped read carries no nested plan".to_string());
        }
        Ok(())
    }

    /// Checks the request-scoped parts of the payload: the anchor must be one of the regions, and
    /// every region must belong to the captured logical table.
    pub fn validate_for_request(&self, anchor: RegionId) -> std::result::Result<(), String> {
        self.validate_shape()?;
        if !self.region_ids.contains(&anchor.as_u64()) {
            return Err(format!(
                "the request anchor region {anchor} is not one of the grouped regions {:?}",
                self.region_ids
            ));
        }
        for region_id in &self.region_ids {
            let region = RegionId::from_u64(*region_id);
            if region.table_id() != self.table_id {
                return Err(format!(
                    "region {region} belongs to table {}, but the grouped read names table {}",
                    region.table_id(),
                    self.table_id
                ));
            }
        }
        Ok(())
    }
}

/// Why a serialized request is not a usable grouped read envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupedReadEnvelopeError {
    /// The envelope is ours, but this build does not read its version.
    UnsupportedVersion(u32),
    /// The root identifies our envelope, but its structure or payload is invalid.
    Malformed(String),
}

impl fmt::Display for GroupedReadEnvelopeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion(version) => write!(
                f,
                "unsupported `{DATANODE_GROUPED_READ_NAME}` version {version}, this build reads \
                 version {DATANODE_GROUPED_READ_VERSION}"
            ),
            Self::Malformed(reason) => write!(f, "malformed grouped read envelope: {reason}"),
        }
    }
}

impl std::error::Error for GroupedReadEnvelopeError {}

/// Peeks at the root relation of a serialized region request.
///
/// Ordinary plans and other extensions are left to the ordinary decoder. Only a root leaf
/// identified as our envelope is validated here, including the single-root requirement.
pub fn peek_grouped_read_envelope(
    plan: &[u8],
) -> std::result::Result<Option<GroupedReadPayload>, GroupedReadEnvelopeError> {
    use substrait::substrait_proto_df::proto::{Plan, plan_rel, rel};

    let Ok(decoded) = Plan::decode(plan) else {
        return Ok(None);
    };
    for relation in &decoded.relations {
        let root = match relation.rel_type.as_ref() {
            Some(plan_rel::RelType::Rel(root)) => Some(root),
            Some(plan_rel::RelType::Root(root)) => root.input.as_ref(),
            None => None,
        };
        let Some(rel::RelType::ExtensionLeaf(leaf)) = root.and_then(|root| root.rel_type.as_ref())
        else {
            continue;
        };
        let Some(detail) = leaf.detail.as_ref() else {
            continue;
        };
        if detail.type_url != DATANODE_GROUPED_READ_NAME {
            continue;
        }
        if decoded.relations.len() != 1 {
            return Err(GroupedReadEnvelopeError::Malformed(format!(
                "a grouped read envelope must be the only root relation, found {}",
                decoded.relations.len()
            )));
        }
        return GroupedReadPayload::decode(detail.value.as_ref()).map(Some);
    }
    Ok(None)
}

/// The logical node of the envelope: a leaf that carries the grouped read.
#[derive(Clone)]
pub struct DatanodeGroupedReadPlan {
    /// The payload bytes exactly as the node sends them. The datanode validates them; the encoder
    /// must not, because a malformed envelope has to reach a datanode to be rejected there.
    payload_bytes: Vec<u8>,
    /// The parsed payload when the bytes were valid at construction, for `EXPLAIN`.
    payload: Option<GroupedReadPayload>,
    schema: DFSchemaRef,
}

impl DatanodeGroupedReadPlan {
    /// Builds the node for `payload`, whose schema is the schema of the nested plan it carries.
    pub fn try_new(
        payload: GroupedReadPayload,
        schema: DFSchemaRef,
    ) -> std::result::Result<Self, String> {
        let payload_bytes = payload.encode()?;
        Ok(Self {
            payload_bytes,
            payload: Some(payload),
            schema,
        })
    }

    /// Builds the node from raw payload bytes without validating them.
    ///
    /// The frontend builds envelopes through [`build_grouped_read_plan`], which checks its payload.
    /// This constructor exists so a producer that only forwards bytes can re-wrap them, and so a
    /// test can send a malformed envelope and observe the datanode rejecting it.
    pub fn from_raw_payload(payload_bytes: Vec<u8>, schema: DFSchemaRef) -> Self {
        let payload = GroupedReadPayload::decode(&payload_bytes).ok();
        Self {
            payload_bytes,
            payload,
            schema,
        }
    }

    /// Builds the node from already encoded payload bytes. Used by the serializer, which receives
    /// the JSON the producing side wrote.
    pub fn from_payload_bytes(
        bytes: &[u8],
        schema: DFSchemaRef,
    ) -> std::result::Result<Self, GroupedReadEnvelopeError> {
        let payload = GroupedReadPayload::decode(bytes)?;
        Self::try_new(payload, schema).map_err(GroupedReadEnvelopeError::Malformed)
    }

    pub fn name() -> &'static str {
        DATANODE_GROUPED_READ_NAME
    }

    /// The parsed payload, when this node was built from valid bytes.
    pub fn payload(&self) -> Option<&GroupedReadPayload> {
        self.payload.as_ref()
    }

    /// The payload bytes this node sends.
    pub fn payload_bytes(&self) -> &[u8] {
        &self.payload_bytes
    }

    pub fn into_logical_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }

    fn ord_key(&self) -> (Vec<u8>, String) {
        (self.payload_bytes.clone(), format!("{}", self.schema))
    }
}

impl fmt::Debug for DatanodeGroupedReadPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl Hash for DatanodeGroupedReadPlan {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.payload_bytes.hash(state);
    }
}

impl PartialEq for DatanodeGroupedReadPlan {
    fn eq(&self, other: &Self) -> bool {
        self.payload_bytes == other.payload_bytes && self.schema == other.schema
    }
}

impl Eq for DatanodeGroupedReadPlan {}

impl PartialOrd for DatanodeGroupedReadPlan {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.ord_key().cmp(&other.ord_key()))
    }
}

impl UserDefinedLogicalNodeCore for DatanodeGroupedReadPlan {
    fn name(&self) -> &str {
        Self::name()
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        Vec::new()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Some(payload) = &self.payload else {
            return write!(
                f,
                "{}: unparsed payload of {} bytes",
                Self::name(),
                self.payload_bytes.len()
            );
        };
        let regions = payload
            .region_ids
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        write!(
            f,
            "{}: version={}, table_id={}, table={}, regions=[{}]",
            Self::name(),
            payload.version,
            payload.table_id,
            payload.table_name,
            regions
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        if !inputs.is_empty() {
            return Err(DataFusionError::Internal(format!(
                "{} is a leaf and takes no inputs, found {}",
                Self::name(),
                inputs.len()
            )));
        }
        Ok(self.clone())
    }
}

/// Encodes a plain per-region plan as the nested plan of a grouped read.
pub fn encode_nested_plan(plan: &LogicalPlan) -> DataFusionResult<Vec<u8>> {
    let encoded = DFLogicalSubstraitConvertor
        .encode(plan, DefaultSerializer)
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    Ok(encoded.to_vec())
}

/// Builds the grouped read envelope for `regions` of the table `nested_plan` reads.
///
/// The envelope names the table exactly as the nested plan does, so a datanode can compare the
/// decoded plan's table with it without resolving catalog or schema defaults; `table_id` and the
/// region ids pin the identity of the table.
pub fn build_grouped_read_plan(
    regions: &[RegionId],
    table_id: u32,
    nested_plan: &LogicalPlan,
) -> DataFusionResult<LogicalPlan> {
    let Some(table) = nested_plan_table(nested_plan) else {
        return Err(DataFusionError::Plan(format!(
            "the nested plan is not an eligible grouped read shape: expected one ordinary table \
             scan under projections, filters and aliases only, got `{}`",
            nested_plan.display_indent()
        )));
    };
    let nested = encode_nested_plan(nested_plan)?;
    let payload = GroupedReadPayload::try_new(
        regions.iter().map(RegionId::as_u64).collect(),
        table_id,
        table.table_name,
        nested,
    )
    .map_err(DataFusionError::Plan)?;
    let node = DatanodeGroupedReadPlan::try_new(payload, nested_plan.schema().clone())
        .map_err(DataFusionError::Plan)?;
    Ok(node.into_logical_plan())
}

/// Whether `plan` is a shape a grouped read may carry: one ordinary table scan, under any number
/// of projections, filters and aliases, and nothing else.
///
/// Everything else keeps the ordinary one-request-per-region path: joins and second scans would
/// make the envelope ambiguous about which table it belongs to, a sort or a limit would break the
/// ordering or row-count promise the frontend plan makes per region, and aggregates, windows,
/// flow plans and extension nodes are not plain row-preserving scan shapes.
pub fn eligible_remote_plan(plan: &LogicalPlan) -> bool {
    fn walk(plan: &LogicalPlan, scans: &mut usize) -> bool {
        match plan {
            LogicalPlan::TableScan(scan) => {
                if ordinary_scan_table(scan).is_none() {
                    return false;
                }
                *scans += 1;
                *scans == 1
            }
            LogicalPlan::Projection(projection) => walk(&projection.input, scans),
            LogicalPlan::Filter(filter) => walk(&filter.input, scans),
            LogicalPlan::SubqueryAlias(alias) => walk(&alias.input, scans),
            _ => false,
        }
    }

    let mut scans = 0usize;
    walk(plan, &mut scans) && scans == 1
}

/// The one ordinary table `plan` reads, when it is an eligible grouped read shape.
pub fn nested_plan_table(plan: &LogicalPlan) -> Option<TableName> {
    fn walk(plan: &LogicalPlan, table: &mut Option<TableName>) -> bool {
        match plan {
            LogicalPlan::TableScan(scan) => {
                let Some(name) = ordinary_scan_table(scan) else {
                    return false;
                };
                *table = Some(name);
                true
            }
            LogicalPlan::Projection(projection) => walk(&projection.input, table),
            LogicalPlan::Filter(filter) => walk(&filter.input, table),
            LogicalPlan::SubqueryAlias(alias) => walk(&alias.input, table),
            _ => false,
        }
    }

    let mut table = None;
    walk(plan, &mut table).then_some(table).flatten()
}

/// The name of an ordinary table scan: a `DefaultTableSource` over a base table.
fn ordinary_scan_table(scan: &datafusion_expr::TableScan) -> Option<TableName> {
    let source = scan.source.downcast_ref::<DefaultTableSource>()?;
    if let Some(provider) = source
        .table_provider
        .downcast_ref::<DfTableProviderAdapter>()
    {
        let table = provider.table();
        if table.table_type() != TableType::Base {
            return None;
        }
        let info = table.table_info();
        return Some(TableName::new(
            info.catalog_name.clone(),
            info.schema_name.clone(),
            info.name.clone(),
        ));
    }
    match &scan.table_name {
        datafusion_common::TableReference::Full {
            catalog,
            schema,
            table,
        } => Some(TableName::new(
            catalog.to_string(),
            schema.to_string(),
            table.to_string(),
        )),
        datafusion_common::TableReference::Partial { schema, table } => {
            Some(TableName::new("", schema.to_string(), table.to_string()))
        }
        datafusion_common::TableReference::Bare { table } => {
            Some(TableName::new("", "", table.to_string()))
        }
    }
}

/// Checks a decoded nested plan against the payload that carried it.
///
/// The plan must be an eligible single-table shape, and the one table it reads must be the table
/// the payload names. The region ids are checked separately, per request.
pub fn validate_grouped_read_nested_plan(
    payload: &GroupedReadPayload,
    nested_plan: &LogicalPlan,
) -> std::result::Result<(), String> {
    payload.validate_shape()?;
    if payload.table_id == 0 {
        return Err("a grouped read names no logical table id".to_string());
    }
    let Some(table_name) = nested_plan_table(nested_plan) else {
        return Err(format!(
            "the nested plan is not an eligible grouped read shape: expected one ordinary table \
             scan under projections, filters and aliases only, got `{}`",
            nested_plan.display_indent()
        ));
    };
    // The reference in the nested plan may be unqualified, so only the table name is compared
    // here; the payload's table id and every region's table id carry the identity of the table.
    if table_name.table_name != payload.table_name {
        return Err(format!(
            "the nested plan reads table `{}`, but the grouped read names `{}`",
            table_name.table_name, payload.table_name
        ));
    }
    Ok(())
}

/// One datanode request of a grouped read: the frozen target that serves it and the regions its
/// envelope names, in the order the datanode executes them.
#[derive(Clone, Debug)]
pub struct DatanodeReadGroup {
    target: RegionQueryTarget,
    regions: Vec<RegionId>,
}

impl DatanodeReadGroup {
    pub fn try_new(
        target: RegionQueryTarget,
        regions: Vec<RegionId>,
    ) -> std::result::Result<Self, String> {
        if regions.is_empty() {
            return Err("a grouped read group names no region".to_string());
        }
        let mut seen = std::collections::BTreeSet::new();
        for region_id in &regions {
            if !seen.insert(*region_id) {
                return Err(format!("group {target:?} names region {region_id} twice"));
            }
        }
        Ok(Self { target, regions })
    }

    pub fn target(&self) -> &RegionQueryTarget {
        &self.target
    }

    pub fn regions(&self) -> &[RegionId] {
        &self.regions
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datafusion::datasource::DefaultTableSource;
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::logical_expr::{LogicalPlanBuilder, col, lit};
    use datafusion_common::TableReference;

    use super::*;

    fn metrics_source() -> Arc<dyn datafusion::logical_expr::TableSource> {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("v", DataType::Float64, true),
        ]));
        Arc::new(DefaultTableSource::new(Arc::new(EmptyTable::new(schema))))
    }

    fn metrics_scan() -> LogicalPlanBuilder {
        LogicalPlanBuilder::scan(TableReference::bare("metrics"), metrics_source(), None).unwrap()
    }

    fn project_scan() -> LogicalPlan {
        metrics_scan()
            .filter(col("v").gt(lit(1.0)))
            .unwrap()
            .project(vec![col("ts"), col("v")])
            .unwrap()
            .build()
            .unwrap()
    }

    fn table() -> TableName {
        TableName::new("", "", "metrics")
    }

    fn regions(count: u32) -> Vec<RegionId> {
        (1..=count).map(|n| RegionId::new(1024, n)).collect()
    }

    #[test]
    fn envelope_round_trips_through_the_root_extension() {
        let plan = project_scan();
        let envelope = build_grouped_read_plan(&regions(3), 1024, &plan).unwrap();
        let encoded = DFLogicalSubstraitConvertor
            .encode(&envelope, DefaultSerializer)
            .unwrap()
            .to_vec();

        let payload = peek_grouped_read_envelope(&encoded).unwrap().unwrap();
        assert_eq!(DATANODE_GROUPED_READ_VERSION, payload.version());
        assert_eq!(
            vec![
                RegionId::new(1024, 1).as_u64(),
                RegionId::new(1024, 2).as_u64(),
                RegionId::new(1024, 3).as_u64()
            ],
            payload.region_ids()
        );
        assert_eq!(1024, payload.table_id());
        assert_eq!("metrics", payload.table_name());
        assert_eq!(Some(RegionId::new(1024, 1)), payload.anchor_region_id());
        payload
            .validate_for_request(RegionId::new(1024, 2))
            .unwrap();
        assert!(
            payload
                .validate_for_request(RegionId::new(1024, 9))
                .is_err()
        );
    }

    #[test]
    fn ordinary_plan_is_not_an_envelope() {
        let encoded = DFLogicalSubstraitConvertor
            .encode(&project_scan(), DefaultSerializer)
            .unwrap()
            .to_vec();
        assert!(peek_grouped_read_envelope(&encoded).unwrap().is_none());
        assert!(
            peek_grouped_read_envelope(b"not a substrait plan")
                .unwrap()
                .is_none()
        );
    }

    fn mutate_root_leaf(
        mut encoded: Vec<u8>,
        mutate: impl FnOnce(&mut String, &mut Vec<u8>),
    ) -> Vec<u8> {
        let mut raw =
            substrait::substrait_proto_df::proto::Plan::decode(encoded.as_slice()).unwrap();
        {
            let relation = raw.relations.first_mut().unwrap();
            let rel = match relation.rel_type.as_mut() {
                Some(substrait::substrait_proto_df::proto::plan_rel::RelType::Rel(rel)) => rel,
                Some(substrait::substrait_proto_df::proto::plan_rel::RelType::Root(root)) => {
                    root.input.as_mut().unwrap()
                }
                None => unreachable!("the envelope is a root relation"),
            };
            let Some(substrait::substrait_proto_df::proto::rel::RelType::ExtensionLeaf(leaf)) =
                rel.rel_type.as_mut()
            else {
                unreachable!("the envelope is an extension leaf")
            };
            let detail = leaf.detail.as_mut().unwrap();
            let (mut type_url, mut value) = (detail.type_url.clone(), detail.value.to_vec());
            mutate(&mut type_url, &mut value);
            detail.type_url = type_url;
            detail.value = value.into();
        }
        encoded.clear();
        raw.encode(&mut encoded).unwrap();
        encoded
    }

    #[test]
    fn unsupported_version_is_rejected_at_peek() {
        let envelope = build_grouped_read_plan(&regions(2), 1024, &project_scan()).unwrap();
        let encoded = DFLogicalSubstraitConvertor
            .encode(&envelope, DefaultSerializer)
            .unwrap()
            .to_vec();
        let mutated = mutate_root_leaf(encoded, |_type_url, value| {
            let mut payload: serde_json::Value = serde_json::from_slice(value).unwrap();
            payload["version"] = serde_json::json!(DATANODE_GROUPED_READ_VERSION + 1);
            *value = serde_json::to_vec(&payload).unwrap();
        });
        assert_eq!(
            Err(GroupedReadEnvelopeError::UnsupportedVersion(
                DATANODE_GROUPED_READ_VERSION + 1
            )),
            peek_grouped_read_envelope(&mutated)
        );
    }

    #[test]
    fn other_root_extension_is_left_to_ordinary_decoder() {
        let envelope = build_grouped_read_plan(&regions(2), 1024, &project_scan()).unwrap();
        let encoded = DFLogicalSubstraitConvertor
            .encode(&envelope, DefaultSerializer)
            .unwrap()
            .to_vec();
        let mutated = mutate_root_leaf(encoded, |type_url, _value| {
            *type_url = "SomeoneElsesRead".to_string();
        });
        assert_eq!(Ok(None), peek_grouped_read_envelope(&mutated));
    }

    #[test]
    fn grouped_read_peek_rejects_multi_root_and_malformed_own_payload_only() {
        use substrait::substrait_proto_df::proto::Plan;

        let encoded = DFLogicalSubstraitConvertor
            .encode(
                &build_grouped_read_plan(&regions(2), 1024, &project_scan()).unwrap(),
                DefaultSerializer,
            )
            .unwrap()
            .to_vec();
        let mut plan = Plan::decode(encoded.as_slice()).unwrap();
        plan.relations.push(plan.relations[0].clone());
        assert!(matches!(
            peek_grouped_read_envelope(&plan.encode_to_vec()),
            Err(GroupedReadEnvelopeError::Malformed(_))
        ));
        plan.relations.swap(0, 1);
        let unrelated = Plan::decode(
            DFLogicalSubstraitConvertor
                .encode(&project_scan(), DefaultSerializer)
                .unwrap()
                .as_ref(),
        )
        .unwrap()
        .relations
        .remove(0);
        plan.relations[0] = unrelated;
        assert!(matches!(
            peek_grouped_read_envelope(&plan.encode_to_vec()),
            Err(GroupedReadEnvelopeError::Malformed(_))
        ));
        let malformed =
            mutate_root_leaf(encoded.clone(), |_, value| *value = b"bad payload".to_vec());
        assert!(matches!(
            peek_grouped_read_envelope(&malformed),
            Err(GroupedReadEnvelopeError::Malformed(_))
        ));
        let unrelated = mutate_root_leaf(encoded, |name, value| {
            *name = "OtherExtension".to_string();
            // Raw-byte searches must not mistake embedded envelope identifiers for a root envelope.
            value.extend_from_slice(b"DatanodeGroupedRead {not an envelope}");
        });
        assert_eq!(Ok(None), peek_grouped_read_envelope(&unrelated));
    }

    #[test]
    fn payload_validation_rejects_duplicate_empty_and_wrong_table() {
        assert!(
            GroupedReadPayload::try_new(vec![], 1024, "metrics".to_string(), b"nested".to_vec())
                .is_err()
        );
        assert!(
            GroupedReadPayload::try_new(
                vec![
                    RegionId::new(1024, 1).as_u64(),
                    RegionId::new(1024, 1).as_u64()
                ],
                1024,
                "metrics".to_string(),
                b"nested".to_vec(),
            )
            .is_err()
        );
        let wrong_table = GroupedReadPayload::try_new(
            vec![RegionId::new(2048, 1).as_u64()],
            1024,
            "metrics".to_string(),
            b"nested".to_vec(),
        )
        .unwrap();
        assert!(
            wrong_table
                .validate_for_request(RegionId::new(2048, 1))
                .is_err()
        );
        let payload = GroupedReadPayload::try_new(
            vec![RegionId::new(1024, 1).as_u64()],
            1024,
            "metrics".to_string(),
            b"nested".to_vec(),
        )
        .unwrap();
        assert!(validate_grouped_read_nested_plan(&payload, &project_scan()).is_ok());
        let misnamed = GroupedReadPayload::try_new(
            vec![RegionId::new(1024, 1).as_u64()],
            1024,
            "greptime.public.other".to_string(),
            b"nested".to_vec(),
        )
        .unwrap();
        assert!(validate_grouped_read_nested_plan(&misnamed, &project_scan()).is_err());
    }

    #[test]
    fn ineligible_shapes_are_not_grouped() {
        let sorted = metrics_scan()
            .sort(vec![col("ts").sort(true, false)])
            .unwrap()
            .build()
            .unwrap();
        let limited = metrics_scan().limit(0, Some(1)).unwrap().build().unwrap();
        let aggregated = metrics_scan()
            .aggregate(
                Vec::<Expr>::new(),
                vec![datafusion::functions_aggregate::count::count(lit(1i32))],
            )
            .unwrap()
            .build()
            .unwrap();
        let joined = metrics_scan()
            .join(
                metrics_scan().alias("other").unwrap().build().unwrap(),
                datafusion_expr::JoinType::Inner,
                (vec!["metrics.ts"], vec!["other.ts"]),
                None,
            )
            .unwrap()
            .build()
            .unwrap();

        for (name, plan) in [
            ("sort", sorted),
            ("limit", limited),
            ("aggregate", aggregated),
            ("join", joined),
        ] {
            assert!(
                !eligible_remote_plan(&plan),
                "`{name}` must keep the ordinary path: {}",
                plan.display_indent()
            );
        }
        assert!(eligible_remote_plan(&project_scan()));
        assert_eq!(Some(table()), nested_plan_table(&project_scan()));
    }
}
