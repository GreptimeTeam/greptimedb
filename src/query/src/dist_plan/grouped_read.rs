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

//! Multi-input grouped-read envelope.
//!
//! A grouped read is one `GroupRead` root marker around `UnionAll(RegionPlan(...), ...)`, where
//! each `RegionPlan` is a wire extension leaf carrying one region id and the complete per-region
//! plan bytes. A datanode intercepts the root, replaces every `RegionPlan` leaf with that region's
//! bound logical subtree and executes the resulting ordinary `UnionAll` through the query engine.
//! The wire is deliberately narrow: the marker's only child is a `UnionAll` of region leaves (a
//! single member may degrade to a bare leaf), and every region plan is a single-table
//! `Read`/`Project`/`Filter` chain. Neither the outer request nor a region plan may carry an
//! aggregate, join, sort or any other extension, so a datanode never decodes an arbitrary plan.
//!
//! `GroupRead` is the root relation of the serialized request and is an
//! [`ExtensionSingleRel`](substrait::substrait_proto_df::proto::ExtensionSingleRel). `RegionPlan`
//! is an [`ExtensionLeafRel`](substrait::substrait_proto_df::proto::ExtensionLeafRel).
//! Ordinary plans are left untouched: a datanode peeks only at the root before constructing a
//! region-scoped decoder.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::DefaultTableSource;
use datafusion::execution::context::SessionState;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchemaRef, DataFusionError};
use datafusion_expr::{
    Expr, Extension, LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNodeCore,
};
use prost::Message;
use serde::{Deserialize, Serialize};
use store_api::storage::RegionId;
use substrait::substrait_proto_df::proto::{Plan, Rel, plan_rel, read_rel, rel, set_rel::SetOp};
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use table::metadata::TableType;
use table::table::adapter::DfTableProviderAdapter;
use table::table_name::TableName;

use crate::dummy_catalog::DummyTableProvider;
use crate::query_engine::DefaultSerializer;
use crate::region_query::RegionQueryTarget;

/// The version of the grouped-read model this build produces and accepts.
///
/// The envelope was never released, so there is exactly one version.
pub const GROUP_READ_VERSION: u32 = 1;

/// The wire name of the root multi-input marker.
pub const GROUP_READ_NAME: &str = "GroupRead";

/// The wire name of one region-scoped plan leaf.
pub const REGION_PLAN_NAME: &str = "RegionPlan";

/// Query-context marker set by the datanode for a grouped composite request.
pub const GROUP_READ_CONTEXT_EXTENSION: &str = "greptime.grouped_read";

/// Upper bound on the number of regions one grouped read may name.
pub const MAX_GROUP_READ_REGIONS: usize = 1024;

/// Upper bound on the serialized grouped-read request.
pub const MAX_GROUP_READ_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;

/// Upper bound on the relations one `RegionPlan`'s raw plan may contain.
///
/// A production regional plan is a single table read under a few projections and filters, so this
/// bound only has to leave room for planner-added wrappers.
pub const MAX_REGION_PLAN_NODES: usize = 24;

/// Upper bound on the depth of one `RegionPlan`'s raw plan.
pub const MAX_REGION_PLAN_DEPTH: usize = 32;

/// Upper bound on the `UnionAll` nesting depth of one grouped-read combine tree.
pub const MAX_GROUP_READ_UNION_DEPTH: usize = 32;

/// Conservative bytes one RegionPlan contributes after JSON encoding.
///
/// A `Vec<u8>` is serialized as a JSON number array, so the worst case is four characters per
/// byte. The fixed framing covers field names, region id and container punctuation.
fn estimated_region_plan_json_bytes(nested_plan_len: usize) -> usize {
    nested_plan_len.saturating_mul(4).saturating_add(160)
}

/// Maximum RegionPlan leaves that fit the request budget for one shared regional template.
pub fn max_regions_for_group_read_payload(nested_plan_len: usize, table_name_len: usize) -> usize {
    let per_region = estimated_region_plan_json_bytes(nested_plan_len).saturating_add(16);
    if per_region == 0 {
        return MAX_GROUP_READ_REGIONS;
    }
    let framing = table_name_len.saturating_add(128);
    MAX_GROUP_READ_PAYLOAD_BYTES
        .saturating_mul(9)
        .saturating_div(10)
        .saturating_sub(framing)
        .checked_div(per_region)
        .unwrap_or(0)
        .min(MAX_GROUP_READ_REGIONS)
}

fn check_region_list(region_ids: &[u64]) -> std::result::Result<(), String> {
    if region_ids.is_empty() {
        return Err("a grouped read names no region".to_string());
    }
    if region_ids.len() > MAX_GROUP_READ_REGIONS {
        return Err(format!(
            "a grouped read names {} regions, above the {MAX_GROUP_READ_REGIONS} bound",
            region_ids.len()
        ));
    }
    let mut seen = std::collections::BTreeSet::new();
    for region_id in region_ids {
        if !seen.insert(*region_id) {
            return Err(format!("region {region_id} is named twice"));
        }
    }
    Ok(())
}

/// Metadata of the `GroupRead` root marker.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct GroupReadPayload {
    version: u32,
    region_ids: Vec<u64>,
    table_id: u32,
    table_name: String,
}

impl GroupReadPayload {
    pub fn try_new(
        region_ids: Vec<u64>,
        table_id: u32,
        table_name: String,
    ) -> std::result::Result<Self, String> {
        let payload = Self {
            version: GROUP_READ_VERSION,
            region_ids,
            table_id,
            table_name,
        };
        payload.validate_shape()?;
        Ok(payload)
    }

    fn from_validated_parts(region_ids: Vec<u64>, table_id: u32, table_name: String) -> Self {
        Self {
            version: GROUP_READ_VERSION,
            region_ids,
            table_id,
            table_name,
        }
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

    pub fn anchor_region_id(&self) -> Option<RegionId> {
        self.region_ids.first().copied().map(RegionId::from_u64)
    }

    pub fn encode(&self) -> std::result::Result<Vec<u8>, String> {
        self.validate_shape()?;
        let encoded = serde_json::to_vec(self)
            .map_err(|error| format!("cannot serialize grouped read payload: {error}"))?;
        if encoded.len() > MAX_GROUP_READ_PAYLOAD_BYTES {
            return Err(format!(
                "grouped read metadata is {} bytes, above the {MAX_GROUP_READ_PAYLOAD_BYTES} \
                 byte bound",
                encoded.len()
            ));
        }
        Ok(encoded)
    }

    pub fn decode(bytes: &[u8]) -> std::result::Result<Self, GroupReadEnvelopeError> {
        if bytes.len() > MAX_GROUP_READ_PAYLOAD_BYTES {
            return Err(GroupReadEnvelopeError::Malformed(format!(
                "grouped read metadata is {} bytes, above the {MAX_GROUP_READ_PAYLOAD_BYTES} \
                 byte bound",
                bytes.len()
            )));
        }
        let payload: Self = serde_json::from_slice(bytes).map_err(|error| {
            GroupReadEnvelopeError::Malformed(format!(
                "cannot parse grouped read metadata: {error}"
            ))
        })?;
        if payload.version != GROUP_READ_VERSION {
            return Err(GroupReadEnvelopeError::UnsupportedVersion(payload.version));
        }
        payload
            .validate_shape()
            .map_err(GroupReadEnvelopeError::Malformed)?;
        Ok(payload)
    }

    fn validate_shape(&self) -> std::result::Result<(), String> {
        if self.version != GROUP_READ_VERSION {
            return Err(format!(
                "unsupported grouped read version {}, this build reads version {}",
                self.version, GROUP_READ_VERSION
            ));
        }
        check_region_list(&self.region_ids)?;
        if self.table_name.is_empty() {
            return Err("a grouped read names no table".to_string());
        }
        if self.table_id == 0 {
            return Err("a grouped read names no logical table id".to_string());
        }
        Ok(())
    }

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

    /// Checks one `RegionPlan` member against the root metadata before it is decoded.
    ///
    /// A member must be named by the payload and its region must belong to the payload's table;
    /// duplicates are checked by the caller so it can keep one set across members.
    fn validate_region_member(&self, region_id: RegionId) -> std::result::Result<(), String> {
        self.validate_shape()?;
        if !self.region_ids.contains(&region_id.as_u64()) {
            return Err(format!(
                "region {region_id} is not one of the grouped regions {:?}",
                self.region_ids
            ));
        }
        if region_id.table_id() != self.table_id {
            return Err(format!(
                "region {region_id} belongs to table {}, but the grouped read names table {}",
                region_id.table_id(),
                self.table_id
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RegionPlanPayload {
    region_id: u64,
    plan: Vec<u8>,
}

impl RegionPlanPayload {
    pub fn try_new(region_id: RegionId, plan: Vec<u8>) -> std::result::Result<Self, String> {
        if plan.is_empty() {
            return Err("a RegionPlan carries no plan".to_string());
        }
        Ok(Self {
            region_id: region_id.as_u64(),
            plan,
        })
    }

    pub fn region_id(&self) -> RegionId {
        RegionId::from_u64(self.region_id)
    }

    pub fn plan(&self) -> &[u8] {
        &self.plan
    }

    pub fn encode(&self) -> std::result::Result<Vec<u8>, String> {
        let encoded = serde_json::to_vec(self)
            .map_err(|error| format!("cannot serialize RegionPlan payload: {error}"))?;
        if encoded.len() > MAX_GROUP_READ_PAYLOAD_BYTES {
            return Err(format!(
                "a RegionPlan payload is {} bytes, above the {MAX_GROUP_READ_PAYLOAD_BYTES} \
                 byte bound",
                encoded.len()
            ));
        }
        Ok(encoded)
    }

    pub fn decode(bytes: &[u8]) -> std::result::Result<Self, String> {
        if bytes.len() > MAX_GROUP_READ_PAYLOAD_BYTES {
            return Err(format!(
                "a RegionPlan payload is {} bytes, above the {MAX_GROUP_READ_PAYLOAD_BYTES} \
                 byte bound",
                bytes.len()
            ));
        }
        let payload: Self = serde_json::from_slice(bytes)
            .map_err(|error| format!("cannot parse RegionPlan payload: {error}"))?;
        if payload.plan.is_empty() {
            return Err("a RegionPlan carries no plan".to_string());
        }
        Ok(payload)
    }
}

/// Why a serialized request is not a usable grouped-read envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupReadEnvelopeError {
    UnsupportedVersion(u32),
    Malformed(String),
}

impl fmt::Display for GroupReadEnvelopeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion(version) => write!(
                f,
                "unsupported `{GROUP_READ_NAME}` version {version}, this build reads \
                 version {GROUP_READ_VERSION}"
            ),
            Self::Malformed(reason) => write!(f, "malformed grouped read envelope: {reason}"),
        }
    }
}

impl std::error::Error for GroupReadEnvelopeError {}

/// Whether `needle` occurs in `haystack`.
fn contains_marker(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

/// Peeks at the root relation of a serialized region request.
///
/// Only a unique `GroupRead` `ExtensionSingleRel` root is ours. An old append-only root leaf is
/// rejected explicitly so an old producer cannot be mistaken for an ordinary plan.
pub fn peek_group_read_envelope(
    plan: &[u8],
) -> std::result::Result<Option<GroupReadPayload>, GroupReadEnvelopeError> {
    // Cheap pre-filter: an ordinary remote plan never carries either marker name, so most requests
    // are answered without paying for a full protobuf decode. `RegionPlan` is included so a bare
    // region leaf at the root is still rejected as a malformed grouped read instead of being
    // silently left to the ordinary decoder.
    if !contains_marker(plan, GROUP_READ_NAME.as_bytes())
        && !contains_marker(plan, REGION_PLAN_NAME.as_bytes())
    {
        return Ok(None);
    }

    let Ok(decoded) = Plan::decode(plan) else {
        return Ok(None);
    };
    if decoded.relations.len() != 1 {
        let has_group_read = decoded.relations.iter().any(|relation| {
            let root = match relation.rel_type.as_ref() {
                Some(plan_rel::RelType::Rel(root)) => Some(root),
                Some(plan_rel::RelType::Root(root)) => root.input.as_ref(),
                None => None,
            };
            matches!(
                root.and_then(|root| root.rel_type.as_ref()),
                Some(rel::RelType::ExtensionSingle(single))
                    if single
                        .detail
                        .as_ref()
                        .is_some_and(|detail| detail.type_url == GROUP_READ_NAME)
            )
        });
        if has_group_read {
            return Err(GroupReadEnvelopeError::Malformed(format!(
                "a GroupRead request must have exactly one root relation, found {}",
                decoded.relations.len()
            )));
        }
        return Ok(None);
    }
    let relation = &decoded.relations[0];
    let root = match relation.rel_type.as_ref() {
        Some(plan_rel::RelType::Rel(root)) => Some(root),
        Some(plan_rel::RelType::Root(root)) => root.input.as_ref(),
        None => None,
    };
    let Some(root) = root else {
        return Ok(None);
    };
    match root.rel_type.as_ref() {
        Some(rel::RelType::ExtensionSingle(single)) => {
            let Some(detail) = single.detail.as_ref() else {
                return Ok(None);
            };
            if detail.type_url != GROUP_READ_NAME {
                return Ok(None);
            }
            GroupReadPayload::decode(detail.value.as_ref()).map(Some)
        }
        Some(rel::RelType::ExtensionLeaf(leaf)) => {
            let Some(detail) = leaf.detail.as_ref() else {
                return Ok(None);
            };
            if detail.type_url == GROUP_READ_NAME || detail.type_url == REGION_PLAN_NAME {
                return Err(GroupReadEnvelopeError::Malformed(format!(
                    "`{}` is not a valid root relation in grouped-read version {}",
                    detail.type_url, GROUP_READ_VERSION
                )));
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

/// The `GroupRead` logical marker: one marker around the combine tree.
#[derive(Clone)]
pub struct GroupReadPlan {
    payload_bytes: Vec<u8>,
    payload: Option<GroupReadPayload>,
    input: Arc<LogicalPlan>,
    schema: DFSchemaRef,
}

impl GroupReadPlan {
    pub fn try_new(
        payload: GroupReadPayload,
        input: LogicalPlan,
    ) -> std::result::Result<Self, String> {
        let payload_bytes = payload.encode()?;
        Ok(Self {
            payload_bytes,
            payload: Some(payload),
            schema: input.schema().clone(),
            input: Arc::new(input),
        })
    }

    pub fn name() -> &'static str {
        GROUP_READ_NAME
    }

    pub fn payload(&self) -> Option<&GroupReadPayload> {
        self.payload.as_ref()
    }

    pub fn payload_bytes(&self) -> &[u8] {
        &self.payload_bytes
    }

    pub fn input(&self) -> &LogicalPlan {
        &self.input
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

impl fmt::Debug for GroupReadPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl Hash for GroupReadPlan {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.payload_bytes.hash(state);
        format!("{}", self.schema).hash(state);
    }
}

impl PartialEq for GroupReadPlan {
    fn eq(&self, other: &Self) -> bool {
        self.payload_bytes == other.payload_bytes && self.input == other.input
    }
}

impl Eq for GroupReadPlan {}

impl PartialOrd for GroupReadPlan {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.ord_key().cmp(&other.ord_key()))
    }
}

impl UserDefinedLogicalNodeCore for GroupReadPlan {
    fn name(&self) -> &str {
        Self::name()
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
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
        write!(
            f,
            "{}: version={}, table_id={}, table={}, regions={:?}",
            Self::name(),
            payload.version,
            payload.table_id,
            payload.table_name,
            payload.region_ids
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        if inputs.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "{} takes exactly one combine-tree input, found {}",
                Self::name(),
                inputs.len()
            )));
        }
        let input = inputs.remove(0);
        let mut plan = self.clone();
        plan.schema = input.schema().clone();
        plan.input = Arc::new(input);
        Ok(plan)
    }
}

/// One region-scoped plan leaf. Its schema is the schema of the encoded regional plan.
#[derive(Clone)]
pub struct RegionPlan {
    payload_bytes: Vec<u8>,
    region_id: RegionId,
    schema: DFSchemaRef,
}

impl RegionPlan {
    pub fn try_new(
        region_id: RegionId,
        plan_bytes: Vec<u8>,
        schema: DFSchemaRef,
    ) -> std::result::Result<Self, String> {
        let payload = RegionPlanPayload::try_new(region_id, plan_bytes)?;
        let payload_bytes = payload.encode()?;
        Ok(Self {
            payload_bytes,
            region_id,
            schema,
        })
    }

    pub fn name() -> &'static str {
        REGION_PLAN_NAME
    }

    pub fn region_id(&self) -> RegionId {
        self.region_id
    }

    pub fn payload_bytes(&self) -> &[u8] {
        &self.payload_bytes
    }

    pub fn into_logical_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }

    fn ord_key(&self) -> (u64, Vec<u8>, String) {
        (
            self.region_id.as_u64(),
            self.payload_bytes.clone(),
            format!("{}", self.schema),
        )
    }
}

impl fmt::Debug for RegionPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl Hash for RegionPlan {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.payload_bytes.hash(state);
        format!("{}", self.schema).hash(state);
    }
}

impl PartialEq for RegionPlan {
    fn eq(&self, other: &Self) -> bool {
        self.payload_bytes == other.payload_bytes && self.schema == other.schema
    }
}

impl Eq for RegionPlan {}

impl PartialOrd for RegionPlan {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.ord_key().cmp(&other.ord_key()))
    }
}

impl UserDefinedLogicalNodeCore for RegionPlan {
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
        write!(f, "{}: region={}", Self::name(), self.region_id())
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

/// Encodes a plain per-region plan as the plan inside one `RegionPlan`.
pub fn encode_nested_plan(plan: &LogicalPlan) -> DataFusionResult<Vec<u8>> {
    let encoded = DFLogicalSubstraitConvertor
        .encode(plan, DefaultSerializer)
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    Ok(encoded.to_vec())
}

/// Builds a balanced `UnionAll` tree.
///
/// A left-deep union of a thousand region leaves would overflow the stack while dropping or
/// dropping/formatting the plan. Balanced unions keep the tree depth logarithmic.
fn union_balanced(mut plans: Vec<LogicalPlan>, table_name: &str) -> DataFusionResult<LogicalPlan> {
    if plans.len() == 1 {
        return Ok(plans.pop().unwrap());
    }
    let right = plans.split_off(plans.len() / 2);
    let left = union_balanced(plans, table_name)?;
    let right = union_balanced(right, table_name)?;
    LogicalPlanBuilder::from(left)
        .union(right)
        .map_err(|error| {
            DataFusionError::Plan(format!(
                "regions of table {table_name} do not share one output schema: {error}"
            ))
        })?
        .build()
}

/// Builds a GroupRead from one already encoded regional template.
pub fn build_group_read_plan_from_nested(
    regions: &[RegionId],
    table_id: u32,
    table_name: String,
    nested: &[u8],
    schema: DFSchemaRef,
) -> DataFusionResult<LogicalPlan> {
    if regions.is_empty() {
        return Err(DataFusionError::Plan(
            "a grouped read needs at least one region".to_string(),
        ));
    }
    if regions.len() > MAX_GROUP_READ_REGIONS {
        return Err(DataFusionError::Plan(format!(
            "a grouped read names {} regions, above the {MAX_GROUP_READ_REGIONS} bound",
            regions.len()
        )));
    }
    let mut seen = BTreeSet::new();
    for region in regions {
        if !seen.insert(region.as_u64()) {
            return Err(DataFusionError::Plan(format!(
                "region {region} is named more than once in the grouped read"
            )));
        }
    }
    if regions.iter().any(|region| region.table_id() != table_id) {
        return Err(DataFusionError::Plan(format!(
            "the RegionPlan list names a region outside table {table_id}"
        )));
    }
    // The datanode re-runs this pre-check before decoding anything; refusing to produce a wire
    // the consumer must reject keeps the producer and consumer on the same narrow contract.
    preflight_region_plan(nested).map_err(DataFusionError::Plan)?;
    let estimated = estimated_region_plan_json_bytes(nested.len())
        .saturating_mul(regions.len())
        .saturating_add(table_name.len())
        .saturating_add(128);
    if estimated > MAX_GROUP_READ_PAYLOAD_BYTES {
        return Err(DataFusionError::Plan(format!(
            "estimated grouped read envelope is {estimated} bytes, above the \
             {MAX_GROUP_READ_PAYLOAD_BYTES} byte bound"
        )));
    }
    let mut plans = Vec::with_capacity(regions.len());
    for region in regions {
        let leaf = RegionPlan::try_new(*region, nested.to_vec(), schema.clone())
            .map_err(DataFusionError::Plan)?;
        plans.push(leaf.into_logical_plan());
    }
    let union = union_balanced(plans, &table_name)?;
    let payload = GroupReadPayload::from_validated_parts(
        regions.iter().map(RegionId::as_u64).collect(),
        table_id,
        table_name,
    );
    let node = GroupReadPlan::try_new(payload, union).map_err(DataFusionError::Plan)?;
    Ok(node.into_logical_plan())
}

/// Builds the only production grouped read: `GroupRead(UnionAll(RegionPlan...))`.
pub fn build_group_read_plan(
    regions: &[RegionId],
    table_id: u32,
    nested_plan: &LogicalPlan,
) -> DataFusionResult<LogicalPlan> {
    let Some(table) = nested_plan_table(nested_plan) else {
        return Err(DataFusionError::Plan(format!(
            "the regional plan is not a single ordinary table scan: got `{}`",
            nested_plan.display_indent()
        )));
    };
    let nested = encode_nested_plan(nested_plan)?;
    build_group_read_plan_from_nested(
        regions,
        table_id,
        table.table_name,
        &nested,
        nested_plan.schema().clone(),
    )
}

/// Whether `plan` is a shape a grouped read may carry: one ordinary table scan under projections,
/// filters and aliases only.
pub fn eligible_remote_plan(plan: &LogicalPlan) -> bool {
    // The wire only carries plain expressions: a subquery or DML would be rejected by the
    // datanode, so such a plan must keep the ordinary path instead of building an envelope.
    !plan_has_forbidden_nodes(plan)
        && single_table_scan(plan).is_some_and(|scan| ordinary_scan_table(scan).is_some())
}

/// The one table scan `plan` reads through only projections, filters and aliases.
fn single_table_scan(plan: &LogicalPlan) -> Option<&datafusion_expr::TableScan> {
    match plan {
        LogicalPlan::TableScan(scan) => Some(scan),
        LogicalPlan::Projection(projection) => single_table_scan(&projection.input),
        LogicalPlan::Filter(filter) => single_table_scan(&filter.input),
        LogicalPlan::SubqueryAlias(alias) => single_table_scan(&alias.input),
        _ => None,
    }
}

/// The one ordinary table `plan` reads, when it is an eligible grouped read shape.
pub fn nested_plan_table(plan: &LogicalPlan) -> Option<TableName> {
    single_table_scan(plan).and_then(ordinary_scan_table)
}

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

/// Validates one decoded regional plan against the root metadata and its region id.
///
/// The decoded plan must be a single ordinary table read whose provider is the region provider
/// bound to `region_id`. The wire table name is not an identity: the region catalog resolves any
/// name to the region provider, so a matching name alone proves nothing.
pub fn validate_region_plan(
    payload: &GroupReadPayload,
    region_id: RegionId,
    plan: &LogicalPlan,
) -> std::result::Result<(), String> {
    payload.validate_region_member(region_id)?;
    if plan_has_forbidden_nodes(plan) {
        return Err(format!(
            "region {region_id} decoded a plan with a subquery or DML node"
        ));
    }
    let Some(scan) = single_table_scan(plan) else {
        return Err(format!(
            "region {region_id} decoded a plan that is not a single ordinary table read: `{}`",
            plan.display_indent()
        ));
    };
    validate_region_scan(payload, region_id, scan)
}

/// Checks that one decoded scan reads exactly the region provider bound to `region_id`.
fn validate_region_scan(
    payload: &GroupReadPayload,
    region_id: RegionId,
    scan: &datafusion_expr::TableScan,
) -> std::result::Result<(), String> {
    let Some(source) = scan.source.downcast_ref::<DefaultTableSource>() else {
        return Err(format!("region {region_id} does not read a table provider"));
    };
    let scan_table = scan.table_name.table();
    if scan_table != payload.table_name.as_str() {
        return Err(format!(
            "region {region_id} reads table `{scan_table}`, but the grouped read names `{}`",
            payload.table_name
        ));
    }
    // The datanode resolves every name to the region provider, so the provider itself is the
    // only identity that matters. Inspection, reserved and other logical sources are not it.
    if let Some(provider) = source.table_provider.downcast_ref::<DummyTableProvider>() {
        let metadata = provider.region_metadata();
        if metadata.region_id != region_id {
            return Err(format!(
                "region {region_id} decoded the provider of region {}",
                metadata.region_id
            ));
        }
        if metadata.region_id.table_id() != payload.table_id {
            return Err(format!(
                "region {region_id} decoded a provider of table {}, but the grouped read names \
                 table {}",
                metadata.region_id.table_id(),
                payload.table_id
            ));
        }
        return Ok(());
    }
    Err(format!(
        "region {region_id} decoded a provider that is not the bound region provider"
    ))
}

fn expr_has_subquery(expr: &Expr) -> bool {
    let mut found = false;
    expr.apply(|expr| {
        if matches!(
            expr,
            Expr::ScalarSubquery(_)
                | Expr::InSubquery(_)
                | Expr::Exists(_)
                | Expr::SetComparison(_)
        ) {
            found = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })
    .unwrap();
    found
}

fn plan_has_forbidden_nodes(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Dml(_))
        || plan.expressions().iter().any(expr_has_subquery)
        || plan
            .inputs()
            .iter()
            .any(|input| plan_has_forbidden_nodes(input))
}

fn rel_kind_name(rel: &rel::RelType) -> &'static str {
    match rel {
        rel::RelType::Read(_) => "a plain read",
        rel::RelType::Filter(_) => "Filter",
        rel::RelType::Fetch(_) => "Fetch",
        rel::RelType::Aggregate(_) => "Aggregate",
        rel::RelType::Sort(_) => "Sort",
        rel::RelType::Join(_) => "Join",
        rel::RelType::Project(_) => "Projection",
        rel::RelType::Set(_) => "Set",
        rel::RelType::ExtensionSingle(_) | rel::RelType::ExtensionMulti(_) => "an extension",
        rel::RelType::ExtensionLeaf(_) => "an extension leaf",
        rel::RelType::Cross(_) => "CrossJoin",
        rel::RelType::Reference(_) => "Reference",
        rel::RelType::Write(_) => "Write",
        rel::RelType::Ddl(_) => "Ddl",
        rel::RelType::Update(_) => "Update",
        rel::RelType::HashJoin(_) => "HashJoin",
        rel::RelType::MergeJoin(_) => "MergeJoin",
        rel::RelType::NestedLoopJoin(_) => "NestedLoopJoin",
        rel::RelType::Window(_) => "Window",
        rel::RelType::Exchange(_) => "Exchange",
        rel::RelType::Expand(_) => "Expand",
    }
}

/// Returns the single root relation of a serialized plan.
fn plan_root_rel(plan: &Plan) -> std::result::Result<&Rel, String> {
    if plan.relations.len() != 1 {
        return Err(format!(
            "a plan must have exactly one root relation, found {}",
            plan.relations.len()
        ));
    }
    match plan.relations[0].rel_type.as_ref() {
        Some(plan_rel::RelType::Rel(root)) => Ok(root),
        Some(plan_rel::RelType::Root(root)) => root
            .input
            .as_ref()
            .ok_or_else(|| "the plan root relation is empty".to_string()),
        None => Err("the plan has no root relation".to_string()),
    }
}

/// Cheap pre-check of one `RegionPlan`'s raw plan, before any generic decoder sees it.
///
/// The production wire only carries a single-table `Read` under `Project` and `Filter` nodes.
/// Checking the raw proto tree first bounds the work a malformed wire can make a decoder do.
pub fn preflight_region_plan(plan_bytes: &[u8]) -> std::result::Result<(), String> {
    let plan = Plan::decode(plan_bytes)
        .map_err(|error| format!("cannot parse a RegionPlan plan: {error}"))?;
    let root = plan_root_rel(&plan)?;
    let mut nodes = 0usize;
    check_region_rel(root, 1, &mut nodes)
}

fn check_region_rel(rel: &Rel, depth: usize, nodes: &mut usize) -> std::result::Result<(), String> {
    if depth > MAX_REGION_PLAN_DEPTH {
        return Err(format!(
            "a RegionPlan plan is deeper than the {MAX_REGION_PLAN_DEPTH} node bound"
        ));
    }
    *nodes += 1;
    if *nodes > MAX_REGION_PLAN_NODES {
        return Err(format!(
            "a RegionPlan plan has more than the {MAX_REGION_PLAN_NODES} node bound"
        ));
    }
    match rel.rel_type.as_ref() {
        Some(rel::RelType::Read(read)) => {
            if matches!(
                read.read_type.as_ref(),
                Some(read_rel::ReadType::NamedTable(_))
            ) {
                Ok(())
            } else {
                Err("a RegionPlan plan may only read one named table".to_string())
            }
        }
        Some(rel::RelType::Project(project)) => {
            let input = project
                .input
                .as_ref()
                .ok_or_else(|| "a RegionPlan projection has no input".to_string())?;
            check_region_rel(input, depth + 1, nodes)
        }
        Some(rel::RelType::Filter(filter)) => {
            let input = filter
                .input
                .as_ref()
                .ok_or_else(|| "a RegionPlan filter has no input".to_string())?;
            check_region_rel(input, depth + 1, nodes)
        }
        Some(other) => Err(format!(
            "a RegionPlan plan may only contain Read, Projection and Filter nodes, found {}",
            rel_kind_name(other)
        )),
        None => Err("a RegionPlan plan has an empty relation".to_string()),
    }
}

/// Resolves one `RegionPlan` leaf against its region catalog.
#[async_trait]
pub trait RegionPlanResolver: Send + Sync {
    async fn resolve_region_plan(
        &self,
        region_id: RegionId,
        plan: &[u8],
    ) -> std::result::Result<LogicalPlan, String>;
}

/// Decodes a serialized `GroupRead` request into an ordinary logical plan.
///
/// Only the narrow production wire is accepted: a `GroupRead` root around `UnionAll(RegionPlan...)`
/// (a single member may be a bare leaf). Every leaf is checked against the root metadata, its raw
/// plan is preflighted as a `Read`/`Project`/`Filter` chain before the resolver decodes it, and the
/// decoded plan must be the region provider bound to that leaf. The final region set must match the
/// root metadata exactly.
pub async fn decode_group_read_request(
    plan_bytes: &[u8],
    _state: &SessionState,
    payload: GroupReadPayload,
    resolver: &dyn RegionPlanResolver,
) -> std::result::Result<LogicalPlan, String> {
    let plan = Plan::decode(plan_bytes)
        .map_err(|error| format!("cannot parse GroupRead request: {error}"))?;
    let root = plan_root_rel(&plan)?;
    let input = match root.rel_type.as_ref() {
        Some(rel::RelType::ExtensionSingle(single)) => {
            let detail = single
                .detail
                .as_ref()
                .ok_or_else(|| "the GroupRead root has no detail".to_string())?;
            if detail.type_url != GROUP_READ_NAME {
                return Err(format!(
                    "a GroupRead request root must be the `{GROUP_READ_NAME}` marker, found `{}`",
                    detail.type_url
                ));
            }
            let root_payload = GroupReadPayload::decode(detail.value.as_ref())
                .map_err(|error| error.to_string())?;
            if root_payload != payload {
                return Err("the root marker and the intercepted metadata differ".to_string());
            }
            single
                .input
                .as_ref()
                .ok_or_else(|| "the GroupRead root has no combine tree".to_string())?
        }
        Some(other) => {
            return Err(format!(
                "a GroupRead request root must be the `{GROUP_READ_NAME}` marker, found {}",
                rel_kind_name(other)
            ));
        }
        None => return Err("the request root relation is empty".to_string()),
    };

    let leaves = group_read_leaf_rels(input)?;
    if leaves.len() > MAX_GROUP_READ_REGIONS {
        return Err(format!(
            "a grouped read names {} regions, above the {MAX_GROUP_READ_REGIONS} bound",
            leaves.len()
        ));
    }
    // First pass: check every member and bound its raw plan before any decoder runs.
    let mut members = Vec::with_capacity(leaves.len());
    let mut seen = BTreeSet::new();
    for leaf in leaves {
        let Some(rel::RelType::ExtensionLeaf(leaf_rel)) = leaf.rel_type.as_ref() else {
            return Err(format!(
                "a GroupRead member must be a RegionPlan leaf, found {}",
                leaf.rel_type
                    .as_ref()
                    .map(rel_kind_name)
                    .unwrap_or("an empty relation")
            ));
        };
        let detail = leaf_rel
            .detail
            .as_ref()
            .ok_or_else(|| "a RegionPlan leaf has no detail".to_string())?;
        if detail.type_url != REGION_PLAN_NAME {
            return Err(format!(
                "unexpected extension leaf `{}` in a GroupRead combine tree",
                detail.type_url
            ));
        }
        let leaf_payload = RegionPlanPayload::decode(detail.value.as_ref())?;
        let region_id = leaf_payload.region_id();
        payload.validate_region_member(region_id)?;
        if !seen.insert(region_id.as_u64()) {
            return Err(format!(
                "region {region_id} appears more than once in the combine tree"
            ));
        }
        preflight_region_plan(leaf_payload.plan())?;
        members.push((region_id, leaf_payload));
    }
    let expected = payload
        .region_ids()
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if seen != expected {
        return Err(format!(
            "the combine tree does not name the grouped regions exactly once: consumed {seen:?}, \
             expected {expected:?}"
        ));
    }
    // Second pass: decode each preflighted member and accept only its bound region provider.
    let mut plans = Vec::with_capacity(members.len());
    let mut expected_schema: Option<DFSchemaRef> = None;
    for (region_id, leaf_payload) in members {
        let plan = resolver
            .resolve_region_plan(region_id, leaf_payload.plan())
            .await?;
        validate_region_plan(&payload, region_id, &plan)?;
        let schema = plan.schema().clone();
        match expected_schema.as_ref() {
            Some(expected) if expected != &schema => {
                return Err(format!(
                    "region {region_id} decodes to a different output schema"
                ));
            }
            Some(_) => {}
            None => expected_schema = Some(schema),
        }
        plans.push(plan);
    }
    union_balanced(plans, payload.table_name())
        .map_err(|error| format!("cannot combine region plans: {error}"))
}

/// Returns the `RegionPlan` leaves directly below the `GroupRead` marker.
///
/// The builder balances a large union into a tree, so the combine tree is a `UnionAll` tree whose
/// leaves are exactly the region plans; nothing else may appear in it.
fn group_read_leaf_rels<'a>(input: &'a Rel) -> std::result::Result<Vec<&'a Rel>, String> {
    let mut leaves = Vec::new();
    collect_group_read_leaves(input, 1, &mut leaves)?;
    Ok(leaves)
}

fn collect_group_read_leaves<'a>(
    rel: &'a Rel,
    depth: usize,
    leaves: &mut Vec<&'a Rel>,
) -> std::result::Result<(), String> {
    if depth > MAX_GROUP_READ_UNION_DEPTH {
        return Err(format!(
            "a GroupRead combine tree is deeper than the {MAX_GROUP_READ_UNION_DEPTH} UnionAll \
             bound"
        ));
    }
    match rel.rel_type.as_ref() {
        Some(rel::RelType::Set(set)) => {
            let op = SetOp::try_from(set.op)
                .map_err(|_| format!("unknown set operator {} in a GroupRead", set.op))?;
            if op != SetOp::UnionAll {
                return Err(format!(
                    "a GroupRead combine tree must be UnionAll, found {op:?}"
                ));
            }
            if set.inputs.is_empty() {
                return Err("a GroupRead combine tree names no RegionPlan".to_string());
            }
            for input in &set.inputs {
                collect_group_read_leaves(input, depth + 1, leaves)?;
            }
            Ok(())
        }
        // A single-region grouped read degrades to one bare `RegionPlan` leaf.
        Some(rel::RelType::ExtensionLeaf(_)) => {
            if leaves.len() >= MAX_GROUP_READ_REGIONS {
                return Err(format!(
                    "a grouped read names more than the {MAX_GROUP_READ_REGIONS} region bound"
                ));
            }
            leaves.push(rel);
            Ok(())
        }
        Some(other) => Err(format!(
            "a GroupRead combine tree must be UnionAll(RegionPlan...), found {}",
            rel_kind_name(other)
        )),
        None => Err("the GroupRead root input is empty".to_string()),
    }
}

/// One datanode request of a grouped read: the frozen target and the regions its combine tree
/// names. The order is the envelope order; UnionAll itself promises no order.
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
        if regions.len() > MAX_GROUP_READ_REGIONS {
            return Err(format!(
                "group {target:?} names {} regions, above the {MAX_GROUP_READ_REGIONS} bound",
                regions.len()
            ));
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
    use async_trait::async_trait;
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datafusion::catalog::MemTable;
    use datafusion::datasource::DefaultTableSource;
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::functions_aggregate::sum::sum;
    use datafusion::logical_expr::{LogicalPlanBuilder, col, lit};
    use datafusion_common::TableReference;
    use prost::Message;

    use super::*;
    use crate::dist_plan::MergeScanLogicalPlan;

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

    fn filtered_scan(filters: usize) -> LogicalPlan {
        let mut builder = metrics_scan();
        for _ in 0..filters {
            builder = builder.filter(col("v").gt(lit(1.0))).unwrap();
        }
        builder.build().unwrap()
    }

    fn regions(count: u32) -> Vec<RegionId> {
        (1..=count).map(|n| RegionId::new(1024, n)).collect()
    }

    fn encode_plan(plan: &LogicalPlan) -> Vec<u8> {
        DFLogicalSubstraitConvertor
            .encode(plan, DefaultSerializer)
            .unwrap()
            .to_vec()
    }

    fn encoded_template() -> Vec<u8> {
        encode_nested_plan(&project_scan()).unwrap()
    }

    fn region_leaf(region: RegionId) -> LogicalPlan {
        RegionPlan::try_new(region, encoded_template(), project_scan().schema().clone())
            .unwrap()
            .into_logical_plan()
    }

    fn region_union(regions: &[RegionId]) -> LogicalPlan {
        union_balanced(
            regions.iter().map(|region| region_leaf(*region)).collect(),
            "metrics",
        )
        .unwrap()
    }

    /// A decoded regional plan whose scan is bound to the real region provider of `region_id`.
    fn region_scan(region_id: RegionId) -> LogicalPlan {
        let provider = crate::optimizer::test_util::mock_table_provider(region_id);
        LogicalPlanBuilder::scan(
            TableReference::bare("metrics"),
            Arc::new(DefaultTableSource::new(Arc::new(provider))),
            None,
        )
        .unwrap()
        .project(vec![col("k0"), col("v0")])
        .unwrap()
        .build()
        .unwrap()
    }

    /// A scan over an in-memory provider, mimicking a reserved inspection provider.
    fn inspect_scan() -> LogicalPlan {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "ts",
            DataType::Int64,
            false,
        )]));
        let table = MemTable::try_new(schema, vec![]).unwrap();
        LogicalPlanBuilder::scan(
            TableReference::bare("metrics"),
            Arc::new(DefaultTableSource::new(Arc::new(table))),
            None,
        )
        .unwrap()
        .build()
        .unwrap()
    }

    /// Builds a `GroupRead` request around an arbitrary combine tree.
    fn group_read_request(
        regions: &[RegionId],
        table_id: u32,
        input: LogicalPlan,
    ) -> (Vec<u8>, GroupReadPayload) {
        let payload = GroupReadPayload::try_new(
            regions.iter().map(RegionId::as_u64).collect(),
            table_id,
            "metrics".to_string(),
        )
        .unwrap();
        let node = GroupReadPlan::try_new(payload.clone(), input).unwrap();
        (encode_plan(&node.into_logical_plan()), payload)
    }

    fn rewrite_root_payload(plan_bytes: Vec<u8>, payload: &GroupReadPayload) -> Vec<u8> {
        let mut plan = Plan::decode(plan_bytes.as_slice()).unwrap();
        let relation = plan.relations.first_mut().unwrap();
        let root = match relation.rel_type.as_mut().unwrap() {
            plan_rel::RelType::Rel(root) => root,
            plan_rel::RelType::Root(root) => root.input.as_mut().unwrap(),
        };
        let rel::RelType::ExtensionSingle(single) = root.rel_type.as_mut().unwrap() else {
            panic!("the fixture must have a GroupRead root");
        };
        single.detail.as_mut().unwrap().value = payload.encode().unwrap().into();
        plan.encode_to_vec()
    }

    fn count_table_scans(plan: &LogicalPlan) -> usize {
        let mut count = usize::from(matches!(plan, LogicalPlan::TableScan(_)));
        for input in plan.inputs() {
            count += count_table_scans(input);
        }
        count
    }

    struct BoundRegionResolver;

    #[async_trait]
    impl RegionPlanResolver for BoundRegionResolver {
        async fn resolve_region_plan(
            &self,
            region_id: RegionId,
            _plan: &[u8],
        ) -> std::result::Result<LogicalPlan, String> {
            Ok(region_scan(region_id))
        }
    }

    struct ForeignRegionResolver;

    #[async_trait]
    impl RegionPlanResolver for ForeignRegionResolver {
        async fn resolve_region_plan(
            &self,
            _region_id: RegionId,
            _plan: &[u8],
        ) -> std::result::Result<LogicalPlan, String> {
            Ok(region_scan(RegionId::new(1024, 77)))
        }
    }

    struct InspectProviderResolver;

    #[async_trait]
    impl RegionPlanResolver for InspectProviderResolver {
        async fn resolve_region_plan(
            &self,
            _region_id: RegionId,
            _plan: &[u8],
        ) -> std::result::Result<LogicalPlan, String> {
            Ok(inspect_scan())
        }
    }

    struct MultiInputResolver;

    #[async_trait]
    impl RegionPlanResolver for MultiInputResolver {
        async fn resolve_region_plan(
            &self,
            region_id: RegionId,
            _plan: &[u8],
        ) -> std::result::Result<LogicalPlan, String> {
            Ok(LogicalPlanBuilder::from(region_scan(region_id))
                .union(region_scan(region_id))
                .unwrap()
                .build()
                .unwrap())
        }
    }

    struct PanicResolver;

    #[async_trait]
    impl RegionPlanResolver for PanicResolver {
        async fn resolve_region_plan(
            &self,
            _region_id: RegionId,
            _plan: &[u8],
        ) -> std::result::Result<LogicalPlan, String> {
            panic!("the resolver must not see an invalid RegionPlan member")
        }
    }

    fn session_state() -> SessionState {
        SessionStateBuilder::new().build()
    }

    #[test]
    fn group_read_version_is_one() {
        assert_eq!(1, GROUP_READ_VERSION);
        let payload = GroupReadPayload::try_new(
            vec![RegionId::new(1024, 1).as_u64()],
            1024,
            "metrics".to_string(),
        )
        .unwrap();
        assert_eq!(1, payload.version());

        let mut value: serde_json::Value =
            serde_json::from_slice(&payload.encode().unwrap()).unwrap();
        value["version"] = serde_json::json!(2);
        let error = GroupReadPayload::decode(&serde_json::to_vec(&value).unwrap()).unwrap_err();
        assert_eq!(GroupReadEnvelopeError::UnsupportedVersion(2), error);
    }

    #[test]
    fn group_read_round_trips_as_single_root_with_region_plan_leaves() {
        let envelope = build_group_read_plan(&regions(3), 1024, &project_scan()).unwrap();
        let encoded = encode_plan(&envelope);
        let payload = peek_group_read_envelope(&encoded).unwrap().unwrap();
        assert_eq!(GROUP_READ_VERSION, payload.version());
        assert_eq!(
            regions(3),
            payload
                .region_ids()
                .iter()
                .map(|id| RegionId::from_u64(*id))
                .collect::<Vec<_>>()
        );
        assert_eq!(1024, payload.table_id());
        assert_eq!("metrics", payload.table_name());

        let plan = Plan::decode(encoded.as_slice()).unwrap();
        assert_eq!(1, plan.relations.len());
        let root = match plan.relations[0].rel_type.as_ref().unwrap() {
            plan_rel::RelType::Rel(rel) => rel,
            plan_rel::RelType::Root(root) => root.input.as_ref().unwrap(),
        };
        let rel::RelType::ExtensionSingle(single) = root.rel_type.as_ref().unwrap() else {
            panic!("GroupRead must be an ExtensionSingleRel");
        };
        assert_eq!(GROUP_READ_NAME, single.detail.as_ref().unwrap().type_url);
        // The builder balances a three-region union into nested UnionAll nodes.
        let rel::RelType::Set(set) = single.input.as_ref().unwrap().rel_type.as_ref().unwrap()
        else {
            panic!("three regions must be a UnionAll tree");
        };
        assert_eq!(SetOp::UnionAll, SetOp::try_from(set.op).unwrap());
    }

    #[tokio::test]
    async fn balanced_union_of_three_regions_decodes_every_leaf() {
        let listed = regions(3);
        let (bytes, payload) = group_read_request(&listed, 1024, region_union(&listed));
        let decoded =
            decode_group_read_request(&bytes, &session_state(), payload, &BoundRegionResolver)
                .await
                .unwrap();
        assert_eq!(3, count_table_scans(&decoded));

        let listed = regions(9);
        let (bytes, payload) = group_read_request(&listed, 1024, region_union(&listed));
        let decoded =
            decode_group_read_request(&bytes, &session_state(), payload, &BoundRegionResolver)
                .await
                .unwrap();
        assert_eq!(9, count_table_scans(&decoded));
    }

    #[tokio::test]
    async fn single_region_group_read_is_a_bare_leaf() {
        let only = regions(1);
        let envelope = build_group_read_plan(&only, 1024, &project_scan()).unwrap();
        let bytes = encode_plan(&envelope);
        let payload = peek_group_read_envelope(&bytes).unwrap().unwrap();

        let plan = Plan::decode(bytes.as_slice()).unwrap();
        let root = plan_root_rel(&plan).unwrap();
        let rel::RelType::ExtensionSingle(single) = root.rel_type.as_ref().unwrap() else {
            panic!("GroupRead must be an ExtensionSingleRel");
        };
        let input = single.input.as_ref().unwrap();
        assert!(
            matches!(
                input.rel_type.as_ref(),
                Some(rel::RelType::ExtensionLeaf(_))
            ),
            "a single-region grouped read must be a bare RegionPlan leaf"
        );
        assert_eq!(
            REGION_PLAN_NAME,
            match input.rel_type.as_ref().unwrap() {
                rel::RelType::ExtensionLeaf(leaf) =>
                    leaf.detail.as_ref().unwrap().type_url.as_str(),
                _ => unreachable!(),
            }
        );

        let decoded =
            decode_group_read_request(&bytes, &session_state(), payload, &BoundRegionResolver)
                .await
                .unwrap();
        assert_eq!(1, count_table_scans(&decoded));
    }

    #[tokio::test]
    async fn outer_projection_filter_aggregate_and_sort_are_rejected() {
        let state = session_state();
        let envelope = build_group_read_plan(&regions(2), 1024, &project_scan()).unwrap();
        let payload = peek_group_read_envelope(&encode_plan(&envelope))
            .unwrap()
            .unwrap();
        let wrapped = vec![
            (
                "Projection",
                LogicalPlanBuilder::from(envelope.clone())
                    .project(vec![col("ts")])
                    .unwrap()
                    .build()
                    .unwrap(),
            ),
            (
                "Filter",
                LogicalPlanBuilder::from(envelope.clone())
                    .filter(col("v").gt(lit(1.0)))
                    .unwrap()
                    .build()
                    .unwrap(),
            ),
            (
                "Aggregate",
                LogicalPlanBuilder::from(envelope.clone())
                    .aggregate(Vec::<Expr>::new(), vec![sum(col("v"))])
                    .unwrap()
                    .build()
                    .unwrap(),
            ),
            (
                "Sort",
                LogicalPlanBuilder::from(envelope)
                    .sort(vec![col("ts").sort(true, false)])
                    .unwrap()
                    .build()
                    .unwrap(),
            ),
        ];
        for (kind, plan) in wrapped {
            let bytes = encode_plan(&plan);
            let error =
                decode_group_read_request(&bytes, &state, payload.clone(), &BoundRegionResolver)
                    .await
                    .unwrap_err();
            assert!(
                error.contains(&format!("found {kind}")),
                "{kind} must be rejected: {error}"
            );
        }
    }

    #[tokio::test]
    async fn combine_tree_must_be_union_all_of_region_plans() {
        let state = session_state();
        let union = region_union(&regions(2));
        let join = LogicalPlanBuilder::from(region_union(&regions(2)))
            .cross_join(
                LogicalPlanBuilder::from(project_scan())
                    .alias("right")
                    .unwrap()
                    .build()
                    .unwrap(),
            )
            .unwrap()
            .build()
            .unwrap();
        let nested_group_read = build_group_read_plan(&regions(1), 1024, &project_scan()).unwrap();
        let other_extension =
            MergeScanLogicalPlan::new(region_union(&regions(2)), true).into_logical_plan();
        let cases = vec![
            (
                "Projection",
                LogicalPlanBuilder::from(union.clone())
                    .project(vec![col("ts")])
                    .unwrap()
                    .build()
                    .unwrap(),
            ),
            (
                "Filter",
                LogicalPlanBuilder::from(union.clone())
                    .filter(col("v").gt(lit(1.0)))
                    .unwrap()
                    .build()
                    .unwrap(),
            ),
            (
                "Aggregate",
                LogicalPlanBuilder::from(union.clone())
                    .aggregate(Vec::<Expr>::new(), vec![sum(col("v"))])
                    .unwrap()
                    .build()
                    .unwrap(),
            ),
            (
                "Sort",
                LogicalPlanBuilder::from(union)
                    .sort(vec![col("ts").sort(true, false)])
                    .unwrap()
                    .build()
                    .unwrap(),
            ),
            ("Join", join),
            ("a plain read", project_scan()),
            ("an extension", nested_group_read),
            ("an extension", other_extension),
        ];
        for (kind, input) in cases {
            let (bytes, payload) = group_read_request(&regions(2), 1024, input);
            let error = decode_group_read_request(&bytes, &state, payload, &BoundRegionResolver)
                .await
                .unwrap_err();
            assert!(
                error.contains(&format!("found {kind}")),
                "a {kind} combine tree must be rejected: {error}"
            );
        }
    }

    #[tokio::test]
    async fn duplicate_missing_and_foreign_regions_are_rejected() {
        let state = session_state();

        // The same region twice is rejected before any decoder runs.
        let duplicate = regions(1)[0];
        let input = LogicalPlanBuilder::from(region_leaf(duplicate))
            .union(region_leaf(duplicate))
            .unwrap()
            .build()
            .unwrap();
        let (bytes, payload) = group_read_request(&[duplicate], 1024, input);
        let error = decode_group_read_request(&bytes, &state, payload, &PanicResolver)
            .await
            .unwrap_err();
        assert!(error.contains("appears more than once"), "{error}");

        // A payload member without a leaf is rejected after all leaves are consumed.
        let listed = regions(2);
        let (bytes, mut payload) = group_read_request(&listed, 1024, region_union(&listed));
        payload.region_ids.push(RegionId::new(1024, 9).as_u64());
        let bytes = rewrite_root_payload(bytes, &payload);
        let error = decode_group_read_request(&bytes, &state, payload, &PanicResolver)
            .await
            .unwrap_err();
        assert!(
            error.contains("does not name the grouped regions exactly once"),
            "{error}"
        );

        // A leaf the payload does not name is rejected before any decoder runs.
        let own = RegionId::new(1024, 1);
        let (bytes, payload) =
            group_read_request(&[own], 1024, region_leaf(RegionId::new(1024, 2)));
        let error = decode_group_read_request(&bytes, &state, payload, &PanicResolver)
            .await
            .unwrap_err();
        assert!(
            error.contains("is not one of the grouped regions"),
            "{error}"
        );

        // A region of another table is rejected before any decoder runs.
        let foreign = RegionId::new(2048, 1);
        let (bytes, payload) = group_read_request(&[foreign], 1024, region_leaf(foreign));
        let error = decode_group_read_request(&bytes, &state, payload, &PanicResolver)
            .await
            .unwrap_err();
        assert!(error.contains("belongs to table 2048"), "{error}");
    }

    #[tokio::test]
    async fn decoded_region_must_be_the_bound_region_provider() {
        let state = session_state();
        let listed = regions(2);
        let (bytes, payload) = group_read_request(&listed, 1024, region_union(&listed));

        let error =
            decode_group_read_request(&bytes, &state, payload.clone(), &ForeignRegionResolver)
                .await
                .unwrap_err();
        assert!(
            error.contains("decoded the provider of region 1024_77"),
            "{error}"
        );

        let error =
            decode_group_read_request(&bytes, &state, payload.clone(), &InspectProviderResolver)
                .await
                .unwrap_err();
        assert!(error.contains("not the bound region provider"), "{error}");

        let error = decode_group_read_request(&bytes, &state, payload, &MultiInputResolver)
            .await
            .unwrap_err();
        assert!(
            error.contains("not a single ordinary table read"),
            "{error}"
        );
    }

    #[test]
    fn region_plan_raw_tree_is_bounded_before_decode() {
        let ok = encode_nested_plan(&project_scan()).unwrap();
        preflight_region_plan(&ok).unwrap();

        let too_many = encode_nested_plan(&filtered_scan(MAX_REGION_PLAN_NODES)).unwrap();
        let error = preflight_region_plan(&too_many).unwrap_err();
        assert!(error.contains("node bound"), "{error}");

        let too_deep = encode_nested_plan(&filtered_scan(MAX_REGION_PLAN_DEPTH + 1)).unwrap();
        let error = preflight_region_plan(&too_deep).unwrap_err();
        assert!(error.contains("node bound"), "{error}");
    }

    #[test]
    fn region_plan_raw_tree_allows_only_read_project_filter() {
        // An aggregate nested plan is rejected by the preflight without decoding it.
        let aggregate = LogicalPlanBuilder::from(project_scan())
            .aggregate(Vec::<Expr>::new(), vec![sum(col("v"))])
            .unwrap()
            .build()
            .unwrap();
        let error = preflight_region_plan(&encode_nested_plan(&aggregate).unwrap()).unwrap_err();
        assert!(error.contains("may only contain"), "{error}");

        let join = LogicalPlanBuilder::from(project_scan())
            .cross_join(
                LogicalPlanBuilder::from(project_scan())
                    .alias("right")
                    .unwrap()
                    .build()
                    .unwrap(),
            )
            .unwrap()
            .build()
            .unwrap();
        let error = preflight_region_plan(&encode_nested_plan(&join).unwrap()).unwrap_err();
        assert!(error.contains("may only contain"), "{error}");
    }

    #[test]
    fn subquery_expressions_are_forbidden() {
        use datafusion_expr::Subquery;

        let plan = LogicalPlanBuilder::from(project_scan())
            .project(vec![
                Expr::ScalarSubquery(Subquery {
                    subquery: Arc::new(project_scan()),
                    outer_ref_columns: vec![],
                    spans: Default::default(),
                })
                .alias("subq"),
            ])
            .unwrap()
            .build()
            .unwrap();
        assert!(plan_has_forbidden_nodes(&plan));
    }

    #[test]
    fn region_plan_payload_carries_region_and_complete_plan() {
        let plan = project_scan();
        let bytes = encode_nested_plan(&plan).unwrap();
        let payload = RegionPlanPayload::try_new(RegionId::new(1024, 7), bytes.clone()).unwrap();
        assert_eq!(RegionId::new(1024, 7), payload.region_id());
        assert_eq!(bytes, payload.plan());
        assert_eq!(
            payload,
            RegionPlanPayload::decode(&payload.encode().unwrap()).unwrap()
        );
    }

    #[test]
    fn ordinary_plan_peek_does_not_decode() {
        let encoded = encode_plan(&project_scan());
        assert!(!contains_marker(&encoded, GROUP_READ_NAME.as_bytes()));
        assert!(peek_group_read_envelope(&encoded).unwrap().is_none());
        assert!(
            peek_group_read_envelope(b"not a substrait plan")
                .unwrap()
                .is_none()
        );

        // A GroupRead marker used as a root leaf is ours but malformed.
        let envelope = build_group_read_plan(&regions(1), 1024, &project_scan()).unwrap();
        let mut leaf_root = Plan::decode(encode_plan(&envelope).as_slice()).unwrap();
        let relation = leaf_root.relations.first_mut().unwrap();
        let root = match relation.rel_type.as_mut().unwrap() {
            plan_rel::RelType::Rel(root) => root,
            plan_rel::RelType::Root(root) => root.input.as_mut().unwrap(),
        };
        let Some(rel::RelType::ExtensionSingle(single)) = root.rel_type.take() else {
            panic!("the fixture must have a GroupRead root");
        };
        root.rel_type = Some(rel::RelType::ExtensionLeaf(
            substrait::substrait_proto_df::proto::ExtensionLeafRel {
                common: None,
                detail: single.detail,
            },
        ));
        let error = peek_group_read_envelope(&leaf_root.encode_to_vec()).unwrap_err();
        assert!(matches!(error, GroupReadEnvelopeError::Malformed(_)));
    }

    #[test]
    fn payload_validation_rejects_duplicate_empty_and_wrong_table() {
        assert!(GroupReadPayload::try_new(vec![], 1024, "metrics".to_string()).is_err());
        assert!(
            GroupReadPayload::try_new(
                vec![
                    RegionId::new(1024, 1).as_u64(),
                    RegionId::new(1024, 1).as_u64()
                ],
                1024,
                "metrics".to_string(),
            )
            .is_err()
        );
        let payload = GroupReadPayload::try_new(
            vec![RegionId::new(1024, 1).as_u64()],
            1024,
            "metrics".to_string(),
        )
        .unwrap();
        let region = RegionId::new(1024, 1);
        assert!(
            validate_region_plan(
                &payload,
                RegionId::new(2048, 1),
                &region_scan(RegionId::new(1024, 1))
            )
            .is_err()
        );
        assert!(validate_region_plan(&payload, region, &region_scan(region)).is_ok());
        assert!(validate_region_plan(&payload, region, &project_scan()).is_err());
    }

    #[test]
    fn builder_rejects_invalid_membership_before_materializing_leaves() {
        // More than the hard 1024-region cap is rejected before leaves are built.
        let error = build_group_read_plan(&regions(1025), 1024, &project_scan()).unwrap_err();
        assert!(error.to_string().contains("above the 1024 bound"));

        // Duplicate members are rejected.
        let duplicate = vec![RegionId::new(1024, 1), RegionId::new(1024, 1)];
        let error = build_group_read_plan(&duplicate, 1024, &project_scan()).unwrap_err();
        assert!(error.to_string().contains("named more than once"));

        // A region of another table is rejected.
        let foreign = vec![RegionId::new(2048, 1)];
        let error = build_group_read_plan(&foreign, 1024, &project_scan()).unwrap_err();
        assert!(error.to_string().contains("outside table 1024"));

        // A multi-input template is not a grouped-read member.
        let join = LogicalPlanBuilder::from(project_scan())
            .cross_join(
                LogicalPlanBuilder::from(project_scan())
                    .alias("right")
                    .unwrap()
                    .build()
                    .unwrap(),
            )
            .unwrap()
            .build()
            .unwrap();
        let error = build_group_read_plan(&regions(2), 1024, &join).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("not a single ordinary table scan")
        );
    }

    #[test]
    fn builder_rejects_cumulative_budget() {
        let bulky = LogicalPlanBuilder::from(project_scan())
            .project(vec![
                col("ts"),
                col("v"),
                lit("x".repeat(30 * 1024)).alias("padding"),
            ])
            .unwrap()
            .build()
            .unwrap();
        let error = build_group_read_plan(&regions(100), 1024, &bulky).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("estimated grouped read envelope")
        );

        // One such leaf is still valid; the rejection is cumulative.
        build_group_read_plan(&regions(1), 1024, &bulky).unwrap();
    }

    #[test]
    fn subquery_scans_are_not_eligible() {
        use datafusion_expr::Subquery;

        let plan = LogicalPlanBuilder::from(project_scan())
            .filter(Expr::ScalarSubquery(Subquery {
                subquery: Arc::new(project_scan()),
                outer_ref_columns: vec![],
                spans: Default::default(),
            }))
            .unwrap()
            .build()
            .unwrap();
        assert!(plan_has_forbidden_nodes(&plan));
        assert!(!eligible_remote_plan(&plan));
    }

    #[test]
    fn ineligible_shapes_are_not_grouped() {
        let sorted = metrics_scan()
            .sort(vec![col("ts").sort(true, false)])
            .unwrap()
            .build()
            .unwrap();
        let limited = metrics_scan().limit(0, Some(1)).unwrap().build().unwrap();
        for (name, plan) in [("sort", sorted), ("limit", limited)] {
            assert!(!eligible_remote_plan(&plan), "{name}");
        }
        assert!(eligible_remote_plan(&project_scan()));
    }
}
