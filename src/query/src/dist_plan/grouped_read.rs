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
//! A grouped read is one `GroupRead` root marker around a combine tree. The default combine tree
//! is `UnionAll(RegionPlan(...), ...)`, where each `RegionPlan` is a wire extension leaf carrying
//! one region id and the complete per-region plan bytes. A datanode intercepts the root, decodes
//! every `RegionPlan` against that region's catalog, replaces the leaf with the bound logical
//! subtree and executes the resulting ordinary multi-input plan through the query engine. Nothing
//! is merged here by the wire layer: a combine tree that contains an aggregate is planned and run
//! exactly like the same logical plan in a local query.
//!
//! `GroupRead` is the root relation of the serialized request and is an
//! [`ExtensionSingleRel`](substrait::substrait_proto_df::proto::ExtensionSingleRel). Its child is
//! the combine tree. `RegionPlan` is an [`ExtensionLeafRel`](substrait::substrait_proto_df::proto::ExtensionLeafRel).
//! Ordinary plans are left untouched: a datanode peeks only at the root before constructing a
//! region-scoped decoder.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::DefaultTableSource;
use datafusion::execution::FunctionRegistry;
use datafusion::execution::context::SessionState;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchema, DFSchemaRef, DataFusionError, TableReference};
use datafusion_expr::{
    Expr, Extension, LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNodeCore,
};
use datatypes::schema::{Schema, SchemaRef};
use prost::Message;
use serde::{Deserialize, Serialize};
use store_api::storage::RegionId;
use substrait::df_logical_plan::consumer::{SubstraitConsumer, from_substrait_rel};
use substrait::substrait_proto_df::proto::{
    ExtensionLeafRel, ExtensionSingleRel, ReadRel, Rel, rel, set_rel::SetOp,
};
use substrait::{DFLogicalSubstraitConvertor, Extensions, SubstraitPlan};
use table::metadata::TableType;
use table::table::adapter::DfTableProviderAdapter;
use table::table_name::TableName;

use crate::query_engine::DefaultSerializer;
use crate::region_query::RegionQueryTarget;

/// The version of the grouped-read model this build produces and accepts.
pub const GROUP_READ_VERSION: u32 = 2;

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
}

/// The payload of one `RegionPlan` wire leaf.
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

/// Peeks at the root relation of a serialized region request.
///
/// Only a unique `GroupRead` `ExtensionSingleRel` root is ours. An old append-only root leaf is
/// rejected explicitly so an old producer cannot be mistaken for an ordinary plan.
pub fn peek_group_read_envelope(
    plan: &[u8],
) -> std::result::Result<Option<GroupReadPayload>, GroupReadEnvelopeError> {
    use substrait::substrait_proto_df::proto::{Plan, plan_rel, rel};

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

/// Builds the default `UnionAll(RegionPlan...)` combine tree.
pub fn build_region_plan_union(
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
    if regions.iter().any(|region| region.table_id() != table_id) {
        return Err(DataFusionError::Plan(format!(
            "the RegionPlan list names a region outside table {table_id}"
        )));
    }
    let nested = encode_nested_plan(nested_plan)?;
    let mut plans = Vec::with_capacity(regions.len());
    for region in regions {
        let leaf = RegionPlan::try_new(*region, nested.clone(), nested_plan.schema().clone())
            .map_err(DataFusionError::Plan)?;
        plans.push(leaf.into_logical_plan());
    }
    if plans.is_empty() {
        return Err(DataFusionError::Plan(
            "a grouped read needs at least one region".to_string(),
        ));
    }
    union_balanced(plans, &table.table_name)
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

/// Builds `GroupRead(combine(UnionAll(RegionPlan...)))` with a caller-supplied combine tree.
pub fn build_group_read_plan_with_combine(
    regions: &[RegionId],
    table_id: u32,
    nested_plan: &LogicalPlan,
    combine: impl FnOnce(LogicalPlan) -> DataFusionResult<LogicalPlan>,
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
        combine,
    )
}

/// Builds a GroupRead from one already encoded regional template.
pub fn build_group_read_plan_from_nested(
    regions: &[RegionId],
    table_id: u32,
    table_name: String,
    nested: &[u8],
    schema: DFSchemaRef,
    combine: impl FnOnce(LogicalPlan) -> DataFusionResult<LogicalPlan>,
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
    let combine = combine(union)?;
    let payload = GroupReadPayload::from_validated_parts(
        regions.iter().map(RegionId::as_u64).collect(),
        table_id,
        table_name,
    );
    let node = GroupReadPlan::try_new(payload, combine).map_err(DataFusionError::Plan)?;
    Ok(node.into_logical_plan())
}

/// Builds the default append-equivalent grouped read.
pub fn build_group_read_plan(
    regions: &[RegionId],
    table_id: u32,
    nested_plan: &LogicalPlan,
) -> DataFusionResult<LogicalPlan> {
    build_group_read_plan_with_combine(regions, table_id, nested_plan, Ok)
}

/// Whether `plan` is a shape a grouped read may carry: one ordinary table scan under projections,
/// filters and aliases only.
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
pub fn validate_region_plan(
    payload: &GroupReadPayload,
    region_id: RegionId,
    plan: &LogicalPlan,
) -> std::result::Result<(), String> {
    payload.validate_for_request(region_id)?;
    if plan_has_forbidden_nodes(plan) {
        return Err(format!(
            "region {region_id} decoded a plan with a subquery or DML node"
        ));
    }
    let Some(table_name) = nested_plan_table(plan) else {
        return Err(format!(
            "region {region_id} decoded a plan that is not a single ordinary table read: `{}`",
            plan.display_indent()
        ));
    };
    if table_name.table_name != payload.table_name {
        return Err(format!(
            "region {region_id} reads table `{}`, but the grouped read names `{}`",
            table_name.table_name, payload.table_name
        ));
    }
    Ok(())
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
/// The request-scoped consumer has the session state and the region resolver, so it can replace
/// every wire `RegionPlan` leaf with its bound logical subtree. Plain table reads and any other
/// extension leaf are rejected; the final region set must match the root metadata exactly.
pub async fn decode_group_read_request(
    plan_bytes: &[u8],
    state: &SessionState,
    payload: GroupReadPayload,
    resolver: &dyn RegionPlanResolver,
) -> std::result::Result<LogicalPlan, String> {
    use substrait::substrait_proto_df::proto::Plan;

    let plan = Plan::decode(plan_bytes)
        .map_err(|error| format!("cannot parse GroupRead request: {error}"))?;
    let extensions = Extensions::try_from(&plan.extensions)
        .map_err(|error| format!("cannot read GroupRead extensions: {error}"))?;
    let consumer = GroupReadRequestConsumer {
        resolver,
        state,
        payload,
        extensions,
        outer_schemas: Mutex::new(Vec::new()),
        consumed: Mutex::new(BTreeSet::new()),
        expected_schema: Mutex::new(None),
        root_seen: AtomicBool::new(false),
    };
    let decoded = DFLogicalSubstraitConvertor
        .decode_with_consumer(&plan, state, &consumer)
        .await
        .map_err(|error| format!("cannot decode GroupRead combine tree: {error}"))?;
    consumer.validate_complete()?;
    if plan_has_forbidden_nodes(&decoded) {
        return Err("the GroupRead combine tree contains a subquery or DML node".to_string());
    }
    Ok(decoded)
}

struct GroupReadRequestConsumer<'a> {
    resolver: &'a dyn RegionPlanResolver,
    state: &'a SessionState,
    payload: GroupReadPayload,
    extensions: Extensions,
    outer_schemas: Mutex<Vec<Arc<DFSchema>>>,
    consumed: Mutex<BTreeSet<u64>>,
    expected_schema: Mutex<Option<SchemaRef>>,
    root_seen: AtomicBool,
}

impl GroupReadRequestConsumer<'_> {
    fn validate_complete(&self) -> std::result::Result<(), String> {
        let consumed = self.consumed.lock().unwrap();
        let expected = self
            .payload
            .region_ids()
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if *consumed != expected {
            return Err(format!(
                "the combine tree does not name the grouped regions exactly once: consumed {:?}, \
                 expected {:?}",
                *consumed, expected
            ));
        }
        if self.expected_schema.lock().unwrap().is_none() {
            return Err("the combine tree decoded no RegionPlan".to_string());
        }
        Ok(())
    }
}

fn group_read_error(reason: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Plan(reason.to_string())
}

#[async_trait]
impl SubstraitConsumer for GroupReadRequestConsumer<'_> {
    async fn resolve_table_ref(
        &self,
        table_ref: &TableReference,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        let table = table_ref.table().to_string();
        let schema = self.state.schema_for_ref(table_ref.clone())?;
        let table_provider = schema.table(&table).await?;
        Ok(table_provider)
    }

    fn get_extensions(&self) -> &Extensions {
        &self.extensions
    }

    fn get_function_registry(&self) -> &impl FunctionRegistry {
        self.state
    }

    fn push_outer_schema(&self, schema: Arc<DFSchema>) {
        self.outer_schemas.lock().unwrap().push(schema);
    }

    fn pop_outer_schema(&self) {
        self.outer_schemas.lock().unwrap().pop();
    }

    fn get_outer_schema(&self, steps_out: usize) -> Option<Arc<DFSchema>> {
        let schemas = self.outer_schemas.lock().unwrap();
        schemas
            .len()
            .checked_sub(steps_out)
            .and_then(|idx| schemas.get(idx).cloned())
    }

    async fn consume_rel(&self, rel: &Rel) -> datafusion::common::Result<LogicalPlan> {
        let allowed = match rel.rel_type.as_ref() {
            Some(rel::RelType::Set(set)) => SetOp::try_from(set.op).ok() == Some(SetOp::UnionAll),
            Some(rel::RelType::Project(_))
            | Some(rel::RelType::Filter(_))
            | Some(rel::RelType::Aggregate(_))
            | Some(rel::RelType::ExtensionLeaf(_))
            | Some(rel::RelType::ExtensionSingle(_)) => true,
            _ => false,
        };
        if !allowed {
            return Err(group_read_error(
                "only UnionAll, Projection, Filter, Aggregate and RegionPlan are allowed in a \
                 GroupRead combine tree",
            ));
        }
        from_substrait_rel(self, rel).await
    }

    async fn consume_read(&self, _rel: &ReadRel) -> datafusion::common::Result<LogicalPlan> {
        Err(group_read_error(
            "plain table read outside a RegionPlan is not allowed in a GroupRead combine tree",
        ))
    }

    async fn consume_extension_leaf(
        &self,
        rel: &ExtensionLeafRel,
    ) -> datafusion::common::Result<LogicalPlan> {
        let detail = rel
            .detail
            .as_ref()
            .ok_or_else(|| group_read_error("a RegionPlan leaf has no detail"))?;
        if detail.type_url != REGION_PLAN_NAME {
            return Err(group_read_error(format!(
                "unexpected extension leaf `{}` in a GroupRead combine tree",
                detail.type_url
            )));
        }
        let payload = RegionPlanPayload::decode(detail.value.as_ref()).map_err(group_read_error)?;
        let region_id = payload.region_id();
        if !self.consumed.lock().unwrap().insert(region_id.as_u64()) {
            return Err(group_read_error(format!(
                "region {region_id} appears more than once in the combine tree"
            )));
        }
        let plan = self
            .resolver
            .resolve_region_plan(region_id, payload.plan())
            .await
            .map_err(group_read_error)?;
        validate_region_plan(&self.payload, region_id, &plan).map_err(group_read_error)?;
        let schema = SchemaRef::from(
            Schema::try_from(plan.schema().clone())
                .map_err(|error| group_read_error(format!("invalid region schema: {error}")))?,
        );
        {
            let mut expected = self.expected_schema.lock().unwrap();
            match expected.as_ref() {
                Some(expected) if expected != &schema => {
                    return Err(group_read_error(format!(
                        "region {region_id} decodes to a different output schema"
                    )));
                }
                Some(_) => {}
                None => *expected = Some(schema),
            }
        }
        Ok(plan)
    }

    async fn consume_extension_single(
        &self,
        rel: &ExtensionSingleRel,
    ) -> datafusion::common::Result<LogicalPlan> {
        if self.root_seen.swap(true, AtomicOrdering::SeqCst) {
            return Err(group_read_error("nested GroupRead is not allowed"));
        }
        let detail = rel
            .detail
            .as_ref()
            .ok_or_else(|| group_read_error("the GroupRead root has no detail"))?;
        if detail.type_url != GROUP_READ_NAME {
            return Err(group_read_error(format!(
                "expected the `{GROUP_READ_NAME}` root extension, found `{}`",
                detail.type_url
            )));
        }
        let root = GroupReadPayload::decode(detail.value.as_ref()).map_err(group_read_error)?;
        if root != self.payload {
            return Err(group_read_error(
                "the root marker and the intercepted metadata differ",
            ));
        }
        let input = rel
            .input
            .as_ref()
            .ok_or_else(|| group_read_error("the GroupRead root has no combine tree"))?;
        self.consume_rel(input).await
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

    fn regions(count: u32) -> Vec<RegionId> {
        (1..=count).map(|n| RegionId::new(1024, n)).collect()
    }

    #[test]
    fn group_read_round_trips_as_single_root_with_region_plan_leaves() {
        use substrait::substrait_proto_df::proto::{Plan, plan_rel, rel};

        let envelope = build_group_read_plan(&regions(3), 1024, &project_scan()).unwrap();
        let encoded = DFLogicalSubstraitConvertor
            .encode(&envelope, DefaultSerializer)
            .unwrap()
            .to_vec();
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
    }

    struct TestRegionPlanResolver;

    #[async_trait]
    impl RegionPlanResolver for TestRegionPlanResolver {
        async fn resolve_region_plan(
            &self,
            _region_id: RegionId,
            _plan: &[u8],
        ) -> std::result::Result<LogicalPlan, String> {
            Ok(project_scan())
        }
    }

    #[tokio::test]
    async fn group_read_consumer_replaces_every_region_plan_and_checks_completeness() {
        use datafusion::execution::SessionStateBuilder;

        let plan = project_scan();
        let envelope = build_group_read_plan(&regions(2), 1024, &plan).unwrap();
        let encoded = DFLogicalSubstraitConvertor
            .encode(&envelope, DefaultSerializer)
            .unwrap()
            .to_vec();
        let payload = peek_group_read_envelope(&encoded).unwrap().unwrap();
        let state = SessionStateBuilder::new().build();
        let decoded = decode_group_read_request(&encoded, &state, payload, &TestRegionPlanResolver)
            .await
            .unwrap();
        assert_eq!(plan.schema().as_arrow(), decoded.schema().as_arrow());

        let mut payload = peek_group_read_envelope(&encoded).unwrap().unwrap();
        payload.region_ids.push(RegionId::new(1024, 9).as_u64());
        let payload_bytes = payload.encode().unwrap();
        let mut raw =
            substrait::substrait_proto_df::proto::Plan::decode(encoded.as_slice()).unwrap();
        let relation = raw.relations.first_mut().unwrap();
        let root = match relation.rel_type.as_mut().unwrap() {
            substrait::substrait_proto_df::proto::plan_rel::RelType::Rel(root) => root,
            substrait::substrait_proto_df::proto::plan_rel::RelType::Root(root) => {
                root.input.as_mut().unwrap()
            }
        };
        let substrait::substrait_proto_df::proto::rel::RelType::ExtensionSingle(single) =
            root.rel_type.as_mut().unwrap()
        else {
            panic!("the fixture must have a GroupRead root");
        };
        single.detail.as_mut().unwrap().value = payload_bytes.into();
        let encoded = raw.encode_to_vec();
        let error = decode_group_read_request(&encoded, &state, payload, &TestRegionPlanResolver)
            .await
            .unwrap_err();
        assert!(error.contains("does not name the grouped regions exactly once"));
    }

    #[tokio::test]
    async fn group_read_consumer_rejects_join_sort_and_nested_group_read() {
        use datafusion::execution::SessionStateBuilder;

        let plan = project_scan();
        let state = SessionStateBuilder::new().build();

        let join = build_group_read_plan_with_combine(&regions(2), 1024, &plan, |_union| {
            let right = LogicalPlanBuilder::from(project_scan())
                .alias("right")?
                .build()?;
            LogicalPlanBuilder::from(project_scan())
                .cross_join(right)?
                .build()
        })
        .unwrap();
        let error = decode_group_read_request(
            &DFLogicalSubstraitConvertor
                .encode(&join, DefaultSerializer)
                .unwrap(),
            &state,
            peek_group_read_envelope(
                &DFLogicalSubstraitConvertor
                    .encode(&join, DefaultSerializer)
                    .unwrap(),
            )
            .unwrap()
            .unwrap(),
            &TestRegionPlanResolver,
        )
        .await
        .unwrap_err();
        assert!(error.contains("GroupRead combine tree"), "{error}");

        let sorted = build_group_read_plan_with_combine(&regions(2), 1024, &plan, |union| {
            LogicalPlanBuilder::from(union)
                .sort(vec![col("ts").sort(true, false)])?
                .build()
        })
        .unwrap();
        let sorted_bytes = DFLogicalSubstraitConvertor
            .encode(&sorted, DefaultSerializer)
            .unwrap();
        let error = decode_group_read_request(
            &sorted_bytes,
            &state,
            peek_group_read_envelope(&sorted_bytes).unwrap().unwrap(),
            &TestRegionPlanResolver,
        )
        .await
        .unwrap_err();
        assert!(error.contains("GroupRead combine tree"), "{error}");

        let inner = build_group_read_plan(&regions(1), 1024, &plan).unwrap();
        let nested =
            build_group_read_plan_with_combine(&regions(1), 1024, &plan, |_| Ok(inner)).unwrap();
        let nested_bytes = DFLogicalSubstraitConvertor
            .encode(&nested, DefaultSerializer)
            .unwrap();
        let error = decode_group_read_request(
            &nested_bytes,
            &state,
            peek_group_read_envelope(&nested_bytes).unwrap().unwrap(),
            &TestRegionPlanResolver,
        )
        .await
        .unwrap_err();
        assert!(error.contains("GroupRead combine tree"), "{error}");
    }

    #[test]
    fn custom_combine_rejects_invalid_membership_before_materializing_leaves() {
        use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

        let called = AtomicBool::new(false);
        // More than the hard 1024-region cap is rejected before leaves/combine.
        let error =
            build_group_read_plan_with_combine(&regions(1025), 1024, &project_scan(), |_| {
                called.store(true, AtomicOrdering::SeqCst);
                Ok(project_scan())
            })
            .unwrap_err();
        assert!(error.to_string().contains("above the 1024 bound"));
        assert!(!called.load(AtomicOrdering::SeqCst));

        // Duplicate members are rejected before leaves/combine.
        let duplicate = vec![RegionId::new(1024, 1), RegionId::new(1024, 1)];
        let error = build_group_read_plan_with_combine(&duplicate, 1024, &project_scan(), |_| {
            called.store(true, AtomicOrdering::SeqCst);
            Ok(project_scan())
        })
        .unwrap_err();
        assert!(error.to_string().contains("named more than once"));
        assert!(!called.load(AtomicOrdering::SeqCst));
    }

    #[test]
    fn custom_combine_rejects_cumulative_budget_before_materializing_leaves() {
        use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

        let bulky = LogicalPlanBuilder::from(project_scan())
            .project(vec![
                col("ts"),
                col("v"),
                lit("x".repeat(30 * 1024)).alias("padding"),
            ])
            .unwrap()
            .build()
            .unwrap();
        let called = AtomicBool::new(false);
        let error = build_group_read_plan_with_combine(&regions(100), 1024, &bulky, |_| {
            called.store(true, AtomicOrdering::SeqCst);
            Ok(project_scan())
        })
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("estimated grouped read envelope")
        );
        assert!(
            !called.load(AtomicOrdering::SeqCst),
            "custom combine must not run after the pre-materialization budget check"
        );

        // One such leaf is still valid; the rejection is cumulative.
        build_group_read_plan(&regions(1), 1024, &bulky).unwrap();
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
    fn ordinary_plan_and_old_root_leaf_are_not_the_new_envelope() {
        let encoded = DFLogicalSubstraitConvertor
            .encode(&project_scan(), DefaultSerializer)
            .unwrap()
            .to_vec();
        assert!(peek_group_read_envelope(&encoded).unwrap().is_none());
        assert!(
            peek_group_read_envelope(b"not a substrait plan")
                .unwrap()
                .is_none()
        );
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
        assert!(validate_region_plan(&payload, RegionId::new(2048, 1), &project_scan()).is_err());
        assert!(validate_region_plan(&payload, RegionId::new(1024, 1), &project_scan()).is_ok());
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
