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

//! Fail-closed codec used only for region remote reads.
//!
//! The ordinary Substrait serializer intentionally remains reusable by views,
//! flows, and internal RPCs.  Remote reads instead carry this root envelope so
//! their target table identity and semantic metadata can be checked before the
//! decoded plan is executed against the region's current provider.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use api::v1::SemanticType;
use bytes::Bytes;
use common_error::ext::{BoxedError, PlainError, RetryHint};
use common_error::status_code::StatusCode;
use common_query::logical_plan::SubstraitPlanDecoder;
use datafusion::catalog::{CatalogProviderList, MemTable};
use datafusion::datasource::DefaultTableSource;
use datafusion_common::DataFusionError;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_expr::{Expr, LogicalPlan};
use datatypes::arrow::datatypes::{DataType as ArrowDataType, Field};
use datatypes::data_type::DataType;
use greptime_proto::substrait_extension::{
    RemoteReadColumnV1, RemoteReadInternalV1, RemoteReadSemanticTypeV1, RemoteReadTableV1,
};
use pbjson_types::Any;
use prost::Message;
use store_api::metadata::RegionMetadata;
use store_api::region_info::RegionInfoEntry;
use store_api::sst_entry::{ManifestSstEntry, PuffinIndexMetaEntry, StorageSstEntry};
use store_api::storage::RegionId;
use substrait::substrait_proto_df::proto;
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use table::metadata::TableInfo;
use table::table::adapter::DfTableProviderAdapter;

use crate::dummy_catalog::DummyTableProvider;
use crate::plan::ExtractExpr;
use crate::query_engine::DefaultSerializer;

/// Mandatory contract for production region reads.
pub const REMOTE_READ_TABLE_V1_TYPE_URL: &str =
    "type.googleapis.com/substrait_extension.RemoteReadTableV1";
/// Envelope for an inspection-only remote plan. It has no payload and must not
/// resolve a [`DummyTableProvider`].
pub const REMOTE_READ_INTERNAL_V1_TYPE_URL: &str =
    "type.googleapis.com/substrait_extension.RemoteReadInternalV1";

/// Returns whether `name` is one of the inspection-only table names supported
/// by a region remote-read catalog.
///
/// This is deliberately the single source of truth shared with datanode's
/// name-aware catalog: any other source at this boundary is rejected.
pub fn is_reserved_internal_table_name(name: &str) -> bool {
    name.eq_ignore_ascii_case(ManifestSstEntry::reserved_table_name_for_inspection())
        || name.eq_ignore_ascii_case(StorageSstEntry::reserved_table_name_for_inspection())
        || name.eq_ignore_ascii_case(PuffinIndexMetaEntry::reserved_table_name_for_inspection())
        || name.eq_ignore_ascii_case(RegionInfoEntry::reserved_table_name_for_inspection())
}

#[derive(Debug)]
enum Envelope {
    Target(RemoteReadTableV1),
    Internal,
}

#[derive(Debug)]
struct ReadSchema {
    table_name: String,
    table_reference: Vec<String>,
    names: Vec<String>,
    types: Vec<proto::Type>,
}

/// Encodes a plan for `RegionRequester::handle_query`.
///
/// A base Greptime table must be represented by exactly one table identity and
/// that identity must agree with the destination region. Plans containing only
/// recognized synthetic sources use the explicit internal envelope instead.
pub fn encode_remote_plan(
    plan: &LogicalPlan,
    region_id: RegionId,
) -> std::result::Result<Bytes, BoxedError> {
    reject_logical_subqueries(plan)?;
    let target = extract_target_table(plan)?;
    let mut encoded = DFLogicalSubstraitConvertor
        .encode(plan, DefaultSerializer)
        .map_err(|error| remote_error(error.to_string()))?;
    let mut substrait_plan =
        proto::Plan::decode(encoded.as_ref()).map_err(|error| remote_error(error.to_string()))?;

    let envelope = match target {
        Some(table) => {
            if table.table_id() != region_id.table_id() {
                return Err(remote_error(format!(
                    "planned table {} does not match remote region table {}",
                    table.table_id(),
                    region_id.table_id()
                )));
            }
            Any {
                type_url: REMOTE_READ_TABLE_V1_TYPE_URL.to_string(),
                value: table_contract(&table)?.encode_to_vec().into(),
            }
        }
        None => Any {
            type_url: REMOTE_READ_INTERNAL_V1_TYPE_URL.to_string(),
            value: RemoteReadInternalV1 {}.encode_to_vec().into(),
        },
    };

    wrap_root(&mut substrait_plan, envelope)?;
    encoded = Bytes::from(substrait_plan.encode_to_vec());
    Ok(encoded)
}

/// Decodes a region remote-read envelope and validates its current provider.
///
/// This is deliberately not used by persisted-view, flow, or generic logical
/// plan decode paths. The stock decoder still performs its normal logical-type
/// compatibility check after this function restores the standard root relation.
pub async fn decode_remote_plan(
    decoder: &(dyn SubstraitPlanDecoder + Send + Sync),
    message: Bytes,
    catalog_list: Arc<dyn CatalogProviderList>,
) -> common_query::error::Result<LogicalPlan> {
    let mut plan =
        proto::Plan::decode(message.as_ref()).map_err(|error| decode_error(error.to_string()))?;

    // Plans without the v1 root envelope were produced by an older frontend.
    // During rolling upgrades a mixed FE/DN cluster must keep working, so decode
    // them with the base semantics: the DataFusion fork's schema-compatibility
    // check still fails safely on column drop/rename/type-change.
    if is_legacy_unwrapped_root(&plan) {
        return decoder
            .decode(Bytes::from(plan.encode_to_vec()), catalog_list, false)
            .await;
    }

    let envelope = unwrap_root(&mut plan)?;
    let read_schemas = collect_read_schemas(root_input(&plan).map_err(decode_error)?)?;

    let plan = decoder
        // A remote plan is always decoded unoptimized. Validation must observe
        // every resolved scan before any optimizer can remove or rewrite it.
        .decode(Bytes::from(plan.encode_to_vec()), catalog_list, false)
        .await?;
    validate_decoded_sources(&plan, &envelope, &read_schemas)?;
    Ok(plan)
}

/// Returns whether `plan` is a legacy remote-read plan encoded without the v1
/// root envelope, i.e. its root relation is not an [`proto::ExtensionSingleRel`].
fn is_legacy_unwrapped_root(plan: &proto::Plan) -> bool {
    match root_input(plan) {
        Ok(root) => !matches!(root.rel_type, Some(proto::rel::RelType::ExtensionSingle(_))),
        Err(_) => false,
    }
}

fn extract_target_table(
    plan: &LogicalPlan,
) -> std::result::Result<Option<Arc<TableInfo>>, BoxedError> {
    let mut target: Option<Arc<TableInfo>> = None;
    let mut classification_error = None;
    plan.apply(|node| {
        let LogicalPlan::TableScan(scan) = node else {
            return Ok(TreeNodeRecursion::Continue);
        };
        if is_reserved_internal_table_name(scan.table_name.table()) {
            return Ok(TreeNodeRecursion::Continue);
        }
        let Some(source) = scan.source.as_any().downcast_ref::<DefaultTableSource>() else {
            classification_error = Some(format!(
                "remote read table {} has an unknown table source",
                scan.table_name
            ));
            return Ok(TreeNodeRecursion::Stop);
        };
        let Some(provider) = source
            .table_provider
            .as_any()
            .downcast_ref::<DfTableProviderAdapter>()
        else {
            classification_error = Some(format!(
                "remote read table {} has an unrecognized table provider",
                scan.table_name
            ));
            return Ok(TreeNodeRecursion::Stop);
        };
        let table = provider.table();
        if table.table_type() != table::metadata::TableType::Base {
            classification_error = Some(format!(
                "remote read table {} is not a base table provider",
                scan.table_name
            ));
            return Ok(TreeNodeRecursion::Stop);
        }
        let info = table.table_info();
        if let Some(existing) = &target
            && existing.table_id() != info.table_id()
        {
            return Err(DataFusionError::Plan(format!(
                "remote read has multiple protected table IDs: {} and {}",
                existing.table_id(),
                info.table_id()
            )));
        }
        target = Some(info);
        Ok(TreeNodeRecursion::Continue)
    })
    .map_err(|error| remote_error(error.to_string()))?;
    if let Some(error) = classification_error {
        return Err(remote_error(error));
    }
    Ok(target)
}

fn reject_logical_subqueries(plan: &LogicalPlan) -> std::result::Result<(), BoxedError> {
    let mut subquery = None;
    plan.apply(|node| {
        for expression in node.expressions_consider_join() {
            expression.apply(|expression| {
                if matches!(
                    expression,
                    Expr::ScalarSubquery(_)
                        | Expr::Exists(_)
                        | Expr::InSubquery(_)
                        | Expr::SetComparison(_)
                ) {
                    subquery = Some("logical subquery".to_string());
                    Ok(TreeNodeRecursion::Stop)
                } else {
                    Ok(TreeNodeRecursion::Continue)
                }
            })?;
            if subquery.is_some() {
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .map_err(|error| remote_error(error.to_string()))?;
    subquery.map_or(Ok(()), |expression| {
        Err(remote_error(format!(
            "remote reads do not support logical subqueries ({expression})"
        )))
    })
}

fn table_contract(table: &TableInfo) -> std::result::Result<RemoteReadTableV1, BoxedError> {
    let schema = table.meta.schema.column_schemas();
    let ids = table.name_to_ids().ok_or_else(|| {
        remote_error(format!(
            "remote read table {} has incomplete column ID metadata",
            table.table_id()
        ))
    })?;
    if ids.len() != schema.len() {
        return Err(remote_error(
            "remote read table has incomplete column ID metadata",
        ));
    }
    let primary_key_indices =
        if table.meta.engine == store_api::metric_engine_consts::METRIC_ENGINE_NAME {
            // MetricEngine synthesizes logical RegionMetadata by sorting logical
            // columns by name before deriving primary-key IDs. Match that order;
            // `schema` is TableInfo's logical schema, so physical reserved columns
            // are intentionally not considered here.
            let mut indexed_names = table
                .meta
                .primary_key_indices
                .iter()
                .map(|index| {
                    schema
                        .get(*index)
                        .map(|column| (*index, column.name.as_str()))
                        .ok_or_else(|| {
                            remote_error("remote read table has an invalid primary key index")
                        })
                })
                .collect::<std::result::Result<Vec<_>, BoxedError>>()?;
            indexed_names.sort_by(|left, right| left.1.cmp(right.1));
            indexed_names.into_iter().map(|(index, _)| index).collect()
        } else {
            table.meta.primary_key_indices.clone()
        };

    let mut seen_ids = HashSet::with_capacity(ids.len());
    let columns = schema
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let column_id = *ids.get(&column.name).ok_or_else(|| {
                remote_error(format!("missing stable ID for column {}", column.name))
            })?;
            if !seen_ids.insert(column_id) {
                return Err(remote_error(format!(
                    "duplicate stable ID {column_id} in remote read table"
                )));
            }
            let primary_key_ordinal = primary_key_indices
                .iter()
                .position(|primary_key_index| *primary_key_index == index)
                .map(|ordinal| ordinal as u32);
            let semantic_type = if column.is_time_index() {
                SemanticType::Timestamp
            } else if primary_key_ordinal.is_some() {
                SemanticType::Tag
            } else {
                SemanticType::Field
            };
            Ok(RemoteReadColumnV1 {
                name: column.name.clone(),
                column_id,
                semantic_type: Some(api_semantic_to_remote(semantic_type)? as i32),
                is_time_index: Some(column.is_time_index()),
                primary_key_ordinal,
            })
        })
        .collect::<std::result::Result<Vec<_>, BoxedError>>()?;

    Ok(RemoteReadTableV1 {
        table_id: table.table_id(),
        // This is diagnostic only. It is intentionally never compared at decode.
        table_version: Some(table.ident.version),
        columns,
    })
}

fn wrap_root(plan: &mut proto::Plan, detail: Any) -> std::result::Result<(), BoxedError> {
    let input = root_input_mut(plan)
        .map_err(remote_error)?
        .take()
        .ok_or_else(|| remote_error("remote plan root has no input"))?;
    *root_input_mut(plan).map_err(remote_error)? = Some(proto::Rel {
        rel_type: Some(proto::rel::RelType::ExtensionSingle(Box::new(
            proto::ExtensionSingleRel {
                common: None,
                detail: Some(detail),
                input: Some(Box::new(input)),
            },
        ))),
    });
    Ok(())
}

fn unwrap_root(plan: &mut proto::Plan) -> common_query::error::Result<Envelope> {
    let input = root_input_mut(plan)
        .map_err(decode_error)?
        .take()
        .ok_or_else(|| decode_error("remote plan root has no input"))?;
    let Some(proto::rel::RelType::ExtensionSingle(extension)) = input.rel_type else {
        return Err(decode_error("remote read requires a v1 root envelope"));
    };
    let detail = extension
        .detail
        .ok_or_else(|| decode_error("remote read envelope is missing detail"))?;
    let inner = extension
        .input
        .ok_or_else(|| decode_error("remote read envelope is missing input"))?;
    let envelope = match detail.type_url.as_str() {
        REMOTE_READ_TABLE_V1_TYPE_URL => {
            if detail.value.is_empty() {
                return Err(decode_error("remote read target contract is empty"));
            }
            let contract = RemoteReadTableV1::decode(detail.value.as_ref())
                .map_err(|error| decode_error(error.to_string()))?;
            if contract.table_id == 0 || contract.columns.is_empty() {
                return Err(decode_error("remote read target contract is incomplete"));
            }
            Envelope::Target(contract)
        }
        REMOTE_READ_INTERNAL_V1_TYPE_URL => {
            RemoteReadInternalV1::decode(detail.value.as_ref())
                .map_err(|error| decode_error(error.to_string()))?;
            Envelope::Internal
        }
        _ => return Err(decode_error("unknown remote read envelope URL or version")),
    };
    *root_input_mut(plan).map_err(decode_error)? = Some(*inner);
    Ok(envelope)
}

fn root_input(plan: &proto::Plan) -> std::result::Result<&proto::Rel, String> {
    if plan.relations.len() != 1 {
        return Err("remote plan must contain exactly one relation tree".to_string());
    }
    match plan.relations[0].rel_type.as_ref() {
        Some(proto::plan_rel::RelType::Root(root)) => root
            .input
            .as_ref()
            .ok_or_else(|| "remote plan root has no input".to_string()),
        _ => Err("remote plan must use a root relation".to_string()),
    }
}

fn root_input_mut(plan: &mut proto::Plan) -> std::result::Result<&mut Option<proto::Rel>, String> {
    if plan.relations.len() != 1 {
        return Err("remote plan must contain exactly one relation tree".to_string());
    }
    match plan.relations[0].rel_type.as_mut() {
        Some(proto::plan_rel::RelType::Root(root)) => Ok(&mut root.input),
        _ => Err("remote plan must use a root relation".to_string()),
    }
}

fn collect_read_schemas(root: &proto::Rel) -> common_query::error::Result<Vec<ReadSchema>> {
    let mut reads = Vec::new();
    collect_read_schemas_inner(root, &mut reads)?;
    Ok(reads)
}

fn collect_read_schemas_inner(
    rel: &proto::Rel,
    reads: &mut Vec<ReadSchema>,
) -> common_query::error::Result<()> {
    use proto::rel::RelType;

    let Some(rel_type) = rel.rel_type.as_ref() else {
        return Err(decode_error("remote plan contains an empty relation"));
    };
    match rel_type {
        RelType::Read(read) => {
            let Some(proto::read_rel::ReadType::NamedTable(named_table)) = read.read_type.as_ref()
            else {
                return Err(decode_error(
                    "remote read supports only named-table ReadRel providers",
                ));
            };
            let Some(table_name) = named_table.names.last().filter(|name| !name.is_empty()) else {
                return Err(decode_error("remote read named table has no name"));
            };
            let schema = read
                .base_schema
                .as_ref()
                .ok_or_else(|| decode_error("protected read has no base schema"))?;
            let struct_type = schema
                .r#struct
                .as_ref()
                .ok_or_else(|| decode_error("protected read has no type struct"))?;
            let (names, types) = extract_top_level_named_struct(schema, struct_type)?;
            reads.push(ReadSchema {
                table_name: table_name.clone(),
                table_reference: named_table.names.clone(),
                names,
                types,
            });
        }
        RelType::Filter(rel) => collect_optional_input(rel.input.as_ref(), reads)?,
        RelType::Fetch(rel) => collect_optional_input(rel.input.as_ref(), reads)?,
        RelType::Aggregate(rel) => collect_optional_input(rel.input.as_ref(), reads)?,
        RelType::Sort(rel) => collect_optional_input(rel.input.as_ref(), reads)?,
        RelType::Project(rel) => collect_optional_input(rel.input.as_ref(), reads)?,
        RelType::ExtensionSingle(rel) => collect_optional_input(rel.input.as_ref(), reads)?,
        RelType::Exchange(rel) => collect_optional_input(rel.input.as_ref(), reads)?,
        RelType::Expand(rel) => collect_optional_input(rel.input.as_ref(), reads)?,
        RelType::Window(rel) => collect_optional_input(rel.input.as_ref(), reads)?,
        RelType::Join(rel) => {
            collect_optional_input(rel.left.as_ref(), reads)?;
            collect_optional_input(rel.right.as_ref(), reads)?;
        }
        RelType::Cross(rel) => {
            collect_optional_input(rel.left.as_ref(), reads)?;
            collect_optional_input(rel.right.as_ref(), reads)?;
        }
        RelType::Set(rel) => {
            for input in &rel.inputs {
                collect_read_schemas_inner(input, reads)?;
            }
        }
        RelType::ExtensionMulti(rel) => {
            for input in &rel.inputs {
                collect_read_schemas_inner(input, reads)?;
            }
        }
        RelType::HashJoin(rel) => {
            collect_optional_input(rel.left.as_ref(), reads)?;
            collect_optional_input(rel.right.as_ref(), reads)?;
        }
        RelType::MergeJoin(rel) => {
            collect_optional_input(rel.left.as_ref(), reads)?;
            collect_optional_input(rel.right.as_ref(), reads)?;
        }
        RelType::NestedLoopJoin(rel) => {
            collect_optional_input(rel.left.as_ref(), reads)?;
            collect_optional_input(rel.right.as_ref(), reads)?;
        }
        // The walk only collects ReadRels; it does not fail-closed on relation
        // shapes. Every decoded scan is still matched against the collected
        // ReadRels by `validate_decoded_sources`, so a scan that was never
        // collected still fails validation.
        RelType::Write(write) => collect_optional_input(write.input.as_ref(), reads)?,
        RelType::Ddl(ddl) => collect_optional_input(ddl.view_definition.as_ref(), reads)?,
        RelType::Update(_) | RelType::ExtensionLeaf(_) | RelType::Reference(_) => {}
    }
    Ok(())
}

/// Rebuild just the top-level field association from Substrait's flattened
/// `NamedStruct.names` representation. This mirrors DataFusion 53's name
/// consumption rules while retaining the original top-level type for remote
/// nullability validation.
fn extract_top_level_named_struct(
    schema: &proto::NamedStruct,
    struct_type: &proto::r#type::Struct,
) -> common_query::error::Result<(Vec<String>, Vec<proto::Type>)> {
    let mut name_index = 0;
    let mut names = Vec::with_capacity(struct_type.types.len());
    for type_ in &struct_type.types {
        let name = consume_flattened_type_name(type_, &schema.names, &mut name_index, true)?;
        names.push(name);
    }
    if name_index != schema.names.len()
        || names.iter().any(|name| name.is_empty())
        || names.iter().collect::<HashSet<_>>().len() != names.len()
    {
        return Err(decode_error(
            "protected read base schema is incomplete or ambiguous",
        ));
    }
    Ok((names, struct_type.types.clone()))
}

fn consume_flattened_type_name(
    type_: &proto::Type,
    names: &[String],
    name_index: &mut usize,
    consume_self: bool,
) -> common_query::error::Result<String> {
    let own_name = if consume_self {
        let name = names
            .get(*name_index)
            .filter(|name| !name.is_empty())
            .ok_or_else(|| decode_error("protected read base schema has too few names"))?
            .clone();
        *name_index += 1;
        name
    } else {
        String::new()
    };
    let kind = type_
        .kind
        .as_ref()
        .ok_or_else(|| decode_error("protected read type is unspecified"))?;
    match kind {
        proto::r#type::Kind::Struct(struct_type) => {
            for child in &struct_type.types {
                consume_flattened_type_name(child, names, name_index, true)?;
            }
        }
        proto::r#type::Kind::List(list) => {
            consume_flattened_type_name(
                list.r#type
                    .as_deref()
                    .ok_or_else(|| decode_error("protected list has no child type"))?,
                names,
                name_index,
                false,
            )?;
        }
        proto::r#type::Kind::Map(map) => {
            consume_flattened_type_name(
                map.key
                    .as_deref()
                    .ok_or_else(|| decode_error("protected map has no key type"))?,
                names,
                name_index,
                false,
            )?;
            consume_flattened_type_name(
                map.value
                    .as_deref()
                    .ok_or_else(|| decode_error("protected map has no value type"))?,
                names,
                name_index,
                false,
            )?;
        }
        _ => {}
    }
    Ok(own_name)
}

fn collect_optional_input(
    input: Option<&Box<proto::Rel>>,
    reads: &mut Vec<ReadSchema>,
) -> common_query::error::Result<()> {
    let input = input.ok_or_else(|| decode_error("remote plan relation has no input"))?;
    collect_read_schemas_inner(input.as_ref(), reads)
}

fn validate_decoded_sources(
    plan: &LogicalPlan,
    envelope: &Envelope,
    read_schemas: &[ReadSchema],
) -> common_query::error::Result<()> {
    let mut protected = 0;
    let mut validation_error = None;
    let mut reads_by_table = HashMap::<&str, Vec<&ReadSchema>>::new();
    for read in read_schemas {
        if let Some(existing) = reads_by_table.get(read.table_name.as_str())
            && existing
                .iter()
                .any(|existing| existing.table_reference != read.table_reference)
        {
            return Err(decode_error(format!(
                "remote read has ambiguous qualified references for table {}",
                read.table_name
            )));
        }
        reads_by_table
            .entry(read.table_name.as_str())
            .or_default()
            .push(read);
    }
    plan.apply(|node| {
        let LogicalPlan::TableScan(scan) = node else {
            return Ok(TreeNodeRecursion::Continue);
        };
        let Some(source) = scan.source.as_any().downcast_ref::<DefaultTableSource>() else {
            validation_error = Some("remote read resolved an unknown table source".to_string());
            return Ok(TreeNodeRecursion::Stop);
        };
        let table_name = scan.table_name.table();
        let Some(reads) = reads_by_table.get_mut(table_name) else {
            validation_error = Some(format!(
                "decoded remote table scan {table_name} has no matching ReadRel"
            ));
            return Ok(TreeNodeRecursion::Stop);
        };
        let Some(read) = reads.pop() else {
            validation_error = Some(format!(
                "decoded remote table scan {table_name} has no remaining ReadRel"
            ));
            return Ok(TreeNodeRecursion::Stop);
        };
        let provider = &source.table_provider;
        if let Some(dummy) = provider.as_any().downcast_ref::<DummyTableProvider>() {
            protected += 1;
            match envelope {
                Envelope::Target(contract) => {
                    if let Err(error) = validate_target(dummy.region_metadata().as_ref(), contract)
                        .and_then(|_| {
                            validate_read_schema(dummy.region_metadata().as_ref(), contract, read)
                        })
                    {
                        validation_error = Some(error);
                        return Ok(TreeNodeRecursion::Stop);
                    }
                }
                Envelope::Internal => {
                    validation_error = Some(
                        "internal remote plan resolved a protected region provider".to_string(),
                    );
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
        } else if provider.as_any().downcast_ref::<MemTable>().is_some() {
            if !is_reserved_internal_table_name(table_name) {
                validation_error = Some(format!(
                    "remote read MemTable source {table_name} is not a reserved inspection source"
                ));
                return Ok(TreeNodeRecursion::Stop);
            }
            if let Err(error) = validate_internal_read_schema(provider.schema().as_ref(), read) {
                validation_error = Some(error);
                return Ok(TreeNodeRecursion::Stop);
            }
        } else {
            validation_error =
                Some("remote read resolved an unrecognized table provider".to_string());
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .map_err(|error| decode_error(error.to_string()))?;

    if let Some(error) = validation_error {
        return Err(decode_error(error));
    }
    if reads_by_table.values().any(|reads| !reads.is_empty()) {
        return Err(decode_error(
            "remote read contains ReadRel entries not resolved by the decoded plan",
        ));
    }
    match envelope {
        Envelope::Target(_) if protected == 0 => Err(decode_error(
            "remote target contract consumed no protected read",
        )),
        Envelope::Internal if protected != 0 => Err(decode_error(
            "internal remote plan consumed a protected read",
        )),
        _ => Ok(()),
    }
}

fn validate_target(
    metadata: &RegionMetadata,
    contract: &RemoteReadTableV1,
) -> std::result::Result<(), String> {
    // TableInfo and RegionMetadata versions are deliberately diagnostic only.
    if metadata.region_id.table_id() != contract.table_id {
        return Err(format!(
            "remote table ID {} does not match region table ID {}",
            contract.table_id,
            metadata.region_id.table_id()
        ));
    }
    let mut contract_columns = HashMap::with_capacity(contract.columns.len());
    for column in &contract.columns {
        if column.name.is_empty()
            || column.semantic_type.is_none()
            || column.is_time_index.is_none()
            || contract_columns
                .insert(column.name.as_str(), column)
                .is_some()
        {
            return Err("remote target contract has incomplete or duplicate columns".to_string());
        }
    }
    for expected in contract_columns.values() {
        let Some(current) = metadata.column_by_name(&expected.name) else {
            return Err(format!("remote target column {} is missing", expected.name));
        };
        let expected_semantic = remote_semantic_to_api(expected.semantic_type.unwrap())?;
        if expected.column_id != current.column_id
            || expected_semantic != current.semantic_type
            || expected.is_time_index != Some(current.semantic_type == SemanticType::Timestamp)
            || expected.primary_key_ordinal
                != metadata
                    .primary_key_index(current.column_id)
                    .map(|index| index as u32)
        {
            return Err(format!(
                "remote target column {} changed identity or semantic role",
                expected.name
            ));
        }
    }
    Ok(())
}

fn validate_read_schema(
    metadata: &RegionMetadata,
    contract: &RemoteReadTableV1,
    read_schema: &ReadSchema,
) -> std::result::Result<(), String> {
    let contract_columns = contract
        .columns
        .iter()
        .map(|column| (column.name.as_str(), column))
        .collect::<HashMap<_, _>>();
    if read_schema.names.len() != contract_columns.len()
        || read_schema.names.iter().collect::<HashSet<_>>().len() != read_schema.names.len()
        || read_schema
            .names
            .iter()
            .any(|name| !contract_columns.contains_key(name.as_str()))
    {
        return Err("protected read schema does not exactly cover the target contract".to_string());
    }
    for (name, expected_type) in read_schema.names.iter().zip(&read_schema.types) {
        let current = metadata
            .column_by_name(name)
            .ok_or_else(|| format!("protected read column {name} is missing"))?;
        let current_field = Field::new(
            &current.column_schema.name,
            current.column_schema.data_type.as_arrow_type(),
            current.column_schema.is_nullable(),
        );
        validate_nullability(expected_type, &current_field)?;
    }
    Ok(())
}

fn validate_internal_read_schema(
    schema: &datatypes::arrow::datatypes::Schema,
    read_schema: &ReadSchema,
) -> std::result::Result<(), String> {
    for (name, expected_type) in read_schema.names.iter().zip(&read_schema.types) {
        let actual = schema
            .field_with_name(name)
            .map_err(|_| format!("internal inspection column {name} is missing"))?;
        validate_nullability(expected_type, actual)?;
    }
    Ok(())
}

fn remote_semantic_to_api(value: i32) -> std::result::Result<SemanticType, String> {
    match RemoteReadSemanticTypeV1::try_from(value) {
        Ok(RemoteReadSemanticTypeV1::RemoteReadSemanticTypeTag) => Ok(SemanticType::Tag),
        Ok(RemoteReadSemanticTypeV1::RemoteReadSemanticTypeField) => Ok(SemanticType::Field),
        Ok(RemoteReadSemanticTypeV1::RemoteReadSemanticTypeTimestamp) => {
            Ok(SemanticType::Timestamp)
        }
        Ok(RemoteReadSemanticTypeV1::RemoteReadSemanticTypeUnspecified) | Err(_) => {
            Err("remote target contract has an unspecified or invalid semantic type".to_string())
        }
    }
}

fn api_semantic_to_remote(
    value: SemanticType,
) -> std::result::Result<RemoteReadSemanticTypeV1, BoxedError> {
    match value {
        SemanticType::Tag => Ok(RemoteReadSemanticTypeV1::RemoteReadSemanticTypeTag),
        SemanticType::Field => Ok(RemoteReadSemanticTypeV1::RemoteReadSemanticTypeField),
        SemanticType::Timestamp => Ok(RemoteReadSemanticTypeV1::RemoteReadSemanticTypeTimestamp),
    }
}

fn validate_nullability(expected: &proto::Type, actual: &Field) -> std::result::Result<(), String> {
    let kind = expected
        .kind
        .as_ref()
        .ok_or_else(|| "protected read type is unspecified".to_string())?;
    let nullability = type_nullability(kind).ok_or_else(|| {
        "protected read type has unsupported or unspecified nullability".to_string()
    })?;
    if nullability == proto::r#type::Nullability::Required && actual.is_nullable() {
        return Err(format!(
            "protected read field {} changed from required to nullable",
            actual.name()
        ));
    }
    match kind {
        proto::r#type::Kind::Struct(expected) => match actual.data_type() {
            ArrowDataType::Struct(fields) if fields.len() == expected.types.len() => {
                for (expected, actual) in expected.types.iter().zip(fields) {
                    validate_nullability(expected, actual)?;
                }
            }
            _ => {
                return Err(format!(
                    "protected read struct field {} changed shape",
                    actual.name()
                ));
            }
        },
        proto::r#type::Kind::List(expected) => match actual.data_type() {
            ArrowDataType::List(field)
            | ArrowDataType::LargeList(field)
            | ArrowDataType::FixedSizeList(field, _) => {
                validate_nullability(
                    expected
                        .r#type
                        .as_ref()
                        .ok_or_else(|| "protected list has no child type".to_string())?,
                    field,
                )?;
            }
            _ => {
                return Err(format!(
                    "protected read list field {} changed shape",
                    actual.name()
                ));
            }
        },
        proto::r#type::Kind::Map(expected) => match actual.data_type() {
            ArrowDataType::Map(entries, _) => match entries.data_type() {
                ArrowDataType::Struct(fields) if fields.len() == 2 => {
                    validate_nullability(
                        expected
                            .key
                            .as_ref()
                            .ok_or_else(|| "protected map has no key type".to_string())?,
                        &fields[0],
                    )?;
                    validate_nullability(
                        expected
                            .value
                            .as_ref()
                            .ok_or_else(|| "protected map has no value type".to_string())?,
                        &fields[1],
                    )?;
                }
                _ => {
                    return Err(format!(
                        "protected read map field {} changed shape",
                        actual.name()
                    ));
                }
            },
            _ => {
                return Err(format!(
                    "protected read map field {} changed shape",
                    actual.name()
                ));
            }
        },
        _ => {}
    }
    Ok(())
}

fn type_nullability(kind: &proto::r#type::Kind) -> Option<proto::r#type::Nullability> {
    use proto::r#type::Kind;
    let nullability = match kind {
        Kind::Bool(value) => value.nullability,
        Kind::I8(value) => value.nullability,
        Kind::I16(value) => value.nullability,
        Kind::I32(value) => value.nullability,
        Kind::I64(value) => value.nullability,
        Kind::Fp32(value) => value.nullability,
        Kind::Fp64(value) => value.nullability,
        Kind::String(value) => value.nullability,
        Kind::Binary(value) => value.nullability,
        Kind::Timestamp(value) => value.nullability,
        Kind::Date(value) => value.nullability,
        Kind::Time(value) => value.nullability,
        Kind::IntervalYear(value) => value.nullability,
        Kind::IntervalDay(value) => value.nullability,
        Kind::IntervalCompound(value) => value.nullability,
        Kind::TimestampTz(value) => value.nullability,
        Kind::Uuid(value) => value.nullability,
        Kind::FixedChar(value) => value.nullability,
        Kind::Varchar(value) => value.nullability,
        Kind::FixedBinary(value) => value.nullability,
        Kind::Decimal(value) => value.nullability,
        Kind::PrecisionTime(value) => value.nullability,
        Kind::PrecisionTimestamp(value) => value.nullability,
        Kind::PrecisionTimestampTz(value) => value.nullability,
        Kind::Struct(value) => value.nullability,
        Kind::List(value) => value.nullability,
        Kind::Map(value) => value.nullability,
        Kind::UserDefined(value) => value.nullability,
        Kind::UserDefinedTypeReference(_) | Kind::Alias(_) => return None,
    };
    match proto::r#type::Nullability::try_from(nullability).ok()? {
        proto::r#type::Nullability::Nullable => Some(proto::r#type::Nullability::Nullable),
        proto::r#type::Nullability::Required => Some(proto::r#type::Nullability::Required),
        _ => None,
    }
}

fn remote_error(message: impl Into<String>) -> BoxedError {
    // Encode-side errors are local frontend errors, not schema races, so they
    // keep the non-retryable InvalidArguments status.
    BoxedError::new(PlainError::new(
        message.into(),
        StatusCode::InvalidArguments,
    ))
}

/// A remote-read contract violation detected at decode time.
///
/// These are transient: the frontend planned against a schema the region has
/// since changed (a schema race). They are marked retryable so the caller can
/// retry against fresh metadata.
#[derive(Debug)]
struct ContractViolation {
    message: String,
}

impl std::fmt::Display for ContractViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ContractViolation {}

impl common_error::ext::ErrorExt for ContractViolation {
    fn status_code(&self) -> StatusCode {
        StatusCode::Internal
    }

    fn retry_hint(&self) -> RetryHint {
        RetryHint::Retryable
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl common_error::ext::StackError for ContractViolation {
    fn debug_fmt(&self, layer: usize, buf: &mut Vec<String>) {
        buf.push(format!("{}: {}", layer, self.message))
    }

    fn next(&self) -> Option<&dyn common_error::ext::StackError> {
        None
    }
}

fn decode_error(message: impl Into<String>) -> common_query::error::Error {
    // Decode-side contract violations are transient schema races and must be
    // retryable.
    common_query::error::Error::DecodePlan {
        source: BoxedError::new(ContractViolation {
            message: message.into(),
        }),
        location: snafu::location!(),
    }
}

#[cfg(test)]
mod tests {
    use datafusion::catalog::CatalogProviderList;
    use datafusion_expr::LogicalPlanBuilder;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, SchemaBuilder};
    use session::context::QueryContext;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use table::metadata::{TableInfoBuilder, TableMeta, TableType};
    use table::table::adapter::DfTableProviderAdapter;
    use table::test_util::EmptyTable;

    use super::*;
    use crate::QueryEngineFactory;
    use crate::dummy_catalog::DummyCatalogList;
    use crate::optimizer::test_util::{MetaRegionEngine, mock_table_provider};
    use crate::options::QueryOptions;

    fn contract_from_current(metadata: &RegionMetadata) -> RemoteReadTableV1 {
        RemoteReadTableV1 {
            table_id: metadata.region_id.table_id(),
            table_version: Some(1),
            columns: metadata
                .column_metadatas
                .iter()
                .map(|column| RemoteReadColumnV1 {
                    name: column.column_schema.name.clone(),
                    column_id: column.column_id,
                    semantic_type: Some(match column.semantic_type {
                        SemanticType::Tag => RemoteReadSemanticTypeV1::RemoteReadSemanticTypeTag,
                        SemanticType::Field => {
                            RemoteReadSemanticTypeV1::RemoteReadSemanticTypeField
                        }
                        SemanticType::Timestamp => {
                            RemoteReadSemanticTypeV1::RemoteReadSemanticTypeTimestamp
                        }
                    } as i32),
                    is_time_index: Some(column.semantic_type == SemanticType::Timestamp),
                    primary_key_ordinal: metadata
                        .primary_key_index(column.column_id)
                        .map(|index| index as u32),
                })
                .collect(),
        }
    }

    fn encoder_table_info(table_id: u32, version: u64) -> TableInfo {
        let schema = Arc::new(
            SchemaBuilder::try_from_columns(vec![
                ColumnSchema::new("k0", ConcreteDataType::string_datatype(), true),
                ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
                ColumnSchema::new("v0", ConcreteDataType::float64_datatype(), false),
            ])
            .unwrap()
            .build()
            .unwrap(),
        );
        TableInfoBuilder::default()
            .table_id(table_id)
            .table_version(version)
            .name("remote_codec")
            .meta(TableMeta {
                schema,
                primary_key_indices: vec![0],
                value_indices: vec![1, 2],
                engine: "mito".to_string(),
                next_column_id: 4,
                options: Default::default(),
                created_on: Default::default(),
                updated_on: Default::default(),
                partition_key_indices: vec![],
                column_ids: vec![1, 2, 3],
            })
            .table_type(TableType::Base)
            .build()
            .unwrap()
    }

    fn primary_key_order_table_info(engine: &str) -> TableInfo {
        let schema = Arc::new(
            SchemaBuilder::try_from_columns(vec![
                ColumnSchema::new("z", ConcreteDataType::string_datatype(), false),
                ColumnSchema::new("a", ConcreteDataType::string_datatype(), false),
                ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
                ColumnSchema::new("value", ConcreteDataType::float64_datatype(), true),
            ])
            .unwrap()
            .build()
            .unwrap(),
        );
        TableInfoBuilder::default()
            .table_id(88)
            .table_version(1)
            .name("metric_primary_key_order")
            .meta(TableMeta {
                schema,
                // Deliberately declared as (z, a), not alphabetical.
                primary_key_indices: vec![0, 1],
                value_indices: vec![2, 3],
                engine: engine.to_string(),
                next_column_id: 100,
                options: Default::default(),
                created_on: Default::default(),
                updated_on: Default::default(),
                partition_key_indices: vec![],
                // These are stable IDs sourced from the physical metric table.
                column_ids: vec![71, 41, 2, 99],
            })
            .table_type(TableType::Base)
            .build()
            .unwrap()
    }

    fn adapter_plan(table_id: u32) -> LogicalPlan {
        let info = encoder_table_info(table_id, 7);
        let table = EmptyTable::from_table_info(&info);
        let source = Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(table),
        )));
        LogicalPlanBuilder::scan("remote_codec", source, None)
            .unwrap()
            .build()
            .unwrap()
    }

    fn dn_column(
        name: &str,
        data_type: ConcreteDataType,
        nullable: bool,
        semantic_type: SemanticType,
        column_id: u32,
    ) -> ColumnMetadata {
        ColumnMetadata {
            column_schema: ColumnSchema::new(name, data_type, nullable),
            semantic_type,
            column_id,
        }
    }

    fn dn_metadata(
        region_id: RegionId,
        columns: Vec<ColumnMetadata>,
        primary_key: Vec<u32>,
    ) -> RegionMetadata {
        let mut builder = RegionMetadataBuilder::new(region_id);
        for column in columns {
            builder.push_column_metadata(column);
        }
        builder.primary_key(primary_key);
        builder.build().unwrap()
    }

    fn standard_dn_metadata(region_id: RegionId) -> RegionMetadata {
        dn_metadata(
            region_id,
            vec![
                dn_column(
                    "k0",
                    ConcreteDataType::string_datatype(),
                    true,
                    SemanticType::Tag,
                    1,
                ),
                dn_column(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                    SemanticType::Timestamp,
                    2,
                ),
                dn_column(
                    "v0",
                    ConcreteDataType::float64_datatype(),
                    false,
                    SemanticType::Field,
                    3,
                ),
            ],
            vec![1],
        )
    }

    fn provider_with_metadata(metadata: RegionMetadata) -> DummyTableProvider {
        let region_id = metadata.region_id;
        let metadata = Arc::new(metadata);
        let engine = Arc::new(MetaRegionEngine::with_metadata(metadata.clone()));
        DummyTableProvider::new(region_id, engine, metadata)
    }

    fn remote_decoder() -> common_query::logical_plan::SubstraitPlanDecoderRef {
        let catalog_manager = catalog::memory::new_memory_catalog_manager().unwrap();
        let factory = QueryEngineFactory::new(
            catalog_manager,
            None,
            None,
            None,
            None,
            false,
            QueryOptions::default(),
        );
        factory
            .query_engine()
            .engine_context(QueryContext::arc())
            .new_plan_decoder()
            .unwrap()
    }

    async fn decode_with_provider(
        message: Bytes,
        provider: DummyTableProvider,
    ) -> common_query::error::Result<LogicalPlan> {
        let decoder = remote_decoder();
        let catalog_list: Arc<dyn CatalogProviderList> =
            Arc::new(DummyCatalogList::with_table_provider(Arc::new(provider)));
        decode_remote_plan(decoder.as_ref(), message, catalog_list).await
    }

    fn encoded_remote_plan(region_id: RegionId) -> Bytes {
        encode_remote_plan(&adapter_plan(region_id.table_id()), region_id).unwrap()
    }

    fn inspection_memtable_plan(name: &str) -> LogicalPlan {
        let schema = Arc::new(datatypes::arrow::datatypes::Schema::empty());
        let provider = Arc::new(MemTable::try_new(schema, vec![vec![]]).unwrap());
        LogicalPlanBuilder::scan(name, Arc::new(DefaultTableSource::new(provider)), None)
            .unwrap()
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_decode_remote_plan_accepts_matching_fe_and_dn_schema() {
        let region_id = RegionId::new(42, 1);
        let plan = decode_with_provider(
            encoded_remote_plan(region_id),
            mock_table_provider(region_id),
        )
        .await
        .unwrap();

        assert!(matches!(plan, LogicalPlan::TableScan(_)));
    }

    #[tokio::test]
    async fn test_decode_remote_plan_tolerates_legacy_unwrapped_plan() {
        let region_id = RegionId::new(42, 1);
        // An older frontend encodes the plan without the v1 root envelope.
        let mut legacy = proto::Plan::decode(encoded_remote_plan(region_id).as_ref()).unwrap();
        unwrap_root(&mut legacy).unwrap();
        let legacy_bytes = Bytes::from(legacy.encode_to_vec());

        // The new datanode accepts the legacy plan and decodes it with base
        // semantics.
        let decoded = decode_with_provider(legacy_bytes.clone(), mock_table_provider(region_id))
            .await
            .unwrap();
        assert!(matches!(decoded, LogicalPlan::TableScan(_)));

        // It is still safe: the base decoder's schema-compatibility check
        // rejects a column whose type changed between planning and execution.
        let mut metadata = standard_dn_metadata(region_id);
        metadata.column_metadatas[2].column_schema =
            ColumnSchema::new("v0", ConcreteDataType::int64_datatype(), false);
        let metadata = RegionMetadataBuilder::from_existing(metadata)
            .build()
            .unwrap();
        assert!(
            decode_with_provider(legacy_bytes, provider_with_metadata(metadata))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_decode_remote_plan_rejects_required_to_nullable_drift() {
        let region_id = RegionId::new(42, 1);
        let mut metadata = standard_dn_metadata(region_id);
        metadata.column_metadatas[2].column_schema =
            ColumnSchema::new("v0", ConcreteDataType::float64_datatype(), true);
        let metadata = RegionMetadataBuilder::from_existing(metadata)
            .build()
            .unwrap();

        assert!(
            decode_with_provider(
                encoded_remote_plan(region_id),
                provider_with_metadata(metadata)
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn test_decode_remote_plan_accepts_nullable_to_required_drift() {
        let region_id = RegionId::new(42, 1);
        let mut metadata = standard_dn_metadata(region_id);
        metadata.column_metadatas[0].column_schema =
            ColumnSchema::new("k0", ConcreteDataType::string_datatype(), false);
        let metadata = RegionMetadataBuilder::from_existing(metadata)
            .build()
            .unwrap();

        assert!(
            decode_with_provider(
                encoded_remote_plan(region_id),
                provider_with_metadata(metadata),
            )
            .await
            .is_ok()
        );
    }

    #[tokio::test]
    async fn test_decode_remote_plan_rejects_changed_stable_column_id() {
        let region_id = RegionId::new(42, 1);
        let metadata = dn_metadata(
            region_id,
            vec![
                dn_column(
                    "k0",
                    ConcreteDataType::string_datatype(),
                    true,
                    SemanticType::Tag,
                    11,
                ),
                dn_column(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                    SemanticType::Timestamp,
                    2,
                ),
                dn_column(
                    "v0",
                    ConcreteDataType::float64_datatype(),
                    false,
                    SemanticType::Field,
                    3,
                ),
            ],
            vec![11],
        );

        assert!(
            decode_with_provider(
                encoded_remote_plan(region_id),
                provider_with_metadata(metadata)
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn test_decode_remote_plan_accepts_reordered_schema_with_added_column() {
        let region_id = RegionId::new(42, 1);
        let metadata = dn_metadata(
            region_id,
            vec![
                dn_column(
                    "v0",
                    ConcreteDataType::float64_datatype(),
                    false,
                    SemanticType::Field,
                    3,
                ),
                dn_column(
                    "extra",
                    ConcreteDataType::int64_datatype(),
                    true,
                    SemanticType::Field,
                    4,
                ),
                dn_column(
                    "k0",
                    ConcreteDataType::string_datatype(),
                    true,
                    SemanticType::Tag,
                    1,
                ),
                dn_column(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                    SemanticType::Timestamp,
                    2,
                ),
            ],
            vec![1],
        );

        assert!(
            decode_with_provider(
                encoded_remote_plan(region_id),
                provider_with_metadata(metadata)
            )
            .await
            .is_ok()
        );
    }

    #[tokio::test]
    async fn test_decode_remote_plan_rejects_missing_or_type_changed_column() {
        let region_id = RegionId::new(42, 1);
        let mut missing = standard_dn_metadata(region_id);
        missing.column_metadatas.pop();
        let missing = RegionMetadataBuilder::from_existing(missing)
            .build()
            .unwrap();
        assert!(
            decode_with_provider(
                encoded_remote_plan(region_id),
                provider_with_metadata(missing)
            )
            .await
            .is_err()
        );

        let mut changed_type = standard_dn_metadata(region_id);
        changed_type.column_metadatas[2].column_schema =
            ColumnSchema::new("v0", ConcreteDataType::int64_datatype(), false);
        let changed_type = RegionMetadataBuilder::from_existing(changed_type)
            .build()
            .unwrap();
        assert!(
            decode_with_provider(
                encoded_remote_plan(region_id),
                provider_with_metadata(changed_type),
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn test_default_decoder_rejects_wrapped_remote_plan() {
        let region_id = RegionId::new(42, 1);
        let decoder = remote_decoder();
        let catalog_list: Arc<dyn CatalogProviderList> =
            Arc::new(DummyCatalogList::with_table_provider(Arc::new(
                provider_with_metadata(standard_dn_metadata(region_id)),
            )));

        assert!(
            decoder
                .decode(encoded_remote_plan(region_id), catalog_list, false)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_decode_remote_plan_rejects_missing_or_unknown_envelope() {
        let region_id = RegionId::new(42, 1);
        assert!(
            decode_with_provider(
                Bytes::from(rooted_plan().encode_to_vec()),
                mock_table_provider(region_id),
            )
            .await
            .is_err()
        );

        let input = Some(Box::new(proto::Rel {
            rel_type: Some(proto::rel::RelType::Read(Box::default())),
        }));
        let plan = plan_with_envelope(
            Some(Any {
                type_url: "type.googleapis.com/substrait_extension.RemoteReadTableV2".to_string(),
                value: vec![].into(),
            }),
            input,
        );
        assert!(
            decode_with_provider(
                Bytes::from(plan.encode_to_vec()),
                mock_table_provider(region_id),
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn test_decode_remote_plan_rejects_internal_envelope_with_region_provider() {
        let region_id = RegionId::new(42, 1);
        let mut plan = proto::Plan::decode(encoded_remote_plan(region_id).as_ref()).unwrap();
        unwrap_root(&mut plan).unwrap();
        wrap_root(
            &mut plan,
            Any {
                type_url: REMOTE_READ_INTERNAL_V1_TYPE_URL.to_string(),
                value: RemoteReadInternalV1 {}.encode_to_vec().into(),
            },
        )
        .unwrap();
        assert!(
            decode_with_provider(
                Bytes::from(plan.encode_to_vec()),
                mock_table_provider(region_id),
            )
            .await
            .is_err()
        );
    }

    #[test]
    fn test_encode_real_adapter_contract_and_request_identity() {
        let plan = adapter_plan(42);
        let bytes = encode_remote_plan(&plan, RegionId::new(42, 1)).unwrap();
        let mut plan = proto::Plan::decode(bytes.as_ref()).unwrap();
        let Envelope::Target(contract) = unwrap_root(&mut plan).unwrap() else {
            panic!("expected target envelope");
        };
        assert_eq!(42, contract.table_id);
        assert_eq!(Some(7), contract.table_version);
        assert_eq!(
            vec![1, 2, 3],
            contract
                .columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>()
        );
        assert_eq!(Some(0), contract.columns[0].primary_key_ordinal);
        assert_eq!(Some(true), contract.columns[1].is_time_index);
        assert!(encode_remote_plan(&plan_from_adapter(42), RegionId::new(43, 1)).is_err());
    }

    #[test]
    fn test_encode_rejects_multiple_real_protected_table_ids() {
        let left = adapter_plan(42);
        let right = adapter_plan(43);
        let plan = LogicalPlanBuilder::from(left)
            .union(right)
            .unwrap()
            .build()
            .unwrap();
        assert!(encode_remote_plan(&plan, RegionId::new(42, 1)).is_err());
    }

    #[test]
    fn test_encode_classifies_only_reserved_memtables_as_internal() {
        let internal =
            inspection_memtable_plan(ManifestSstEntry::reserved_table_name_for_inspection());
        let bytes = encode_remote_plan(&internal, RegionId::new(42, 1)).unwrap();
        let mut plan = proto::Plan::decode(bytes.as_ref()).unwrap();
        assert!(matches!(unwrap_root(&mut plan), Ok(Envelope::Internal)));

        assert!(
            encode_remote_plan(
                &inspection_memtable_plan("unrecognized_memtable"),
                RegionId::new(42, 1),
            )
            .is_err()
        );
    }

    #[test]
    fn test_encode_allows_target_with_reserved_inspection_source() {
        let target = adapter_plan(42);
        let inspection =
            inspection_memtable_plan(ManifestSstEntry::reserved_table_name_for_inspection());
        let plan = LogicalPlanBuilder::from(target)
            .cross_join(inspection)
            .unwrap()
            .build()
            .unwrap();
        let bytes = encode_remote_plan(&plan, RegionId::new(42, 1)).unwrap();
        let mut plan = proto::Plan::decode(bytes.as_ref()).unwrap();
        assert!(matches!(unwrap_root(&mut plan), Ok(Envelope::Target(_))));
    }

    #[test]
    fn test_read_schema_rejects_distinct_qualified_references_with_same_bare_name() {
        let read = |reference: &[&str]| ReadSchema {
            table_name: "same".to_string(),
            table_reference: reference.iter().map(|part| (*part).to_string()).collect(),
            names: vec![],
            types: vec![],
        };
        assert!(
            validate_decoded_sources(
                &adapter_plan(42),
                &Envelope::Internal,
                &[
                    read(&["catalog_a", "public", "same"]),
                    read(&["catalog_b", "public", "same"])
                ]
            )
            .is_err()
        );
    }

    #[test]
    fn test_same_table_duplicate_scans_remain_unambiguous() {
        let left = adapter_plan(42);
        let plan = LogicalPlanBuilder::from(left.clone())
            .union(left)
            .unwrap()
            .build()
            .unwrap();
        assert!(encode_remote_plan(&plan, RegionId::new(42, 1)).is_ok());
    }

    fn plan_from_adapter(table_id: u32) -> LogicalPlan {
        adapter_plan(table_id)
    }

    #[test]
    fn test_target_allows_added_unrelated_current_column() {
        let provider = mock_table_provider(RegionId::new(1024, 1));
        let metadata = provider.region_metadata();
        let mut contract = contract_from_current(&metadata);
        contract.columns.pop();

        assert!(validate_target(&metadata, &contract).is_ok());
    }

    #[test]
    fn test_target_rejects_changed_column_id_and_unspecified_semantic_type() {
        let provider = mock_table_provider(RegionId::new(1024, 1));
        let metadata = provider.region_metadata();
        let mut contract = contract_from_current(&metadata);
        contract.columns[0].column_id += 100;
        assert!(validate_target(&metadata, &contract).is_err());

        let mut contract = contract_from_current(&metadata);
        contract.columns[0].semantic_type =
            Some(RemoteReadSemanticTypeV1::RemoteReadSemanticTypeUnspecified as i32);
        assert!(validate_target(&metadata, &contract).is_err());
    }

    #[test]
    fn test_target_contract_compatibility_matrix() {
        let provider = mock_table_provider(RegionId::new(1024, 1));
        let metadata = provider.region_metadata();

        let contract = contract_from_current(&metadata);
        assert!(validate_target(&metadata, &contract).is_ok());

        let mut changed = contract.clone();
        changed.table_id += 1;
        assert!(validate_target(&metadata, &changed).is_err());

        let mut changed = contract.clone();
        changed.columns[0].name = "missing".to_string();
        assert!(validate_target(&metadata, &changed).is_err());

        let mut changed = contract.clone();
        changed.columns[0].semantic_type =
            Some(RemoteReadSemanticTypeV1::RemoteReadSemanticTypeField as i32);
        assert!(validate_target(&metadata, &changed).is_err());

        let time_index = contract
            .columns
            .iter()
            .position(|column| column.is_time_index == Some(true))
            .expect("mock metadata has a time index");
        let mut changed = contract.clone();
        changed.columns[time_index].is_time_index = Some(false);
        assert!(validate_target(&metadata, &changed).is_err());

        let mut changed = contract.clone();
        changed.columns[0].primary_key_ordinal = None;
        assert!(validate_target(&metadata, &changed).is_err());

        let mut changed = contract.clone();
        changed.columns[0].semantic_type = None;
        assert!(validate_target(&metadata, &changed).is_err());

        let mut changed = contract.clone();
        changed.columns[0].is_time_index = None;
        assert!(validate_target(&metadata, &changed).is_err());

        let mut compatible = contract.clone();
        compatible.columns.reverse();
        compatible.table_version = Some(999);
        assert!(validate_target(&metadata, &compatible).is_ok());
    }

    #[test]
    fn test_metric_engine_primary_key_ordinal_matches_logical_region_metadata() {
        let table =
            primary_key_order_table_info(store_api::metric_engine_consts::METRIC_ENGINE_NAME);
        let contract = table_contract(&table).unwrap();
        let ordinal = |name: &str| {
            contract
                .columns
                .iter()
                .find(|column| column.name == name)
                .unwrap()
                .primary_key_ordinal
        };
        // MetricEngineInner::load_logical_columns sorts logical columns by name
        // before RegionMetadata derives primary_key IDs (read.rs:239-268).
        assert_eq!(Some(0), ordinal("a"));
        assert_eq!(Some(1), ordinal("z"));

        let metadata = dn_metadata(
            RegionId::new(88, 1),
            vec![
                dn_column(
                    "a",
                    ConcreteDataType::string_datatype(),
                    false,
                    SemanticType::Tag,
                    41,
                ),
                dn_column(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                    SemanticType::Timestamp,
                    2,
                ),
                dn_column(
                    "value",
                    ConcreteDataType::float64_datatype(),
                    true,
                    SemanticType::Field,
                    99,
                ),
                dn_column(
                    "z",
                    ConcreteDataType::string_datatype(),
                    false,
                    SemanticType::Tag,
                    71,
                ),
            ],
            // This mirrors the logical metadata synthesized from alphabetically
            // ordered logical columns, with no physical reserved columns.
            vec![41, 71],
        );
        assert!(validate_target(&metadata, &contract).is_ok());
    }

    #[test]
    fn test_ordinary_engine_primary_key_ordinal_preserves_declared_order() {
        let table = primary_key_order_table_info("mito");
        let contract = table_contract(&table).unwrap();
        let ordinal = |name: &str| {
            contract
                .columns
                .iter()
                .find(|column| column.name == name)
                .unwrap()
                .primary_key_ordinal
        };
        assert_eq!(Some(0), ordinal("z"));
        assert_eq!(Some(1), ordinal("a"));
    }

    fn scalar_type(nullability: i32) -> proto::Type {
        proto::Type {
            kind: Some(proto::r#type::Kind::I64(proto::r#type::I64 {
                type_variation_reference: 0,
                nullability,
            })),
        }
    }

    #[test]
    fn test_raw_nullability_direction_is_fail_closed() {
        let nullable = Field::new("x", ArrowDataType::Int64, true);
        let required = Field::new("x", ArrowDataType::Int64, false);
        assert!(
            validate_nullability(
                &scalar_type(proto::r#type::Nullability::Required as i32),
                &nullable
            )
            .is_err()
        );
        assert!(
            validate_nullability(
                &scalar_type(proto::r#type::Nullability::Nullable as i32),
                &required
            )
            .is_ok()
        );
        assert!(validate_nullability(&scalar_type(0), &required).is_err());
    }

    #[test]
    fn test_raw_nested_nullability_direction_is_fail_closed() {
        let required_child = scalar_type(proto::r#type::Nullability::Required as i32);
        let struct_type = proto::Type {
            kind: Some(proto::r#type::Kind::Struct(proto::r#type::Struct {
                types: vec![required_child.clone()],
                type_variation_reference: 0,
                nullability: proto::r#type::Nullability::Nullable as i32,
            })),
        };
        let struct_field = Field::new(
            "s",
            ArrowDataType::Struct(
                vec![std::sync::Arc::new(Field::new(
                    "child",
                    ArrowDataType::Int64,
                    true,
                ))]
                .into(),
            ),
            false,
        );
        assert!(validate_nullability(&struct_type, &struct_field).is_err());

        let list_type = proto::Type {
            kind: Some(proto::r#type::Kind::List(Box::new(proto::r#type::List {
                r#type: Some(Box::new(required_child.clone())),
                type_variation_reference: 0,
                nullability: proto::r#type::Nullability::Nullable as i32,
            }))),
        };
        let list_field = Field::new(
            "l",
            ArrowDataType::List(std::sync::Arc::new(Field::new(
                "item",
                ArrowDataType::Int64,
                true,
            ))),
            false,
        );
        assert!(validate_nullability(&list_type, &list_field).is_err());

        let map_type = proto::Type {
            kind: Some(proto::r#type::Kind::Map(Box::new(proto::r#type::Map {
                key: Some(Box::new(scalar_type(
                    proto::r#type::Nullability::Nullable as i32,
                ))),
                value: Some(Box::new(required_child)),
                type_variation_reference: 0,
                nullability: proto::r#type::Nullability::Nullable as i32,
            }))),
        };
        let entries = Field::new(
            "entries",
            ArrowDataType::Struct(
                vec![
                    std::sync::Arc::new(Field::new("key", ArrowDataType::Int64, false)),
                    std::sync::Arc::new(Field::new("value", ArrowDataType::Int64, true)),
                ]
                .into(),
            ),
            false,
        );
        let map_field = Field::new(
            "m",
            ArrowDataType::Map(std::sync::Arc::new(entries), false),
            false,
        );
        assert!(validate_nullability(&map_type, &map_field).is_err());
    }

    #[test]
    fn test_named_struct_extracts_top_level_names_after_nested_flattening() {
        use proto::r#type::{List, Map, Struct};

        let required = || scalar_type(proto::r#type::Nullability::Required as i32);
        let nested_struct = proto::Type {
            kind: Some(proto::r#type::Kind::Struct(Struct {
                types: vec![required()],
                type_variation_reference: 0,
                nullability: proto::r#type::Nullability::Nullable as i32,
            })),
        };
        let list_of_struct = proto::Type {
            kind: Some(proto::r#type::Kind::List(Box::new(List {
                r#type: Some(Box::new(nested_struct.clone())),
                type_variation_reference: 0,
                nullability: proto::r#type::Nullability::Nullable as i32,
            }))),
        };
        let map_of_struct = proto::Type {
            kind: Some(proto::r#type::Kind::Map(Box::new(Map {
                key: Some(Box::new(required())),
                value: Some(Box::new(nested_struct.clone())),
                type_variation_reference: 0,
                nullability: proto::r#type::Nullability::Nullable as i32,
            }))),
        };
        let named = proto::NamedStruct {
            // This is the exact flattening order emitted by DataFusion 53.
            names: vec!["s", "s_child", "l", "l_child", "m", "m_value_child"]
                .into_iter()
                .map(str::to_string)
                .collect(),
            r#struct: Some(Struct {
                types: vec![nested_struct, list_of_struct, map_of_struct],
                type_variation_reference: 0,
                nullability: proto::r#type::Nullability::Required as i32,
            }),
        };
        let (names, types) =
            extract_top_level_named_struct(&named, named.r#struct.as_ref().unwrap()).unwrap();
        assert_eq!(names, vec!["s", "l", "m"]);
        assert_eq!(types.len(), 3);
    }

    #[test]
    fn test_internal_envelope_uses_generated_marker() {
        let detail = Any {
            type_url: REMOTE_READ_INTERNAL_V1_TYPE_URL.to_string(),
            value: RemoteReadInternalV1 {}.encode_to_vec().into(),
        };
        assert!(RemoteReadInternalV1::decode(detail.value.as_ref()).is_ok());
        assert_eq!(detail.type_url, REMOTE_READ_INTERNAL_V1_TYPE_URL);
    }

    fn rooted_plan() -> proto::Plan {
        proto::Plan {
            relations: vec![proto::PlanRel {
                rel_type: Some(proto::plan_rel::RelType::Root(proto::RelRoot {
                    input: Some(proto::Rel {
                        rel_type: Some(proto::rel::RelType::Read(Box::default())),
                    }),
                    names: vec![],
                })),
            }],
            ..Default::default()
        }
    }

    fn plan_with_envelope(detail: Option<Any>, input: Option<Box<proto::Rel>>) -> proto::Plan {
        proto::Plan {
            relations: vec![proto::PlanRel {
                rel_type: Some(proto::plan_rel::RelType::Root(proto::RelRoot {
                    input: Some(proto::Rel {
                        rel_type: Some(proto::rel::RelType::ExtensionSingle(Box::new(
                            proto::ExtensionSingleRel {
                                common: None,
                                detail,
                                input,
                            },
                        ))),
                    }),
                    names: vec![],
                })),
            }],
            ..Default::default()
        }
    }

    #[test]
    fn test_root_envelope_roundtrip_and_rejection_matrix() {
        let provider = mock_table_provider(RegionId::new(1024, 1));
        let contract = contract_from_current(&provider.region_metadata());
        let mut plan = rooted_plan();
        wrap_root(
            &mut plan,
            Any {
                type_url: REMOTE_READ_TABLE_V1_TYPE_URL.to_string(),
                value: contract.encode_to_vec().into(),
            },
        )
        .unwrap();
        assert!(matches!(unwrap_root(&mut plan), Ok(Envelope::Target(_))));

        let mut plan = rooted_plan();
        wrap_root(
            &mut plan,
            Any {
                type_url: REMOTE_READ_INTERNAL_V1_TYPE_URL.to_string(),
                value: RemoteReadInternalV1 {}.encode_to_vec().into(),
            },
        )
        .unwrap();
        assert!(matches!(unwrap_root(&mut plan), Ok(Envelope::Internal)));

        assert!(unwrap_root(&mut rooted_plan()).is_err());

        let input = Some(Box::new(proto::Rel {
            rel_type: Some(proto::rel::RelType::Read(Box::default())),
        }));
        assert!(unwrap_root(&mut plan_with_envelope(None, input.clone())).is_err());
        assert!(
            unwrap_root(&mut plan_with_envelope(
                Some(Any {
                    type_url: REMOTE_READ_TABLE_V1_TYPE_URL.to_string(),
                    value: vec![0xff].into(),
                }),
                input.clone(),
            ))
            .is_err()
        );
        assert!(
            unwrap_root(&mut plan_with_envelope(
                Some(Any {
                    type_url: REMOTE_READ_INTERNAL_V1_TYPE_URL.to_string(),
                    value: vec![0xff].into(),
                }),
                input.clone(),
            ))
            .is_err()
        );
        assert!(
            unwrap_root(&mut plan_with_envelope(
                Some(Any {
                    type_url: "type.googleapis.com/substrait_extension.RemoteReadTableV2"
                        .to_string(),
                    value: vec![].into(),
                }),
                input.clone(),
            ))
            .is_err()
        );
        assert!(
            unwrap_root(&mut plan_with_envelope(
                Some(Any {
                    type_url: REMOTE_READ_INTERNAL_V1_TYPE_URL.to_string(),
                    value: RemoteReadInternalV1 {}.encode_to_vec().into(),
                }),
                None,
            ))
            .is_err()
        );
    }
}
