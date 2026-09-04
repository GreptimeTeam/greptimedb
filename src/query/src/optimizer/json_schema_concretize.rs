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

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::config::ConfigOptions;
use datafusion_common::tree_node::Transformed;
use datafusion_common::{DFSchema, DFSchemaRef, Result};
use datafusion_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_optimizer::analyzer::AnalyzerRule;
use datatypes::extension::json::{
    Json2ExtensionType, is_json2_extension_type, is_legacy_json2_extension_type,
};
use datatypes::types::json_type::JsonNativeType;

use crate::dist_plan::MergeScanLogicalPlan;
use crate::optimizer::json_type_concretize::deduce_json_types;

/// Keeps JSON2 schemas consistent across distributed query boundaries.
///
/// An unresolved JSON2 column is represented as an empty `Struct`, while a remote stage always
/// emits a concrete Arrow array. DataFusion expects the schema declared by a logical node, the
/// schema used to build its physical plan, and the schema of its record batches to agree. This rule
/// gives each [`MergeScanLogicalPlan`] the concrete schema emitted by its remote stage and propagates
/// that schema through the local plan.
///
/// For example:
///
/// - `SELECT j FROM t` transfers the complete JSON2 value as `Binary` (`Variant`). A projection or
///   window above the MergeScan must therefore also describe `j` as `Binary`, not an empty `Struct`.
/// - `SELECT j.a FROM t` may transfer the complete `j` as `Binary` and extract `a` locally, or
///   transfer only the extracted scalar when the expression runs remotely. The boundary schema
///   must describe the remote output rather than the local expression that consumes it.
/// - `SELECT l.j, r.j FROM l JOIN r ON l.k = r.k` has two independent boundaries. Each input and the
///   join schema must agree on the concrete type of its JSON2 column.
///
/// Correcting only the physical MergeScan schema can make simple queries work because many
/// operators access columns by position, but it leaves the logical plan describing a different
/// type. Keeping the schemas consistent lets optimizers, physical planners, validators, and future
/// type-aware operators rely on the normal DataFusion contract. It also keeps the generic physical
/// MergeScan implementation independent of JSON2.
///
/// The boundary schema and the storage read layout answer different questions. For
/// `SELECT j.a::BIGINT FROM t`, the storage scan may use a structured `{a: Int64}` layout, while the
/// distributed boundary can still emit either the complete `j` as `Binary` or only `a` as `Int64`.
/// Therefore this rule cannot replace the separate JSON2 scan-type inference.
#[derive(Debug)]
pub(crate) struct JsonSchemaConcretizeRule;

impl AnalyzerRule for JsonSchemaConcretizeRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        let plan = plan.transform_up_with_subqueries(|plan| {
            let LogicalPlan::Extension(mut extension) = plan else {
                return Ok(Transformed::no(plan));
            };
            let Some(merge_scan) = extension
                .node
                .as_any()
                .downcast_ref::<MergeScanLogicalPlan>()
            else {
                return Ok(Transformed::no(LogicalPlan::Extension(extension)));
            };

            // Infer the boundary schema from the hidden remote plan, not its local consumer.
            let json_types = deduce_json_types(merge_scan.input())?;
            if json_types.is_empty() {
                return Ok(Transformed::no(LogicalPlan::Extension(extension)));
            }
            let schema = concretize_json2_schema(merge_scan.schema(), &json_types)?;
            if schema.as_ref() == merge_scan.schema().as_ref() {
                return Ok(Transformed::no(LogicalPlan::Extension(extension)));
            }

            extension.node = Arc::new(merge_scan.clone().with_output_schema(schema));
            Ok(Transformed::yes(LogicalPlan::Extension(extension)))
        })?;

        if plan.transformed {
            plan.data
                .transform_up_with_subqueries(|plan| {
                    if matches!(plan, LogicalPlan::Extension(_)) || plan.inputs().is_empty() {
                        Ok(Transformed::no(plan))
                    } else {
                        plan.recompute_schema().map(Transformed::yes)
                    }
                })
                .map(|x| x.data)
        } else {
            Ok(plan.data)
        }
    }

    fn name(&self) -> &str {
        "JsonSchemaConcretizeRule"
    }
}

fn concretize_json2_schema(
    schema: &DFSchemaRef,
    json_types: &HashMap<String, JsonNativeType>,
) -> Result<DFSchemaRef> {
    if !schema
        .iter()
        .any(|(_, field)| json_types.contains_key(field.name()) && is_json2_extension_type(field))
    {
        return Ok(schema.clone());
    }

    let mut changed = false;
    let fields = schema
        .iter()
        .map(|(qualifier, field)| {
            let Some(json_type) = json_types
                .get(field.name())
                .filter(|_| is_json2_extension_type(field))
            else {
                return (qualifier.cloned(), field.clone());
            };
            let data_type = json_type.as_arrow_type();
            if field.data_type() == &data_type {
                return (qualifier.cloned(), field.clone());
            }

            changed = true;

            // Before type hints, JSON2 used the `greptime.json` marker together with
            // `json_structure_settings`. Once concretized to `Binary`, that field no longer
            // matches the legacy JSON2 shape and could be mistaken for JSONB, so upgrade its
            // marker. Do not replace modern markers because that would discard their JSON
            // settings and layout version.
            let legacy = is_legacy_json2_extension_type(field);
            let mut field = field.as_ref().clone().with_data_type(data_type);
            if legacy {
                field = field.with_extension_type(Json2ExtensionType::default());
            }
            (qualifier.cloned(), Arc::new(field))
        })
        .collect();

    if changed {
        let schema = DFSchema::new_with_metadata(fields, schema.metadata().clone())?
            .with_functional_dependencies(schema.functional_dependencies().clone())?;
        Ok(Arc::new(schema))
    } else {
        Ok(schema.clone())
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::extension::{
        EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY, ExtensionType,
    };
    use arrow_schema::{DataType, Field, Fields, Schema};
    use datafusion_common::DFSchema;
    use datafusion_expr::{LogicalPlanBuilder, col};
    use datatypes::extension::json::{JsonExtensionType, is_json2_extension_type};

    use super::*;

    #[test]
    fn test_json_schema_concretize_rule_updates_merge_scan() -> Result<()> {
        let field = Field::new("j", DataType::Struct(Fields::empty()), true).with_metadata(
            HashMap::from([
                (
                    EXTENSION_TYPE_NAME_KEY.to_string(),
                    JsonExtensionType::NAME.to_string(),
                ),
                (
                    EXTENSION_TYPE_METADATA_KEY.to_string(),
                    serde_json::json!({
                        "json_structure_settings": { "Structured": null }
                    })
                    .to_string(),
                ),
            ]),
        );
        let schema = Arc::new(DFSchema::try_from(Schema::new(vec![field]))?);
        let input = LogicalPlan::EmptyRelation(datafusion_expr::logical_plan::EmptyRelation {
            produce_one_row: false,
            schema,
        });
        let merge_scan =
            MergeScanLogicalPlan::new(input, false, Default::default()).into_logical_plan();
        let plan = LogicalPlanBuilder::from(merge_scan)
            .project(vec![col("j")])?
            .build()?;

        let plan = JsonSchemaConcretizeRule.analyze(plan, &ConfigOptions::default())?;
        let field = plan.schema().field(0);
        assert_eq!(&DataType::Binary, field.data_type());
        assert_eq!(Some(Json2ExtensionType::NAME), field.extension_type_name());
        assert!(is_json2_extension_type(field));
        Ok(())
    }
}
