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

use arrow_schema::DataType;
use common_function::scalars::json::json_get::JsonGetWithType;
use datafusion::datasource::DefaultTableSource;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{ExprSchema, Result, plan_datafusion_err, plan_err};
use datafusion_expr::utils::merge_schema;
use datafusion_expr::{Expr, LogicalPlan};
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};
use datatypes::extension::json::is_structured_json_field;
use datatypes::types::json_type::{JsonNativeType, JsonObjectType};

use crate::dummy_catalog::DummyTableProvider;

/// Concretize (deduce) the expected JSON type from query.
/// For example, we can concretize a JSON type of `{ a: { b: Number } }` from `select j.a.b::Int64`.
/// The JSON type will be later set into the scan request, for converting the JSON arrays.
#[derive(Debug)]
pub(crate) struct JsonTypeConcretizeRule;

impl OptimizerRule for JsonTypeConcretizeRule {
    fn name(&self) -> &str {
        "JsonTypeConcretizeRule"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        ensure_no_whole_json2_read(&plan)?;

        let json_types = deduce_json_types(&plan)?;
        if json_types.is_empty() {
            return Ok(Transformed::no(plan));
        }

        plan.transform_down(|plan| match &plan {
            LogicalPlan::TableScan(table_scan) => {
                let Some(source) = table_scan
                    .source
                    .as_any()
                    .downcast_ref::<DefaultTableSource>()
                else {
                    return Ok(Transformed::no(plan));
                };

                let Some(adapter) = source
                    .table_provider
                    .as_any()
                    .downcast_ref::<DummyTableProvider>()
                else {
                    return Ok(Transformed::no(plan));
                };

                adapter.with_json_type_hint(json_types.clone());
                Ok(Transformed::yes(plan))
            }
            _ => Ok(Transformed::no(plan)),
        })
    }
}

/// Rejects unsupported whole-column JSON2 reads in a logical plan.
fn ensure_no_whole_json2_read(plan: &LogicalPlan) -> Result<()> {
    // Reject whole JSON2 columns in the final query output, including `SELECT *`.
    for field in plan.schema().fields() {
        if is_structured_json_field(field) {
            return plan_err!(
                "Querying the whole JSON2 column '{}' is currently not supported; use json_get to select its fields",
                field.name()
            );
        }
    }

    // Reject whole JSON2 columns consumed by intermediate expressions, for example:
    // `SELECT count(*) FROM (SELECT j FROM t GROUP BY j)`.
    plan.apply(|plan| {
        let input_schema = merge_schema(&plan.inputs());
        for expr in plan.expressions() {
            // A bare column in an intermediate projection is only passed through, not consumed.
            if matches!(plan, LogicalPlan::Projection(_)) && is_passthrough_column(&expr) {
                continue;
            }

            expr.apply(|expr| {
                // For JSON2, `json_get` is allowed only with a non-empty path; skip its arguments
                // after validation.
                if let Expr::ScalarFunction(function) = expr
                    && function.name().eq_ignore_ascii_case(JsonGetWithType::NAME)
                {
                    let Some(Expr::Column(col)) = function.args.first() else {
                        return Ok(TreeNodeRecursion::Jump);
                    };
                    let Some(path) = function
                        .args
                        .get(1)
                        .and_then(Expr::as_literal)
                        .and_then(|value| value.try_as_str())
                        .flatten()
                    else {
                        return Ok(TreeNodeRecursion::Jump);
                    };
                    let reads_whole_column = path
                        .trim_start_matches('$')
                        .split('.')
                        .all(str::is_empty);
                    if !reads_whole_column {
                        return Ok(TreeNodeRecursion::Jump);
                    }

                    let field = input_schema
                        .field_from_column(col)
                        .or_else(|_| plan.schema().field_from_column(col))?;
                    if is_structured_json_field(field) {
                        return plan_err!(
                            "Querying the whole JSON2 column '{}' is currently not supported; use json_get to select its fields",
                            col.name
                        );
                    }
                    return Ok(TreeNodeRecursion::Jump);
                }

                // Any remaining JSON2 column reference is a whole-column read.
                let Expr::Column(col) = expr else {
                    return Ok(TreeNodeRecursion::Continue);
                };
                let field = input_schema
                    .field_from_column(col)
                    .or_else(|_| plan.schema().field_from_column(col))?;
                if is_structured_json_field(field) {
                    return plan_err!(
                        "Querying the whole JSON2 column '{}' is currently not supported; use json_get to select its fields",
                        col.name
                    );
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    Ok(())
}

fn is_passthrough_column(expr: &Expr) -> bool {
    match expr {
        Expr::Column(_) => true,
        Expr::Alias(alias) => is_passthrough_column(&alias.expr),
        _ => false,
    }
}

fn deduce_json_types(plan: &LogicalPlan) -> Result<HashMap<String, JsonNativeType>> {
    let mut json_types = HashMap::<String, JsonNativeType>::new();

    plan.apply(|plan| {
        for expr in plan.expressions() {
            expr.apply(|expr| {
                if let Some((column, json_type)) = deduce_json_type(expr)? {
                    json_types.entry(column).or_default().merge(&json_type);
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(json_types)
}

fn deduce_json_type(expr: &Expr) -> Result<Option<(String, JsonNativeType)>> {
    let f = match expr {
        Expr::ScalarFunction(f) if f.name().eq_ignore_ascii_case(JsonGetWithType::NAME) => f,
        _ => return Ok(None),
    };

    let Some(Expr::Column(column)) = f.args.first() else {
        return plan_err!(
            "First argument of {} is expected to be a column expr, actual: {:?}",
            JsonGetWithType::NAME,
            f.args.first()
        );
    };

    let Some(path) = f
        .args
        .get(1)
        .and_then(|expr| expr.as_literal())
        .and_then(|x| x.try_as_str())
        .flatten()
    else {
        return plan_err!(
            "Second argument of {} is expected to be a string literal, actual: {:?}",
            JsonGetWithType::NAME,
            f.args.get(1)
        );
    };

    let with_type = f
        .args
        .get(2)
        .and_then(|expr| expr.as_literal())
        .map(|x| x.data_type())
        .unwrap_or(DataType::Utf8View);
    let with_type =
        JsonNativeType::try_from(&with_type).map_err(|e| plan_datafusion_err!("{e:?}"))?;

    let mut split = path.rsplit(".");
    let Some(leaf) = split.next() else {
        return Ok(Some((column.name.clone(), JsonNativeType::String)));
    };

    let mut object = JsonObjectType::new();
    object.insert(leaf.to_string(), with_type);
    let mut root = JsonNativeType::Object(object);

    for s in split {
        let mut object = JsonObjectType::new();
        object.insert(s.to_string(), root);
        root = JsonNativeType::Object(object);
    }

    Ok(Some((column.name.clone(), root)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::SemanticType;
    use common_function::scalars::udf::create_udf;
    use datafusion::datasource::provider_as_source;
    use datafusion::functions_aggregate::expr_fn::count;
    use datafusion_common::{Column, ScalarValue};
    use datafusion_expr::expr::ScalarFunction;
    use datafusion_expr::{LogicalPlanBuilder, col, lit};
    use datafusion_optimizer::OptimizerContext;
    use datatypes::extension::json::{JsonExtensionType, JsonMetadata};
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::{ConcreteDataType, RegionId};

    use super::*;
    use crate::optimizer::test_util::{MetaRegionEngine, mock_table_provider};

    fn json_get_expr(base: Expr, path: Expr, with_type: Option<DataType>) -> Result<Expr> {
        let json_get = Arc::new(create_udf(Arc::new(JsonGetWithType::default())));
        let mut args = vec![base, path];
        if let Some(with_type) = with_type {
            let with_type = ScalarValue::try_new_null(&with_type)?;
            args.push(Expr::Literal(with_type, None));
        }
        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
            json_get, args,
        )))
    }

    fn path_expr(path: &str) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some(path.to_string())), None)
    }

    fn build_plan(exprs: Vec<Expr>) -> Result<(Arc<DummyTableProvider>, LogicalPlan)> {
        let provider = Arc::new(mock_table_provider(RegionId::new(1024, 1)));
        let plan = LogicalPlanBuilder::scan("t", provider_as_source(provider.clone()), None)?
            .project(exprs)?
            .build()?;
        Ok((provider, plan))
    }

    fn build_json2_scan() -> Result<(Arc<DummyTableProvider>, LogicalPlanBuilder)> {
        let region_id = RegionId::new(1024, 2);
        let mut builder = RegionMetadataBuilder::new(region_id);
        let mut json_column = ColumnSchema::new(
            "j",
            ConcreteDataType::json2(JsonNativeType::Object(JsonObjectType::new())),
            true,
        );
        json_column
            .with_extension_type(&JsonExtensionType::new(Arc::new(JsonMetadata::default())))
            .unwrap();
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: json_column,
                semantic_type: SemanticType::Field,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 2,
            });
        let metadata = Arc::new(builder.build().unwrap());
        let engine = Arc::new(MetaRegionEngine::with_metadata(metadata.clone()));
        let provider = Arc::new(DummyTableProvider::new(region_id, engine, metadata));
        let plan = LogicalPlanBuilder::scan("t", provider_as_source(provider.clone()), None)?;
        Ok((provider, plan))
    }

    fn build_json2_plan(exprs: Vec<Expr>) -> Result<(Arc<DummyTableProvider>, LogicalPlan)> {
        let (provider, plan) = build_json2_scan()?;
        let plan = plan.project(exprs)?.build()?;
        Ok((provider, plan))
    }

    #[test]
    fn test_json_type_concretize_rule_rewrite() -> Result<()> {
        let exprs = vec![
            json_get_expr(col("k0"), path_expr("a.b"), Some(DataType::Int64))?.alias("ab"),
            json_get_expr(col("k0"), path_expr("a.c"), None)?.alias("ac"),
            json_get_expr(col("k0"), path_expr("d"), Some(DataType::Boolean))?.alias("d"),
        ];
        let (provider, plan) = build_plan(exprs)?;

        assert!(
            JsonTypeConcretizeRule
                .rewrite(plan, &OptimizerContext::default())?
                .transformed
        );

        let expected = JsonNativeType::Object(JsonObjectType::from([
            (
                "a".to_string(),
                JsonNativeType::Object(JsonObjectType::from([
                    ("b".to_string(), JsonNativeType::i64()),
                    ("c".to_string(), JsonNativeType::String),
                ])),
            ),
            ("d".to_string(), JsonNativeType::Bool),
        ]));

        let request = provider.scan_request();
        assert_eq!(1, request.json_type_hint.len());
        assert_eq!(Some(&expected), request.json_type_hint.get("k0"));
        Ok(())
    }

    #[test]
    fn test_json_type_concretize_rule_conflict_to_variant() -> Result<()> {
        let exprs = vec![
            json_get_expr(col("k0"), path_expr("a"), Some(DataType::Int64))?.alias("a_num"),
            json_get_expr(col("k0"), path_expr("a.b"), Some(DataType::Boolean))?.alias("a_obj"),
        ];
        let (provider, plan) = build_plan(exprs)?;

        assert!(
            JsonTypeConcretizeRule
                .rewrite(plan, &OptimizerContext::default())?
                .transformed
        );

        let expected = JsonNativeType::Object(JsonObjectType::from([(
            "a".to_string(),
            JsonNativeType::Variant,
        )]));
        assert_eq!(
            Some(&expected),
            provider.scan_request().json_type_hint.get("k0")
        );
        Ok(())
    }

    #[test]
    fn test_json_type_concretize_rule_no_json_get() -> Result<()> {
        let (provider, plan) = build_plan(vec![col("k0"), col("v0")])?;

        assert!(
            !JsonTypeConcretizeRule
                .rewrite(plan, &OptimizerContext::default())?
                .transformed
        );
        assert!(provider.scan_request().json_type_hint.is_empty());
        Ok(())
    }

    #[test]
    fn test_reject_whole_json2_projection() -> Result<()> {
        for (exprs, output_name) in [
            (vec![col("j")], "j"),
            (vec![col("j").alias("json"), col("ts")], "json"),
        ] {
            let (_, plan) = build_json2_plan(exprs)?;
            let err = JsonTypeConcretizeRule
                .rewrite(plan, &OptimizerContext::default())
                .unwrap_err();
            assert!(err.to_string().contains(&format!(
                "Querying the whole JSON2 column '{output_name}' is currently not supported"
            )));
        }
        Ok(())
    }

    #[test]
    fn test_reject_whole_json2_output_without_projection() -> Result<()> {
        let (_, plan) = build_json2_scan()?;
        let plan = plan.sort(vec![col("ts").sort(true, false)])?.build()?;

        let err = JsonTypeConcretizeRule
            .rewrite(plan, &OptimizerContext::default())
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("Querying the whole JSON2 column 'j' is currently not supported")
        );
        Ok(())
    }

    #[test]
    fn test_reject_whole_json2_use_in_intermediate_plan() -> Result<()> {
        let (_, plan) = build_json2_scan()?;
        let plan = plan
            .aggregate(vec![col("j")], Vec::<Expr>::new())?
            .aggregate(Vec::<Expr>::new(), vec![count(lit(1))])?
            .build()?;

        let err = JsonTypeConcretizeRule
            .rewrite(plan, &OptimizerContext::default())
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("Querying the whole JSON2 column 'j' is currently not supported")
        );
        Ok(())
    }

    #[test]
    fn test_allow_json2_path_use_in_intermediate_plan() -> Result<()> {
        let json_get = json_get_expr(col("j"), path_expr("a"), Some(DataType::Int64))?;
        let (provider, plan) = build_json2_scan()?;
        let plan = plan
            .aggregate(vec![json_get], Vec::<Expr>::new())?
            .aggregate(Vec::<Expr>::new(), vec![count(lit(1))])?
            .build()?;

        assert!(
            JsonTypeConcretizeRule
                .rewrite(plan, &OptimizerContext::default())?
                .transformed
        );
        assert!(provider.scan_request().json_type_hint.contains_key("j"));
        Ok(())
    }

    #[test]
    fn test_allow_json2_passthrough_for_later_projection() -> Result<()> {
        let json_get = json_get_expr(col("j"), path_expr("a"), Some(DataType::Int64))?;
        let (provider, plan) = build_json2_scan()?;
        let plan = plan
            .project(vec![json_get.alias("__common_expr"), col("j")])?
            .aggregate(Vec::<Expr>::new(), vec![count(lit(1))])?
            .build()?;

        assert!(
            JsonTypeConcretizeRule
                .rewrite(plan, &OptimizerContext::default())?
                .transformed
        );
        assert!(provider.scan_request().json_type_hint.contains_key("j"));
        Ok(())
    }

    #[test]
    fn test_allow_json2_projection_by_path() -> Result<()> {
        let expr = json_get_expr(col("j"), path_expr("a"), Some(DataType::Int64))?;
        let (provider, plan) = build_json2_plan(vec![expr])?;

        assert!(
            JsonTypeConcretizeRule
                .rewrite(plan, &OptimizerContext::default())?
                .transformed
        );
        assert_eq!(
            Some(&JsonNativeType::Object(JsonObjectType::from([(
                "a".to_string(),
                JsonNativeType::i64(),
            )]))),
            provider.scan_request().json_type_hint.get("j")
        );
        Ok(())
    }

    #[test]
    fn test_reject_json2_projection_with_empty_path() -> Result<()> {
        for path in ["", "$", ".", "$."] {
            let expr = json_get_expr(col("j"), path_expr(path), Some(DataType::Utf8View))?;
            let (_, plan) = build_json2_plan(vec![expr])?;

            let err = JsonTypeConcretizeRule
                .rewrite(plan, &OptimizerContext::default())
                .unwrap_err();
            assert!(
                err.to_string()
                    .contains("Querying the whole JSON2 column 'j' is currently not supported")
            );
        }
        Ok(())
    }

    #[test]
    fn test_deduce_json_type_with_non_column_base() -> Result<()> {
        let expr = json_get_expr(
            Expr::Literal(ScalarValue::Utf8(Some("{}".to_string())), None),
            path_expr("a"),
            Some(DataType::Int64),
        )?;

        let err = deduce_json_type(&expr).unwrap_err();
        assert!(
            err.to_string()
                .contains("First argument of json_get is expected to be a column expr")
        );
        Ok(())
    }

    #[test]
    fn test_deduce_json_type_with_non_literal_path() -> Result<()> {
        let expr = json_get_expr(
            Expr::Column(Column::new_unqualified("k0")),
            Expr::Column(Column::new_unqualified("path_col")),
            Some(DataType::Int64),
        )?;

        let err = deduce_json_type(&expr).unwrap_err();
        assert!(
            err.to_string()
                .contains("Second argument of json_get is expected to be a string literal")
        );
        Ok(())
    }

    #[test]
    fn test_deduce_json_type_default_string() -> Result<()> {
        let expr = json_get_expr(
            Expr::Column(Column::new_unqualified("k0")),
            path_expr("a.b"),
            None,
        )?;

        let deduced = deduce_json_type(&expr)?;
        let expected = JsonNativeType::Object(JsonObjectType::from([(
            "a".to_string(),
            JsonNativeType::Object(JsonObjectType::from([(
                "b".to_string(),
                JsonNativeType::String,
            )])),
        )]));

        assert_eq!(Some(("k0".to_string(), expected)), deduced);
        Ok(())
    }
}
