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
use datafusion::datasource::{DefaultTableSource, TableProvider};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Result, plan_datafusion_err, plan_err};
use datafusion_expr::{Expr, LogicalPlan};
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};
use datatypes::extension::json::is_json2_extension_type;
use datatypes::types::json_type::{JsonNativeType, JsonObjectType};
use table::table::adapter::DfTableProviderAdapter;

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

                if apply_json_type_hint(source.table_provider.as_ref(), &json_types) {
                    Ok(Transformed::yes(plan))
                } else {
                    Ok(Transformed::no(plan))
                }
            }
            _ => Ok(Transformed::no(plan)),
        })
    }
}

// FIXME: `json_types` is keyed only by unqualified column name. In joins with
// same-named JSON2 columns, a hint deduced from one scan can be applied to
// another scan. Carry the originating relation/scan when deducing hints.
/// Applies JSON type hints to providers that can carry scan request hints.
///
/// Returns `true` if at least one JSON2 hint is retained and written to the provider.
fn apply_json_type_hint(
    provider: &dyn TableProvider,
    json_types: &HashMap<String, JsonNativeType>,
) -> bool {
    let schema = provider.schema();
    let json_types = json_types
        .iter()
        .filter(|(column, _)| {
            schema
                .fields()
                .iter()
                .any(|field| field.name() == *column && is_json2_extension_type(field))
        })
        .map(|(column, json_type)| (column.clone(), json_type.clone()))
        .collect::<HashMap<_, _>>();

    if json_types.is_empty() {
        return false;
    }

    if let Some(adapter) = provider.as_any().downcast_ref::<DummyTableProvider>() {
        adapter.with_json_type_hint(json_types);
        return true;
    }

    if let Some(adapter) = provider.as_any().downcast_ref::<DfTableProviderAdapter>() {
        adapter.with_json_type_hint(json_types);
        return true;
    }

    false
}

fn deduce_json_types(plan: &LogicalPlan) -> Result<HashMap<String, JsonNativeType>> {
    let mut json_types = HashMap::<String, JsonNativeType>::new();

    // JSON2 columns in the final output must retain their complete values even when
    // predicates or other expressions access only specific paths.
    // For example, `SELECT j FROM t WHERE json_get(j, 'a') = 1`.
    plan.schema()
        .fields()
        .iter()
        .filter(|field| is_json2_extension_type(field))
        .for_each(|field| {
            json_types.insert(field.name().clone(), JsonNativeType::Variant);
        });

    plan.apply(|plan| {
        for expr in plan.expressions() {
            // Optimizer-generated projections may keep the JSON root only so later json_get
            // expressions can access another path. A same-name pass-through does not require the
            // complete root by itself; any real whole-column consumer above it is visited
            // separately, and a whole root in the final output is captured from the plan schema.
            if matches!(plan, LogicalPlan::Projection(_)) && is_same_name_column_projection(&expr) {
                continue;
            }
            expr.apply(|expr| {
                if let Some((column, json_type)) = deduce_json_type(expr)? {
                    json_types.entry(column).or_default().merge(&json_type);
                    Ok(TreeNodeRecursion::Jump)
                } else {
                    Ok(TreeNodeRecursion::Continue)
                }
            })?;
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(json_types)
}

fn is_same_name_column_projection(expr: &Expr) -> bool {
    match expr {
        Expr::Column(_) => true,
        Expr::Alias(alias) => {
            matches!(alias.expr.as_ref(), Expr::Column(column) if column.name == alias.name)
        }
        _ => false,
    }
}

fn deduce_json_type(expr: &Expr) -> Result<Option<(String, JsonNativeType)>> {
    let f = match expr {
        Expr::ScalarFunction(f) if f.name().eq_ignore_ascii_case(JsonGetWithType::NAME) => f,
        Expr::Column(c) => return Ok(Some((c.name.clone(), JsonNativeType::Variant))),
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
    let Some(leaf) = split.next().filter(|&x| !x.is_empty() && x != "$") else {
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
    use datatypes::extension::json::Json2ExtensionType;
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
        json_column.with_extension_type(&Json2ExtensionType::default());
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
            json_get_expr(col("j"), path_expr("a.b"), Some(DataType::Int64))?.alias("ab"),
            json_get_expr(col("j"), path_expr("a.c"), None)?.alias("ac"),
            json_get_expr(col("j"), path_expr("d"), Some(DataType::Boolean))?.alias("d"),
        ];
        let (provider, plan) = build_json2_plan(exprs)?;

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
        assert_eq!(Some(&expected), request.json_type_hint.get("j"));
        Ok(())
    }

    #[test]
    fn test_json_type_concretize_rule_conflict_to_variant() -> Result<()> {
        let exprs = vec![
            json_get_expr(col("j"), path_expr("a"), Some(DataType::Int64))?.alias("a_num"),
            json_get_expr(col("j"), path_expr("a.b"), Some(DataType::Boolean))?.alias("a_obj"),
        ];
        let (provider, plan) = build_json2_plan(exprs)?;

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
            provider.scan_request().json_type_hint.get("j")
        );
        Ok(())
    }

    #[test]
    fn test_json_type_concretize_rule_ignores_non_json2_columns() -> Result<()> {
        let exprs =
            vec![json_get_expr(col("k0"), path_expr("a.b"), Some(DataType::Int64))?.alias("ab")];
        let (provider, plan) = build_plan(exprs)?;

        assert!(
            !JsonTypeConcretizeRule
                .rewrite(plan, &OptimizerContext::default())?
                .transformed
        );
        assert!(provider.scan_request().json_type_hint.is_empty());
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
    fn test_allow_json2_filter_with_root_projection() -> Result<()> {
        let predicate =
            json_get_expr(col("j"), path_expr("a"), Some(DataType::Int64))?.eq(lit(1_i64));
        let (provider, plan) = build_json2_scan()?;
        let plan = plan.filter(predicate)?.build()?;

        assert!(
            JsonTypeConcretizeRule
                .rewrite(plan, &OptimizerContext::default())?
                .transformed
        );
        assert_eq!(
            Some(&JsonNativeType::Variant),
            provider.scan_request().json_type_hint.get("j")
        );
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
