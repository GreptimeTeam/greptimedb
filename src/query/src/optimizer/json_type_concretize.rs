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

//! Applies JSON2 type hints to untyped JSON path accesses and deduces storage read types.
//!
//! An explicit `json_get` type, injected from a SQL `CAST`, takes precedence over a JSON2 type
//! hint. Without an explicit type, an exact type-hint path is injected into `json_get`, making it
//! both the expression result type and storage read type. Unhinted paths retain `STRING`.

use std::any::Any;
use std::collections::HashMap;

use arrow_schema::DataType;
use arrow_schema::extension::ExtensionType;
use common_function::scalars::json::json_get::{JsonGetWithType, parse_json_get_path};
use datafusion::datasource::{DefaultTableSource, TableProvider};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Result, ScalarValue, plan_datafusion_err, plan_err};
use datafusion_expr::{Expr, LogicalPlan};
use datafusion_optimizer::analyzer::AnalyzerRule;
use datafusion_optimizer::utils::NamePreserver;
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};
use datatypes::extension::json::{
    Json2ExtensionType, is_json2_extension_type, parse_legacy_json2_settings,
};
use datatypes::json::JsonSettings;
use datatypes::types::json_type::{JsonNativeType, JsonObjectType};
use jsonb::jsonpath::Path;
use table::table::adapter::DfTableProviderAdapter;

use crate::dummy_catalog::DummyTableProvider;

/// Concretize (deduce) the expected JSON type from query.
/// For example, we can concretize a JSON type of `{ a: { b: Number } }` from `select j.a.b::Int64`.
/// The JSON type will be later set into the scan request, for converting the JSON arrays.
#[derive(Debug)]
pub(crate) struct JsonTypeConcretizeRule;

/// Injects JSON2 type hints before distributed planning observes JSON path expressions.
#[derive(Debug)]
pub(crate) struct JsonTypeHintRule;

impl AnalyzerRule for JsonTypeHintRule {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &datafusion::config::ConfigOptions,
    ) -> Result<LogicalPlan> {
        inject_json_type_hints(plan).map(|transformed| transformed.data)
    }

    fn name(&self) -> &str {
        "JsonTypeHintRule"
    }
}

impl OptimizerRule for JsonTypeConcretizeRule {
    fn name(&self) -> &str {
        "JsonTypeConcretizeRule"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        inject_json_type_hints(plan)?.transform_data(|plan| {
            let json_types = deduce_json_types(&plan)?;
            if json_types.is_empty() {
                return Ok(Transformed::no(plan));
            }

            plan.transform_down(|plan| match &plan {
                LogicalPlan::TableScan(table_scan) => {
                    let Some(source) = table_scan.source.downcast_ref::<DefaultTableSource>()
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
        })
    }
}

/// Adds the type-hint type argument to untyped JSON2 path accesses.
///
/// The third `json_get` argument is the expression result type as well as the storage read type.
/// Never replace an existing argument: it represents an explicit SQL cast (or another prior type
/// coercion) and must take precedence over a JSON2 type hint.
pub(crate) fn inject_json_type_hints(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let json_type_hints = collect_json_type_hints(&plan)?;
    plan.transform_up(|plan| {
        let mut changed = false;
        let name_preserver = NamePreserver::new(&plan);
        let expressions = plan
            .expressions()
            .into_iter()
            .map(|expr| {
                let original_name = name_preserver.save(&expr);
                let transformed = expr.transform_up(|mut expr| {
                    let Expr::ScalarFunction(function) = &mut expr else {
                        return Ok(Transformed::no(expr));
                    };
                    if !function.name().eq_ignore_ascii_case(JsonGetWithType::NAME)
                        || function.args.len() != 2
                    {
                        return Ok(Transformed::no(expr));
                    }

                    let Some(Expr::Column(column)) = function.args.first() else {
                        return Ok(Transformed::no(expr));
                    };
                    let Some(path) = json_get_path(function) else {
                        return Ok(Transformed::no(expr));
                    };
                    let Some(json_type) = json_type_from_hint(&json_type_hints, &column.name, path)
                    else {
                        return Ok(Transformed::no(expr));
                    };

                    let type_arg = ScalarValue::try_new_null(&json_type.as_arrow_type())?;
                    function.args.push(Expr::Literal(type_arg, None));
                    Ok(Transformed::yes(expr))
                })?;
                changed |= transformed.transformed;
                Ok(original_name.restore(transformed.data))
            })
            .collect::<Result<Vec<_>>>()?;

        if changed {
            let inputs = plan.inputs().into_iter().cloned().collect();
            Ok(Transformed::yes(plan.with_new_exprs(expressions, inputs)?))
        } else {
            Ok(Transformed::no(plan))
        }
    })
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

    if let Some(adapter) = (provider as &dyn Any).downcast_ref::<DummyTableProvider>() {
        adapter.with_json_type_hint(json_types);
        return true;
    }

    if let Some(adapter) = (provider as &dyn Any).downcast_ref::<DfTableProviderAdapter>() {
        adapter.with_json_type_hint(json_types);
        return true;
    }

    false
}

pub(crate) fn deduce_json_types(plan: &LogicalPlan) -> Result<HashMap<String, JsonNativeType>> {
    let mut json_types = HashMap::<String, JsonNativeType>::new();
    let json_type_hints = collect_json_type_hints(plan)?;

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
                if let Some((column, json_type)) = deduce_json_type(expr, &json_type_hints)? {
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

/// Collects JSON2 type hints from table-scan schemas.
fn collect_json_type_hints(plan: &LogicalPlan) -> Result<HashMap<String, JsonSettings>> {
    let mut json_type_hints = HashMap::new();

    plan.apply(|plan| {
        let LogicalPlan::TableScan(table_scan) = plan else {
            return Ok(TreeNodeRecursion::Continue);
        };
        let Some(source) = table_scan.source.downcast_ref::<DefaultTableSource>() else {
            return Ok(TreeNodeRecursion::Continue);
        };

        for field in source.table_provider.schema().fields() {
            if !is_json2_extension_type(field) {
                continue;
            }
            let settings = if field.extension_type_name() == Some(Json2ExtensionType::NAME) {
                let extension = field
                    .try_extension_type::<Json2ExtensionType>()
                    .map_err(|e| plan_datafusion_err!("invalid JSON2 extension metadata: {e}"))?;
                Some(extension.metadata().json_settings().clone())
            } else {
                parse_legacy_json2_settings(field.metadata())
                    .map_err(|e| plan_datafusion_err!("invalid JSON2 extension metadata: {e}"))?
            };
            if let Some(settings) = settings {
                json_type_hints.insert(field.name().clone(), settings);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    Ok(json_type_hints)
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

fn json_get_path(function: &datafusion_expr::expr::ScalarFunction) -> Option<&str> {
    function
        .args
        .get(1)
        .and_then(|expr| expr.as_literal())
        .and_then(|value| value.try_as_str())
        .flatten()
}

fn json_type_from_hint(
    json_type_hints: &HashMap<String, JsonSettings>,
    column: &str,
    path: &str,
) -> Option<JsonNativeType> {
    if path.contains('[') {
        return None;
    }

    json_type_hints.get(column).and_then(|settings| {
        settings
            .type_hints()
            .iter()
            .find(|hint| hint.path.iter().map(String::as_str).eq(path.split('.')))
            .map(|hint| JsonNativeType::from(&hint.data_type))
    })
}

fn deduce_json_type(
    expr: &Expr,
    json_type_hints: &HashMap<String, JsonSettings>,
) -> Result<Option<(String, JsonNativeType)>> {
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

    let Some(path) = json_get_path(f) else {
        return plan_err!(
            "Second argument of {} is expected to be a string literal, actual: {:?}",
            JsonGetWithType::NAME,
            f.args.get(1)
        );
    };

    let path = parse_json_get_path(path)
        .map_err(|e| plan_datafusion_err!("Invalid JSONPath {path:?}: {e}"))?;

    if path
        .paths
        .iter()
        .all(|segment| matches!(segment, Path::Root))
    {
        return Ok(Some((column.name.clone(), JsonNativeType::String)));
    }

    let with_type = f
        .args
        .get(2)
        .and_then(|expr| expr.as_literal())
        .map(|x| x.data_type())
        .map(|with_type| {
            JsonNativeType::try_from(&with_type).map_err(|e| plan_datafusion_err!("{e:?}"))
        })
        .transpose()?
        .or_else(|| json_type_from_hint(json_type_hints, &column.name, path))
        .unwrap_or(JsonNativeType::String);

    let mut root = with_type;
    for segment in path.paths.into_iter().rev() {
        let name = match segment {
            Path::Root => continue,
            Path::DotField(name) | Path::ColonField(name) | Path::ObjectField(name) => name,
            // A full JSONPath expression can select arrays or use filters/wildcards.
            // Keep the entire value when an object projection cannot represent it.
            _ => return Ok(Some((column.name.clone(), JsonNativeType::Variant))),
        };
        let mut object = JsonObjectType::new();
        object.insert(name.into_owned(), root);
        root = JsonNativeType::Object(object);
    }

    Ok(Some((column.name.clone(), root)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::SemanticType;
    use arrow_schema::DataType;
    use common_function::scalars::udf::create_udf;
    use datafusion::datasource::provider_as_source;
    use datafusion::functions_aggregate::expr_fn::count;
    use datafusion_common::{Column, ScalarValue};
    use datafusion_expr::expr::ScalarFunction;
    use datafusion_expr::{LogicalPlanBuilder, col, lit};
    use datafusion_optimizer::OptimizerContext;
    use datatypes::extension::json::{Json2ExtensionType, JsonMetadata};
    use datatypes::json::{JsonSettings, JsonTypeHint};
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
        build_json2_scan_with_settings(JsonSettings::default())
    }

    fn build_json2_scan_with_settings(
        settings: JsonSettings,
    ) -> Result<(Arc<DummyTableProvider>, LogicalPlanBuilder)> {
        let region_id = RegionId::new(1024, 2);
        let mut builder = RegionMetadataBuilder::new(region_id);
        let mut json_column = ColumnSchema::new(
            "j",
            ConcreteDataType::json2(JsonNativeType::Object(JsonObjectType::new())),
            true,
        );
        json_column.with_extension_type(&Json2ExtensionType::new(Arc::new(JsonMetadata::new(
            settings,
        ))));
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

    fn json_type_hint(path: &[&str], data_type: ConcreteDataType) -> JsonTypeHint {
        JsonTypeHint {
            path: path.iter().map(ToString::to_string).collect(),
            data_type,
            nullable: true,
            default_constraint: None,
            inverted_index: false,
        }
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
    fn test_deduce_json_type_object_paths() -> Result<()> {
        for paths in [
            ["a.b", r#"$."a"."b""#, r#"["a"]["b"]"#],
            [r#"$."a.b"."c.d""#, r#"["a.b"]["c.d"]"#, r#"$."a.b"["c.d"]"#],
        ] {
            let deduce = |path| {
                deduce_json_type(&json_get_expr(
                    col("j"),
                    path_expr(path),
                    Some(DataType::Int64),
                )?)
            };
            let expected = deduce(paths[0])?;
            assert!(!matches!(expected, Some((_, JsonNativeType::Variant))));
            for path in &paths[1..] {
                assert_eq!(deduce(path)?, expected);
            }
        }
        Ok(())
    }

    #[test]
    fn test_deduce_json_type_invalid_path() -> Result<()> {
        let expr = json_get_expr(col("j"), path_expr("$.a["), Some(DataType::Int64))?;
        let err = deduce_json_type(&expr).unwrap_err();
        assert!(err.to_string().contains("Invalid JSONPath"), "{err}");
        Ok(())
    }

    #[test]
    fn test_deduce_json_type_with_list_index() -> Result<()> {
        for path in [
            "l[0]",
            "$.l[0]",
            "$.l[*]",
            "$.o.*",
            "$.l[0 to 2]",
            "$.l ? (@.a == 1)",
        ] {
            let expr = json_get_expr(col("j"), path_expr(path), Some(DataType::Int64))?;
            assert_eq!(
                Some(("j".to_string(), JsonNativeType::Variant)),
                deduce_json_type(&expr, &HashMap::new())?,
                "{path}"
            );
        }
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
    fn test_json_type_hint_supplies_untyped_json_get_read_type() -> Result<()> {
        let settings = JsonSettings::try_new(
            vec![json_type_hint(&["a"], ConcreteDataType::int64_datatype())],
            None,
        )
        .map_err(|e| plan_datafusion_err!("{e}"))?;
        let (provider, plan) = build_json2_scan_with_settings(settings)?;
        let plan = plan
            .project(vec![json_get_expr(col("j"), path_expr("a"), None)?])?
            .build()?;

        let rewritten = JsonTypeConcretizeRule.rewrite(plan, &OptimizerContext::default())?;
        assert!(rewritten.transformed);
        assert_eq!(
            rewritten.data.schema().field(0).data_type(),
            &DataType::Int64
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
    fn test_explicit_json_get_type_overrides_json_type_hint() -> Result<()> {
        let settings = JsonSettings::try_new(
            vec![json_type_hint(&["a"], ConcreteDataType::int64_datatype())],
            None,
        )
        .map_err(|e| plan_datafusion_err!("{e}"))?;
        let (provider, plan) = build_json2_scan_with_settings(settings)?;
        let plan = plan
            .project(vec![json_get_expr(
                col("j"),
                path_expr("a"),
                Some(DataType::Utf8View),
            )?])?
            .build()?;

        let rewritten = JsonTypeConcretizeRule.rewrite(plan, &OptimizerContext::default())?;
        assert!(rewritten.transformed);
        assert_eq!(
            rewritten.data.schema().field(0).data_type(),
            &DataType::Utf8View
        );
        assert_eq!(
            Some(&JsonNativeType::Object(JsonObjectType::from([(
                "a".to_string(),
                JsonNativeType::String,
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

        let err = deduce_json_type(&expr, &HashMap::new()).unwrap_err();
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

        let err = deduce_json_type(&expr, &HashMap::new()).unwrap_err();
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

        let deduced = deduce_json_type(&expr, &HashMap::new())?;
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

    #[test]
    fn test_unhinted_json_get_keeps_string_return_type() -> Result<()> {
        let (provider, plan) =
            build_json2_plan(vec![json_get_expr(col("j"), path_expr("a"), None)?])?;

        let rewritten = JsonTypeConcretizeRule.rewrite(plan, &OptimizerContext::default())?;
        assert!(rewritten.transformed);
        assert_eq!(
            rewritten.data.schema().field(0).data_type(),
            &DataType::Utf8View
        );
        assert_eq!(
            Some(&JsonNativeType::Object(JsonObjectType::from([(
                "a".to_string(),
                JsonNativeType::String,
            )]))),
            provider.scan_request().json_type_hint.get("j")
        );
        Ok(())
    }
}
