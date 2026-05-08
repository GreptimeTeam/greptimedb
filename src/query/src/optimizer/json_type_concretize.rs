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
use datafusion_common::{Result, plan_datafusion_err, plan_err};
use datafusion_expr::{Expr, LogicalPlan};
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};
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
            "First argument of {} is expected to be a column expr, actual: {}",
            JsonGetWithType::NAME,
            f.args[0]
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
            "Second argument of {} is expected to be a string literal, actual: {}",
            JsonGetWithType::NAME,
            f.args[1]
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

    use common_function::scalars::udf::create_udf;
    use datafusion::datasource::provider_as_source;
    use datafusion_common::{Column, ScalarValue};
    use datafusion_expr::expr::ScalarFunction;
    use datafusion_expr::{LogicalPlanBuilder, col};
    use datafusion_optimizer::OptimizerContext;
    use store_api::storage::RegionId;

    use super::*;
    use crate::optimizer::test_util::mock_table_provider;

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
