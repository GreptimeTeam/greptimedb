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

//! Applies JSON2 type hints to `json_get` expressions.
//!
//! For example, with `j JSON2 (a BIGINT)`, `SELECT j.a FROM t` initially plans
//! as `json_get(j, "$.a")`. This rule rewrites it to `json_get(j, "$.a", NULL::Int64)`.
//!
//! This rule should run before distributed planning so remote plans carry
//! matching JSON2 type hints. Existing explicit result types are preserved.

use std::collections::HashMap;

use arrow_schema::extension::ExtensionType;
use common_function::scalars::json::json_get::{JsonGetWithType, parse_json_get_path};
use datafusion::config::ConfigOptions;
use datafusion::datasource::DefaultTableSource;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Result, ScalarValue, plan_datafusion_err};
use datafusion_expr::{Expr, LogicalPlan};
use datafusion_optimizer::analyzer::AnalyzerRule;
use datafusion_optimizer::utils::NamePreserver;
use datatypes::extension::json::{
    Json2ExtensionType, is_json2_extension_type, parse_legacy_json2_settings,
};
use datatypes::json::JsonSettings;
use datatypes::types::json_type::JsonNativeType;
use jsonb::jsonpath::Path;

/// Applies JSON2 type hints to untyped `json_get` expressions.
#[derive(Debug)]
pub(crate) struct JsonGetTypeHintRule;

impl AnalyzerRule for JsonGetTypeHintRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        inject_json_get_type_hints(plan).map(|transformed| transformed.data)
    }

    fn name(&self) -> &str {
        "JsonGetTypeHintRule"
    }
}

/// Adds matching JSON2 type hints to untyped path accesses.
///
/// The third `json_get` argument is the expression result type as well as the
/// storage read type. Never replace an existing argument: it represents an
/// explicit SQL cast (or another prior type coercion) and must take precedence
/// over a JSON2 type hint. Paths without a matching hint remain untyped so
/// expression planning can infer their type from context.
pub(crate) fn inject_json_get_type_hints(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
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

        if !changed {
            return Ok(Transformed::no(plan));
        }

        let inputs = plan.inputs().into_iter().cloned().collect();
        Ok(Transformed::yes(plan.with_new_exprs(expressions, inputs)?))
    })
}

/// Collects JSON2 type hints from table-scan schemas.
///
/// FIXME(fys): Key settings by qualified column or table-scan identity so
/// same-named JSON2 columns in joins do not overwrite each other.
pub(crate) fn collect_json_type_hints(plan: &LogicalPlan) -> Result<HashMap<String, JsonSettings>> {
    let mut json_type_hints = HashMap::new();

    plan.apply(|plan| {
        let LogicalPlan::TableScan(table_scan) = plan else {
            return Ok(TreeNodeRecursion::Continue);
        };

        let Some(table_source) = table_scan.source.downcast_ref::<DefaultTableSource>() else {
            return Ok(TreeNodeRecursion::Continue);
        };

        for field in table_source.table_provider.schema().fields() {
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

            json_type_hints.insert(field.name().clone(), settings.unwrap_or_default());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    Ok(json_type_hints)
}

/// Returns the literal JSON path argument of a `json_get` call.
pub(crate) fn json_get_path(function: &datafusion_expr::expr::ScalarFunction) -> Option<&str> {
    function
        .args
        .get(1)
        .and_then(|expr| expr.as_literal())
        .and_then(|value| value.try_as_str())
        .flatten()
}

/// Returns the configured type for an exact JSON2 hint path.
pub(crate) fn json_type_from_hint(
    json_type_hints: &HashMap<String, JsonSettings>,
    column: &str,
    path: &str,
) -> Option<JsonNativeType> {
    let path = parse_json_get_path(path).ok()?;
    let mut segments = Vec::with_capacity(path.paths.len());
    for segment in path.paths {
        match segment {
            Path::Root => {}
            Path::DotField(name) | Path::ColonField(name) | Path::ObjectField(name) => {
                segments.push(name);
            }
            _ => return None,
        }
    }

    json_type_hints.get(column).and_then(|settings| {
        settings
            .type_hints()
            .iter()
            .find(|hint| {
                hint.path
                    .iter()
                    .map(String::as_str)
                    .eq(segments.iter().map(AsRef::as_ref))
            })
            .map(|hint| JsonNativeType::from(&hint.data_type))
    })
}
