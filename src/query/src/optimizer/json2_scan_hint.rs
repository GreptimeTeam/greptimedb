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

use common_function::scalars::json::json_get::JsonGetWithType;
use datafusion::datasource::DefaultTableSource;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Result, ScalarValue, TableReference, internal_err};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{Expr, LogicalPlan};
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};
use datatypes::data_type::ConcreteDataType;
use datatypes::json::requirement::JsonPathTarget;
use datatypes::types::JsonFormat;

use crate::dummy_catalog::DummyTableProvider;

#[derive(Debug)]
pub struct Json2ScanHintRule;

impl OptimizerRule for Json2ScanHintRule {
    fn name(&self) -> &str {
        "Json2ScanHintRule"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let requirements = Json2TypeRequirements::collect(&plan)?;
        if requirements.is_empty() {
            return Ok(Transformed::no(plan));
        }

        plan.transform_down(&mut |plan| match &plan {
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

                let hints =
                    requirements.merge(&table_scan.table_name, &adapter.region_metadata().schema);
                adapter.with_json2_type_hint(&hints);
                Ok(Transformed::yes(plan))
            }
            _ => Ok(Transformed::no(plan)),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Json2ColumnKey {
    relation: Option<TableReference>,
    name: String,
}

#[derive(Debug, Default)]
struct Json2TypeRequirements {
    path_targets: HashMap<Json2ColumnKey, JsonPathTarget>,
}

impl Json2TypeRequirements {
    fn collect(plan: &LogicalPlan) -> Result<Self> {
        let mut collector = Self::default();
        plan.apply(|node| {
            for expr in node.expressions() {
                let _ = expr.apply(|expr| {
                    if let Some((column, path, data_type)) = extract_json_get(expr)? {
                        collector
                            .path_targets
                            .entry(column)
                            .or_default()
                            .require_typed_path(&path, data_type);
                    }
                    Ok(TreeNodeRecursion::Continue)
                })?;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(collector)
    }

    fn is_empty(&self) -> bool {
        self.path_targets.is_empty()
    }

    fn merge(
        &self,
        table_name: &TableReference,
        schema: &datatypes::schema::SchemaRef,
    ) -> HashMap<String, ConcreteDataType> {
        let mut types = HashMap::new();

        for column_schema in schema.column_schemas() {
            let ConcreteDataType::Json(json_type) = &column_schema.data_type else {
                continue;
            };
            if !matches!(json_type.format, JsonFormat::Json2(_)) {
                continue;
            }

            let matching_keys = self
                .path_targets
                .iter()
                .filter(|(key, _)| {
                    key.name == column_schema.name
                        && key.relation.as_ref().is_none_or(|x| x == table_name)
                })
                .map(|(_, target)| target.clone())
                .collect::<Vec<_>>();
            if matching_keys.is_empty() {
                continue;
            }

            let mut merged = JsonPathTarget::default();
            for target in matching_keys {
                if let Some(data_type) = target.build_type() {
                    merge_path_target_from_type(&mut merged, &data_type, "");
                }
            }
            if let Some(data_type) = merged.build_type() {
                let _ = types.insert(column_schema.name.clone(), data_type);
            }
        }

        types
    }
}

fn extract_json_get(expr: &Expr) -> Result<Option<(Json2ColumnKey, String, ConcreteDataType)>> {
    let Expr::ScalarFunction(ScalarFunction { func, args }) = expr else {
        return Ok(None);
    };
    if func.name() != JsonGetWithType::NAME {
        return Ok(None);
    }
    if args.len() != 3 {
        return internal_err!("function {} must have 3 arguments", JsonGetWithType::NAME);
    }

    let Expr::Column(column) = &args[0] else {
        return Ok(None);
    };

    let path = match &args[1] {
        Expr::Literal(ScalarValue::Utf8(Some(path)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(path)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(path)), _) => path.clone(),
        _ => return Ok(None),
    };

    let data_type = args
        .get(2)
        .and_then(extract_expected_type)
        .unwrap_or_else(ConcreteDataType::string_datatype);

    Ok(Some((
        Json2ColumnKey {
            relation: column.relation.clone(),
            name: column.name.clone(),
        },
        path,
        data_type,
    )))
}

fn extract_expected_type(expr: &Expr) -> Option<ConcreteDataType> {
    match expr {
        Expr::Literal(value, _) => {
            let data_type = value.data_type();
            Some(ConcreteDataType::from_arrow_type(&data_type))
        }
        _ => None,
    }
}

fn merge_path_target_from_type(
    target: &mut JsonPathTarget,
    data_type: &ConcreteDataType,
    prefix: &str,
) {
    match data_type {
        ConcreteDataType::Struct(struct_type) => {
            let fields = struct_type.fields();
            for field in fields.iter() {
                let path = if prefix.is_empty() {
                    field.name().to_string()
                } else {
                    format!("{prefix}.{}", field.name())
                };
                merge_path_target_from_type(target, field.data_type(), &path);
            }
        }
        _ => {
            if !prefix.is_empty() {
                target.require_typed_path(prefix, data_type.clone());
            }
        }
    }
}
