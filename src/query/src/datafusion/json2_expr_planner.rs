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

use std::sync::Arc;

use arrow_schema::Field;
use arrow_schema::extension::ExtensionType;
use common_function::scalars::json::json_get::JsonGetWithType;
use common_function::scalars::udf::create_udf;
use datafusion_common::arrow::datatypes::DataType;
use datafusion_common::{Column, DataFusionError, Result, ScalarValue, TableReference};
use datafusion_expr::expr::{BinaryExpr, ScalarFunction};
use datafusion_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr};
use datafusion_expr::{Expr, ExprSchemable, Operator};
use datatypes::extension::json::JsonExtensionType;
use sqlparser::ast::BinaryOperator;

#[derive(Debug)]
pub(crate) struct Json2ExprPlanner;

fn json_get(base: Expr, path: String) -> Result<Expr> {
    let args = vec![
        base,
        Expr::Literal(ScalarValue::Utf8(Some(path)), None),
        datatype_expr(&DataType::Utf8View)?,
    ];
    let function = create_udf(Arc::new(JsonGetWithType::default()));
    Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::new(function),
        args,
    )))
}

impl ExprPlanner for Json2ExprPlanner {
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        schema: &datafusion_common::DFSchema,
    ) -> Result<PlannerResult<RawBinaryExpr>> {
        let Some(operator) = parse_sql_binary_op(&expr.op) else {
            return Ok(PlannerResult::Original(expr));
        };

        let left_type = expr.left.get_type(schema)?;
        let right_type = expr.right.get_type(schema)?;
        let left_rewritten = rewrite_expr_json_get(&expr.left, right_type)?;
        let right_rewritten = rewrite_expr_json_get(&expr.right, left_type)?;
        if left_rewritten.is_none() && right_rewritten.is_none() {
            return Ok(PlannerResult::Original(expr));
        }

        let rewritten = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left_rewritten.unwrap_or(expr.left)),
            operator,
            Box::new(right_rewritten.unwrap_or(expr.right)),
        ));
        common_telemetry::debug!("json2 plan_binary_op: rewritten={rewritten:?}");
        Ok(PlannerResult::Planned(rewritten))
    }

    fn plan_compound_identifier(
        &self,
        field: &Field,
        qualifier: Option<&TableReference>,
        nested_names: &[String],
    ) -> Result<PlannerResult<Vec<Expr>>> {
        if field.extension_type_name() != Some(JsonExtensionType::NAME) {
            return Ok(PlannerResult::Original(Vec::new()));
        }

        let path = nested_names.join(".");
        let column = Column::from((qualifier, field));
        json_get(Expr::Column(column), path).map(PlannerResult::Planned)
    }
}

fn rewrite_expr_json_get(expr: &Expr, data_type: DataType) -> Result<Option<Expr>> {
    let Expr::ScalarFunction(func) = expr else {
        return Ok(None);
    };
    if func.func.name() != JsonGetWithType::NAME {
        return Ok(None);
    }
    if func.args.len() != 3 {
        return Err(DataFusionError::Internal(format!(
            "Function {} is expected to have 3 arguments!",
            func.name()
        )));
    }

    let expected_expr = datatype_expr(&data_type)?;
    let rewritten = Expr::ScalarFunction(ScalarFunction {
        func: func.func.clone(),
        args: vec![func.args[0].clone(), func.args[1].clone(), expected_expr],
    });
    Ok(Some(rewritten))
}

fn parse_sql_binary_op(op: &BinaryOperator) -> Option<Operator> {
    match *op {
        BinaryOperator::Gt => Some(Operator::Gt),
        BinaryOperator::GtEq => Some(Operator::GtEq),
        BinaryOperator::Lt => Some(Operator::Lt),
        BinaryOperator::LtEq => Some(Operator::LtEq),
        BinaryOperator::Eq => Some(Operator::Eq),
        BinaryOperator::NotEq => Some(Operator::NotEq),
        BinaryOperator::Plus => Some(Operator::Plus),
        BinaryOperator::Minus => Some(Operator::Minus),
        BinaryOperator::Multiply => Some(Operator::Multiply),
        BinaryOperator::Divide => Some(Operator::Divide),
        BinaryOperator::Modulo => Some(Operator::Modulo),
        BinaryOperator::And => Some(Operator::And),
        BinaryOperator::Or => Some(Operator::Or),
        _ => None,
    }
}

fn datatype_expr(data_type: &DataType) -> Result<Expr> {
    ScalarValue::try_new_null(data_type).map(|x| Expr::Literal(x, None))
}
