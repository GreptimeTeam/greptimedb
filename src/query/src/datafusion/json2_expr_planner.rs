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
use common_function::scalars::json::json2_get::Json2GetFunction;
use common_function::scalars::udf::create_udf;
use datafusion_common::{Column, Result, ScalarValue, TableReference};
use datafusion_expr::Expr;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::planner::{ExprPlanner, PlannerResult};
use datatypes::extension::json::JsonExtensionType;

#[derive(Debug)]
pub(crate) struct Json2ExprPlanner;

fn json2_get(base: Expr, path: String) -> Expr {
    let args = vec![base, Expr::Literal(ScalarValue::Utf8(Some(path)), None)];
    let function = create_udf(Arc::new(Json2GetFunction::default()));
    Expr::ScalarFunction(ScalarFunction::new_udf(Arc::new(function), args))
}

impl ExprPlanner for Json2ExprPlanner {
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
        Ok(PlannerResult::Planned(json2_get(
            Expr::Column(column),
            path,
        )))
    }
}
