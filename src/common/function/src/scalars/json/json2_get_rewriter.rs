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

use arrow_schema::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{DFSchema, Result};
use datafusion::logical_expr::expr_rewriter::FunctionRewrite;
use datafusion::scalar::ScalarValue;
use datafusion_common::{exec_err, internal_err};
use datafusion_expr::Expr;

use crate::scalars::json::json2_get::{Json2GetFunction, datatype_expr};

#[derive(Debug)]
pub(crate) struct Json2GetRewriter;

impl FunctionRewrite for Json2GetRewriter {
    fn name(&self) -> &'static str {
        "Json2GetRewriter"
    }

    fn rewrite(
        &self,
        expr: Expr,
        _schema: &DFSchema,
        _config: &ConfigOptions,
    ) -> Result<Transformed<Expr>> {
        let (expr, rewritten) = reduce_arrow_cast(expr)?;
        if rewritten {
            Ok(Transformed::yes(expr))
        } else {
            Ok(Transformed::no(expr))
        }
    }
}

// arrow_cast(json2_get(_, _, _), "<type>") => json2_get(_, _, "<type>")
fn reduce_arrow_cast(expr: Expr) -> Result<(Expr, bool)> {
    let mut f = match expr {
        Expr::ScalarFunction(f) => f,
        expr => return Ok((expr, false)),
    };
    if f.name() != "arrow_cast" {
        return Ok((Expr::ScalarFunction(f), false));
    }
    if !matches!(&f.args[0], Expr::ScalarFunction(f) if f.name() == Json2GetFunction::NAME) {
        return Ok((Expr::ScalarFunction(f), false));
    }

    if f.args.len() != 2 {
        return internal_err!("arrow_cast must have 2 arguments");
    }
    let target_type = match &f.args[1] {
        Expr::Literal(ScalarValue::Utf8(Some(target_type)), _) => target_type
            .parse::<DataType>()
            .map_err(Into::into)
            .and_then(|x| datatype_expr(&x))?,
        x => return exec_err!("arrow_cast expects 2nd argument a string, got: {:?}", x),
    };

    let Expr::ScalarFunction(mut json2_get) = f.args.remove(0) else {
        // checked in above "matches!"
        unreachable!()
    };
    if json2_get.args.len() != 3 {
        return internal_err!("function {} must have 3 arguments", Json2GetFunction::NAME);
    }
    json2_get.args[2] = target_type;
    Ok((Expr::ScalarFunction(json2_get), true))
}
