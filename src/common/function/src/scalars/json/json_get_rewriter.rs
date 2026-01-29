use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{DFSchema, Result};
use datafusion::logical_expr::expr::{Cast, Expr, ScalarFunction};
use datafusion::logical_expr::expr_rewriter::FunctionRewrite;
use datafusion::scalar::ScalarValue;

use crate::scalars::json::JsonGetWithType;

#[derive(Debug)]
pub struct JsonGetRewriter;

impl FunctionRewrite for JsonGetRewriter {
    fn name(&self) -> &'static str {
        "JsonGetRewriter"
    }

    fn rewrite(
        &self,
        expr: Expr,
        _schema: &DFSchema,
        _config: &ConfigOptions,
    ) -> Result<Transformed<Expr>> {
        let transform = match &expr {
            Expr::Cast(cast) => rewrite_json_get_cast(cast),
            _ => None,
        };
        Ok(transform.unwrap_or_else(|| Transformed::no(expr)))
    }
}

fn rewrite_json_get_cast(cast: &Cast) -> Option<Transformed<Expr>> {
    let scalar_func = extract_scalar_function(&cast.expr)?;
    if scalar_func.func.name().to_ascii_lowercase() == JsonGetWithType::NAME
        && scalar_func.args.len() == 2
    {
        let null_expr = Expr::Literal(ScalarValue::Null, None);
        let null_cast = Expr::Cast(datafusion::logical_expr::expr::Cast {
            expr: Box::new(null_expr),
            data_type: cast.data_type.clone(),
        });

        let mut args = scalar_func.args.clone();
        args.push(null_cast);

        Some(Transformed::yes(Expr::ScalarFunction(ScalarFunction {
            func: scalar_func.func.clone(),
            args,
        })))
    } else {
        None
    }
}

fn extract_scalar_function(expr: &Expr) -> Option<&ScalarFunction> {
    match expr {
        Expr::ScalarFunction(func) => Some(func),
        _ => None,
    }
}
