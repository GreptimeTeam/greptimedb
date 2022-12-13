use std::sync::Arc;

use promql_parser::parser::{Expr, Value};

use crate::engine::Context;
use crate::error::Result;

/// An evaluator evaluates given expressions over given fixed timestamps.
pub struct Evaluator {}

impl Evaluator {
    pub fn eval(_ctx: &Context, _expr: &Expr) -> Result<Arc<dyn Value>> {
        unimplemented!();
    }
}
