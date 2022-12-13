use std::sync::Arc;

/// An evaluator evaluates given expressions over given fixed timestamps.
use promql_parser::parser::{Expr, Value};

use crate::engine::Context;
use crate::error::Result;

pub struct Evaluator {}

impl Evaluator {
    pub fn eval(_ctx: &Context, _expr: &Expr) -> Result<Arc<dyn Value>> {
        unimplemented!();
    }
}
