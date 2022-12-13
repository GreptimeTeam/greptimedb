use std::sync::Arc;

use promql_parser::parser::Value;

use crate::error::Result;

mod evaluator;
mod functions;

pub use evaluator::*;

pub struct Context {}

pub struct Query {}

pub struct Engine {}

impl Engine {
    pub fn exec(_ctx: &Context, _q: Query) -> Result<Arc<dyn Value>> {
        unimplemented!();
    }
}
