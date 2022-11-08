use std::sync::Arc;
mod from_unixtime;

use from_unixtime::FromUnixtimeFunction;

use crate::scalars::function_registry::FunctionRegistry;

pub(crate) struct TimestampFunction;

impl TimestampFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register(Arc::new(FromUnixtimeFunction::default()));
    }
}
