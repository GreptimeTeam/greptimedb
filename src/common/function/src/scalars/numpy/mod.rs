mod clip;

use std::sync::Arc;

use clip::ClipFunction;

use crate::scalars::function_registry::FunctionRegistry;

pub(crate) struct NumpyFunction;

impl NumpyFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register(Arc::new(ClipFunction::default()));
    }
}
