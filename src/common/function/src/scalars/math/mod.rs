mod pow;

use std::sync::Arc;

use pow::PowFunction;

use crate::scalars::function_registry::FunctionRegistry;

pub(crate) struct MathsFunction;

impl MathsFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register(Arc::new(PowFunction {}));
    }
}
