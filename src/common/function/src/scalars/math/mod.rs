mod pow;
mod rate;

use std::sync::Arc;

pub use pow::PowFunction;
pub use rate::RateFunction;

use crate::scalars::function_registry::FunctionRegistry;

pub(crate) struct MathFunction;

impl MathFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register(Arc::new(PowFunction::default()));
        registry.register(Arc::new(RateFunction::default()))
    }
}
