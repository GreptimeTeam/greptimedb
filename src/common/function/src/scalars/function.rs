use std::fmt;

use chrono_tz::Tz;
use common_query::prelude::Signature;
use datatypes::data_type::ConcreteDataType;
use datatypes::vectors::VectorRef;
use dyn_clone::DynClone;

use crate::error::Result;

#[derive(Clone)]
pub struct FunctionContext {
    pub tz: Tz,
}

impl Default for FunctionContext {
    fn default() -> Self {
        Self {
            tz: "UTC".parse::<Tz>().unwrap(),
        }
    }
}

/// Scalar function trait, modified from databend
/// TODO(dennis): optimize function by it's monotonicity
pub trait Function: fmt::Display + Sync + Send + DynClone {
    /// Returns the name of the function, should be unique.
    fn name(&self) -> &str;

    fn return_type(&self, input_types: &[ConcreteDataType]) -> Result<ConcreteDataType>;

    fn signature(&self) -> Signature;

    /// Evaluate the function, e.g. run/execute the function.
    fn eval(&self, _func_ctx: FunctionContext, _columns: &[VectorRef]) -> Result<VectorRef>;
}

dyn_clone::clone_trait_object!(Function);
