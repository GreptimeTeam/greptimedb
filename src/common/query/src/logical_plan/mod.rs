mod expr;
mod udf;

use std::sync::Arc;

use datatypes::prelude::ConcreteDataType;

pub use self::expr::Expr;
pub use self::udf::ScalarUDF;
use crate::function::{ReturnTypeFunction, ScalarFunctionImplementation};
use crate::signature::{Signature, Volatility};

/// Creates a new UDF with a specific signature and specific return type.
/// This is a helper function to create a new UDF.
/// The function `create_udf` returns a subset of all possible `ScalarFunction`:
/// * the UDF has a fixed return type
/// * the UDF has a fixed signature (e.g. [f64, f64])
pub fn create_udf(
    name: &str,
    input_types: Vec<ConcreteDataType>,
    return_type: Arc<ConcreteDataType>,
    volatility: Volatility,
    fun: ScalarFunctionImplementation,
) -> ScalarUDF {
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(return_type.clone()));
    ScalarUDF::new(
        name,
        &Signature::exact(input_types, volatility),
        &return_type,
        &fun,
    )
}
