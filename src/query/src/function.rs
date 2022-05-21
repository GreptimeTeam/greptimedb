//! GreptimeDB builtin functions

use std::sync::Arc;

use common_query::error::Result;
use datatypes::prelude::ScalarVector;
use datatypes::prelude::Vector;
use datatypes::vectors::{Float64Vector, VectorRef};

pub fn pow(args: &[VectorRef]) -> Result<VectorRef> {
    // in DataFusion, all `args` and output are dynamically-typed arrays, which means that we need to:
    // 1. cast the values to the type we want
    // 2. perform the computation for every element in the array (using a loop or SIMD) and construct the result

    // this is guaranteed by DataFusion based on the function's signature.
    assert_eq!(args.len(), 2);

    // 1. cast both arguments to f64. These casts MUST be aligned with the signature or this function panics!
    let base = &args[0]
        .as_any()
        .downcast_ref::<Float64Vector>()
        .expect("cast failed");
    let exponent = &args[1]
        .as_any()
        .downcast_ref::<Float64Vector>()
        .expect("cast failed");

    // this is guaranteed by DataFusion. We place it just to make it obvious.
    assert_eq!(exponent.len(), base.len());

    // 2. perform the computation
    let v = base
        .iter_data()
        .zip(exponent.iter_data())
        .map(|(base, exponent)| {
            match (base, exponent) {
                // in arrow, any value can be null.
                // Here we decide to make our UDF to return null when either base or exponent is null.
                (Some(base), Some(exponent)) => Some(base.powf(exponent)),
                _ => None,
            }
        })
        .collect::<Float64Vector>();

    // `Ok` because no error occurred during the calculation (we should add one if exponent was [0, 1[ and the base < 0 because that panics!)
    // `Arc` because arrays are immutable, thread-safe, trait objects.
    Ok(Arc::new(v) as _)
}
