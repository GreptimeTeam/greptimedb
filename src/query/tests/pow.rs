use std::sync::Arc;

use common_query::error::Result;
use datatypes::prelude::ScalarVector;
use datatypes::prelude::Vector;
use datatypes::vectors::{UInt32Vector, VectorRef};

pub fn pow(args: &[VectorRef]) -> Result<VectorRef> {
    assert_eq!(args.len(), 2);

    let base = &args[0]
        .as_any()
        .downcast_ref::<UInt32Vector>()
        .expect("cast failed");
    let exponent = &args[1]
        .as_any()
        .downcast_ref::<UInt32Vector>()
        .expect("cast failed");

    assert_eq!(exponent.len(), base.len());

    let v = base
        .iter_data()
        .zip(exponent.iter_data())
        .map(|(base, exponent)| {
            match (base, exponent) {
                // in arrow, any value can be null.
                // Here we decide to make our UDF to return null when either base or exponent is null.
                (Some(base), Some(exponent)) => Some(base.pow(exponent)),
                _ => None,
            }
        })
        .collect::<UInt32Vector>();

    Ok(Arc::new(v) as _)
}
