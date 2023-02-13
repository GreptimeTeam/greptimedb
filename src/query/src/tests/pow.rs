// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use common_query::error::Result;
use datatypes::prelude::{ScalarVector, Vector};
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

    let iter = base
        .iter_data()
        .zip(exponent.iter_data())
        .map(|(base, exponent)| {
            match (base, exponent) {
                // in arrow, any value can be null.
                // Here we decide to make our UDF to return null when either base or exponent is null.
                (Some(base), Some(exponent)) => Some(base.pow(exponent)),
                _ => None,
            }
        });
    let v = UInt32Vector::from_owned_iterator(iter);

    Ok(Arc::new(v) as _)
}
