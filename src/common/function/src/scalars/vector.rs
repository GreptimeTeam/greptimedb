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

mod convert;
mod distance;
mod elem_product;
mod elem_sum;
pub mod impl_conv;
pub(crate) mod product;
mod scalar_add;
mod scalar_mul;
pub(crate) mod sum;
mod vector_add;
mod vector_dim;
mod vector_div;
mod vector_mul;
mod vector_norm;
mod vector_sub;
mod vector_subvector;

use std::sync::Arc;

use crate::function_registry::FunctionRegistry;

pub(crate) struct VectorFunction;

impl VectorFunction {
    pub fn register(registry: &FunctionRegistry) {
        // conversion
        registry.register(Arc::new(convert::ParseVectorFunction));
        registry.register(Arc::new(convert::VectorToStringFunction));

        // distance
        registry.register(Arc::new(distance::CosDistanceFunction));
        registry.register(Arc::new(distance::DotProductFunction));
        registry.register(Arc::new(distance::L2SqDistanceFunction));

        // scalar calculation
        registry.register(Arc::new(scalar_add::ScalarAddFunction));
        registry.register(Arc::new(scalar_mul::ScalarMulFunction));

        // vector calculation
        registry.register(Arc::new(vector_add::VectorAddFunction));
        registry.register(Arc::new(vector_sub::VectorSubFunction));
        registry.register(Arc::new(vector_mul::VectorMulFunction));
        registry.register(Arc::new(vector_div::VectorDivFunction));
        registry.register(Arc::new(vector_norm::VectorNormFunction));
        registry.register(Arc::new(vector_dim::VectorDimFunction));
        registry.register(Arc::new(vector_subvector::VectorSubvectorFunction));
        registry.register(Arc::new(elem_sum::ElemSumFunction));
        registry.register(Arc::new(elem_product::ElemProductFunction));
    }
}
