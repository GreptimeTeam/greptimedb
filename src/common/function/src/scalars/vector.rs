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
mod scalar_add;
mod scalar_mul;
mod vector_add;
mod vector_dim;
mod vector_div;
mod vector_kth_elem;
mod vector_mul;
mod vector_norm;
mod vector_sub;
mod vector_subvector;

use crate::function_registry::FunctionRegistry;
pub(crate) struct VectorFunction;

impl VectorFunction {
    pub fn register(registry: &FunctionRegistry) {
        // conversion
        registry.register_scalar(convert::ParseVectorFunction);
        registry.register_scalar(convert::VectorToStringFunction);

        // distance
        registry.register_scalar(distance::CosDistanceFunction);
        registry.register_scalar(distance::DotProductFunction);
        registry.register_scalar(distance::L2SqDistanceFunction);

        // scalar calculation
        registry.register_scalar(scalar_add::ScalarAddFunction);
        registry.register_scalar(scalar_mul::ScalarMulFunction);

        // vector calculation
        registry.register_scalar(vector_add::VectorAddFunction);
        registry.register_scalar(vector_sub::VectorSubFunction);
        registry.register_scalar(vector_mul::VectorMulFunction);
        registry.register_scalar(vector_div::VectorDivFunction);
        registry.register_scalar(vector_norm::VectorNormFunction);
        registry.register_scalar(vector_dim::VectorDimFunction);
        registry.register_scalar(vector_kth_elem::VectorKthElemFunction);
        registry.register_scalar(vector_subvector::VectorSubvectorFunction);
        registry.register_scalar(elem_sum::ElemSumFunction);
        registry.register_scalar(elem_product::ElemProductFunction);
    }
}
