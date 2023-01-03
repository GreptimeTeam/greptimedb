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

//! Signature module contains foundational types that are used to represent signatures, types,
//! and return types of functions.
//! Copied and modified from datafusion.
pub use datafusion_expr::Volatility;
use datafusion_expr::{Signature as DfSignature, TypeSignature as DfTypeSignature};
use datatypes::arrow::datatypes::DataType as ArrowDataType;
use datatypes::data_type::DataType;
use datatypes::prelude::ConcreteDataType;

/// A function's type signature, which defines the function's supported argument types.
#[derive(Debug, Clone, PartialEq)]
pub enum TypeSignature {
    /// arbitrary number of arguments of an common type out of a list of valid types
    // A function such as `concat` is `Variadic(vec![ConcreteDataType::String, ConcreteDataType::String])`
    Variadic(Vec<ConcreteDataType>),
    /// arbitrary number of arguments of an arbitrary but equal type
    // A function such as `array` is `VariadicEqual`
    // The first argument decides the type used for coercion
    VariadicEqual,
    /// fixed number of arguments of an arbitrary but equal type out of a list of valid types
    // A function of one argument of f64 is `Uniform(1, vec![ConcreteDataType::Float64])`
    // A function of one argument of f64 or f32 is `Uniform(1, vec![ConcreteDataType::Float32, ConcreteDataType::Float64])`
    Uniform(usize, Vec<ConcreteDataType>),
    /// exact number of arguments of an exact type
    Exact(Vec<ConcreteDataType>),
    /// fixed number of arguments of arbitrary types
    Any(usize),
    /// One of a list of signatures
    OneOf(Vec<TypeSignature>),
}

///The Signature of a function defines its supported input types as well as its volatility.
#[derive(Debug, Clone, PartialEq)]
pub struct Signature {
    /// type_signature - The types that the function accepts. See [TypeSignature] for more information.
    pub type_signature: TypeSignature,
    /// volatility - The volatility of the function. See [Volatility] for more information.
    pub volatility: Volatility,
}

#[inline]
fn concrete_types_to_arrow_types(ts: Vec<ConcreteDataType>) -> Vec<ArrowDataType> {
    ts.iter().map(ConcreteDataType::as_arrow_type).collect()
}

impl Signature {
    /// new - Creates a new Signature from any type signature and the volatility.
    pub fn new(type_signature: TypeSignature, volatility: Volatility) -> Self {
        Signature {
            type_signature,
            volatility,
        }
    }
    /// variadic - Creates a variadic signature that represents an arbitrary number of arguments all from a type in common_types.
    pub fn variadic(common_types: Vec<ConcreteDataType>, volatility: Volatility) -> Self {
        Self {
            type_signature: TypeSignature::Variadic(common_types),
            volatility,
        }
    }
    /// variadic_equal - Creates a variadic signature that represents an arbitrary number of arguments of the same type.
    pub fn variadic_equal(volatility: Volatility) -> Self {
        Self {
            type_signature: TypeSignature::VariadicEqual,
            volatility,
        }
    }
    /// uniform - Creates a function with a fixed number of arguments of the same type, which must be from valid_types.
    pub fn uniform(
        arg_count: usize,
        valid_types: Vec<ConcreteDataType>,
        volatility: Volatility,
    ) -> Self {
        Self {
            type_signature: TypeSignature::Uniform(arg_count, valid_types),
            volatility,
        }
    }
    /// exact - Creates a signature which must match the types in exact_types in order.
    pub fn exact(exact_types: Vec<ConcreteDataType>, volatility: Volatility) -> Self {
        Signature {
            type_signature: TypeSignature::Exact(exact_types),
            volatility,
        }
    }
    /// any - Creates a signature which can a be made of any type but of a specified number
    pub fn any(arg_count: usize, volatility: Volatility) -> Self {
        Signature {
            type_signature: TypeSignature::Any(arg_count),
            volatility,
        }
    }
    /// one_of Creates a signature which can match any of the [TypeSignature]s which are passed in.
    pub fn one_of(type_signatures: Vec<TypeSignature>, volatility: Volatility) -> Self {
        Signature {
            type_signature: TypeSignature::OneOf(type_signatures),
            volatility,
        }
    }
}

/// Conversations between datafusion signature and our signature
impl From<TypeSignature> for DfTypeSignature {
    fn from(type_signature: TypeSignature) -> DfTypeSignature {
        match type_signature {
            TypeSignature::Variadic(types) => {
                DfTypeSignature::Variadic(concrete_types_to_arrow_types(types))
            }
            TypeSignature::VariadicEqual => DfTypeSignature::VariadicEqual,
            TypeSignature::Uniform(n, types) => {
                DfTypeSignature::Uniform(n, concrete_types_to_arrow_types(types))
            }
            TypeSignature::Exact(types) => {
                DfTypeSignature::Exact(concrete_types_to_arrow_types(types))
            }
            TypeSignature::Any(n) => DfTypeSignature::Any(n),
            TypeSignature::OneOf(ts) => {
                DfTypeSignature::OneOf(ts.into_iter().map(Into::into).collect())
            }
        }
    }
}

impl From<Signature> for DfSignature {
    fn from(sig: Signature) -> DfSignature {
        DfSignature::new(sig.type_signature.into(), sig.volatility)
    }
}

#[cfg(test)]
mod tests {
    use datatypes::arrow::datatypes::DataType;

    use super::*;

    #[test]
    fn test_into_df_signature() {
        let types = vec![
            ConcreteDataType::int8_datatype(),
            ConcreteDataType::float32_datatype(),
            ConcreteDataType::float64_datatype(),
        ];
        let sig = Signature::exact(types.clone(), Volatility::Immutable);

        assert_eq!(Volatility::Immutable, sig.volatility);
        assert!(matches!(&sig.type_signature, TypeSignature::Exact(x) if x.clone() == types));

        let df_sig = DfSignature::from(sig);
        assert_eq!(Volatility::Immutable, df_sig.volatility);
        let types = vec![DataType::Int8, DataType::Float32, DataType::Float64];
        assert!(matches!(df_sig.type_signature, DfTypeSignature::Exact(x) if x == types));
    }
}
