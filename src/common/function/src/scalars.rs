use datatypes::prelude::ConcreteDataType;
pub mod expression;
pub mod function;
pub mod function_registry;
pub mod math;
pub mod numpy;
#[cfg(test)]
pub(crate) mod test;
pub mod udf;

pub use function::{Function, FunctionRef};
pub use function_registry::{FunctionRegistry, FUNCTION_REGISTRY};

pub(crate) fn numerics() -> Vec<ConcreteDataType> {
    vec![
        ConcreteDataType::int8_datatype(),
        ConcreteDataType::int16_datatype(),
        ConcreteDataType::int32_datatype(),
        ConcreteDataType::int64_datatype(),
        ConcreteDataType::uint8_datatype(),
        ConcreteDataType::uint16_datatype(),
        ConcreteDataType::uint32_datatype(),
        ConcreteDataType::uint64_datatype(),
        ConcreteDataType::float32_datatype(),
        ConcreteDataType::float64_datatype(),
    ]
}
