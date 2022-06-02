use datatypes::prelude::ConcreteDataType;
pub mod expression;
pub mod function;
pub mod math;
pub mod numpy;

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
