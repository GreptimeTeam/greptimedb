#[cfg(any(test, feature = "test"))]
use crate::data_type::ConcreteDataType;

/// Unique identifier for logical data type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogicalTypeId {
    Null,

    // Numeric types:
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,

    // String types:
    String,
    Binary,

    // Date & Time types:
    /// Date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in days (32 bits).
    Date,
    /// Datetime representing the elapsed time since UNIX epoch (1970-01-01) in
    /// seconds/milliseconds/microseconds/nanoseconds, determined by precision.
    DateTime,

    Timestamp,

    List,
}

impl LogicalTypeId {
    /// Create ConcreteDataType based on this id. This method is for test only as it
    /// would lost some info.
    ///
    /// # Panics
    /// Panics if data type is not supported.
    #[cfg(any(test, feature = "test"))]
    pub fn data_type(&self) -> ConcreteDataType {
        match self {
            LogicalTypeId::Null => ConcreteDataType::null_datatype(),
            LogicalTypeId::Boolean => ConcreteDataType::boolean_datatype(),
            LogicalTypeId::Int8 => ConcreteDataType::int8_datatype(),
            LogicalTypeId::Int16 => ConcreteDataType::int16_datatype(),
            LogicalTypeId::Int32 => ConcreteDataType::int32_datatype(),
            LogicalTypeId::Int64 => ConcreteDataType::int64_datatype(),
            LogicalTypeId::UInt8 => ConcreteDataType::uint8_datatype(),
            LogicalTypeId::UInt16 => ConcreteDataType::uint16_datatype(),
            LogicalTypeId::UInt32 => ConcreteDataType::uint32_datatype(),
            LogicalTypeId::UInt64 => ConcreteDataType::uint64_datatype(),
            LogicalTypeId::Float32 => ConcreteDataType::float32_datatype(),
            LogicalTypeId::Float64 => ConcreteDataType::float64_datatype(),
            LogicalTypeId::String => ConcreteDataType::string_datatype(),
            LogicalTypeId::Binary => ConcreteDataType::binary_datatype(),
            LogicalTypeId::Date => ConcreteDataType::date_datatype(),
            LogicalTypeId::DateTime => ConcreteDataType::datetime_datatype(),
            LogicalTypeId::Timestamp => ConcreteDataType::timestamp_millis_datatype(), // to timestamp type with default time unit
            LogicalTypeId::List => {
                ConcreteDataType::list_datatype(ConcreteDataType::null_datatype())
            }
        }
    }
}
