/// Unique identifier for logical data type.
#[derive(Clone, Copy, Debug, PartialEq)]
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
}
