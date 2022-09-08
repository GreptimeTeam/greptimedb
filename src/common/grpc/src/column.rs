use arrow::datatypes::DataType;

use crate::error::Error;
use crate::error::UnsupportedDataTypeSnafu;

pub const I8_INDEX: i32 = 0;
pub const I16_INDEX: i32 = 1;
pub const I32_INDEX: i32 = 2;
pub const I64_INDEX: i32 = 3;
pub const U8_INDEX: i32 = 4;
pub const U16_INDEX: i32 = 5;
pub const U32_INDEX: i32 = 6;
pub const U64_INDEX: i32 = 7;
pub const F32_INDEX: i32 = 8;
pub const F64_INDEX: i32 = 9;
pub const BOOL_INDEX: i32 = 10;
pub const BYTES_INDEX: i32 = 11;
pub const STRING_INDEX: i32 = 12;

pub struct ValueIndex {
    pub idx: i32,
}

impl TryFrom<&DataType> for ValueIndex {
    type Error = Error;

    fn try_from(datatype: &DataType) -> Result<Self, Self::Error> {
        let idx = match datatype {
            DataType::Int8 => I8_INDEX,
            DataType::Int16 => I16_INDEX,
            DataType::Int32 => I32_INDEX,
            DataType::Int64 => I64_INDEX,

            DataType::UInt8 => U8_INDEX,
            DataType::UInt16 => U16_INDEX,
            DataType::UInt32 => U32_INDEX,
            DataType::UInt64 => U64_INDEX,

            DataType::Float32 => F32_INDEX,
            DataType::Float64 => F64_INDEX,

            DataType::Boolean => BOOL_INDEX,

            DataType::Binary => BYTES_INDEX,
            DataType::LargeBinary => BYTES_INDEX,

            DataType::Utf8 => STRING_INDEX,
            DataType::LargeUtf8 => STRING_INDEX,
            other => {
                return UnsupportedDataTypeSnafu {
                    datatype: format!("{:?}", other),
                }
                .fail()?
            }
        };
        Ok(Self { idx })
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use crate::column::{ValueIndex, I8_INDEX, STRING_INDEX};

    #[test]
    fn test_value_index() {
        let datatype = &DataType::UInt8;
        let value_index: ValueIndex = datatype.try_into().unwrap();
        assert_eq!(I8_INDEX, value_index.idx);

        let datatype = &DataType::Utf8;
        let value_index: ValueIndex = datatype.try_into().unwrap();
        assert_eq!(STRING_INDEX, value_index.idx);
    }
}
