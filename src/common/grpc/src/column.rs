use api::v1::ColumnDataType;
use arrow::datatypes::DataType;

use crate::error::Error;
use crate::error::UnsupportedDataTypeSnafu;

pub struct ColumnDataTypeWrapper {
    pub datatype: ColumnDataType,
}

impl TryFrom<&DataType> for ColumnDataTypeWrapper {
    type Error = Error;

    fn try_from(datatype: &DataType) -> Result<Self, Self::Error> {
        let datatype = match datatype {
            DataType::Int8 => ColumnDataType::Int8,
            DataType::Int16 => ColumnDataType::Int16,
            DataType::Int32 => ColumnDataType::Int32,
            DataType::Int64 => ColumnDataType::Int64,

            DataType::UInt8 => ColumnDataType::Uint8,
            DataType::UInt16 => ColumnDataType::Uint16,
            DataType::UInt32 => ColumnDataType::Uint32,
            DataType::UInt64 => ColumnDataType::Uint64,

            DataType::Float32 => ColumnDataType::Float32,
            DataType::Float64 => ColumnDataType::Float64,

            DataType::Boolean => ColumnDataType::Boolean,

            DataType::Binary => ColumnDataType::Binary,
            DataType::LargeBinary => ColumnDataType::Binary,

            DataType::Utf8 => ColumnDataType::String,
            DataType::LargeUtf8 => ColumnDataType::String,
            other => {
                return UnsupportedDataTypeSnafu {
                    datatype: format!("{:?}", other),
                }
                .fail()?
            }
        };
        Ok(Self { datatype })
    }
}

#[cfg(test)]
mod tests {
    use api::v1::ColumnDataType;
    use arrow::datatypes::DataType;

    use crate::column::ColumnDataTypeWrapper;

    #[test]
    fn test_type_convert() {
        let types = vec![
            (DataType::Int8, ColumnDataType::Int8),
            (DataType::Int16, ColumnDataType::Int16),
            (DataType::Int32, ColumnDataType::Int32),
            (DataType::Int64, ColumnDataType::Int64),
            (DataType::UInt8, ColumnDataType::Uint8),
            (DataType::UInt16, ColumnDataType::Uint16),
            (DataType::UInt32, ColumnDataType::Uint32),
            (DataType::UInt64, ColumnDataType::Uint64),
            (DataType::Float32, ColumnDataType::Float32),
            (DataType::Float64, ColumnDataType::Float64),
            (DataType::Boolean, ColumnDataType::Boolean),
            (DataType::Binary, ColumnDataType::Binary),
            (DataType::LargeBinary, ColumnDataType::Binary),
            (DataType::Utf8, ColumnDataType::String),
            (DataType::LargeUtf8, ColumnDataType::String),
        ];
        for (t1, t2) in types {
            assert_eq!(
                t2,
                TryInto::<ColumnDataTypeWrapper>::try_into(&t1)
                    .unwrap()
                    .datatype
            );
        }
    }
}
