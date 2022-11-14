use client::api::v1::{column::Values, ColumnDataType};

pub fn values_to_string(data_type: ColumnDataType, values: Values) -> Vec<String> {
    match data_type {
        ColumnDataType::Int64 => values
            .i64_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::Float64 => values
            .f64_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::String => values.string_values,
        ColumnDataType::Boolean => values
            .bool_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::Int8 => values
            .i8_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::Int16 => values
            .i16_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::Int32 => values
            .i32_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::Uint8 => values
            .u8_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::Uint16 => values
            .u16_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::Uint32 => values
            .u32_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::Uint64 => values
            .u64_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::Float32 => values
            .f32_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::Binary => values
            .binary_values
            .into_iter()
            .map(|val| format!("{:?}", val))
            .collect(),
        ColumnDataType::Datetime => values
            .i64_values
            .into_iter()
            .map(|v| v.to_string())
            .collect(),
        ColumnDataType::Date => values
            .i32_values
            .into_iter()
            .map(|v| v.to_string())
            .collect(),
        ColumnDataType::Timestamp => values
            .ts_millis_values
            .into_iter()
            .map(|v| v.to_string())
            .collect(),
    }
}
