pub mod grpc {
    use std::usize;

    use api::v1::{Column, ColumnDataType, InsertRequest};

    use crate::{wcu, WcuCalc};

    /// Calculate WCUs for Grpc insert request
    impl WcuCalc for InsertRequest {
        fn wcu_num(&self) -> u32 {
            let total_bytes = self
                .columns
                .iter()
                .map(|column| byte_num(self.row_count, column))
                .sum();

            wcu(total_bytes)
        }
    }

    /// Calculate the byte number of Grpc Column
    fn byte_num(row_count: u32, column: &Column) -> u32 {
        match column.datatype() {
            ColumnDataType::Uint8 | ColumnDataType::Boolean | ColumnDataType::Int8 => row_count,
            ColumnDataType::Uint16 | ColumnDataType::Int16 => 2 * row_count,
            ColumnDataType::Int32
            | ColumnDataType::Uint32
            | ColumnDataType::Float32
            | ColumnDataType::Date => 4 * row_count,
            ColumnDataType::Int64
            | ColumnDataType::Uint64
            | ColumnDataType::Float64
            | ColumnDataType::Datetime
            | ColumnDataType::TimestampSecond
            | ColumnDataType::TimestampMillisecond
            | ColumnDataType::TimestampMicrosecond
            | ColumnDataType::TimestampNanosecond => 8 * row_count,
            ColumnDataType::Binary => column
                .values
                .as_ref()
                .map(|vals| vals.binary_values.iter().map(|v| v.len()).sum::<usize>() as u32)
                .unwrap_or_default(),
            ColumnDataType::String => column
                .values
                .as_ref()
                .map(|vals| vals.string_values.iter().map(|v| v.len()).sum::<usize>() as u32)
                .unwrap_or_default(),
        }
    }

    #[cfg(test)]
    mod tests {
        use api::v1::column::{SemanticType, Values};
        use api::v1::{Column, ColumnDataType, InsertRequest};

        use super::byte_num;
        use crate::WcuCalc;

        #[test]
        fn test_bytes() {
            let row_count = 3;
            let column = mock_num_column("number1", row_count);
            assert_eq!(3 * 4, byte_num(row_count, &column));

            let row_count = 256;
            let column = mock_num_column("number2", row_count);
            assert_eq!(256 * 4, byte_num(row_count, &column));

            let row_count = 3;
            let column = mock_string_column("string1", row_count);
            assert_eq!(3 * 20, byte_num(row_count, &column));

            let row_count = 256;
            let column = mock_string_column("string2", row_count);
            assert_eq!(256 * 20, byte_num(row_count, &column));
        }

        #[test]
        fn test_wcus() {
            let insert_request = mock_insert_request_1();
            assert_eq!((256 * 4 + 256 * 20) / 1024 + 1, insert_request.wcu_num());

            let insert_request = mock_insert_request_2();
            assert_eq!(1, insert_request.wcu_num());
        }

        fn mock_insert_request_1() -> InsertRequest {
            let row_count = 256;
            let column1 = mock_num_column("number2", row_count);
            let column2 = mock_string_column("string2", row_count);

            let columns = vec![column1, column2];

            InsertRequest {
                table_name: "mock_table".to_string(),
                columns,
                row_count,
                region_number: 1,
            }
        }

        fn mock_insert_request_2() -> InsertRequest {
            let row_count = 3;
            let column1 = mock_num_column("number2", row_count);
            let column2 = mock_string_column("string2", row_count);

            let columns = vec![column1, column2];

            InsertRequest {
                table_name: "mock_table".to_string(),
                columns,
                row_count,
                region_number: 1,
            }
        }

        fn mock_num_column(column_name: impl Into<String>, row_count: u32) -> Column {
            let mut u32_values = Vec::with_capacity(row_count as usize);

            for i in 0..row_count {
                u32_values.push(i);
            }

            let values = Values {
                u32_values,
                ..Default::default()
            };

            let column_name = column_name.into();

            Column {
                column_name,
                semantic_type: SemanticType::Field.into(),
                values: Some(values),
                null_mask: Vec::default(),
                datatype: ColumnDataType::Uint32.into(),
            }
        }

        fn mock_string_column(column_name: impl Into<String>, row_count: u32) -> Column {
            let mut string_values = Vec::with_capacity(row_count as usize);

            for i in 0..row_count {
                string_values.push(format!("This is a desc{:06}", i));
            }

            let values = Values {
                string_values,
                ..Default::default()
            };

            let column_name = column_name.into();

            Column {
                column_name,
                semantic_type: SemanticType::Field.into(),
                values: Some(values),
                null_mask: Vec::default(),
                datatype: ColumnDataType::String.into(),
            }
        }
    }
}
