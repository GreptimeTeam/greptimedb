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

use std::collections::HashMap;
use std::fmt::{self, Display};
use std::sync::Arc;

use datafusion_common::arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, DictionaryArray, ListBuilder, StringBuilder,
};
use datafusion_common::arrow::datatypes::{DataType, Field};
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};
use datatypes::arrow::datatypes::UInt32Type;
use datatypes::value::Value;
use mito_codec::row_converter::{
    CompositeValues, PrimaryKeyCodec, SortField, build_primary_key_codec_with_fields,
};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadata;
use store_api::storage::ColumnId;
use store_api::storage::consts::{PRIMARY_KEY_COLUMN_NAME, ReservedColumnId};

use crate::function::{Function, extract_args};
use crate::function_registry::FunctionRegistry;

type NameValuePair = (String, Option<String>);

#[derive(Clone, Debug)]
pub(crate) struct DecodePrimaryKeyFunction {
    signature: Signature,
}

const NAME: &str = "decode_primary_key";
const NULL_VALUE_LITERAL: &str = "null";

impl Default for DecodePrimaryKeyFunction {
    fn default() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

impl DecodePrimaryKeyFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register_scalar(Self::default());
    }

    fn return_data_type() -> DataType {
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
    }
}

impl Function for DecodePrimaryKeyFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(Self::return_data_type())
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [encoded, _, _] = extract_args(self.name(), &args)?;
        let number_rows = args.number_rows;

        let encoding = parse_encoding(&args.args[1])?;
        let metadata = parse_region_metadata(&args.args[2])?;
        let codec = build_codec(&metadata, encoding);
        let name_lookup: HashMap<_, _> = metadata
            .column_metadatas
            .iter()
            .map(|c| (c.column_id, c.column_schema.name.clone()))
            .collect();

        let decoded_rows = decode_primary_keys(encoded, number_rows, codec.as_ref(), &name_lookup)?;
        let array = build_list_array(&decoded_rows)?;

        Ok(ColumnarValue::Array(array))
    }
}

impl Display for DecodePrimaryKeyFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DECODE_PRIMARY_KEY")
    }
}

fn parse_encoding(arg: &ColumnarValue) -> datafusion_common::Result<PrimaryKeyEncoding> {
    let encoding = match arg {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(v)))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(v))) => v.as_str(),
        ColumnarValue::Scalar(value) => {
            return Err(DataFusionError::Execution(format!(
                "encoding must be a string literal, got {value:?}"
            )));
        }
        ColumnarValue::Array(_) => {
            return Err(DataFusionError::Execution(
                "encoding must be a scalar string".to_string(),
            ));
        }
    };

    match encoding.to_ascii_lowercase().as_str() {
        "dense" => Ok(PrimaryKeyEncoding::Dense),
        "sparse" => Ok(PrimaryKeyEncoding::Sparse),
        _ => Err(DataFusionError::Execution(format!(
            "unsupported primary key encoding: {encoding}"
        ))),
    }
}

fn build_codec(
    metadata: &RegionMetadata,
    encoding: PrimaryKeyEncoding,
) -> Arc<dyn PrimaryKeyCodec> {
    let fields = metadata.primary_key_columns().map(|c| {
        (
            c.column_id,
            SortField::new(c.column_schema.data_type.clone()),
        )
    });
    build_primary_key_codec_with_fields(encoding, fields)
}

fn parse_region_metadata(arg: &ColumnarValue) -> datafusion_common::Result<RegionMetadata> {
    let json = match arg {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(v)))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(v))) => v.as_str(),
        ColumnarValue::Scalar(value) => {
            return Err(DataFusionError::Execution(format!(
                "region metadata must be a string literal, got {value:?}"
            )));
        }
        ColumnarValue::Array(_) => {
            return Err(DataFusionError::Execution(
                "region metadata must be a scalar string".to_string(),
            ));
        }
    };

    RegionMetadata::from_json(json)
        .map_err(|e| DataFusionError::Execution(format!("failed to parse region metadata: {e:?}")))
}

fn decode_primary_keys(
    encoded: ArrayRef,
    number_rows: usize,
    codec: &dyn PrimaryKeyCodec,
    name_lookup: &HashMap<ColumnId, String>,
) -> datafusion_common::Result<Vec<Vec<NameValuePair>>> {
    if let Some(dict) = encoded
        .as_any()
        .downcast_ref::<DictionaryArray<UInt32Type>>()
    {
        decode_dictionary(dict, number_rows, codec, name_lookup)
    } else if let Some(array) = encoded.as_any().downcast_ref::<BinaryArray>() {
        decode_binary_array(array, codec, name_lookup)
    } else if let Some(array) = encoded.as_any().downcast_ref::<BinaryViewArray>() {
        decode_binary_view_array(array, codec, name_lookup)
    } else {
        Err(DataFusionError::Execution(format!(
            "column {PRIMARY_KEY_COLUMN_NAME} must be binary or dictionary(binary) array"
        )))
    }
}

fn decode_dictionary(
    dict: &DictionaryArray<UInt32Type>,
    number_rows: usize,
    codec: &dyn PrimaryKeyCodec,
    name_lookup: &HashMap<ColumnId, String>,
) -> datafusion_common::Result<Vec<Vec<NameValuePair>>> {
    let values = dict
        .values()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| {
            DataFusionError::Execution("primary key dictionary values are not binary".to_string())
        })?;

    let mut decoded_values = Vec::with_capacity(values.len());
    for i in 0..values.len() {
        let pk = values.value(i);
        let pairs = decode_one(pk, codec, name_lookup)?;
        decoded_values.push(pairs);
    }

    let mut rows = Vec::with_capacity(number_rows);
    let keys = dict.keys();
    for i in 0..number_rows {
        let dict_index = keys.value(i) as usize;
        rows.push(decoded_values[dict_index].clone());
    }

    Ok(rows)
}

fn decode_binary_array(
    array: &BinaryArray,
    codec: &dyn PrimaryKeyCodec,
    name_lookup: &HashMap<ColumnId, String>,
) -> datafusion_common::Result<Vec<Vec<NameValuePair>>> {
    (0..array.len())
        .map(|i| decode_one(array.value(i), codec, name_lookup))
        .collect()
}

fn decode_binary_view_array(
    array: &BinaryViewArray,
    codec: &dyn PrimaryKeyCodec,
    name_lookup: &HashMap<ColumnId, String>,
) -> datafusion_common::Result<Vec<Vec<NameValuePair>>> {
    (0..array.len())
        .map(|i| decode_one(array.value(i), codec, name_lookup))
        .collect()
}

fn decode_one(
    pk: &[u8],
    codec: &dyn PrimaryKeyCodec,
    name_lookup: &HashMap<ColumnId, String>,
) -> datafusion_common::Result<Vec<NameValuePair>> {
    let decoded = codec
        .decode(pk)
        .map_err(|e| DataFusionError::Execution(format!("failed to decode primary key: {e}")))?;

    Ok(match decoded {
        CompositeValues::Dense(values) => values
            .into_iter()
            .map(|(column_id, value)| (column_name(column_id, name_lookup), value_to_string(value)))
            .collect(),
        CompositeValues::Sparse(values) => {
            let mut values: Vec<_> = values
                .iter()
                .map(|(column_id, value)| {
                    (
                        *column_id,
                        column_name(*column_id, name_lookup),
                        value_to_string(value.clone()),
                    )
                })
                .collect();
            values.sort_by_key(|(column_id, _, _)| {
                (ReservedColumnId::is_reserved(*column_id), *column_id)
            });
            values
                .into_iter()
                .map(|(_, name, value)| (name, value))
                .collect()
        }
    })
}

fn column_name(column_id: ColumnId, name_lookup: &HashMap<ColumnId, String>) -> String {
    if let Some(name) = name_lookup.get(&column_id) {
        return name.clone();
    }

    if column_id == ReservedColumnId::table_id() {
        return "__table_id".to_string();
    }
    if column_id == ReservedColumnId::tsid() {
        return "__tsid".to_string();
    }

    column_id.to_string()
}

fn value_to_string(value: Value) -> Option<String> {
    match value {
        Value::Null => None,
        _ => Some(value.to_string()),
    }
}

fn build_list_array(rows: &[Vec<NameValuePair>]) -> datafusion_common::Result<ArrayRef> {
    let mut builder = ListBuilder::new(StringBuilder::new());

    for row in rows {
        for (key, value) in row {
            let value = value.as_deref().unwrap_or(NULL_VALUE_LITERAL);
            builder.values().append_value(format!("{key} : {value}"));
        }
        builder.append(true);
    }

    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use api::v1::SemanticType;
    use datafusion_common::ScalarValue;
    use datatypes::arrow::array::builder::BinaryDictionaryBuilder;
    use datatypes::arrow::array::{BinaryArray, ListArray, StringArray};
    use datatypes::arrow::datatypes::UInt32Type;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use datatypes::value::Value;
    use mito_codec::row_converter::{
        DensePrimaryKeyCodec, PrimaryKeyCodecExt, SortField, SparsePrimaryKeyCodec,
    };
    use store_api::codec::PrimaryKeyEncoding;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::consts::ReservedColumnId;
    use store_api::storage::{ColumnId, RegionId};

    use super::*;

    fn pk_field() -> Arc<Field> {
        Arc::new(Field::new_dictionary(
            PRIMARY_KEY_COLUMN_NAME,
            DataType::UInt32,
            DataType::Binary,
            false,
        ))
    }

    fn region_metadata_json(
        columns: &[(ColumnId, &str, ConcreteDataType)],
        encoding: PrimaryKeyEncoding,
    ) -> String {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        builder.push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: 100,
        });
        builder.primary_key_encoding(encoding);
        for (id, name, ty) in columns {
            builder.push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new((*name).to_string(), ty.clone(), true),
                semantic_type: SemanticType::Tag,
                column_id: *id,
            });
        }
        builder.primary_key(columns.iter().map(|(id, _, _)| *id).collect());

        builder.build().unwrap().to_json().unwrap()
    }

    fn list_row(list: &ListArray, row_idx: usize) -> Vec<String> {
        let values = list.value(row_idx);
        let values = values.as_any().downcast_ref::<StringArray>().unwrap();
        (0..values.len())
            .map(|i| values.value(i).to_string())
            .collect()
    }

    #[test]
    fn test_decode_dense_primary_key() {
        let columns = vec![
            (0, "host", ConcreteDataType::string_datatype()),
            (1, "core", ConcreteDataType::int64_datatype()),
        ];
        let metadata_json = region_metadata_json(&columns, PrimaryKeyEncoding::Dense);
        let codec = DensePrimaryKeyCodec::with_fields(
            columns
                .iter()
                .map(|(id, _, ty)| (*id, SortField::new(ty.clone())))
                .collect(),
        );

        let rows = vec![
            vec![Value::from("a"), Value::from(1_i64)],
            vec![Value::from("b"), Value::from(2_i64)],
            vec![Value::from("a"), Value::from(1_i64)],
        ];

        let mut builder = BinaryDictionaryBuilder::<UInt32Type>::new();
        for row in &rows {
            let encoded = codec.encode(row.iter().map(|v| v.as_value_ref())).unwrap();
            builder.append(encoded.as_slice()).unwrap();
        }
        let dict_array: ArrayRef = Arc::new(builder.finish());

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(dict_array),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("dense".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(metadata_json))),
            ],
            arg_fields: vec![
                pk_field(),
                Arc::new(Field::new("encoding", DataType::Utf8, false)),
                Arc::new(Field::new("region_metadata", DataType::Utf8, false)),
            ],
            number_rows: 3,
            return_field: Arc::new(Field::new(
                "decoded",
                DecodePrimaryKeyFunction::return_data_type(),
                false,
            )),
            config_options: Default::default(),
        };

        let func = DecodePrimaryKeyFunction::default();
        let result = func
            .invoke_with_args(args)
            .and_then(|v| v.to_array(3))
            .unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();

        let expected = [
            vec!["host : a".to_string(), "core : 1".to_string()],
            vec!["host : b".to_string(), "core : 2".to_string()],
            vec!["host : a".to_string(), "core : 1".to_string()],
        ];

        for (row_idx, expected_row) in expected.iter().enumerate() {
            assert_eq!(*expected_row, list_row(list, row_idx));
        }
    }

    #[test]
    fn test_decode_sparse_primary_key() {
        let columns = vec![
            (10, "k0", ConcreteDataType::string_datatype()),
            (11, "k1", ConcreteDataType::string_datatype()),
        ];
        let metadata_json = region_metadata_json(&columns, PrimaryKeyEncoding::Sparse);
        let codec = SparsePrimaryKeyCodec::schemaless();

        let rows = vec![
            vec![
                (ReservedColumnId::table_id(), Value::UInt32(1)),
                (ReservedColumnId::tsid(), Value::UInt64(100)),
                (10, Value::from("a")),
                (11, Value::from("b")),
            ],
            vec![
                (ReservedColumnId::table_id(), Value::UInt32(1)),
                (ReservedColumnId::tsid(), Value::UInt64(200)),
                (10, Value::from("c")),
                (11, Value::from("d")),
            ],
        ];

        let mut encoded_values = Vec::with_capacity(rows.len());
        for row in &rows {
            let mut buf = Vec::new();
            codec.encode_values(row, &mut buf).unwrap();
            encoded_values.push(buf);
        }

        let pk_array: ArrayRef = Arc::new(BinaryArray::from_iter_values(
            encoded_values.iter().cloned(),
        ));

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(pk_array),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("sparse".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(metadata_json))),
            ],
            arg_fields: vec![
                pk_field(),
                Arc::new(Field::new("encoding", DataType::Utf8, false)),
                Arc::new(Field::new("region_metadata", DataType::Utf8, false)),
            ],
            number_rows: rows.len(),
            return_field: Arc::new(Field::new(
                "decoded",
                DecodePrimaryKeyFunction::return_data_type(),
                false,
            )),
            config_options: Default::default(),
        };

        let func = DecodePrimaryKeyFunction::default();
        let result = func
            .invoke_with_args(args)
            .and_then(|v| v.to_array(rows.len()))
            .unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();

        let expected = [
            vec![
                "k0 : a".to_string(),
                "k1 : b".to_string(),
                "__tsid : 100".to_string(),
                "__table_id : 1".to_string(),
            ],
            vec![
                "k0 : c".to_string(),
                "k1 : d".to_string(),
                "__tsid : 200".to_string(),
                "__table_id : 1".to_string(),
            ],
        ];

        for (row_idx, expected_row) in expected.iter().enumerate() {
            assert_eq!(*expected_row, list_row(list, row_idx));
        }
    }
}
