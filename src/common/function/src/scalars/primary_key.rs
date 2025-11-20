// Copyright 2024 Greptime Team
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

use datafusion_common::arrow::array::builder::MapFieldNames;
use datafusion_common::arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, DictionaryArray, MapBuilder, StringBuilder,
};
use datafusion_common::arrow::datatypes::{DataType, Field};
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};
use datatypes::value::Value;
use mito_codec::row_converter::{
    CompositeValues, PrimaryKeyCodec, SortField, build_primary_key_codec_with_fields,
};
use serde_json;
use store_api::codec::PrimaryKeyEncoding;
use store_api::primary_key_metadata::PrimaryKeyMetadata;
use store_api::storage::consts::{
    PRIMARY_KEY_METADATA_KEY, PRIMARY_KEY_COLUMN_NAME, ReservedColumnId,
};
use store_api::storage::ColumnId;
use datatypes::arrow::datatypes::UInt32Type;

use crate::function::{Function, extract_args};
use crate::function_registry::FunctionRegistry;

#[derive(Clone, Debug)]
pub(crate) struct DecodePrimaryKeyFunction {
    signature: Signature,
}

const NAME: &str = "decode_primary_key";
const MAP_VALUE_NULLABLE: bool = true;

impl Default for DecodePrimaryKeyFunction {
    fn default() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl DecodePrimaryKeyFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register_scalar(Self::default());
    }

    fn return_data_type() -> DataType {
        let entry_field = Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Utf8, MAP_VALUE_NULLABLE),
                ]
                .into(),
            ),
            false,
        );
        DataType::Map(Arc::new(entry_field), false)
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
        let [encoded, _] = extract_args(self.name(), &args)?;
        let number_rows = args.number_rows;
        let metadata = primary_key_metadata(&args.arg_fields)?;

        let encoding = parse_encoding(&args.args[1])?;
        let codec = build_codec(&metadata, encoding);
        let name_lookup: HashMap<_, _> = metadata
            .columns
            .iter()
            .map(|c| (c.column_id, c.name.clone()))
            .collect();

        let decoded_rows =
            decode_primary_keys(encoded, number_rows, codec.as_ref(), &name_lookup)?;
        let array = build_map_array(&decoded_rows)?;

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
            )))
        }
        ColumnarValue::Array(_) => {
            return Err(DataFusionError::Execution(
                "encoding must be a scalar string".to_string(),
            ))
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

fn build_codec(metadata: &PrimaryKeyMetadata, encoding: PrimaryKeyEncoding) -> Arc<dyn PrimaryKeyCodec> {
    let fields = metadata
        .columns
        .iter()
        .map(|c| (c.column_id, SortField::new(c.data_type.clone())));
    build_primary_key_codec_with_fields(encoding, fields)
}

fn primary_key_metadata(arg_fields: &[Arc<Field>]) -> datafusion_common::Result<PrimaryKeyMetadata> {
    let Some(field) = arg_fields.first() else {
        return Err(DataFusionError::Execution(
            "primary key field metadata is missing".to_string(),
        ));
    };

    let Some(value) = field.metadata().get(PRIMARY_KEY_METADATA_KEY) else {
        return Err(DataFusionError::Execution(format!(
            "field {} does not contain required `{PRIMARY_KEY_METADATA_KEY}` metadata",
            field.name()
        )));
    };

    serde_json::from_str(value).map_err(|e| {
        DataFusionError::Execution(format!(
            "failed to parse {PRIMARY_KEY_METADATA_KEY} metadata: {e}"
        ))
    })
}

fn decode_primary_keys(
    encoded: ArrayRef,
    number_rows: usize,
    codec: &dyn PrimaryKeyCodec,
    name_lookup: &HashMap<ColumnId, String>,
) -> datafusion_common::Result<Vec<Vec<(String, Option<String>)>>> {
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
) -> datafusion_common::Result<Vec<Vec<(String, Option<String>)>>> {
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
) -> datafusion_common::Result<Vec<Vec<(String, Option<String>)>>> {
    (0..array.len())
        .map(|i| decode_one(array.value(i), codec, name_lookup))
        .collect()
}

fn decode_binary_view_array(
    array: &BinaryViewArray,
    codec: &dyn PrimaryKeyCodec,
    name_lookup: &HashMap<ColumnId, String>,
) -> datafusion_common::Result<Vec<Vec<(String, Option<String>)>>> {
    (0..array.len())
        .map(|i| decode_one(array.value(i), codec, name_lookup))
        .collect()
}

fn decode_one(
    pk: &[u8],
    codec: &dyn PrimaryKeyCodec,
    name_lookup: &HashMap<ColumnId, String>,
) -> datafusion_common::Result<Vec<(String, Option<String>)>> {
    let decoded = codec
        .decode(pk)
        .map_err(|e| DataFusionError::Execution(format!("failed to decode primary key: {e}")))?;

    Ok(match decoded {
        CompositeValues::Dense(values) => values
            .into_iter()
            .map(|(column_id, value)| {
                (
                    column_name(column_id, name_lookup),
                    value_to_string(value),
                )
            })
            .collect(),
        CompositeValues::Sparse(values) => values
            .iter()
            .map(|(column_id, value)| {
                (
                    column_name(*column_id, name_lookup),
                    value_to_string(value.clone()),
                )
            })
            .collect(),
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

fn build_map_array(rows: &[Vec<(String, Option<String>)>]) -> datafusion_common::Result<ArrayRef> {
    let field_names = MapFieldNames {
        entry: "entries".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    };
    let mut builder =
        MapBuilder::new(Some(field_names), StringBuilder::new(), StringBuilder::new());

    for row in rows {
        for (key, value) in row {
            builder.keys().append_value(key);
            match value {
                Some(v) => builder.values().append_value(v),
                None => builder.values().append_null(),
            }
        }
        builder.append(true)?;
    }

    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion_common::ScalarValue;
    use datatypes::arrow::array::builder::BinaryDictionaryBuilder;
    use datatypes::arrow::array::{BinaryArray, MapArray, StructArray, StringArray};
    use datatypes::arrow::datatypes::UInt32Type;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::value::Value;
    use mito_codec::row_converter::{
        DensePrimaryKeyCodec, PrimaryKeyCodecExt, SortField, SparsePrimaryKeyCodec,
    };
    use store_api::storage::consts::ReservedColumnId;
    use store_api::storage::ColumnId;
    use store_api::primary_key_metadata::PrimaryKeyColumnMetadata;

    use super::*;

    fn make_pk_metadata(columns: &[(ColumnId, &str, ConcreteDataType)]) -> PrimaryKeyMetadata {
        PrimaryKeyMetadata {
            columns: columns
                .iter()
                .map(|(id, name, ty)| PrimaryKeyColumnMetadata {
                    name: (*name).to_string(),
                    column_id: *id,
                    data_type: ty.clone(),
                })
                .collect(),
        }
    }

    fn pk_field_with_metadata(metadata: &PrimaryKeyMetadata) -> Arc<Field> {
        let mut meta = HashMap::new();
        meta.insert(
            PRIMARY_KEY_METADATA_KEY.to_string(),
            serde_json::to_string(metadata).unwrap(),
        );
        Arc::new(Field::new(
            PRIMARY_KEY_COLUMN_NAME,
            DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Binary)),
            false,
        )
        .with_metadata(meta))
    }

    fn map_row(map: &MapArray, row_idx: usize) -> HashMap<String, Option<String>> {
        let struct_values = map.value(row_idx);
        let struct_array = struct_values
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let keys = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = struct_array
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let mut row = HashMap::with_capacity(struct_array.len());
        for i in 0..struct_array.len() {
            let key = keys.value(i).to_string();
            let value = if values.is_null(i) {
                None
            } else {
                Some(values.value(i).to_string())
            };
            row.insert(key, value);
        }

        row
    }

    #[test]
    fn test_decode_dense_primary_key() {
        let columns = vec![
            (0, "host", ConcreteDataType::string_datatype()),
            (1, "core", ConcreteDataType::int64_datatype()),
        ];
        let metadata = make_pk_metadata(&columns);
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
            let encoded = codec
                .encode(row.iter().map(|v| v.as_value_ref()))
                .unwrap();
            builder.append(encoded.as_slice()).unwrap();
        }
        let dict_array: ArrayRef = Arc::new(builder.finish());

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(dict_array),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("dense".to_string()))),
            ],
            arg_fields: vec![
                pk_field_with_metadata(&metadata),
                Arc::new(Field::new("encoding", DataType::Utf8, false)),
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
        let map = result.as_any().downcast_ref::<MapArray>().unwrap();

        let expected = [
            HashMap::from([
                ("host".to_string(), Some("a".to_string())),
                ("core".to_string(), Some("1".to_string())),
            ]),
            HashMap::from([
                ("host".to_string(), Some("b".to_string())),
                ("core".to_string(), Some("2".to_string())),
            ]),
            HashMap::from([
                ("host".to_string(), Some("a".to_string())),
                ("core".to_string(), Some("1".to_string())),
            ]),
        ];

        for (row_idx, expected_row) in expected.iter().enumerate() {
            assert_eq!(*expected_row, map_row(map, row_idx));
        }
    }

    #[test]
    fn test_decode_sparse_primary_key() {
        let columns = vec![
            (10, "k0", ConcreteDataType::string_datatype()),
            (11, "k1", ConcreteDataType::string_datatype()),
        ];
        let metadata = make_pk_metadata(&columns);
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

        let pk_array: ArrayRef =
            Arc::new(BinaryArray::from_iter_values(encoded_values.iter().cloned()));

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(pk_array),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("sparse".to_string()))),
            ],
            arg_fields: vec![
                pk_field_with_metadata(&metadata),
                Arc::new(Field::new("encoding", DataType::Utf8, false)),
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
        let map = result.as_any().downcast_ref::<MapArray>().unwrap();

        let expected = [
            HashMap::from([
                ("__table_id".to_string(), Some("1".to_string())),
                ("__tsid".to_string(), Some("100".to_string())),
                ("k0".to_string(), Some("a".to_string())),
                ("k1".to_string(), Some("b".to_string())),
            ]),
            HashMap::from([
                ("__table_id".to_string(), Some("1".to_string())),
                ("__tsid".to_string(), Some("200".to_string())),
                ("k0".to_string(), Some("c".to_string())),
                ("k1".to_string(), Some("d".to_string())),
            ]),
        ];

        for (row_idx, expected_row) in expected.iter().enumerate() {
            assert_eq!(*expected_row, map_row(map, row_idx));
        }
    }
}
