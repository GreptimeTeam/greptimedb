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
use std::sync::Arc;

use datatypes::arrow::datatypes::{DataType as ArrowDataType, Schema, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::data_type::DataType;
use datatypes::extension::json::is_structured_json_field;
use datatypes::types::JsonType;
use datatypes::vectors::json::array::JsonArray;
use snafu::{OptionExt, ResultExt};

use crate::error::{
    ConvertValueSnafu, DataTypeMismatchSnafu, NewRecordBatchSnafu, Result, UnexpectedSnafu,
};
use crate::memtable::BoxedRecordBatchIterator;

/// Aligns concrete JSON2 Arrow types across record batches.
///
/// JSON2 column concrete Arrow types are derived from data. Different sources
/// may therefore have different concrete types for the same JSON2 column. This
/// helper merges those concrete types and aligns batches to the merged schema.
#[derive(Clone)]
pub(crate) struct Json2Aligner {
    /// Schema after merging all JSON2 column concrete types.
    schema: SchemaRef,
    /// JSON2 columns that may need per-batch alignment.
    json_columns: Vec<(usize, ArrowDataType)>,
}

impl Json2Aligner {
    /// Builds an aligner from input schemas.
    ///
    /// Note: except for JSON2 columns, all input schemas must be identical.
    pub(crate) fn try_new<I>(input_schemas: I) -> Result<Self>
    where
        I: IntoIterator<Item = SchemaRef>,
    {
        let mut input_schemas = input_schemas.into_iter();

        // Use first schema as base: it defines column order and non-JSON types.
        let base_schema = input_schemas.next().context(UnexpectedSnafu {
            reason: "Json2Aligner requires at least one input schema",
        })?;

        // Init merged types from base schema.
        let mut merged_types: HashMap<usize, JsonType> = base_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|&(_idx, field)| is_structured_json_field(field))
            .map(|(idx, field)| (idx, JsonType::from(field.data_type())))
            .collect();

        // No JSON2 columns, no alignment needed.
        if merged_types.is_empty() {
            return Ok(Self {
                schema: base_schema,
                json_columns: Vec::new(),
            });
        }

        // Merge JSON2 types from remaining schemas.
        for schema in input_schemas {
            // Input schemas should only differ in JSON2 concrete types.
            #[cfg(debug_assertions)]
            assert_columns_match_except_json2(&base_schema, &schema);

            for (idx, merged) in &mut merged_types {
                if *idx >= schema.fields().len() {
                    continue;
                }
                merged
                    .merge(&JsonType::from(schema.field(*idx).data_type()))
                    .context(DataTypeMismatchSnafu)?;
            }
        }

        // Build output schema with merged JSON2 types.
        let mut json_columns = Vec::with_capacity(merged_types.len());
        let fields: Vec<_> = base_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| {
                if let Some(merged) = merged_types.get(&idx) {
                    let data_type = merged.as_arrow_type();
                    json_columns.push((idx, data_type.clone()));
                    let mut field = (**field).clone();
                    field.set_data_type(data_type);
                    Arc::new(field)
                } else {
                    field.clone()
                }
            })
            .collect();

        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            base_schema.metadata().clone(),
        ));

        Ok(Self {
            schema,
            json_columns,
        })
    }

    /// Returns the aligned output schema.
    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Aligns a [`RecordBatch`] to [`Self::schema`].
    pub(crate) fn align_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        if self.json_columns.is_empty() {
            return Ok(batch);
        }
        let mut cols = batch.columns().to_vec();
        for (idx, expected_type) in &self.json_columns {
            if batch.schema_ref().field(*idx).data_type() != expected_type {
                cols[*idx] = JsonArray::from(batch.column(*idx))
                    .try_align(expected_type)
                    .context(ConvertValueSnafu)?;
            }
        }
        RecordBatch::try_new(self.schema.clone(), cols).context(NewRecordBatchSnafu)
    }

    /// Aligns [`RecordBatch`]s to [`Self::schema`].
    pub(crate) fn align_batches<I>(&self, batches: I) -> Result<Vec<RecordBatch>>
    where
        I: IntoIterator<Item = RecordBatch>,
    {
        batches
            .into_iter()
            .map(|batch| self.align_batch(batch))
            .collect()
    }

    /// Wraps an iterator so each yielded [`RecordBatch`] is lazily aligned.
    pub(crate) fn wrap_iter(&self, iter: BoxedRecordBatchIterator) -> BoxedRecordBatchIterator {
        let aligner = self.clone();
        Box::new(iter.map(move |batch| aligner.align_batch(batch?)))
    }
}

#[cfg(debug_assertions)]
fn assert_columns_match_except_json2(base_schema: &Schema, schema: &Schema) {
    debug_assert_eq!(
        base_schema.fields().len(),
        schema.fields().len(),
        "input schemas for Json2Aligner must have the same column count"
    );
    for (idx, (base_field, field)) in base_schema.fields().iter().zip(schema.fields()).enumerate() {
        let base_is_json2 = is_structured_json_field(base_field);
        let is_json2 = is_structured_json_field(field);
        debug_assert_eq!(
            base_is_json2, is_json2,
            "column {idx} must be JSON2 in all input schemas or none"
        );
        if !base_is_json2 && !is_json2 {
            debug_assert_eq!(
                base_field, field,
                "non-JSON2 column {idx} must be identical across input schemas"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::arrow::array::{
        Array, ArrayRef, Int64Array, StringViewArray, StructArray, UInt64Array,
    };
    use datatypes::arrow::datatypes::{DataType, Field, Fields, Schema};
    use datatypes::extension::json::{JsonExtensionType, JsonMetadata};

    use super::*;

    #[test]
    fn test_try_new_rejects_empty_input() {
        let err = match Json2Aligner::try_new([]) {
            Ok(_) => panic!("expected empty input to fail"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("Json2Aligner requires at least one input schema")
        );
    }

    #[test]
    fn test_try_new_keeps_non_json_schema_unchanged() {
        let schema = Arc::new(Schema::new(vec![
            Arc::new(Field::new("ts", DataType::Int64, false)),
            Arc::new(Field::new("value", DataType::UInt64, true)),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values([1, 2])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![Some(10), None])) as ArrayRef,
            ],
        )
        .unwrap();

        let aligner = Json2Aligner::try_new([schema.clone()]).unwrap();
        assert!(Arc::ptr_eq(aligner.schema(), &schema));

        let aligned = aligner.align_batch(batch).unwrap();
        assert!(Arc::ptr_eq(aligned.schema_ref(), &schema));
    }

    #[test]
    fn test_try_new_ignores_legacy_jsonb_extension_field() {
        let legacy_jsonb_field = Arc::new(
            Field::new("data", DataType::Binary, true)
                .with_extension_type(JsonExtensionType::new(Arc::new(JsonMetadata::default()))),
        );
        let schema = Arc::new(Schema::new(vec![
            Arc::new(Field::new("ts", DataType::Int64, false)),
            legacy_jsonb_field,
        ]));

        let aligner = Json2Aligner::try_new([schema.clone()]).unwrap();

        assert!(Arc::ptr_eq(aligner.schema(), &schema));
        assert!(aligner.json_columns.is_empty());
    }

    #[test]
    fn test_try_new_merges_json2_object_fields() {
        let id_fields = Fields::from(vec![id_field()]);
        let name_fields = Fields::from(vec![name_field()]);
        let schema_with_id = schema_with_json_field(json_field("data", id_fields));
        let schema_with_name = schema_with_json_field(json_field("data", name_fields));

        let aligner = Json2Aligner::try_new([schema_with_id, schema_with_name]).unwrap();
        let data_field = aligner.schema().field(1);
        let DataType::Struct(fields) = data_field.data_type() else {
            panic!("expected JSON2 field to be a struct");
        };

        assert_eq!(2, fields.len());
        assert_eq!("id", fields[0].name());
        assert_eq!(&DataType::Int64, fields[0].data_type());
        assert_eq!("name", fields[1].name());
        assert_eq!(&DataType::Utf8View, fields[1].data_type());
        assert!(is_structured_json_field(&aligner.schema().fields()[1]));
    }

    #[test]
    fn test_align_batch_fills_missing_json2_fields() {
        let id_fields = Fields::from(vec![id_field()]);
        let name_fields = Fields::from(vec![name_field()]);
        let schema_with_id = schema_with_json_field(json_field("data", id_fields.clone()));
        let schema_with_name = schema_with_json_field(json_field("data", name_fields.clone()));

        let batch_with_id = RecordBatch::try_new(
            schema_with_id.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values([1, 2])) as ArrayRef,
                struct_array(
                    id_fields,
                    vec![Arc::new(Int64Array::from_iter_values([10, 20])) as ArrayRef],
                ),
            ],
        )
        .unwrap();
        let batch_with_name = RecordBatch::try_new(
            schema_with_name.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values([3, 4])) as ArrayRef,
                struct_array(
                    name_fields,
                    vec![
                        Arc::new(StringViewArray::from(vec![Some("alice"), Some("bob")]))
                            as ArrayRef,
                    ],
                ),
            ],
        )
        .unwrap();

        let aligner = Json2Aligner::try_new([schema_with_id, schema_with_name]).unwrap();
        let aligned_with_id = aligner.align_batch(batch_with_id).unwrap();
        let aligned_with_name = aligner.align_batch(batch_with_name).unwrap();

        let data_with_id = aligned_with_id
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let id_values = data_with_id
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let name_values = data_with_id
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        assert_eq!(Some(10), id_values.value(0).into());
        assert!(name_values.is_null(0));

        let data_with_name = aligned_with_name
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let id_values = data_with_name
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let name_values = data_with_name
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        assert!(id_values.is_null(0));
        assert_eq!("alice", name_values.value(0));
    }

    fn schema_with_json_field(field: Field) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Arc::new(Field::new("ts", DataType::Int64, false)),
            Arc::new(field),
        ]))
    }

    fn json_field(name: &str, fields: Fields) -> Field {
        Field::new(name, DataType::Struct(fields), true)
            .with_extension_type(JsonExtensionType::new(Arc::new(JsonMetadata::default())))
    }

    fn id_field() -> Field {
        Field::new("id", DataType::Int64, true)
    }

    fn name_field() -> Field {
        Field::new("name", DataType::Utf8View, true)
    }

    fn struct_array(fields: Fields, arrays: Vec<ArrayRef>) -> ArrayRef {
        Arc::new(StructArray::new(fields, arrays, None))
    }

    /// Verifies that `Json2Aligner` can align a projected batch where the JSON2
    /// column is at a different index than in the full schema used to build the
    /// aligner. Columns are matched by `PARQUET:field_id`, not positional index.
    #[test]
    fn test_align_batch_works_with_projected_batch() {
        let id_fields = Fields::from(vec![id_field()]);
        // Full schema: [ts(id=0), j(id=1)]
        let full_schema = schema_with_json_field(json_field("j", id_fields.clone()));
        let aligner = Json2Aligner::try_new([full_schema]).unwrap();
        assert_eq!(aligner.json_columns.len(), 1);
        assert_eq!(aligner.json_columns[0].0, 1); // column_id = 1

        // Projected batch: only [j(id=1)], at index 0
        let projected_schema = Arc::new(Schema::new(vec![
            Arc::new(json_field("j", id_fields.clone())),
        ]));
        let batch = RecordBatch::try_new(
            projected_schema,
            vec![struct_array(
                id_fields,
                vec![Arc::new(Int64Array::from_iter_values([10, 20])) as ArrayRef],
            )],
        )
        .unwrap();

        // Should NOT panic — aligner finds j by column_id=1, not index
        let aligned = aligner.align_batch(batch).unwrap();
        assert_eq!(aligned.num_columns(), 1);
        assert_eq!(
            aligned.schema_ref().field(0).data_type(),
            &aligner.json_columns[0].1
        );
    }
}
