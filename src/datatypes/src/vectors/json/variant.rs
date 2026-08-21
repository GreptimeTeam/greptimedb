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

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::{ArrowError, DataType, Field};
use parquet_variant::{ObjectFieldBuilder, Variant, VariantBuilderExt, VariantDecimal16};
#[cfg(test)]
use parquet_variant_compute::VariantArrayBuilder;
use parquet_variant_compute::{VariantArray, VariantType};
use parquet_variant_json::VariantToJson;
use snafu::ResultExt;

use crate::error::{ArrowComputeSnafu, Result};
use crate::json::value::{JsonNumber, JsonVariant, JsonVariantRef, decode_json_variant};

/// Returns the canonical Arrow field for an unshredded Parquet Variant array.
pub fn variant_field(name: impl Into<String>, nullable: bool) -> Field {
    Field::new_struct(
        name,
        [
            Arc::new(Field::new("metadata", DataType::BinaryView, false)),
            Arc::new(Field::new("value", DataType::BinaryView, false)),
        ],
        nullable,
    )
    .with_extension_type(VariantType)
}

#[cfg(test)]
pub(crate) fn json_values_to_variant(values: &[Option<serde_json::Value>]) -> Result<ArrayRef> {
    let mut builder = VariantArrayBuilder::new(values.len());
    for value in values {
        match value {
            Some(value) => append_json_value(&mut builder, value).context(ArrowComputeSnafu)?,
            None => builder.append_null(),
        }
    }
    Ok(ArrayRef::from(builder.build()))
}

/// Encodes JSON variants as an unshredded Parquet Variant array.
#[cfg(test)]
fn json_variants_to_variant(values: &[Option<JsonVariant>]) -> Result<ArrayRef> {
    let mut builder = VariantArrayBuilder::new(values.len());
    for value in values {
        match value {
            Some(value) => append_json_variant(&mut builder, value).context(ArrowComputeSnafu)?,
            None => builder.append_null(),
        }
    }
    Ok(ArrayRef::from(builder.build()))
}

pub(super) fn append_json_variant(
    builder: &mut impl VariantBuilderExt,
    value: &JsonVariant,
) -> std::result::Result<(), ArrowError> {
    match value {
        JsonVariant::Null => builder.append_value(Variant::Null),
        JsonVariant::Bool(value) => builder.append_value(*value),
        JsonVariant::Number(JsonNumber::PosInt(value)) => {
            if let Ok(value) = i64::try_from(*value) {
                builder.append_value(value);
            } else {
                append_large_u64(builder, *value)?;
            }
        }
        JsonVariant::Number(JsonNumber::NegInt(value)) => builder.append_value(*value),
        JsonVariant::Number(JsonNumber::Float(value)) => {
            if value.0.is_finite() {
                builder.append_value(value.0)
            } else {
                builder.append_value("NaN")
            }
        }
        JsonVariant::String(value) => builder.append_value(value.as_str()),
        JsonVariant::Array(values) => {
            let mut list = builder.try_new_list()?;
            for value in values {
                append_json_variant(&mut list, value)?;
            }
            list.finish();
        }
        JsonVariant::Object(values) => {
            let mut object = builder.try_new_object()?;
            for (name, value) in values {
                append_json_variant(&mut ObjectFieldBuilder::new(name, &mut object), value)?;
            }
            object.finish();
        }
        JsonVariant::Variant(value) => {
            let value = decode_json_variant(value)
                .map_err(|e| ArrowError::JsonError(format!("Failed to decode JSONB: {e}")))?;
            append_json_value(builder, &value)?;
        }
    }
    Ok(())
}

pub(super) fn append_json_variant_ref(
    builder: &mut impl VariantBuilderExt,
    value: &JsonVariantRef<'_>,
) -> std::result::Result<(), ArrowError> {
    match value {
        JsonVariantRef::Null => builder.append_value(Variant::Null),
        JsonVariantRef::Bool(value) => builder.append_value(*value),
        JsonVariantRef::Number(JsonNumber::PosInt(value)) => {
            if let Ok(value) = i64::try_from(*value) {
                builder.append_value(value);
            } else {
                append_large_u64(builder, *value)?;
            }
        }
        JsonVariantRef::Number(JsonNumber::NegInt(value)) => builder.append_value(*value),
        JsonVariantRef::Number(JsonNumber::Float(value)) => {
            if value.0.is_finite() {
                builder.append_value(value.0)
            } else {
                builder.append_value("NaN")
            }
        }
        JsonVariantRef::String(value) => builder.append_value(*value),
        JsonVariantRef::Array(values) => {
            let mut list = builder.try_new_list()?;
            for value in values {
                append_json_variant_ref(&mut list, value)?;
            }
            list.finish();
        }
        JsonVariantRef::Object(values) => {
            let mut object = builder.try_new_object()?;
            for (name, value) in values {
                append_json_variant_ref(&mut ObjectFieldBuilder::new(name, &mut object), value)?;
            }
            object.finish();
        }
        JsonVariantRef::Variant(value) => {
            let value = decode_json_variant(value)
                .map_err(|e| ArrowError::JsonError(format!("Failed to decode JSONB: {e}")))?;
            append_json_value(builder, &value)?;
        }
    }
    Ok(())
}

fn append_json_value(
    builder: &mut impl VariantBuilderExt,
    value: &serde_json::Value,
) -> std::result::Result<(), ArrowError> {
    match value {
        serde_json::Value::Null => builder.append_value(Variant::Null),
        serde_json::Value::Bool(value) => builder.append_value(*value),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                builder.append_value(value);
            } else if let Some(value) = value.as_u64() {
                append_large_u64(builder, value)?;
            } else if let Some(value) = value.as_f64() {
                builder.append_value(value);
            } else {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Failed to encode JSON number as Variant: {value}"
                )));
            }
        }
        serde_json::Value::String(value) => builder.append_value(value.as_str()),
        serde_json::Value::Array(values) => {
            let mut list = builder.try_new_list()?;
            for value in values {
                append_json_value(&mut list, value)?;
            }
            list.finish();
        }
        serde_json::Value::Object(values) => {
            let mut object = builder.try_new_object()?;
            for (name, value) in values {
                append_json_value(&mut ObjectFieldBuilder::new(name, &mut object), value)?;
            }
            object.finish();
        }
    }
    Ok(())
}

/// Parquet Variant has no unsigned integer primitive. Treat u64 as i64 first, then use Decimal16
/// to represent large (larger than i64::MAX) u64.
fn append_large_u64(
    builder: &mut impl VariantBuilderExt,
    value: u64,
) -> std::result::Result<(), ArrowError> {
    let value = VariantDecimal16::try_new(value as i128, 0).map_err(|e| {
        ArrowError::InvalidArgumentError(format!(
            "Failed to encode JSON large integer as Variant Decimal16: {e}"
        ))
    })?;
    builder.append_value(value);
    Ok(())
}

/// Decodes an unshredded Parquet Variant array into JSON values.
pub fn variant_to_json_values(array: &ArrayRef) -> Result<Vec<Option<serde_json::Value>>> {
    let variants = VariantArray::try_new(array.as_ref()).context(ArrowComputeSnafu)?;
    (0..variants.len())
        .map(|i| {
            if variants.is_null(i) {
                Ok(None)
            } else {
                variants
                    .try_value(i)
                    .and_then(|x| x.to_json_value())
                    .context(ArrowComputeSnafu)
                    .map(Some)
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_variant_field_matches_canonical_layout() {
        let expected = VariantArrayBuilder::new(0)
            .build()
            .field("remainder")
            .with_nullable(true);
        assert_eq!(expected, variant_field("remainder", true));
    }

    #[test]
    fn test_variant_json_round_trip() -> Result<()> {
        let values = vec![
            None,
            Some(serde_json::Value::Null),
            Some(json!({})),
            Some(json!({"nested": {"items": [1, "two", null]}})),
            Some(json!({"unicode": "\u{503c}"})),
            Some(json!({"max_u64": u64::MAX})),
        ];

        let array = json_values_to_variant(&values)?;
        assert_eq!(values, variant_to_json_values(&array)?);

        let variants = values
            .clone()
            .into_iter()
            .map(|x| x.map(JsonVariant::from))
            .collect::<Vec<_>>();
        let array = json_variants_to_variant(&variants)?;
        assert_eq!(values, variant_to_json_values(&array)?);

        let variants = [Some(JsonVariant::Variant(
            jsonb::parse_value(br#"{"nested": true}"#).unwrap().to_vec(),
        ))];
        let array = json_variants_to_variant(&variants)?;
        assert_eq!(
            vec![Some(json!({"nested": true}))],
            variant_to_json_values(&array)?
        );
        Ok(())
    }
}
