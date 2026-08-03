// Copyright 2026 Greptime Team
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

#[cfg(test)]
use std::sync::Arc;

use arrow_array::ArrayRef;
#[cfg(test)]
use arrow_array::StringArray;
use arrow_schema::Field;
#[cfg(test)]
use parquet_variant_compute::json_to_variant;
use parquet_variant_compute::{VariantArray, VariantArrayBuilder};
use parquet_variant_json::VariantToJson;
use snafu::ResultExt;

use crate::error::{ArrowComputeSnafu, Result};

/// Returns the canonical Arrow field for an unshredded Parquet Variant array.
pub fn variant_field(name: impl Into<String>, nullable: bool) -> Field {
    VariantArrayBuilder::new(0)
        .build()
        .field(name)
        .with_nullable(nullable)
}

/// Encodes JSON values as an unshredded Parquet Variant array.
///
/// `None` represents an Arrow null while `Some(Value::Null)` represents a JSON
/// null, preserving the distinction required by JSON2.
#[cfg(test)]
pub(crate) fn json_values_to_variant(values: &[Option<serde_json::Value>]) -> Result<ArrayRef> {
    let json = StringArray::from_iter(
        values
            .iter()
            .map(|x| x.as_ref().map(serde_json::Value::to_string)),
    );
    let json: ArrayRef = Arc::new(json);
    Ok(ArrayRef::from(
        json_to_variant(&json).context(ArrowComputeSnafu)?,
    ))
}

/// Decodes an unshredded Parquet Variant array into JSON values.
///
/// Arrow nulls remain `None`; encoded JSON nulls become `Some(Value::Null)`.
pub(crate) fn variant_to_json_values(array: &ArrayRef) -> Result<Vec<Option<serde_json::Value>>> {
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
    fn test_variant_json_round_trip() -> Result<()> {
        let values = vec![
            None,
            Some(serde_json::Value::Null),
            Some(json!({})),
            Some(json!({"nested": {"items": [1, "two", null]}})),
            Some(json!({"字段": "值"})),
        ];

        let array = json_values_to_variant(&values)?;
        assert_eq!(values, variant_to_json_values(&array)?);
        Ok(())
    }
}
