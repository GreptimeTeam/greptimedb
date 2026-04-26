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

pub mod index;
pub mod transformer;

use std::collections::HashMap;

use api::v1::ColumnDataType;
use api::v1::value::ValueData;
use chrono::Utc;
use datatypes::schema::{FulltextOptions, SkippingIndexOptions};
use snafu::{OptionExt, ResultExt, ensure};
use sql::parsers::utils::{
    validate_column_fulltext_create_option, validate_column_skipping_index_create_option,
};

use crate::error::{
    Error, FieldMustBeTypeSnafu, KeyMustBeStringSnafu, Result, TransformElementMustBeMapSnafu,
    TransformFieldMustBeSetSnafu, TransformIndexOptionMustBeScalarSnafu, TransformIndexOptionSnafu,
    TransformIndexOptionUnsupportedSnafu, TransformIndexOptionsUnsupportedSnafu,
    TransformIndexTypeMismatchSnafu, TransformIndexTypeMustBeSetSnafu,
    TransformIndexUnsupportedFieldSnafu, TransformOnFailureInvalidValueSnafu,
    TransformTypeMustBeSetSnafu, UnsupportedTypeInPipelineSnafu,
};
use crate::etl::field::Fields;
use crate::etl::processor::{yaml_bool, yaml_new_field, yaml_new_fields, yaml_string};
use crate::etl::transform::index::Index;
use crate::etl::value::{parse_str_type, parse_str_value};

const TRANSFORM_FIELD: &str = "field";
const TRANSFORM_FIELDS: &str = "fields";
const TRANSFORM_TYPE: &str = "type";
const TRANSFORM_INDEX: &str = "index";
const TRANSFORM_INDEX_TYPE_FIELD: &str = "index.type";
const TRANSFORM_INDEX_OPTIONS: &str = "options";
const TRANSFORM_INDEX_OPTIONS_FIELD: &str = "index.options";
const TRANSFORM_TAG: &str = "tag";
const TRANSFORM_DEFAULT: &str = "default";
const TRANSFORM_ON_FAILURE: &str = "on_failure";

pub use transformer::greptime::GreptimeTransformer;

/// On Failure behavior when transform fails
#[derive(Debug, Clone, Default, Copy)]
pub enum OnFailure {
    // Return None if transform fails
    #[default]
    Ignore,
    // Return default value of the field if transform fails
    // Default value depends on the type of the field, or explicitly set by user
    Default,
}

impl std::str::FromStr for OnFailure {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "ignore" => Ok(OnFailure::Ignore),
            "default" => Ok(OnFailure::Default),
            _ => TransformOnFailureInvalidValueSnafu { value: s }.fail(),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct Transforms {
    pub(crate) transforms: Vec<Transform>,
}

impl Transforms {
    pub fn transforms(&self) -> &Vec<Transform> {
        &self.transforms
    }
}

impl std::ops::Deref for Transforms {
    type Target = Vec<Transform>;

    fn deref(&self) -> &Self::Target {
        &self.transforms
    }
}

impl std::ops::DerefMut for Transforms {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.transforms
    }
}

impl TryFrom<&Vec<yaml_rust::Yaml>> for Transforms {
    type Error = Error;

    fn try_from(docs: &Vec<yaml_rust::Yaml>) -> Result<Self> {
        let mut transforms = Vec::with_capacity(32);
        let mut all_output_keys: Vec<String> = Vec::with_capacity(32);
        let mut all_required_keys = Vec::with_capacity(32);

        for doc in docs {
            let transform_builder: Transform = doc
                .as_hash()
                .context(TransformElementMustBeMapSnafu)?
                .try_into()?;
            let mut transform_output_keys = transform_builder
                .fields
                .iter()
                .map(|f| f.target_or_input_field().to_string())
                .collect();
            all_output_keys.append(&mut transform_output_keys);

            let mut transform_required_keys = transform_builder
                .fields
                .iter()
                .map(|f| f.input_field().to_string())
                .collect();
            all_required_keys.append(&mut transform_required_keys);

            transforms.push(transform_builder);
        }

        all_required_keys.sort();

        Ok(Transforms { transforms })
    }
}

/// only field is required
#[derive(Debug, Clone)]
pub struct Transform {
    pub fields: Fields,
    pub type_: ColumnDataType,
    pub default: Option<ValueData>,
    pub index: Option<Index>,
    pub index_options: Option<TransformIndexOptions>,
    pub tag: bool,
    pub on_failure: Option<OnFailure>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransformIndexOptions {
    Fulltext(FulltextOptions),
    Skipping(SkippingIndexOptions),
}

impl TransformIndexOptions {
    pub(crate) fn index(&self) -> Index {
        match self {
            TransformIndexOptions::Fulltext(_) => Index::Fulltext,
            TransformIndexOptions::Skipping(_) => Index::Skipping,
        }
    }

    pub(crate) fn as_fulltext(&self) -> Option<&FulltextOptions> {
        match self {
            TransformIndexOptions::Fulltext(options) => Some(options),
            TransformIndexOptions::Skipping(_) => None,
        }
    }

    pub(crate) fn as_skipping(&self) -> Option<&SkippingIndexOptions> {
        match self {
            TransformIndexOptions::Skipping(options) => Some(options),
            TransformIndexOptions::Fulltext(_) => None,
        }
    }
}

// valid types
// ColumnDataType::Int8
// ColumnDataType::Int16
// ColumnDataType::Int32
// ColumnDataType::Int64
// ColumnDataType::Uint8
// ColumnDataType::Uint16
// ColumnDataType::Uint32
// ColumnDataType::Uint64
// ColumnDataType::Float32
// ColumnDataType::Float64
// ColumnDataType::Boolean
// ColumnDataType::String
// ColumnDataType::TimestampNanosecond
// ColumnDataType::TimestampMicrosecond
// ColumnDataType::TimestampMillisecond
// ColumnDataType::TimestampSecond
// ColumnDataType::Binary

impl Transform {
    pub(crate) fn get_default(&self) -> Option<&ValueData> {
        self.default.as_ref()
    }

    pub(crate) fn get_type_matched_default_val(&self) -> Result<ValueData> {
        get_default_for_type(&self.type_)
    }

    pub(crate) fn get_default_value_when_data_is_none(&self) -> Option<ValueData> {
        if is_timestamp_type(&self.type_) && self.index.is_some_and(|i| i == Index::Time) {
            let now = Utc::now();
            match self.type_ {
                ColumnDataType::TimestampSecond => {
                    return Some(ValueData::TimestampSecondValue(now.timestamp()));
                }
                ColumnDataType::TimestampMillisecond => {
                    return Some(ValueData::TimestampMillisecondValue(now.timestamp_millis()));
                }
                ColumnDataType::TimestampMicrosecond => {
                    return Some(ValueData::TimestampMicrosecondValue(now.timestamp_micros()));
                }
                ColumnDataType::TimestampNanosecond => {
                    return Some(ValueData::TimestampNanosecondValue(
                        now.timestamp_nanos_opt()?,
                    ));
                }
                _ => {}
            }
        }
        None
    }

    pub(crate) fn is_timeindex(&self) -> bool {
        self.index.is_some_and(|i| i == Index::Time)
    }
}

fn is_timestamp_type(ty: &ColumnDataType) -> bool {
    matches!(
        ty,
        ColumnDataType::TimestampSecond
            | ColumnDataType::TimestampMillisecond
            | ColumnDataType::TimestampMicrosecond
            | ColumnDataType::TimestampNanosecond
    )
}

fn get_default_for_type(ty: &ColumnDataType) -> Result<ValueData> {
    let v = match ty {
        ColumnDataType::Boolean => ValueData::BoolValue(false),
        ColumnDataType::Int8 => ValueData::I8Value(0),
        ColumnDataType::Int16 => ValueData::I16Value(0),
        ColumnDataType::Int32 => ValueData::I32Value(0),
        ColumnDataType::Int64 => ValueData::I64Value(0),
        ColumnDataType::Uint8 => ValueData::U8Value(0),
        ColumnDataType::Uint16 => ValueData::U16Value(0),
        ColumnDataType::Uint32 => ValueData::U32Value(0),
        ColumnDataType::Uint64 => ValueData::U64Value(0),
        ColumnDataType::Float32 => ValueData::F32Value(0.0),
        ColumnDataType::Float64 => ValueData::F64Value(0.0),
        ColumnDataType::Binary => ValueData::BinaryValue(jsonb::Value::Null.to_vec()),
        ColumnDataType::String => ValueData::StringValue(String::new()),

        ColumnDataType::TimestampSecond => ValueData::TimestampSecondValue(0),
        ColumnDataType::TimestampMillisecond => ValueData::TimestampMillisecondValue(0),
        ColumnDataType::TimestampMicrosecond => ValueData::TimestampMicrosecondValue(0),
        ColumnDataType::TimestampNanosecond => ValueData::TimestampNanosecondValue(0),

        _ => UnsupportedTypeInPipelineSnafu {
            ty: ty.as_str_name(),
        }
        .fail()?,
    };
    Ok(v)
}

fn parse_transform_index(
    value: &yaml_rust::Yaml,
) -> Result<(Index, Option<HashMap<String, String>>)> {
    match value {
        yaml_rust::Yaml::String(_) => {
            let index_str = yaml_string(value, TRANSFORM_INDEX)?;
            Ok((index_str.try_into()?, None))
        }
        yaml_rust::Yaml::Hash(hash) => parse_transform_index_map(hash),
        _ => FieldMustBeTypeSnafu {
            field: TRANSFORM_INDEX,
            ty: "string or map",
        }
        .fail(),
    }
}

fn parse_transform_index_map(
    hash: &yaml_rust::yaml::Hash,
) -> Result<(Index, Option<HashMap<String, String>>)> {
    let mut index = None;
    let mut index_options = None;

    for (k, v) in hash {
        let key = k
            .as_str()
            .with_context(|| KeyMustBeStringSnafu { k: k.clone() })?;
        match key {
            TRANSFORM_TYPE => {
                let index_str = yaml_string(v, TRANSFORM_INDEX_TYPE_FIELD)?;
                index = Some(index_str.try_into()?);
            }
            TRANSFORM_INDEX_OPTIONS => {
                index_options = Some(parse_transform_index_options(v)?);
            }
            _ => {
                return TransformIndexUnsupportedFieldSnafu {
                    field: key.to_string(),
                }
                .fail();
            }
        }
    }

    let index = index.context(TransformIndexTypeMustBeSetSnafu)?;
    Ok((index, index_options))
}

fn parse_transform_index_options(value: &yaml_rust::Yaml) -> Result<HashMap<String, String>> {
    let hash = value.as_hash().context(FieldMustBeTypeSnafu {
        field: TRANSFORM_INDEX_OPTIONS_FIELD,
        ty: "map",
    })?;
    let mut options = HashMap::with_capacity(hash.len());

    for (k, v) in hash {
        let key = k
            .as_str()
            .with_context(|| KeyMustBeStringSnafu { k: k.clone() })?;
        options.insert(
            key.to_string(),
            yaml_scalar_to_string(v, &format!("{TRANSFORM_INDEX_OPTIONS_FIELD}.{key}"))?,
        );
    }

    Ok(options)
}

fn yaml_scalar_to_string(value: &yaml_rust::Yaml, field: &str) -> Result<String> {
    match value {
        yaml_rust::Yaml::String(v) => Ok(v.clone()),
        yaml_rust::Yaml::Boolean(v) => Ok(v.to_string()),
        yaml_rust::Yaml::Integer(v) => Ok(v.to_string()),
        yaml_rust::Yaml::Real(v) => Ok(v.clone()),
        _ => TransformIndexOptionMustBeScalarSnafu {
            field: field.to_string(),
        }
        .fail(),
    }
}

fn validate_index_option_keys(
    index: Index,
    options: &HashMap<String, String>,
    validate: fn(&str) -> bool,
) -> Result<()> {
    for key in options.keys() {
        ensure!(
            validate(key),
            TransformIndexOptionUnsupportedSnafu {
                index: index.to_string(),
                key: key.clone(),
            }
        );
    }

    Ok(())
}

fn lower_typed_transform_index_options<T>(
    index: Index,
    index_options: Option<HashMap<String, String>>,
    validate: fn(&str) -> bool,
    wrap: fn(T) -> TransformIndexOptions,
) -> Result<Option<TransformIndexOptions>>
where
    T: TryFrom<HashMap<String, String>, Error = datatypes::error::Error>,
{
    index_options
        .map(|opts| {
            validate_index_option_keys(index, &opts, validate)?;

            let options = opts.try_into().context(TransformIndexOptionSnafu {
                index: index.to_string(),
            })?;

            Ok(wrap(options))
        })
        .transpose()
}

fn lower_transform_index_options(
    index: Index,
    column_type: &ColumnDataType,
    index_options: Option<HashMap<String, String>>,
) -> Result<Option<TransformIndexOptions>> {
    match index {
        Index::Fulltext => {
            ensure!(
                *column_type == ColumnDataType::String,
                TransformIndexTypeMismatchSnafu {
                    index: index.to_string(),
                    expected: ColumnDataType::String.as_str_name().to_string(),
                    actual: column_type.as_str_name().to_string(),
                }
            );

            lower_typed_transform_index_options(
                index,
                index_options,
                validate_column_fulltext_create_option,
                TransformIndexOptions::Fulltext,
            )
        }
        Index::Skipping => lower_typed_transform_index_options(
            index,
            index_options,
            validate_column_skipping_index_create_option,
            TransformIndexOptions::Skipping,
        ),
        Index::Inverted | Index::Time | Index::Tag => {
            ensure!(
                index_options.is_none(),
                TransformIndexOptionsUnsupportedSnafu {
                    index: index.to_string(),
                }
            );
            Ok(None)
        }
    }
}
impl TryFrom<&yaml_rust::yaml::Hash> for Transform {
    type Error = Error;

    fn try_from(hash: &yaml_rust::yaml::Hash) -> Result<Self> {
        let mut fields = Fields::default();
        let mut default = None;
        let mut index_value = None;
        let mut tag = false;
        let mut on_failure = None;

        let mut type_ = None;

        for (k, v) in hash {
            let key = k
                .as_str()
                .with_context(|| KeyMustBeStringSnafu { k: k.clone() })?;
            match key {
                TRANSFORM_FIELD => {
                    fields = Fields::one(yaml_new_field(v, TRANSFORM_FIELD)?);
                }

                TRANSFORM_FIELDS => {
                    fields = yaml_new_fields(v, TRANSFORM_FIELDS)?;
                }

                TRANSFORM_TYPE => {
                    let t = yaml_string(v, TRANSFORM_TYPE)?;
                    type_ = Some(parse_str_type(&t)?);
                }

                TRANSFORM_INDEX => {
                    index_value = Some(v);
                }

                TRANSFORM_TAG => {
                    tag = yaml_bool(v, TRANSFORM_TAG)?;
                }

                TRANSFORM_DEFAULT => {
                    default = match v {
                        yaml_rust::Yaml::Real(r) => Some(r.clone()),
                        yaml_rust::Yaml::Integer(i) => Some(i.to_string()),
                        yaml_rust::Yaml::String(s) => Some(s.clone()),
                        yaml_rust::Yaml::Boolean(b) => Some(b.to_string()),
                        yaml_rust::Yaml::Array(_)
                        | yaml_rust::Yaml::Hash(_)
                        | yaml_rust::Yaml::Alias(_)
                        | yaml_rust::Yaml::Null
                        | yaml_rust::Yaml::BadValue => None,
                    };
                }

                TRANSFORM_ON_FAILURE => {
                    let on_failure_str = yaml_string(v, TRANSFORM_ON_FAILURE)?;
                    on_failure = Some(on_failure_str.parse()?);
                }

                _ => {}
            }
        }

        // ensure fields and type
        ensure!(!fields.is_empty(), TransformFieldMustBeSetSnafu);
        let type_ = type_.context(TransformTypeMustBeSetSnafu {
            fields: format!("{:?}", fields),
        })?;

        let (index, index_options) = match index_value {
            Some(value) => {
                let (index, raw_index_options) = parse_transform_index(value)?;
                let index_options =
                    lower_transform_index_options(index, &type_, raw_index_options)?;
                (Some(index), index_options)
            }
            None => (None, None),
        };

        let final_default = if let Some(default_value) = default {
            let target = parse_str_value(&type_, &default_value)?;
            on_failure = Some(OnFailure::Default);
            Some(target)
        } else {
            None
        };

        let builder = Transform {
            fields,
            type_,
            default: final_default,
            index,
            index_options,
            on_failure,
            tag,
        };

        Ok(builder)
    }
}

#[cfg(test)]
mod tests {
    use yaml_rust::YamlLoader;

    use super::*;

    fn parse_transform(yaml: &str) -> Result<Transform> {
        let docs = YamlLoader::load_from_str(yaml).unwrap();
        docs[0].as_hash().unwrap().try_into()
    }

    #[test]
    fn test_transform_parses_legacy_string_index() {
        let transform = parse_transform(
            r#"
field: message
type: string
index: fulltext
"#,
        )
        .unwrap();

        assert_eq!(transform.index, Some(Index::Fulltext));
        assert!(transform.index_options.is_none());
    }

    #[test]
    fn test_transform_parses_index_object_without_options() {
        let transform = parse_transform(
            r#"
field: message
type: string
index:
  type: inverted
"#,
        )
        .unwrap();

        assert_eq!(transform.index, Some(Index::Inverted));
        assert!(transform.index_options.is_none());
    }

    #[test]
    fn test_transform_parses_index_object_with_scalar_options() {
        let transform = parse_transform(
            r#"
field: message
type: string
index:
  type: fulltext
  options:
    analyzer: English
    case_sensitive: false
    granularity: 2048
    false_positive_rate: 0.02
"#,
        )
        .unwrap();

        assert_eq!(transform.index, Some(Index::Fulltext));
        let options = transform.index_options.as_ref().unwrap();
        let fulltext = options.as_fulltext().unwrap();
        assert!(fulltext.enable);
        assert_eq!(fulltext.analyzer.to_string(), "English");
        assert!(!fulltext.case_sensitive);
        assert_eq!(fulltext.granularity, 2048);
        assert_eq!(fulltext.false_positive_rate(), 0.02);
    }

    #[test]
    fn test_transform_rejects_invalid_index_options_type() {
        let result = parse_transform(
            r#"
field: message
type: string
index:
  type: fulltext
  options: invalid
"#,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_transform_rejects_non_scalar_index_option_value() {
        let result = parse_transform(
            r#"
field: message
type: string
index:
  type: fulltext
  options:
    analyzer:
      kind: English
"#,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_transform_rejects_unknown_index_field() {
        let result = parse_transform(
            r#"
field: message
type: string
index:
  type: fulltext
  config: {}
"#,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_transform_rejects_unsupported_fulltext_option_key() {
        let result = parse_transform(
            r#"
field: message
type: string
index:
  type: fulltext
  options:
    tokenizer: english
"#,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_transform_rejects_options_for_inverted_index() {
        let result = parse_transform(
            r#"
field: message
type: string
index:
  type: inverted
  options:
    backend: bloom
"#,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_transform_rejects_empty_options_for_unsupported_indexes() {
        for index in ["inverted", "time", "tag"] {
            let yaml = format!(
                r#"
field: message
type: string
index:
  type: {index}
  options: {{}}
"#
            );

            let result = parse_transform(&yaml);
            assert!(
                result.is_err(),
                "expected `{index}` to reject empty options"
            );
        }
    }

    #[test]
    fn test_transform_rejects_fulltext_index_on_non_string_column() {
        let result = parse_transform(
            r#"
field: count
type: int64
index: fulltext
"#,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_transform_allows_skipping_index_on_numeric_column() {
        let transform = parse_transform(
            r#"
field: count
type: int64
index:
  type: skipping
  options:
    granularity: 2048
    false_positive_rate: 0.02
    type: BLOOM
"#,
        )
        .unwrap();

        assert_eq!(transform.index, Some(Index::Skipping));
        let skipping = transform
            .index_options
            .as_ref()
            .unwrap()
            .as_skipping()
            .unwrap();
        assert_eq!(skipping.granularity, 2048);
        assert_eq!(skipping.false_positive_rate(), 0.02);
        assert_eq!(skipping.index_type.to_string(), "BLOOM");
    }
}
