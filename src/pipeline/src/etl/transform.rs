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

use api::v1::value::ValueData;
use api::v1::ColumnDataType;
use chrono::Utc;
use snafu::{ensure, OptionExt};

use crate::error::{
    Error, KeyMustBeStringSnafu, Result, TransformElementMustBeMapSnafu,
    TransformFieldMustBeSetSnafu, TransformOnFailureInvalidValueSnafu, TransformTypeMustBeSetSnafu,
    UnsupportedTypeInPipelineSnafu,
};
use crate::etl::field::Fields;
use crate::etl::processor::{yaml_bool, yaml_new_field, yaml_new_fields, yaml_string};
use crate::etl::transform::index::Index;
use crate::etl::value::{parse_str_type, parse_str_value};

const TRANSFORM_FIELD: &str = "field";
const TRANSFORM_FIELDS: &str = "fields";
const TRANSFORM_TYPE: &str = "type";
const TRANSFORM_INDEX: &str = "index";
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
    pub tag: bool,
    pub on_failure: Option<OnFailure>,
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

impl TryFrom<&yaml_rust::yaml::Hash> for Transform {
    type Error = Error;

    fn try_from(hash: &yaml_rust::yaml::Hash) -> Result<Self> {
        let mut fields = Fields::default();
        let mut default = None;
        let mut index = None;
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
                    let index_str = yaml_string(v, TRANSFORM_INDEX)?;
                    index = Some(index_str.try_into()?);
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
            on_failure,
            tag,
        };

        Ok(builder)
    }
}
