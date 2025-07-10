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

use std::collections::BTreeMap;

use api::v1::value::ValueData;
use api::v1::ColumnDataType;
use ordered_float::NotNan;
use snafu::{OptionExt, ResultExt};
use vrl::prelude::Bytes;
use vrl::value::{KeyString, Value as VrlValue};

use crate::error::{
    FloatIsNanSnafu, Result, ValueDefaultValueUnsupportedSnafu, ValueInvalidResolutionSnafu,
    ValueParseBooleanSnafu, ValueParseFloatSnafu, ValueParseIntSnafu, ValueParseTypeSnafu,
    ValueUnsupportedYamlTypeSnafu, ValueYamlKeyMustBeStringSnafu,
};

pub(crate) const NANOSECOND_RESOLUTION: &str = "nanosecond";
pub(crate) const NANO_RESOLUTION: &str = "nano";
pub(crate) const NS_RESOLUTION: &str = "ns";
pub(crate) const MICROSECOND_RESOLUTION: &str = "microsecond";
pub(crate) const MICRO_RESOLUTION: &str = "micro";
pub(crate) const US_RESOLUTION: &str = "us";
pub(crate) const MILLISECOND_RESOLUTION: &str = "millisecond";
pub(crate) const MILLI_RESOLUTION: &str = "milli";
pub(crate) const MS_RESOLUTION: &str = "ms";
pub(crate) const SECOND_RESOLUTION: &str = "second";
pub(crate) const SEC_RESOLUTION: &str = "sec";
pub(crate) const S_RESOLUTION: &str = "s";

pub(crate) const VALID_RESOLUTIONS: [&str; 12] = [
    NANOSECOND_RESOLUTION,
    NANO_RESOLUTION,
    NS_RESOLUTION,
    MICROSECOND_RESOLUTION,
    MICRO_RESOLUTION,
    US_RESOLUTION,
    MILLISECOND_RESOLUTION,
    MILLI_RESOLUTION,
    MS_RESOLUTION,
    SECOND_RESOLUTION,
    SEC_RESOLUTION,
    S_RESOLUTION,
];

pub fn parse_str_type(t: &str) -> Result<ColumnDataType> {
    let mut parts = t.splitn(2, ',');
    let head = parts.next().unwrap_or_default();
    let tail = parts.next().map(|s| s.trim().to_string());
    match head.to_lowercase().as_str() {
        "int8" => Ok(ColumnDataType::Int8),
        "int16" => Ok(ColumnDataType::Int16),
        "int32" => Ok(ColumnDataType::Int32),
        "int64" => Ok(ColumnDataType::Int64),

        "uint8" => Ok(ColumnDataType::Uint8),
        "uint16" => Ok(ColumnDataType::Uint16),
        "uint32" => Ok(ColumnDataType::Uint32),
        "uint64" => Ok(ColumnDataType::Uint64),

        "float32" => Ok(ColumnDataType::Float32),
        "float64" => Ok(ColumnDataType::Float64),

        "boolean" => Ok(ColumnDataType::Boolean),
        "string" => Ok(ColumnDataType::String),

        "timestamp" | "epoch" | "time" => match tail {
            Some(resolution) if !resolution.is_empty() => match resolution.as_str() {
                NANOSECOND_RESOLUTION | NANO_RESOLUTION | NS_RESOLUTION => {
                    Ok(ColumnDataType::TimestampNanosecond)
                }
                MICROSECOND_RESOLUTION | MICRO_RESOLUTION | US_RESOLUTION => {
                    Ok(ColumnDataType::TimestampMicrosecond)
                }
                MILLISECOND_RESOLUTION | MILLI_RESOLUTION | MS_RESOLUTION => {
                    Ok(ColumnDataType::TimestampMillisecond)
                }
                SECOND_RESOLUTION | SEC_RESOLUTION | S_RESOLUTION => {
                    Ok(ColumnDataType::TimestampSecond)
                }
                _ => ValueInvalidResolutionSnafu {
                    resolution,
                    valid_resolution: VALID_RESOLUTIONS.join(","),
                }
                .fail(),
            },
            _ => Ok(ColumnDataType::TimestampNanosecond),
        },

        // We only consider object and array to be json types. and use Map to represent json
        // TODO(qtang): Needs to be defined with better semantics
        "json" => Ok(ColumnDataType::Binary),

        _ => ValueParseTypeSnafu { t }.fail(),
    }
}

pub fn parse_str_value(type_: &ColumnDataType, v: &str) -> Result<ValueData> {
    match type_ {
        ColumnDataType::Int8 => v
            .parse::<i8>()
            .map(|v| ValueData::I8Value(v as i32))
            .context(ValueParseIntSnafu { ty: "int8", v }),
        ColumnDataType::Int16 => v
            .parse::<i16>()
            .map(|v| ValueData::I16Value(v as i32))
            .context(ValueParseIntSnafu { ty: "int16", v }),
        ColumnDataType::Int32 => v
            .parse::<i32>()
            .map(ValueData::I32Value)
            .context(ValueParseIntSnafu { ty: "int32", v }),
        ColumnDataType::Int64 => v
            .parse::<i64>()
            .map(ValueData::I64Value)
            .context(ValueParseIntSnafu { ty: "int64", v }),

        ColumnDataType::Uint8 => v
            .parse::<u8>()
            .map(|v| ValueData::U8Value(v as u32))
            .context(ValueParseIntSnafu { ty: "uint8", v }),
        ColumnDataType::Uint16 => v
            .parse::<u16>()
            .map(|v| ValueData::U16Value(v as u32))
            .context(ValueParseIntSnafu { ty: "uint16", v }),
        ColumnDataType::Uint32 => v
            .parse::<u32>()
            .map(ValueData::U32Value)
            .context(ValueParseIntSnafu { ty: "uint32", v }),
        ColumnDataType::Uint64 => v
            .parse::<u64>()
            .map(ValueData::U64Value)
            .context(ValueParseIntSnafu { ty: "uint64", v }),

        ColumnDataType::Float32 => v
            .parse::<f32>()
            .map(ValueData::F32Value)
            .context(ValueParseFloatSnafu { ty: "float32", v }),
        ColumnDataType::Float64 => v
            .parse::<f64>()
            .map(ValueData::F64Value)
            .context(ValueParseFloatSnafu { ty: "float64", v }),

        ColumnDataType::Boolean => v
            .parse::<bool>()
            .map(ValueData::BoolValue)
            .context(ValueParseBooleanSnafu { ty: "boolean", v }),
        ColumnDataType::String => Ok(ValueData::StringValue(v.to_string())),

        _ => ValueDefaultValueUnsupportedSnafu {
            value: format!("{:?}", type_),
        }
        .fail(),
    }
}

pub fn yaml_to_vrl_value(v: &yaml_rust::Yaml) -> Result<VrlValue> {
    match v {
        yaml_rust::Yaml::Null => Ok(VrlValue::Null),
        yaml_rust::Yaml::Boolean(v) => Ok(VrlValue::Boolean(*v)),
        yaml_rust::Yaml::Integer(v) => Ok(VrlValue::Integer(*v)),
        yaml_rust::Yaml::Real(v) => {
            let f = v
                .parse::<f64>()
                .context(ValueParseFloatSnafu { ty: "float64", v })?;
            NotNan::new(f).map(VrlValue::Float).context(FloatIsNanSnafu)
        }
        yaml_rust::Yaml::String(v) => Ok(VrlValue::Bytes(Bytes::from(v.to_string()))),
        yaml_rust::Yaml::Array(arr) => {
            let mut values = vec![];
            for v in arr {
                values.push(yaml_to_vrl_value(v)?);
            }
            Ok(VrlValue::Array(values))
        }
        yaml_rust::Yaml::Hash(v) => {
            let mut values = BTreeMap::new();
            for (k, v) in v {
                let key = k
                    .as_str()
                    .with_context(|| ValueYamlKeyMustBeStringSnafu { value: v.clone() })?;
                values.insert(KeyString::from(key), yaml_to_vrl_value(v)?);
            }
            Ok(VrlValue::Object(values))
        }
        _ => ValueUnsupportedYamlTypeSnafu { value: v.clone() }.fail(),
    }
}
