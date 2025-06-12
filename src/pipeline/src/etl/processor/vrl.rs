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

use chrono_tz::Tz;
use snafu::{OptionExt, ResultExt};
use vrl::compiler::runtime::Runtime;
use vrl::compiler::{compile, Program, TargetValue};
use vrl::diagnostic::Formatter;
use vrl::prelude::{Bytes, NotNan, TimeZone};
use vrl::value::{KeyString, Kind, Secrets, Value as VrlValue};

use crate::error::{
    BytesToUtf8Snafu, CompileVrlSnafu, Error, ExecuteVrlSnafu, FloatNaNSnafu,
    InvalidTimestampSnafu, KeyMustBeStringSnafu, Result, VrlRegexValueSnafu, VrlReturnValueSnafu,
};
use crate::etl::processor::yaml_string;
use crate::Value as PipelineValue;

pub(crate) const PROCESSOR_VRL: &str = "vrl";
const SOURCE: &str = "source";

#[derive(Debug)]
pub struct VrlProcessor {
    source: String,
    program: Program,
}

impl VrlProcessor {
    pub fn new(source: String) -> Result<Self> {
        let fns = vrl::stdlib::all();

        let compile_result = compile(&source, &fns).map_err(|e| {
            CompileVrlSnafu {
                msg: Formatter::new(&source, e).to_string(),
            }
            .build()
        })?;

        let program = compile_result.program;

        // check if the return value is have regex
        let result_def = program.final_type_info().result;
        let kind = result_def.kind();
        if !kind.is_object() {
            return VrlReturnValueSnafu.fail();
        }
        check_regex_output(kind)?;

        Ok(Self { source, program })
    }

    pub fn resolve(&self, m: PipelineValue) -> Result<PipelineValue> {
        let pipeline_vrl = pipeline_value_to_vrl_value(m)?;

        let mut target = TargetValue {
            value: pipeline_vrl,
            metadata: VrlValue::Object(BTreeMap::new()),
            secrets: Secrets::default(),
        };

        let timezone = TimeZone::Named(Tz::UTC);
        let mut runtime = Runtime::default();
        let re = runtime
            .resolve(&mut target, &self.program, &timezone)
            .map_err(|e| {
                ExecuteVrlSnafu {
                    msg: e.get_expression_error().to_string(),
                }
                .build()
            })?;

        vrl_value_to_pipeline_value(re)
    }
}

impl TryFrom<&yaml_rust::yaml::Hash> for VrlProcessor {
    type Error = Error;

    fn try_from(value: &yaml_rust::yaml::Hash) -> Result<Self> {
        let mut source = String::new();
        for (k, v) in value.iter() {
            let key = k
                .as_str()
                .with_context(|| KeyMustBeStringSnafu { k: k.clone() })?;
            if key == SOURCE {
                source = yaml_string(v, SOURCE)?;
            }
        }
        let processor = VrlProcessor::new(source)?;
        Ok(processor)
    }
}

impl crate::etl::processor::Processor for VrlProcessor {
    fn kind(&self) -> &str {
        PROCESSOR_VRL
    }

    fn ignore_missing(&self) -> bool {
        true
    }

    fn exec_mut(&self, val: PipelineValue) -> Result<PipelineValue> {
        let val = self.resolve(val)?;

        if let PipelineValue::Map(m) = val {
            Ok(PipelineValue::Map(m.values.into()))
        } else {
            VrlRegexValueSnafu.fail()
        }
    }
}

fn pipeline_value_to_vrl_value(v: PipelineValue) -> Result<VrlValue> {
    match v {
        PipelineValue::Null => Ok(VrlValue::Null),
        PipelineValue::Int8(x) => Ok(VrlValue::Integer(x as i64)),
        PipelineValue::Int16(x) => Ok(VrlValue::Integer(x as i64)),
        PipelineValue::Int32(x) => Ok(VrlValue::Integer(x as i64)),
        PipelineValue::Int64(x) => Ok(VrlValue::Integer(x)),
        PipelineValue::Uint8(x) => Ok(VrlValue::Integer(x as i64)),
        PipelineValue::Uint16(x) => Ok(VrlValue::Integer(x as i64)),
        PipelineValue::Uint32(x) => Ok(VrlValue::Integer(x as i64)),
        PipelineValue::Uint64(x) => Ok(VrlValue::Integer(x as i64)),
        PipelineValue::Float32(x) => NotNan::new(x as f64)
            .map_err(|_| FloatNaNSnafu { input_float: x }.build())
            .map(VrlValue::Float),
        PipelineValue::Float64(x) => NotNan::new(x)
            .map_err(|_| FloatNaNSnafu { input_float: x }.build())
            .map(VrlValue::Float),
        PipelineValue::Boolean(x) => Ok(VrlValue::Boolean(x)),
        PipelineValue::String(x) => Ok(VrlValue::Bytes(Bytes::copy_from_slice(x.as_bytes()))),
        PipelineValue::Timestamp(x) => x
            .to_datetime()
            .context(InvalidTimestampSnafu {
                input: x.to_string(),
            })
            .map(VrlValue::Timestamp),
        PipelineValue::Array(array) => Ok(VrlValue::Array(
            array
                .into_iter()
                .map(pipeline_value_to_vrl_value)
                .collect::<Result<Vec<_>>>()?,
        )),
        PipelineValue::Map(m) => {
            let values = m
                .values
                .into_iter()
                .map(|(k, v)| pipeline_value_to_vrl_value(v).map(|v| (KeyString::from(k), v)))
                .collect::<Result<BTreeMap<_, _>>>()?;
            Ok(VrlValue::Object(values))
        }
    }
}

fn vrl_value_to_pipeline_value(v: VrlValue) -> Result<PipelineValue> {
    match v {
        VrlValue::Bytes(bytes) => String::from_utf8(bytes.to_vec())
            .context(BytesToUtf8Snafu)
            .map(PipelineValue::String),
        VrlValue::Regex(_) => VrlRegexValueSnafu.fail(),
        VrlValue::Integer(x) => Ok(PipelineValue::Int64(x)),
        VrlValue::Float(not_nan) => Ok(PipelineValue::Float64(not_nan.into_inner())),
        VrlValue::Boolean(b) => Ok(PipelineValue::Boolean(b)),
        VrlValue::Timestamp(date_time) => crate::etl::value::Timestamp::from_datetime(date_time)
            .context(InvalidTimestampSnafu {
                input: date_time.to_string(),
            })
            .map(PipelineValue::Timestamp),
        VrlValue::Object(bm) => {
            let b = bm
                .into_iter()
                .map(|(k, v)| vrl_value_to_pipeline_value(v).map(|v| (k.to_string(), v)))
                .collect::<Result<BTreeMap<String, PipelineValue>>>()?;
            Ok(PipelineValue::Map(b.into()))
        }
        VrlValue::Array(values) => {
            let a = values
                .into_iter()
                .map(vrl_value_to_pipeline_value)
                .collect::<Result<Vec<_>>>()?;
            Ok(PipelineValue::Array(a.into()))
        }
        VrlValue::Null => Ok(PipelineValue::Null),
    }
}

fn check_regex_output(output_kind: &Kind) -> Result<()> {
    if output_kind.is_regex() {
        return VrlRegexValueSnafu.fail();
    }

    if let Some(arr) = output_kind.as_array() {
        let k = arr.known();
        for v in k.values() {
            check_regex_output(v)?
        }
    }

    if let Some(obj) = output_kind.as_object() {
        let k = obj.known();
        for v in k.values() {
            check_regex_output(v)?
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::etl::value::Timestamp;
    use crate::Map;

    #[test]
    fn test_vrl() {
        let source = r#"
.name.a = .user_info.name
.name.b = .user_info.name
del(.user_info)
.timestamp = now()
.
"#;

        let v = VrlProcessor::new(source.to_string());
        assert!(v.is_ok());
        let v = v.unwrap();

        let mut n = BTreeMap::new();
        n.insert(
            "name".to_string(),
            PipelineValue::String("certain_name".to_string()),
        );

        let mut m = BTreeMap::new();
        m.insert(
            "user_info".to_string(),
            PipelineValue::Map(Map { values: n }),
        );

        let re = v.resolve(PipelineValue::Map(Map { values: m }));
        assert!(re.is_ok());
        let re = re.unwrap();

        assert!(matches!(re, PipelineValue::Map(_)));
        assert!(re.get("name").is_some());
        let name = re.get("name").unwrap();
        assert!(matches!(name.get("a").unwrap(), PipelineValue::String(x) if x == "certain_name"));
        assert!(matches!(name.get("b").unwrap(), PipelineValue::String(x) if x == "certain_name"));
        assert!(re.get("timestamp").is_some());
        let timestamp = re.get("timestamp").unwrap();
        assert!(matches!(
            timestamp,
            PipelineValue::Timestamp(Timestamp::Nanosecond(_))
        ));
    }

    #[test]
    fn test_yaml_to_vrl() {
        let yaml = r#"
processors:
  - vrl:
      source: |
        .name.a = .user_info.name
        .name.b = .user_info.name
        del(.user_info)
        .timestamp = now()
        .
"#;
        let y = yaml_rust::YamlLoader::load_from_str(yaml).unwrap();
        let vrl_processor_yaml = y
            .first()
            .and_then(|x| x.as_hash())
            .and_then(|x| x.get(&yaml_rust::Yaml::String("processors".to_string())))
            .and_then(|x| x.as_vec())
            .and_then(|x| x.first())
            .and_then(|x| x.as_hash())
            .and_then(|x| x.get(&yaml_rust::Yaml::String("vrl".to_string())))
            .and_then(|x| x.as_hash())
            .unwrap();

        let vrl = VrlProcessor::try_from(vrl_processor_yaml);
        assert!(vrl.is_ok());
        let vrl = vrl.unwrap();

        assert_eq!(vrl.source, ".name.a = .user_info.name\n.name.b = .user_info.name\ndel(.user_info)\n.timestamp = now()\n.\n");
    }

    #[test]
    fn test_regex() {
        let source = r#"
.re = r'(?i)^Hello, World!$'
del(.re)
.re = r'(?i)^Hello, World!$'
.
"#;

        let v = VrlProcessor::new(source.to_string());
        assert!(v.is_err());
    }
}
