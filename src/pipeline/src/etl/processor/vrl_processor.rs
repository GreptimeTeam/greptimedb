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
use snafu::{OptionExt, ensure};
use vrl::compiler::runtime::Runtime;
use vrl::compiler::{Program, TargetValue, compile};
use vrl::diagnostic::Formatter;
use vrl::prelude::TimeZone;
use vrl::value::{Kind, Secrets, Value as VrlValue};

use crate::error::{
    CompileVrlSnafu, Error, ExecuteVrlSnafu, KeyMustBeStringSnafu, Result, VrlRegexValueSnafu,
    VrlReturnValueSnafu,
};
use crate::etl::processor::yaml_string;

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
        // Check if the return type could possibly be an object or array.
        // We use contains_* methods since VRL type inference may return
        // a Kind that represents multiple possible types.
        ensure!(
            kind.contains_object() || kind.contains_array(),
            VrlReturnValueSnafu {
                result_kind: kind.clone(),
            }
        );
        check_regex_output(kind)?;

        Ok(Self { source, program })
    }

    pub fn resolve(&self, value: VrlValue) -> Result<VrlValue> {
        let mut target = TargetValue {
            value,
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

        Ok(re)
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

    fn exec_mut(&self, val: VrlValue) -> Result<VrlValue> {
        self.resolve(val)
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

    use vrl::prelude::Bytes;
    use vrl::value::KeyString;

    use super::*;

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
            KeyString::from("name"),
            VrlValue::Bytes(Bytes::from("certain_name")),
        );

        let mut m = BTreeMap::new();
        m.insert(KeyString::from("user_info"), VrlValue::Object(n));

        let re = v.resolve(VrlValue::Object(m));
        assert!(re.is_ok());
        let re = re.unwrap();

        assert!(matches!(re, VrlValue::Object(_)));
        let re = re.as_object().unwrap();
        assert!(re.get("name").is_some());
        let name = re.get("name").unwrap();
        let name = name.as_object().unwrap();
        assert!(matches!(name.get("a").unwrap(), VrlValue::Bytes(x) if x == "certain_name"));
        assert!(matches!(name.get("b").unwrap(), VrlValue::Bytes(x) if x == "certain_name"));
        assert!(re.get("timestamp").is_some());
        let timestamp = re.get("timestamp").unwrap();
        assert!(matches!(timestamp, VrlValue::Timestamp(_)));
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

        assert_eq!(
            vrl.source,
            ".name.a = .user_info.name\n.name.b = .user_info.name\ndel(.user_info)\n.timestamp = now()\n.\n"
        );
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
