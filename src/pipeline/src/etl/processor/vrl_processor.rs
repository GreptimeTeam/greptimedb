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

use std::cell::RefCell;
use std::collections::BTreeMap;

use chrono_tz::Tz;
use once_cell::sync::Lazy;
use snafu::{OptionExt, ensure};
use vrl::compiler::runtime::Runtime;
use vrl::compiler::{Program, TargetValue, compile};
use vrl::diagnostic::Formatter;
use vrl::prelude::{
    ArgumentList, Compiled, Example, Function, FunctionCompileContext, Parameter, TimeZone,
    TypeState, function,
};
use vrl::value::{Kind, Secrets, Value as VrlValue};

use crate::error::{
    CompileVrlSnafu, ExecuteVrlSnafu, KeyMustBeStringSnafu, Result, VrlRegexValueSnafu,
    VrlReturnValueSnafu,
};
use crate::etl::processor::yaml_string;

pub(crate) const PROCESSOR_VRL: &str = "vrl";
const SOURCE: &str = "source";

static UTC_TIMEZONE: Lazy<TimeZone> = Lazy::new(|| TimeZone::Named(Tz::UTC));

thread_local! {
    static VRL_RUNTIME: RefCell<Runtime> = RefCell::new(Runtime::default());
}

#[derive(Debug)]
pub struct VrlProcessor {
    source: String,
    program: Program,
}

impl VrlProcessor {
    pub fn new(source: String, allow_host_access: bool) -> Result<Self> {
        let fns = functions(allow_host_access);

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

        let re = VRL_RUNTIME
            .with(|runtime| {
                let mut runtime = runtime.borrow_mut();
                runtime.clear();
                let result = runtime.resolve(&mut target, &self.program, &UTC_TIMEZONE);
                runtime.clear();
                result
            })
            .map_err(|e| {
                ExecuteVrlSnafu {
                    msg: e.get_expression_error().to_string(),
                }
                .build()
            })?;

        Ok(re)
    }
}

impl VrlProcessor {
    /// Parses VRL source using the server's host access policy.
    pub fn from_yaml(value: &yaml_rust::yaml::Hash, allow_host_access: bool) -> Result<Self> {
        let mut source = String::new();
        for (k, v) in value.iter() {
            let key = k
                .as_str()
                .with_context(|| KeyMustBeStringSnafu { k: k.clone() })?;
            if key == SOURCE {
                source = yaml_string(v, SOURCE)?;
            }
        }
        Self::new(source, allow_host_access)
    }
}

fn functions(allow_host_access: bool) -> Vec<Box<dyn Function>> {
    let mut functions = vrl::stdlib::all();
    if allow_host_access {
        return functions;
    }

    // These functions bypass VRL's event target, including file reads during compilation.
    // When upgrading VRL, re-audit this denylist and the argument restrictions below;
    // new host-access functions are otherwise enabled by default.
    functions.retain(|function| {
        !matches!(
            function.identifier(),
            "get_env_var"
                | "get_hostname"
                | "get_timezone_name"
                | "http_request"
                | "dns_lookup"
                | "reverse_dns"
                | "encode_proto"
                | "parse_proto"
                | "parse_etld"
                | "validate_json_schema"
                | "log"
        )
    });
    functions
        .into_iter()
        .map(|function| match function.identifier() {
            "parse_groks" | "parse_timestamp" | "format_timestamp" => {
                Box::new(RestrictedFunction(function)) as Box<dyn Function>
            }
            _ => function,
        })
        .collect()
}

/// Retains data-only uses of functions that also accept host-access arguments.
#[derive(Debug)]
struct RestrictedFunction(Box<dyn Function>);

impl Function for RestrictedFunction {
    fn identifier(&self) -> &'static str {
        self.0.identifier()
    }

    fn usage(&self) -> &'static str {
        self.0.usage()
    }

    fn category(&self) -> &'static str {
        self.0.category()
    }

    fn return_kind(&self) -> u16 {
        self.0.return_kind()
    }

    fn examples(&self) -> &'static [Example] {
        self.0.examples()
    }

    fn parameters(&self) -> &'static [Parameter] {
        self.0.parameters()
    }

    fn pure(&self) -> bool {
        self.0.pure()
    }

    fn compile(
        &self,
        state: &TypeState,
        ctx: &mut FunctionCompileContext,
        arguments: ArgumentList,
    ) -> Compiled {
        if self.identifier() == "parse_groks" {
            if arguments
                .optional_array("alias_sources")?
                .is_some_and(|sources| !sources.is_empty())
            {
                return Err(function::Error::InvalidArgument {
                    keyword: "alias_sources",
                    value: "alias_sources".into(),
                    error: "file imports require pipeline.allow_vrl_host_access",
                }
                .into());
            }
        } else if let Some(timezone) = arguments.optional_literal("timezone", state)? {
            // A static named timezone cannot resolve to the host's local timezone at runtime.
            if !matches!(&timezone, VrlValue::Bytes(bytes)
                if matches!(TimeZone::parse(&String::from_utf8_lossy(bytes)), Some(TimeZone::Named(_))))
            {
                return Err(function::Error::InvalidArgument {
                    keyword: "timezone",
                    value: timezone,
                    error: "a named timezone is required when pipeline.allow_vrl_host_access is false",
                }
                .into());
            }
        }
        self.0.compile(state, ctx, arguments)
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
    fn test_host_functions_require_opt_in() {
        let calls = [
            ("get_env_var", r#"get_env_var!("GREPTIME_VRL_TEST_SECRET")"#),
            ("get_hostname", "get_hostname!()"),
            ("get_timezone_name", "get_timezone_name!()"),
            ("http_request", r#"http_request!("http://127.0.0.1:9/")"#),
            ("dns_lookup", r#"dns_lookup!("localhost")"#),
            ("reverse_dns", r#"reverse_dns!("127.0.0.1")"#),
            (
                "parse_proto",
                r#"parse_proto!("", "missing.desc", "Message")"#,
            ),
            (
                "encode_proto",
                r#"encode_proto!({}, "missing.desc", "Message")"#,
            ),
            ("parse_etld", r#"parse_etld!("example.com")"#),
            (
                "validate_json_schema",
                r#"validate_json_schema!("{}", "missing.json")"#,
            ),
            ("log", r#"log("host log write")"#),
        ];
        let unrestricted = functions(true);
        for (name, call) in calls {
            assert!(
                unrestricted
                    .iter()
                    .any(|function| function.identifier() == name)
            );
            let source = format!(".result = {call}\n.");
            let error = VrlProcessor::new(source, false).unwrap_err().to_string();
            assert!(error.contains("undefined function"), "{name}: {error}");
        }

        temp_env::with_var("GREPTIME_VRL_TEST_SECRET", Some("decoy"), || {
            let source = ".secret = get_env_var!(\"GREPTIME_VRL_TEST_SECRET\")\n.";
            let processor = VrlProcessor::new(source.to_string(), true).unwrap();
            let result = processor
                .resolve(VrlValue::Object(BTreeMap::new()))
                .unwrap();
            assert_eq!(
                result.as_object().unwrap()["secret"],
                VrlValue::from("decoy")
            );
            assert!(VrlProcessor::new(source.to_string(), false).is_err());
        });
    }

    #[test]
    fn test_grok_file_import_requires_opt_in() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("aliases.json");
        std::fs::write(&path, r#"{"custom_word":"%{word}"}"#).unwrap();
        let path = serde_json::to_string(&path).unwrap();
        let source = format!(
            r#".parsed = parse_groks!("hello", patterns: ["%{{custom_word:message}}"], alias_sources: [{path}])
."#
        );
        let error = VrlProcessor::new(source.clone(), false)
            .unwrap_err()
            .to_string();
        assert!(error.contains("file imports require pipeline.allow_vrl_host_access"));
        let result = VrlProcessor::new(source, true)
            .unwrap()
            .resolve(VrlValue::Object(BTreeMap::new()))
            .unwrap();
        assert_eq!(
            result.as_object().unwrap()["parsed"].as_object().unwrap()["message"],
            VrlValue::from("hello")
        );

        let source = r#".parsed = parse_groks!("hello", patterns: ["%{custom_word:message}"], aliases: {"custom_word": "%{word}"}, alias_sources: [])
."#;
        assert!(VrlProcessor::new(source.to_string(), false).is_ok());
    }

    #[test]
    fn test_local_and_dynamic_timezones_require_opt_in() {
        for timezone in [r#""local""#, r#""""#, "string!(.timezone)"] {
            for call in [
                format!(
                    r#"format_timestamp!(t'2020-01-01T00:00:00Z', format: "%z", timezone: {timezone})"#
                ),
                format!(
                    r#"parse_timestamp!("2020-01-01", format: "%Y-%m-%d", timezone: {timezone})"#
                ),
            ] {
                let source = format!(".result = {call}\n.");
                assert!(
                    VrlProcessor::new(source.clone(), false).is_err(),
                    "{source}"
                );
                assert!(VrlProcessor::new(source.clone(), true).is_ok(), "{source}");
            }
        }

        let source = r#".parsed = parse_timestamp!("2020-01-01 00:00:00", format: "%Y-%m-%d %H:%M:%S")
.utc = format_timestamp!(.parsed, format: "%z")
.named = format_timestamp!(.parsed, format: "%z", timezone: "Asia/Shanghai")
."#;
        let result = VrlProcessor::new(source.to_string(), false)
            .unwrap()
            .resolve(VrlValue::Object(BTreeMap::new()))
            .unwrap();
        assert_eq!(result.as_object().unwrap()["utc"], VrlValue::from("+0000"));
        assert_eq!(
            result.as_object().unwrap()["named"],
            VrlValue::from("+0800")
        );
    }

    #[test]
    fn test_pipeline_host_policy_is_server_controlled() {
        let yaml = r#"
allow_vrl_host_access: true
processors:
  - vrl:
      allow_vrl_host_access: true
      source: |
        .secret = get_env_var!("GREPTIME_VRL_TEST_SECRET")
        .
transform:
  - field: secret
    type: string
"#;
        assert!(crate::parse(&crate::Content::Yaml(yaml)).is_err());
        let options = crate::PipelineOptions {
            allow_vrl_host_access: true,
            ..Default::default()
        };
        assert!(crate::parse_with_options(&crate::Content::Yaml(yaml), &options).is_ok());
    }

    #[test]
    fn test_vrl() {
        let source = r#"
.name.a = .user_info.name
.name.b = .user_info.name
del(.user_info)
.timestamp = now()
.
"#;

        let v = VrlProcessor::new(source.to_string(), false);
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

        let vrl = VrlProcessor::from_yaml(vrl_processor_yaml, false);
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

        let v = VrlProcessor::new(source.to_string(), false);
        assert!(v.is_err());
    }
}
