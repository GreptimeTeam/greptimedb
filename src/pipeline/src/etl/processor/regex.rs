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

// field_name and prefix with comma separated, like:
// name, new_name
const PATTERNS_NAME: &str = "patterns";

pub(crate) const PROCESSOR_REGEX: &str = "regex";

use std::collections::BTreeMap;

use lazy_static::lazy_static;
use regex::Regex;
use snafu::{OptionExt, ResultExt};
use vrl::prelude::Bytes;
use vrl::value::{KeyString, Value as VrlValue};

use crate::error::{
    Error, KeyMustBeStringSnafu, ProcessorExpectStringSnafu, ProcessorMissingFieldSnafu,
    RegexNamedGroupNotFoundSnafu, RegexNoValidFieldSnafu, RegexNoValidPatternSnafu, RegexSnafu,
    Result, ValueMustBeMapSnafu,
};
use crate::etl::field::Fields;
use crate::etl::processor::{
    yaml_bool, yaml_new_field, yaml_new_fields, yaml_string, yaml_strings, Processor, FIELDS_NAME,
    FIELD_NAME, IGNORE_MISSING_NAME, PATTERN_NAME,
};

lazy_static! {
    static ref GROUPS_NAME_REGEX: Regex = Regex::new(r"\(\?P?<([[:word:]]+)>.+?\)").unwrap();
}

fn get_regex_group_names(s: &str) -> Vec<String> {
    GROUPS_NAME_REGEX
        .captures_iter(s)
        .filter_map(|c| c.get(1).map(|m| m.as_str().to_string()))
        .collect()
}

fn generate_key(prefix: &str, group: &str) -> String {
    format!("{prefix}_{group}")
}

#[derive(Debug)]
struct GroupRegex {
    origin: String,
    regex: Regex,
    groups: Vec<String>,
}

impl std::fmt::Display for GroupRegex {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let groups = self.groups.join(", ");
        write!(f, "{}, groups: [{groups}]", self.origin)
    }
}

impl std::str::FromStr for GroupRegex {
    type Err = Error;

    fn from_str(origin: &str) -> Result<Self> {
        let groups = get_regex_group_names(origin);
        if groups.is_empty() {
            return RegexNamedGroupNotFoundSnafu { origin }.fail();
        }

        let regex = Regex::new(origin).context(RegexSnafu { pattern: origin })?;
        Ok(GroupRegex {
            origin: origin.into(),
            regex,
            groups,
        })
    }
}

impl TryFrom<&yaml_rust::yaml::Hash> for RegexProcessor {
    type Error = Error;

    fn try_from(value: &yaml_rust::yaml::Hash) -> Result<Self> {
        let mut fields = Fields::default();
        let mut patterns: Vec<GroupRegex> = vec![];
        let mut ignore_missing = false;

        for (k, v) in value.iter() {
            let key = k
                .as_str()
                .with_context(|| KeyMustBeStringSnafu { k: k.clone() })?;
            match key {
                FIELD_NAME => {
                    fields = Fields::one(yaml_new_field(v, FIELD_NAME)?);
                }
                FIELDS_NAME => {
                    fields = yaml_new_fields(v, FIELDS_NAME)?;
                }
                PATTERN_NAME => {
                    let pattern = yaml_string(v, PATTERN_NAME)?;
                    let gr = pattern.parse()?;
                    patterns.push(gr);
                }
                PATTERNS_NAME => {
                    for pattern in yaml_strings(v, PATTERNS_NAME)? {
                        let gr = pattern.parse()?;
                        patterns.push(gr);
                    }
                }
                IGNORE_MISSING_NAME => {
                    ignore_missing = yaml_bool(v, IGNORE_MISSING_NAME)?;
                }
                _ => {}
            }
        }

        let processor_builder = RegexProcessor {
            fields,
            patterns,
            ignore_missing,
        };

        processor_builder.check()
    }
}

/// only support string value
/// if no value found from a pattern, the target_field will be ignored
#[derive(Debug, Default)]
pub struct RegexProcessor {
    fields: Fields,
    patterns: Vec<GroupRegex>,
    ignore_missing: bool,
}

impl RegexProcessor {
    fn check(self) -> Result<Self> {
        if self.fields.is_empty() {
            return RegexNoValidFieldSnafu {
                processor: PROCESSOR_REGEX,
            }
            .fail();
        }

        if self.patterns.is_empty() {
            return RegexNoValidPatternSnafu {
                processor: PROCESSOR_REGEX,
            }
            .fail();
        }

        Ok(self)
    }

    fn try_with_patterns(&mut self, patterns: Vec<String>) -> Result<()> {
        let mut rs = vec![];
        for pattern in patterns {
            let gr = pattern.parse()?;
            rs.push(gr);
        }
        self.patterns = rs;
        Ok(())
    }

    fn process(&self, prefix: &str, val: &str) -> Result<BTreeMap<KeyString, VrlValue>> {
        let mut result = BTreeMap::new();
        for gr in self.patterns.iter() {
            if let Some(captures) = gr.regex.captures(val) {
                for group in gr.groups.iter() {
                    if let Some(capture) = captures.name(group) {
                        let value = capture.as_str().to_string();
                        result.insert(
                            KeyString::from(generate_key(prefix, group)),
                            VrlValue::Bytes(Bytes::from(value)),
                        );
                    }
                }
            }
        }
        Ok(result)
    }
}

impl Processor for RegexProcessor {
    fn kind(&self) -> &str {
        PROCESSOR_REGEX
    }

    fn ignore_missing(&self) -> bool {
        self.ignore_missing
    }

    fn exec_mut(&self, mut val: VrlValue) -> Result<VrlValue> {
        for field in self.fields.iter() {
            let index = field.input_field();
            let prefix = field.target_or_input_field();
            let val = val.as_object_mut().context(ValueMustBeMapSnafu)?;
            match val.get(index) {
                Some(VrlValue::Bytes(s)) => {
                    let result = self.process(prefix, String::from_utf8_lossy(s).as_ref())?;
                    val.extend(result);
                }
                Some(VrlValue::Null) | None => {
                    if !self.ignore_missing {
                        return ProcessorMissingFieldSnafu {
                            processor: self.kind(),
                            field: field.input_field(),
                        }
                        .fail();
                    }
                }
                Some(v) => {
                    return ProcessorExpectStringSnafu {
                        processor: self.kind(),
                        v: v.clone(),
                    }
                    .fail();
                }
            }
        }

        Ok(val)
    }
}
#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use vrl::value::Value as VrlValue;

    use super::*;
    use crate::etl::processor::regex::RegexProcessor;

    #[test]
    fn test_simple_parse() {
        let pipeline_str = r#"fields: ["a"]
patterns: ['(?<ar>\d)']
ignore_missing: false"#;

        let processor_yaml = yaml_rust::YamlLoader::load_from_str(pipeline_str)
            .unwrap()
            .pop()
            .unwrap();
        let processor_yaml_hash = processor_yaml.as_hash().unwrap();
        let processor = RegexProcessor::try_from(processor_yaml_hash).unwrap();

        // single field (with prefix), multiple patterns

        let result = processor.process("a", "123").unwrap();

        let v = vec![(KeyString::from("a_ar"), VrlValue::Bytes(Bytes::from("1")))]
            .into_iter()
            .collect::<BTreeMap<KeyString, VrlValue>>();

        assert_eq!(v, result);
    }

    #[test]
    fn test_process() {
        let cc = "[c=c,n=US_CA_SANJOSE,o=55155]";
        let cg = "[a=12.34.567.89,b=12345678,c=g,n=US_CA_SANJOSE,o=20940]";
        let co = "[a=987.654.321.09,c=o]";
        let cp = "[c=p,n=US_CA_SANJOSE,o=55155]";
        let cw = "[c=w,n=US_CA_SANJOSE,o=55155]";
        let breadcrumbs_str = [cc, cg, co, cp, cw].iter().join(",");

        let temporary_map: BTreeMap<KeyString, VrlValue> = [
            (
                "breadcrumbs_parent",
                VrlValue::Bytes(Bytes::from(cc.to_string())),
            ),
            (
                "breadcrumbs_edge",
                VrlValue::Bytes(Bytes::from(cg.to_string())),
            ),
            (
                "breadcrumbs_origin",
                VrlValue::Bytes(Bytes::from(co.to_string())),
            ),
            (
                "breadcrumbs_peer",
                VrlValue::Bytes(Bytes::from(cp.to_string())),
            ),
            (
                "breadcrumbs_wrapper",
                VrlValue::Bytes(Bytes::from(cw.to_string())),
            ),
        ]
        .into_iter()
        .map(|(k, v)| (KeyString::from(k), v))
        .collect();

        {
            // single field (with prefix), multiple patterns

            let pipeline_str = r#"fields: ["breadcrumbs"]
patterns:
  - '(?<parent>\[[^\[]*c=c[^\]]*\])'
  - '(?<edge>\[[^\[]*c=g[^\]]*\])'
  - '(?<origin>\[[^\[]*c=o[^\]]*\])'
  - '(?<peer>\[[^\[]*c=p[^\]]*\])'
  - '(?<wrapper>\[[^\[]*c=w[^\]]*\])'
ignore_missing: false"#;

            let processor_yaml = yaml_rust::YamlLoader::load_from_str(pipeline_str)
                .unwrap()
                .pop()
                .unwrap();
            let processor_yaml_hash = processor_yaml.as_hash().unwrap();
            let processor = RegexProcessor::try_from(processor_yaml_hash).unwrap();

            let result = processor.process("breadcrumbs", &breadcrumbs_str).unwrap();

            assert_eq!(temporary_map, result);
        }

        {
            // multiple fields (with prefix), multiple patterns

            let pipeline_str = r#"fields:
  - breadcrumbs_parent, parent
  - breadcrumbs_edge, edge
  - breadcrumbs_origin, origin
  - breadcrumbs_peer, peer
  - breadcrumbs_wrapper, wrapper
patterns:
  - 'a=(?<ip>[^,\]]+)'
  - 'b=(?<request_id>[^,\]]+)'
  - 'k=(?<request_end_time>[^,\]]+)'
  - 'l=(?<turn_around_time>[^,\]]+)'
  - 'm=(?<dns_lookup_time>[^,\]]+)'
  - 'n=(?<geo>[^,\]]+)'
  - 'o=(?<asn>[^,\]]+)'
ignore_missing: false"#;

            let processor_yaml = yaml_rust::YamlLoader::load_from_str(pipeline_str)
                .unwrap()
                .pop()
                .unwrap();
            let processor_yaml_hash = processor_yaml.as_hash().unwrap();
            let processor = RegexProcessor::try_from(processor_yaml_hash).unwrap();

            let mut result = BTreeMap::new();
            for field in processor.fields.iter() {
                let s = temporary_map.get(field.input_field()).unwrap();
                let s = s.to_string_lossy();
                let prefix = field.target_or_input_field();

                let r = processor.process(prefix, s.as_ref()).unwrap();

                result.extend(r);
            }

            let new_values = vec![
                (
                    "edge_ip",
                    VrlValue::Bytes(Bytes::from("12.34.567.89".to_string())),
                ),
                (
                    "edge_request_id",
                    VrlValue::Bytes(Bytes::from("12345678".to_string())),
                ),
                (
                    "edge_geo",
                    VrlValue::Bytes(Bytes::from("US_CA_SANJOSE".to_string())),
                ),
                (
                    "edge_asn",
                    VrlValue::Bytes(Bytes::from("20940".to_string())),
                ),
                (
                    "origin_ip",
                    VrlValue::Bytes(Bytes::from("987.654.321.09".to_string())),
                ),
                (
                    "peer_asn",
                    VrlValue::Bytes(Bytes::from("55155".to_string())),
                ),
                (
                    "peer_geo",
                    VrlValue::Bytes(Bytes::from("US_CA_SANJOSE".to_string())),
                ),
                (
                    "parent_asn",
                    VrlValue::Bytes(Bytes::from("55155".to_string())),
                ),
                (
                    "parent_geo",
                    VrlValue::Bytes(Bytes::from("US_CA_SANJOSE".to_string())),
                ),
                (
                    "wrapper_asn",
                    VrlValue::Bytes(Bytes::from("55155".to_string())),
                ),
                (
                    "wrapper_geo",
                    VrlValue::Bytes(Bytes::from("US_CA_SANJOSE".to_string())),
                ),
            ]
            .into_iter()
            .map(|(k, v)| (KeyString::from(k), v))
            .collect::<BTreeMap<KeyString, VrlValue>>();

            assert_eq!(result, new_values);
        }
    }
}
