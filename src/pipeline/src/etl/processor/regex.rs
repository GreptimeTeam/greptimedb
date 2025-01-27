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

use lazy_static::lazy_static;
use regex::Regex;
use snafu::{OptionExt, ResultExt};

use super::IntermediateStatus;
use crate::etl::error::{
    Error, KeyMustBeStringSnafu, ProcessorExpectStringSnafu, ProcessorMissingFieldSnafu,
    RegexNamedGroupNotFoundSnafu, RegexNoValidFieldSnafu, RegexNoValidPatternSnafu, RegexSnafu,
    Result,
};
use crate::etl::field::Fields;
use crate::etl::processor::{
    yaml_bool, yaml_new_field, yaml_new_fields, yaml_string, yaml_strings, Processor, FIELDS_NAME,
    FIELD_NAME, IGNORE_MISSING_NAME, PATTERN_NAME,
};
use crate::etl::value::Value;

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

    fn process<'a>(&self, val: &str, gr: &'a GroupRegex) -> Result<Vec<(&'a String, Value)>> {
        let mut result = Vec::new();
        if let Some(captures) = gr.regex.captures(val) {
            for group in gr.groups.iter() {
                if let Some(capture) = captures.name(group) {
                    let value = capture.as_str().to_string();
                    result.push((group, Value::String(value)));
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

    fn exec_mut(&self, val: &mut IntermediateStatus) -> Result<()> {
        for field in self.fields.iter() {
            let index = field.input_field();
            let prefix = field.target_or_input_field();
            let mut result_list = None;
            match val.get(index) {
                Some(Value::String(s)) => {
                    // we get rust borrow checker error here
                    // for (gr_index, gr) in self.patterns.iter().enumerate() {
                    //     let result_list = self.process(s.as_str(), gr, (field_index, gr_index))?;
                    //     for (output_index, result) in result_list {
                    //cannot borrow `*val` as mutable because it is also borrowed as immutable mutable borrow occurs here
                    //         val[output_index] = result;
                    //     }
                    // }
                    for gr in self.patterns.iter() {
                        let result = self.process(s.as_str(), gr)?;
                        if !result.is_empty() {
                            match result_list.as_mut() {
                                None => {
                                    result_list = Some(result);
                                }
                                Some(result_list) => {
                                    result_list.extend(result);
                                }
                            }
                        }
                    }
                }
                Some(Value::Null) | None => {
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
            // safety here
            match result_list {
                None => {}
                Some(result_list) => {
                    for (output_key, result) in result_list {
                        val.insert(generate_key(prefix, output_key), result);
                    }
                }
            }
        }

        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use ahash::{HashMap, HashMapExt};
    use itertools::Itertools;

    use crate::etl::processor::regex::RegexProcessor;
    use crate::etl::value::{Map, Value};

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

        let result = processor
            .process("123", &processor.patterns[0])
            .unwrap()
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();

        let map = Map { values: result };

        let v = Map {
            values: vec![("ar".to_string(), Value::String("1".to_string()))]
                .into_iter()
                .collect(),
        };

        assert_eq!(v, map);
    }

    #[test]
    fn test_process() {
        let cc = "[c=c,n=US_CA_SANJOSE,o=55155]";
        let cg = "[a=12.34.567.89,b=12345678,c=g,n=US_CA_SANJOSE,o=20940]";
        let co = "[a=987.654.321.09,c=o]";
        let cp = "[c=p,n=US_CA_SANJOSE,o=55155]";
        let cw = "[c=w,n=US_CA_SANJOSE,o=55155]";
        let breadcrumbs_str = [cc, cg, co, cp, cw].iter().join(",");

        let values = [
            ("breadcrumbs_parent", Value::String(cc.to_string())),
            ("breadcrumbs_edge", Value::String(cg.to_string())),
            ("breadcrumbs_origin", Value::String(co.to_string())),
            ("breadcrumbs_peer", Value::String(cp.to_string())),
            ("breadcrumbs_wrapper", Value::String(cw.to_string())),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        let temporary_map = Map { values };

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
            let mut result = BTreeMap::new();
            for pattern in processor.patterns.iter() {
                let r = processor
                    .process(&breadcrumbs_str, pattern)
                    .unwrap()
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v))
                    .collect::<BTreeMap<_, _>>();
                result.extend(r);
            }
            let map = Map {
                values: result
                    .into_iter()
                    .map(|(k, v)| (format!("breadcrumbs_{}", k), v))
                    .collect(),
            };
            assert_eq!(temporary_map, map);
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

            let mut result = HashMap::new();
            for field in processor.fields.iter() {
                for pattern in processor.patterns.iter() {
                    let s = temporary_map
                        .get(field.input_field())
                        .unwrap()
                        .to_str_value();
                    let prefix = field.target_or_input_field();
                    let r = processor
                        .process(&s, pattern)
                        .unwrap()
                        .into_iter()
                        .map(|(k, v)| (format!("{}_{}", prefix, k), v))
                        .collect::<HashMap<_, _>>();
                    result.extend(r);
                }
            }

            let new_values = vec![
                ("edge_ip", Value::String("12.34.567.89".to_string())),
                ("edge_request_id", Value::String("12345678".to_string())),
                ("edge_geo", Value::String("US_CA_SANJOSE".to_string())),
                ("edge_asn", Value::String("20940".to_string())),
                ("origin_ip", Value::String("987.654.321.09".to_string())),
                ("peer_asn", Value::String("55155".to_string())),
                ("peer_geo", Value::String("US_CA_SANJOSE".to_string())),
                ("parent_asn", Value::String("55155".to_string())),
                ("parent_geo", Value::String("US_CA_SANJOSE".to_string())),
                ("wrapper_asn", Value::String("55155".to_string())),
                ("wrapper_geo", Value::String("US_CA_SANJOSE".to_string())),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();

            assert_eq!(result, new_values);
        }
    }
}
