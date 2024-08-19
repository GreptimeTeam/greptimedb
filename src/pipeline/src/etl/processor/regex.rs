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

use ahash::{HashSet, HashSetExt};
use lazy_static::lazy_static;
use regex::Regex;

// use super::{yaml_new_field, yaml_new_fileds, ProcessorBuilder, ProcessorKind};
use crate::etl::field::{Fields, InputFieldInfo, NewFields, OneInputMultiOutputField};
use crate::etl::processor::{
    yaml_bool, yaml_new_field, yaml_new_fileds, yaml_string, yaml_strings, Field, Processor,
    ProcessorBuilder, ProcessorKind, FIELDS_NAME, FIELD_NAME, IGNORE_MISSING_NAME, PATTERN_NAME,
};
use crate::etl::value::{Map, Value};

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
    type Err = String;

    fn from_str(origin: &str) -> Result<Self, Self::Err> {
        let groups = get_regex_group_names(origin);
        if groups.is_empty() {
            return Err(format!("no named group found in regex {origin}"));
        }

        let regex = Regex::new(origin).map_err(|e| e.to_string())?;
        Ok(GroupRegex {
            origin: origin.into(),
            regex,
            groups,
        })
    }
}

#[derive(Debug, Default)]
pub struct RegexProcessorBuilder {
    fields: NewFields,
    patterns: Vec<GroupRegex>,
    ignore_missing: bool,
    output_keys: HashSet<String>,
}

impl ProcessorBuilder for RegexProcessorBuilder {
    fn output_keys(&self) -> HashSet<&str> {
        self.output_keys.iter().map(|k| k.as_str()).collect()
    }

    fn input_keys(&self) -> HashSet<&str> {
        self.fields.iter().map(|f| f.input_field()).collect()
    }

    fn build(self, intermediate_keys: &[String]) -> ProcessorKind {
        let processor = Self::build(self, intermediate_keys);
        ProcessorKind::Regex(processor)
    }
}

impl RegexProcessorBuilder {
    fn check(self) -> Result<Self, String> {
        if self.fields.is_empty() {
            return Err(format!(
                "no valid field found in {} processor",
                PROCESSOR_REGEX
            ));
        }

        if self.patterns.is_empty() {
            return Err(format!(
                "no valid pattern found in {} processor",
                PROCESSOR_REGEX
            ));
        }

        Ok(self)
    }

    fn build(self, intermediate_keys: &[String]) -> RegexProcessor {
        let mut real_fields = vec![];

        let output_keys = self
            .patterns
            .iter()
            .map(|pattern| pattern.groups.iter().collect::<Vec<_>>())
            .collect::<Vec<_>>();
        for field in self.fields.into_iter() {
            let input_index = intermediate_keys
                .iter()
                .position(|k| *k == field.input_field())
                // TODO (qtang): handler error
                .unwrap();
            let input_field_info = InputFieldInfo::new(field.input_field(), input_index);

            let result = output_keys
                .iter()
                .flatten()
                .map(|x| generate_key(field.target_or_input_field(), x))
                // TODO (qtang): handler error
                .map(|o| {
                    let index = intermediate_keys.iter().position(|ik| *ik == o).unwrap();
                    (o, index)
                })
                .collect();

            let input = OneInputMultiOutputField::new(input_field_info, result);
            real_fields.push(input);
        }
        let output_info = real_fields
            .iter()
            .map(|f| {
                self.patterns
                    .iter()
                    .map(|p| {
                        p.groups
                            .iter()
                            .map(|g| {
                                let key = generate_key(f.input_name(), g);
                                let index =
                                    intermediate_keys.iter().position(|ik| *ik == key).unwrap();
                                (f.input_name().to_string(), g.to_string(), index)
                            })
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        RegexProcessor {
            // fields: Fields::one(Field::new("test".to_string())),
            real_fields,
            patterns: self.patterns,
            output_info,
            ignore_missing: self.ignore_missing,
        }
    }
}

/// only support string value
/// if no value found from a pattern, the target_field will be ignored
#[derive(Debug, Default)]
pub struct RegexProcessor {
    real_fields: Vec<OneInputMultiOutputField>,
    output_info: Vec<Vec<Vec<(String, String, usize)>>>,
    patterns: Vec<GroupRegex>,
    ignore_missing: bool,
}

impl RegexProcessor {
    fn with_fields(&mut self, fields: Fields) {
        todo!();
        // self.fields = fields;
    }

    fn try_with_patterns(&mut self, patterns: Vec<String>) -> Result<(), String> {
        let mut rs = vec![];
        for pattern in patterns {
            let gr = pattern.parse()?;
            rs.push(gr);
        }
        self.patterns = rs;
        Ok(())
    }

    fn with_ignore_missing(&mut self, ignore_missing: bool) {
        self.ignore_missing = ignore_missing;
    }

    fn process(
        &self,
        val: &str,
        gr: &GroupRegex,
        index: (usize, usize),
    ) -> Result<Vec<(usize, Value)>, String> {
        let mut result = Vec::new();
        if let Some(captures) = gr.regex.captures(val) {
            for (group_index, group) in gr.groups.iter().enumerate() {
                if let Some(capture) = captures.name(group) {
                    let value = capture.as_str().to_string();
                    let index = self.output_info[index.0][index.1][group_index].2;
                    result.push((index, Value::String(value)));
                }
            }
        }
        Ok(result)
    }

    fn process_field(&self, val: &str, field: &Field, gr: &GroupRegex) -> Result<Map, String> {
        let mut map = Map::default();

        if let Some(captures) = gr.regex.captures(val) {
            for group in &gr.groups {
                if let Some(capture) = captures.name(group) {
                    let value = capture.as_str().to_string();
                    let prefix = field.get_target_field();

                    let key = generate_key(prefix, group);

                    map.insert(key, Value::String(value));
                }
            }
        }

        Ok(map)
    }
}

impl TryFrom<&yaml_rust::yaml::Hash> for RegexProcessorBuilder {
    type Error = String;

    fn try_from(value: &yaml_rust::yaml::Hash) -> Result<Self, Self::Error> {
        let mut fields = NewFields::default();
        let mut patterns: Vec<GroupRegex> = vec![];
        let mut ignore_missing = false;

        for (k, v) in value.iter() {
            let key = k
                .as_str()
                .ok_or(format!("key must be a string, but got {k:?}"))?;
            match key {
                FIELD_NAME => {
                    fields = NewFields::one(yaml_new_field(v, FIELD_NAME)?);
                }
                FIELDS_NAME => {
                    fields = yaml_new_fileds(v, FIELDS_NAME)?;
                }
                PATTERN_NAME => {
                    for pattern in vec![yaml_string(v, PATTERN_NAME)?] {
                        let gr = pattern.parse()?;
                        patterns.push(gr);
                    }
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

        let pattern_output_keys = patterns
            .iter()
            .flat_map(|pattern| pattern.groups.iter())
            .collect::<Vec<_>>();
        let mut output_keys = HashSet::new();
        for field in fields.iter() {
            for x in pattern_output_keys.iter() {
                output_keys.insert(generate_key(field.target_or_input_field(), x));
            }
        }

        let processor_builder = RegexProcessorBuilder {
            fields,
            patterns,
            ignore_missing,
            output_keys,
        };

        processor_builder.check()
    }
}

impl Processor for RegexProcessor {
    fn kind(&self) -> &str {
        PROCESSOR_REGEX
    }

    fn ignore_missing(&self) -> bool {
        self.ignore_missing
    }

    fn exec_mut(&self, val: &mut Vec<Value>) -> Result<(), String> {
        for (field_index, field) in self.real_fields.iter().enumerate() {
            let index = field.input_index();
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
                    for (gr_index, gr) in self.patterns.iter().enumerate() {
                        result_list =
                            Some(self.process(s.as_str(), gr, (field_index, gr_index))?);
                    }
                }
                Some(Value::Null) | None => {
                    if !self.ignore_missing {
                        return Err(format!(
                            "{} processor: missing field: {}",
                            self.kind(),
                            field.input_name()
                        ));
                    }
                }
                Some(v) => {
                    return Err(format!(
                        "{} processor: expect string value, but got {v:?}",
                        self.kind()
                    ));
                }
            }
            // safety here
            match result_list {
                None => {}
                Some(result_list) => {
                    for (output_index, result) in result_list {
                        val[output_index] = result;
                    }
                }
            }
        }

        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::RegexProcessor;
    use crate::etl::field::Fields;
    use crate::etl::processor::Processor;
    use crate::etl::value::{Map, Value};

    #[test]
    fn test_simple_parse() {
        // let mut processor = RegexProcessor::default();

        // // single field (with prefix), multiple patterns
        // let f = ["a"].iter().map(|f| f.parse().unwrap()).collect();
        // processor.with_fields(Fields::new(f).unwrap());

        // let ar = "(?<ar>\\d)";

        // let patterns = [ar].iter().map(|p| p.to_string()).collect();
        // processor.try_with_patterns(patterns).unwrap();

        // let mut map = Map::default();
        // map.insert("a", Value::String("123".to_string()));
        // processor.exec_map(&mut map).unwrap();

        // let v = Map {
        //     values: vec![
        //         ("a_ar".to_string(), Value::String("1".to_string())),
        //         ("a".to_string(), Value::String("123".to_string())),
        //     ]
        //     .into_iter()
        //     .collect(),
        // };

        // assert_eq!(v, map);
    }

    #[test]
    fn test_process() {
        let mut processor = RegexProcessor::default();

        let cc = "[c=c,n=US_CA_SANJOSE,o=55155]";
        let cg = "[a=12.34.567.89,b=12345678,c=g,n=US_CA_SANJOSE,o=20940]";
        let co = "[a=987.654.321.09,c=o]";
        let cp = "[c=p,n=US_CA_SANJOSE,o=55155]";
        let cw = "[c=w,n=US_CA_SANJOSE,o=55155]";
        let breadcrumbs = Value::String([cc, cg, co, cp, cw].iter().join(","));

        let values = [
            ("breadcrumbs", breadcrumbs.clone()),
            ("breadcrumbs_parent", Value::String(cc.to_string())),
            ("breadcrumbs_edge", Value::String(cg.to_string())),
            ("breadcrumbs_origin", Value::String(co.to_string())),
            ("breadcrumbs_peer", Value::String(cp.to_string())),
            ("breadcrumbs_wrapper", Value::String(cw.to_string())),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        let mut temporary_map = Map { values };

        {
            // single field (with prefix), multiple patterns
            // let ff = ["breadcrumbs, breadcrumbs"]
            //     .iter()
            //     .map(|f| f.parse().unwrap())
            //     .collect();
            // processor.with_fields(Fields::new(ff).unwrap());

            // let ccr = "(?<parent>\\[[^\\[]*c=c[^\\]]*\\])";
            // let cgr = "(?<edge>\\[[^\\[]*c=g[^\\]]*\\])";
            // let cor = "(?<origin>\\[[^\\[]*c=o[^\\]]*\\])";
            // let cpr = "(?<peer>\\[[^\\[]*c=p[^\\]]*\\])";
            // let cwr = "(?<wrapper>\\[[^\\[]*c=w[^\\]]*\\])";
            // let patterns = [ccr, cgr, cor, cpr, cwr]
            //     .iter()
            //     .map(|p| p.to_string())
            //     .collect();
            // processor.try_with_patterns(patterns).unwrap();

            // let mut map = Map::default();
            // map.insert("breadcrumbs", breadcrumbs.clone());
            // processor.exec_map(&mut map).unwrap();

            // assert_eq!(map, temporary_map);
        }

        {
            // multiple fields (with prefix), multiple patterns
            // let ff = [
            //     "breadcrumbs_parent, parent",
            //     "breadcrumbs_edge, edge",
            //     "breadcrumbs_origin, origin",
            //     "breadcrumbs_peer, peer",
            //     "breadcrumbs_wrapper, wrapper",
            // ]
            // .iter()
            // .map(|f| f.parse().unwrap())
            // .collect();
            // processor.with_fields(Fields::new(ff).unwrap());

            // let patterns = [
            //     "a=(?<ip>[^,\\]]+)",
            //     "b=(?<request_id>[^,\\]]+)",
            //     "k=(?<request_end_time>[^,\\]]+)",
            //     "l=(?<turn_around_time>[^,\\]]+)",
            //     "m=(?<dns_lookup_time>[^,\\]]+)",
            //     "n=(?<geo>[^,\\]]+)",
            //     "o=(?<asn>[^,\\]]+)",
            // ]
            // .iter()
            // .map(|p| p.to_string())
            // .collect();
            // processor.try_with_patterns(patterns).unwrap();

            // let new_values = vec![
            //     ("edge_ip", Value::String("12.34.567.89".to_string())),
            //     ("edge_request_id", Value::String("12345678".to_string())),
            //     ("edge_geo", Value::String("US_CA_SANJOSE".to_string())),
            //     ("edge_asn", Value::String("20940".to_string())),
            //     ("origin_ip", Value::String("987.654.321.09".to_string())),
            //     ("peer_asn", Value::String("55155".to_string())),
            //     ("peer_geo", Value::String("US_CA_SANJOSE".to_string())),
            //     ("parent_asn", Value::String("55155".to_string())),
            //     ("parent_geo", Value::String("US_CA_SANJOSE".to_string())),
            //     ("wrapper_asn", Value::String("55155".to_string())),
            //     ("wrapper_geo", Value::String("US_CA_SANJOSE".to_string())),
            // ]
            // .into_iter()
            // .map(|(k, v)| (k.to_string(), v))
            // .collect();

            // let mut expected_map = temporary_map.clone();
            // processor.exec_map(&mut temporary_map).unwrap();
            // expected_map.extend(Map { values: new_values });

            // assert_eq!(expected_map, temporary_map);
        }
    }
}
