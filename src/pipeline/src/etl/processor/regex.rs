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

use ahash::HashSet;
use lazy_static::lazy_static;
use regex::Regex;

use crate::etl::field::Fields;
use crate::etl::processor::{
    yaml_bool, yaml_field, yaml_fields, yaml_string, yaml_strings, Field, Processor, FIELDS_NAME,
    FIELD_NAME, IGNORE_MISSING_NAME, PATTERN_NAME,
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

/// only support string value
/// if no value found from a pattern, the target_field will be ignored
#[derive(Debug, Default)]
pub struct RegexProcessor {
    fields: Fields,
    patterns: Vec<GroupRegex>,
    ignore_missing: bool,
}

impl RegexProcessor {
    fn with_fields(&mut self, fields: Fields) {
        self.fields = fields;
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

    fn generate_key(prefix: &str, group: &str) -> String {
        format!("{prefix}_{group}")
    }

    fn process_field(&self, val: &str, field: &Field, gr: &GroupRegex) -> Result<Map, String> {
        let mut map = Map::default();

        if let Some(captures) = gr.regex.captures(val) {
            for group in &gr.groups {
                if let Some(capture) = captures.name(group) {
                    let value = capture.as_str().to_string();
                    let prefix = field.get_target_field();

                    let key = Self::generate_key(prefix, group);

                    map.insert(key, Value::String(value));
                }
            }
        }

        Ok(map)
    }

    fn update_output_keys(&mut self) {
        for field in self.fields.iter_mut() {
            for gr in &self.patterns {
                for group in &gr.groups {
                    field
                        .output_fields_index_mapping
                        .insert(Self::generate_key(field.get_target_field(), group), 0_usize);
                }
            }
        }
    }
}

impl TryFrom<&yaml_rust::yaml::Hash> for RegexProcessor {
    type Error = String;

    fn try_from(value: &yaml_rust::yaml::Hash) -> Result<Self, Self::Error> {
        let mut processor = RegexProcessor::default();

        for (k, v) in value.iter() {
            let key = k
                .as_str()
                .ok_or(format!("key must be a string, but got {k:?}"))?;
            match key {
                FIELD_NAME => {
                    processor.with_fields(Fields::one(yaml_field(v, FIELD_NAME)?));
                }
                FIELDS_NAME => {
                    processor.with_fields(yaml_fields(v, FIELDS_NAME)?);
                }
                PATTERN_NAME => {
                    processor.try_with_patterns(vec![yaml_string(v, PATTERN_NAME)?])?;
                }
                PATTERNS_NAME => {
                    processor.try_with_patterns(yaml_strings(v, PATTERNS_NAME)?)?;
                }
                IGNORE_MISSING_NAME => {
                    processor.with_ignore_missing(yaml_bool(v, IGNORE_MISSING_NAME)?);
                }
                _ => {}
            }
        }

        processor.check().map(|mut p| {
            p.update_output_keys();
            p
        })
    }
}

impl Processor for RegexProcessor {
    fn kind(&self) -> &str {
        PROCESSOR_REGEX
    }

    fn ignore_missing(&self) -> bool {
        self.ignore_missing
    }

    fn fields(&self) -> &Fields {
        &self.fields
    }

    fn fields_mut(&mut self) -> &mut Fields {
        &mut self.fields
    }

    fn output_keys(&self) -> HashSet<String> {
        self.fields
            .iter()
            .flat_map(|f| {
                self.patterns.iter().flat_map(move |p| {
                    p.groups
                        .iter()
                        .map(move |g| Self::generate_key(&f.input_field.name, g))
                })
            })
            .collect()
    }

    fn exec_field(&self, val: &Value, field: &Field) -> Result<Map, String> {
        match val {
            Value::String(val) => {
                let mut map = Map::default();
                for gr in &self.patterns {
                    let m = self.process_field(val, field, gr)?;
                    map.extend(m);
                }
                Ok(map)
            }
            _ => Err(format!(
                "{} processor: expect string value, but got {val:?}",
                self.kind()
            )),
        }
    }

    fn exec_mut(&self, val: &mut Vec<Value>) -> Result<(), String> {
        for field in self.fields.iter() {
            let index = field.input_field.index;
            match val.get(index) {
                Some(Value::String(s)) => {
                    let mut map = Map::default();
                    for gr in &self.patterns {
                        // TODO(qtang): Let this method use the intermediate state collection directly.
                        let m = self.process_field(s, field, gr)?;
                        map.extend(m);
                    }

                    field
                        .output_fields_index_mapping
                        .iter()
                        .for_each(|(k, output_index)| {
                            if let Some(v) = map.remove(k) {
                                val[*output_index] = v;
                            }
                        });
                }
                Some(Value::Null) | None => {
                    if !self.ignore_missing {
                        return Err(format!(
                            "{} processor: missing field: {}",
                            self.kind(),
                            field.get_field_name()
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
        let mut processor = RegexProcessor::default();

        // single field (with prefix), multiple patterns
        let f = ["a"].iter().map(|f| f.parse().unwrap()).collect();
        processor.with_fields(Fields::new(f).unwrap());

        let ar = "(?<ar>\\d)";

        let patterns = [ar].iter().map(|p| p.to_string()).collect();
        processor.try_with_patterns(patterns).unwrap();

        let mut map = Map::default();
        map.insert("a", Value::String("123".to_string()));
        let processed_val = processor.exec_map(&mut map).unwrap();

        let v = Map {
            values: vec![
                ("a_ar".to_string(), Value::String("1".to_string())),
                ("a".to_string(), Value::String("123".to_string())),
            ]
            .into_iter()
            .collect(),
        };

        assert_eq!(v, *processed_val);
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
            let ff = ["breadcrumbs, breadcrumbs"]
                .iter()
                .map(|f| f.parse().unwrap())
                .collect();
            processor.with_fields(Fields::new(ff).unwrap());

            let ccr = "(?<parent>\\[[^\\[]*c=c[^\\]]*\\])";
            let cgr = "(?<edge>\\[[^\\[]*c=g[^\\]]*\\])";
            let cor = "(?<origin>\\[[^\\[]*c=o[^\\]]*\\])";
            let cpr = "(?<peer>\\[[^\\[]*c=p[^\\]]*\\])";
            let cwr = "(?<wrapper>\\[[^\\[]*c=w[^\\]]*\\])";
            let patterns = [ccr, cgr, cor, cpr, cwr]
                .iter()
                .map(|p| p.to_string())
                .collect();
            processor.try_with_patterns(patterns).unwrap();

            let mut map = Map::default();
            map.insert("breadcrumbs", breadcrumbs.clone());
            let processed_val = processor.exec_map(&mut map).unwrap();

            assert_eq!(processed_val, &mut temporary_map);
        }

        {
            // multiple fields (with prefix), multiple patterns
            let ff = [
                "breadcrumbs_parent, parent",
                "breadcrumbs_edge, edge",
                "breadcrumbs_origin, origin",
                "breadcrumbs_peer, peer",
                "breadcrumbs_wrapper, wrapper",
            ]
            .iter()
            .map(|f| f.parse().unwrap())
            .collect();
            processor.with_fields(Fields::new(ff).unwrap());

            let patterns = [
                "a=(?<ip>[^,\\]]+)",
                "b=(?<request_id>[^,\\]]+)",
                "k=(?<request_end_time>[^,\\]]+)",
                "l=(?<turn_around_time>[^,\\]]+)",
                "m=(?<dns_lookup_time>[^,\\]]+)",
                "n=(?<geo>[^,\\]]+)",
                "o=(?<asn>[^,\\]]+)",
            ]
            .iter()
            .map(|p| p.to_string())
            .collect();
            processor.try_with_patterns(patterns).unwrap();

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

            let mut expected_map = temporary_map.clone();
            let actual_val = processor.exec_map(&mut temporary_map).unwrap();
            expected_map.extend(Map { values: new_values });

            assert_eq!(&mut expected_map, actual_val);
        }
    }
}
