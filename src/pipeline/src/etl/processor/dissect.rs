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

use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use common_telemetry::warn;
use itertools::Itertools;

use crate::etl::field::{Field, Fields};
use crate::etl::processor::{
    yaml_bool, yaml_field, yaml_fields, yaml_parse_string, yaml_parse_strings, yaml_string,
    Processor, FIELDS_NAME, FIELD_NAME, IGNORE_MISSING_NAME, PATTERNS_NAME, PATTERN_NAME,
};
use crate::etl::value::{Map, Value};

pub(crate) const PROCESSOR_DISSECT: &str = "dissect";

const APPEND_SEPARATOR_NAME: &str = "append_separator";

#[derive(Debug, PartialEq)]
enum StartModifier {
    Append(Option<u32>),
    NamedSkip,
    MapKey,
    MapVal,
}

impl std::fmt::Display for StartModifier {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            StartModifier::Append(o) => match o {
                Some(v) => write!(f, "+/{v}"),
                None => write!(f, "+"),
            },
            StartModifier::NamedSkip => write!(f, "?"),
            StartModifier::MapKey => write!(f, "*"),
            StartModifier::MapVal => write!(f, "&"),
        }
    }
}

#[derive(Debug, PartialEq)]
struct EndModifier;

impl std::fmt::Display for EndModifier {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "->",)
    }
}

#[derive(Debug, PartialEq, Default)]
struct Name {
    name: String,
    start_modifier: Option<StartModifier>,
    end_modifier: Option<EndModifier>,
}

impl Name {
    fn is_name_empty(&self) -> bool {
        self.name.is_empty()
    }

    fn is_empty(&self) -> bool {
        self.name.is_empty() && self.start_modifier.is_none() && self.end_modifier.is_none()
    }

    fn try_start_modifier(&mut self, modifier: StartModifier) -> Result<(), String> {
        match &self.start_modifier {
            Some(m) => Err(format!("'{m}' modifier already set, but found {modifier}",)),
            None => {
                self.start_modifier = Some(modifier);
                Ok(())
            }
        }
    }

    fn try_append_order(&mut self, order: u32) -> Result<(), String> {
        match &mut self.start_modifier {
            Some(StartModifier::Append(o)) => match o {
                Some(n) => Err(format!(
                    "Append Order modifier is already set to '{n}', cannot be set to {order}"
                )),
                None => {
                    *o = Some(order);
                    Ok(())
                }
            },
            Some(m) => Err(format!(
                "Order can only be set to Append Modifier, current modifier is {m}"
            )),
            None => Err("Order can only be set to Append Modifier".to_string()),
        }
    }

    fn try_end_modifier(&mut self) -> Result<(), String> {
        match &self.end_modifier {
            Some(m) => Err(format!("End modifier already set: '{m}'")),
            None => {
                self.end_modifier = Some(EndModifier);
                Ok(())
            }
        }
    }

    fn is_append_modifier_set(&self) -> bool {
        matches!(self.start_modifier, Some(StartModifier::Append(_)))
    }

    fn is_start_modifier_set(&self) -> bool {
        self.start_modifier.is_some()
    }

    fn is_end_modifier_set(&self) -> bool {
        self.end_modifier.is_some()
    }
}

impl std::fmt::Display for Name {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl From<&str> for Name {
    fn from(value: &str) -> Self {
        Name {
            name: value.to_string(),
            start_modifier: None,
            end_modifier: None,
        }
    }
}

#[derive(Debug, PartialEq)]
enum Part {
    Split(String),
    Name(Name),
}

impl Part {
    fn is_empty(&self) -> bool {
        match self {
            Part::Split(v) => v.is_empty(),
            Part::Name(v) => v.is_empty(),
        }
    }

    fn empty_split() -> Self {
        Part::Split(String::new())
    }

    fn empty_name() -> Self {
        Part::Name(Name::default())
    }

    fn push(&mut self, ch: char) {
        match self {
            Part::Split(v) => v.push(ch),
            Part::Name(v) => v.name.push(ch),
        }
    }
}

#[derive(Debug, Default)]
struct Pattern {
    origin: String,
    parts: Vec<Part>,
}

impl std::ops::Deref for Pattern {
    type Target = Vec<Part>;

    fn deref(&self) -> &Self::Target {
        &self.parts
    }
}

impl std::ops::DerefMut for Pattern {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.parts
    }
}

impl std::str::FromStr for Pattern {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = vec![];
        let mut cursor = Part::empty_split();

        let origin = s.to_string();
        let chars: Vec<char> = origin.chars().collect();

        let mut pos = 0;
        while pos < chars.len() {
            let ch = chars[pos];
            match (ch, &mut cursor) {
                // if cursor is Split part, and found %{, then ready to start a Name part
                ('%', Part::Split(_)) if matches!(chars.get(pos + 1), Some('{')) => {
                    if !cursor.is_empty() {
                        parts.push(cursor);
                    }

                    cursor = Part::empty_name();
                    pos += 1; // skip '{'
                }
                // if cursor is Split part, and not found % or {, then continue the Split part
                (_, Part::Split(_)) => {
                    cursor.push(ch);
                }
                // if cursor is Name part, and found }, then end the Name part, start the next Split part
                ('}', Part::Name(_)) => {
                    parts.push(cursor);
                    cursor = Part::empty_split();
                }
                ('+', Part::Name(name)) if !name.is_start_modifier_set() => {
                    name.try_start_modifier(StartModifier::Append(None))?;
                }
                ('/', Part::Name(name)) if name.is_append_modifier_set() => {
                    let mut order = 0;
                    let mut j = pos + 1;
                    while j < chars.len() {
                        let digit = chars[j];
                        if digit.is_ascii_digit() {
                            order = order * 10 + digit.to_digit(10).unwrap();
                            j += 1;
                        } else {
                            break;
                        }
                    }

                    if j == pos + 1 {
                        return Err(format!(
                            "Invalid Pattern: '{s}'. Digit order must be set after '/'",
                        ));
                    }

                    name.try_append_order(order)?;
                    pos = j - 1; // this will change the position to the last digit of the order
                }
                ('?', Part::Name(name)) if !name.is_start_modifier_set() => {
                    name.try_start_modifier(StartModifier::NamedSkip)?;
                }
                ('*', Part::Name(name)) if !name.is_start_modifier_set() => {
                    name.try_start_modifier(StartModifier::MapKey)?;
                }
                ('&', Part::Name(name)) if !name.is_start_modifier_set() => {
                    name.try_start_modifier(StartModifier::MapVal)?;
                }
                ('-', Part::Name(name)) if !name.is_end_modifier_set() => {
                    if let Some('>') = chars.get(pos + 1) {
                    } else {
                        return Err(format!(
                            "Invalid Pattern: '{s}'. expected '->' but only '-'",
                        ));
                    }

                    if let Some('}') = chars.get(pos + 2) {
                    } else {
                        return Err(format!("Invalid Pattern: '{s}'. expected '}}' after '->'",));
                    }

                    name.try_end_modifier()?;
                    pos += 1; // only skip '>', the next loop will skip '}'
                }
                (_, Part::Name(name)) if !is_valid_char(ch) => {
                    let tail: String = if name.is_name_empty() {
                        format!("Invalid '{ch}'")
                    } else {
                        format!("Invalid '{ch}' in '{name}'")
                    };
                    return Err(format!("Invalid Pattern: '{s}'. {tail}"));
                }
                (_, Part::Name(_)) => {
                    cursor.push(ch);
                }
            }

            pos += 1;
        }

        match cursor {
            Part::Split(ref split) if !split.is_empty() => parts.push(cursor),
            Part::Name(name) if !name.is_empty() => {
                return Err(format!("Invalid Pattern: '{s}'. '{name}' is not closed"))
            }
            _ => {}
        }

        let pattern = Self { parts, origin };
        pattern.check()?;
        Ok(pattern)
    }
}

impl Pattern {
    fn check(&self) -> Result<(), String> {
        if self.len() == 0 {
            return Err("Empty pattern is not allowed".to_string());
        }

        let mut map_keys = HashSet::new();
        let mut map_vals = HashSet::new();

        for i in 0..self.len() {
            let this_part = &self[i];
            let next_part = self.get(i + 1);
            match (this_part, next_part) {
                (Part::Split(split), _) if split.is_empty() => {
                    return Err(format!(
                        "Invalid Pattern: '{}'. Empty split is not allowed",
                        self.origin
                    ));
                }
                (Part::Name(name1), Some(Part::Name(name2))) => {
                    return Err(format!(
                        "Invalid Pattern: '{}'. consecutive names are not allowed: '{}' '{}'",
                        self.origin, name1, name2
                    ));
                }
                (Part::Name(name), _) if name.is_name_empty() => {
                    if let Some(ref m) = name.start_modifier {
                        return Err(format!(
                            "Invalid Pattern: '{}'. only '{}' modifier is invalid",
                            self.origin, m
                        ));
                    }
                }
                (Part::Name(name), _) => match name.start_modifier {
                    Some(StartModifier::MapKey) => {
                        if map_keys.contains(&name.name) {
                            return Err(format!(
                                "Invalid Pattern: '{}'. Duplicate map key: '{}'",
                                self.origin, name.name
                            ));
                        } else {
                            map_keys.insert(&name.name);
                        }
                    }
                    Some(StartModifier::MapVal) => {
                        if map_vals.contains(&name.name) {
                            return Err(format!(
                                "Invalid Pattern: '{}'. Duplicate map val: '{}'",
                                self.origin, name.name
                            ));
                        } else {
                            map_vals.insert(&name.name);
                        }
                    }
                    _ => {}
                },
                _ => {}
            }
        }

        if map_keys != map_vals {
            return Err(format!(
                "Invalid Pattern: '{}'. key and value not matched: '{}'",
                self.origin,
                map_keys
                    .symmetric_difference(&map_vals)
                    .map(|s| s.as_str())
                    .collect::<Vec<&str>>()
                    .join(",")
            ));
        }

        Ok(())
    }
}

impl std::fmt::Display for Pattern {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.origin)
    }
}

#[derive(Debug, Default)]
pub struct DissectProcessor {
    fields: Fields,
    patterns: Vec<Pattern>,
    ignore_missing: bool,

    // The character(s) that separate the appended fields. Default is an empty string.
    append_separator: Option<String>,
}

impl DissectProcessor {
    fn with_fields(&mut self, fields: Fields) {
        self.fields = fields;
    }

    fn with_ignore_missing(&mut self, ignore_missing: bool) {
        self.ignore_missing = ignore_missing;
    }

    fn with_patterns(&mut self, patterns: Vec<Pattern>) {
        self.patterns = patterns;
    }

    fn with_append_separator(&mut self, append_separator: String) {
        self.append_separator = Some(append_separator);
    }

    fn process_pattern(&self, chs: &[char], pattern: &Pattern) -> Result<Map, String> {
        let mut map = Map::default();
        let mut pos = 0;

        let mut appends: HashMap<String, Vec<(String, u32)>> = HashMap::new();
        let mut maps: HashMap<String, String> = HashMap::new();

        let mut process_name_value = |name: &Name, value: String| {
            let name_str = name.to_string();
            match name.start_modifier {
                Some(StartModifier::NamedSkip) => {
                    // do nothing, ignore this match
                }
                Some(StartModifier::Append(order)) => {
                    appends
                        .entry(name_str)
                        .or_default()
                        .push((value, order.unwrap_or_default()));
                }
                Some(StartModifier::MapKey) => match maps.get(&name_str) {
                    Some(map_val) => {
                        map.insert(value, Value::String(map_val.to_string()));
                    }
                    None => {
                        maps.insert(name_str, value);
                    }
                },
                Some(StartModifier::MapVal) => match maps.get(&name_str) {
                    Some(map_key) => {
                        map.insert(map_key, Value::String(value));
                    }
                    None => {
                        maps.insert(name_str, value);
                    }
                },
                None => {
                    map.insert(name.to_string(), Value::String(value));
                }
            }
        };

        for i in 0..pattern.len() {
            let this_part = &pattern[i];
            let next_part = pattern.get(i + 1);
            match (this_part, next_part) {
                // if Split part, and exactly matches, then move pos split.len() forward
                (Part::Split(split), _) => {
                    let split_chs = split.chars().collect::<Vec<char>>();
                    let split_len = split_chs.len();
                    if pos + split_len > chs.len() {
                        return Err(format!("'{split}' exceeds the input",));
                    }

                    if &chs[pos..pos + split_len] != split_chs.as_slice() {
                        return Err(format!(
                            "'{split}' does not match the input '{}'",
                            chs[pos..pos + split_len].iter().collect::<String>()
                        ));
                    }

                    pos += split_len;
                }

                (Part::Name(name1), Some(Part::Name(name2))) => {
                    return Err(format!(
                        "consecutive names are not allowed: '{name1}' '{name2}'"
                    ));
                }

                // if Name part is the last part, then the rest of the input is the value
                (Part::Name(name), None) => {
                    let value = chs[pos..].iter().collect::<String>();
                    process_name_value(name, value);
                }

                // if Name part, and next part is Split, then find the matched value of the name
                (Part::Name(name), Some(Part::Split(split))) => {
                    let stop = split
                        .chars()
                        .next()
                        .ok_or("Empty split is not allowed".to_string())?; // this won't happen
                    let mut end = pos;
                    while end < chs.len() && chs[end] != stop {
                        end += 1;
                    }

                    if !name.is_name_empty() {
                        let value = chs[pos..end].iter().collect::<String>();
                        process_name_value(name, value);
                    }

                    if name.is_end_modifier_set() {
                        while end < chs.len() && chs[end] == stop {
                            end += 1;
                        }
                        end -= 1; // leave the last stop character to match the next split
                    }

                    pos = end;
                }
            }
        }

        if !appends.is_empty() {
            let sep = match self.append_separator {
                Some(ref sep) => sep,
                None => " ",
            };

            for (name, mut values) in appends {
                values.sort_by(|a, b| a.1.cmp(&b.1));
                let value = values.into_iter().map(|(a, _)| a).join(sep);
                map.insert(name, Value::String(value));
            }
        }

        Ok(map)
    }

    fn process(&self, val: &str) -> Result<Map, String> {
        let chs = val.chars().collect::<Vec<char>>();

        for pattern in &self.patterns {
            if let Ok(map) = self.process_pattern(&chs, pattern) {
                return Ok(map);
            }
        }

        Err("No matching pattern found".to_string())
    }

    fn update_output_keys(&mut self) {
        for fields in self.fields.iter_mut() {
            for pattern in self.patterns.iter() {
                for part in pattern.iter() {
                    if let Part::Name(name) = part {
                        if !name.is_empty() {
                            fields.output_fields.insert(name.to_string(), 0);
                        }
                    }
                }
            }
        }
    }
}

impl TryFrom<&yaml_rust::yaml::Hash> for DissectProcessor {
    type Error = String;

    fn try_from(value: &yaml_rust::yaml::Hash) -> Result<Self, Self::Error> {
        let mut processor = Self::default();

        for (k, v) in value.iter() {
            let key = k
                .as_str()
                .ok_or(format!("key must be a string, but got '{k:?}'"))?;

            match key {
                FIELD_NAME => processor.with_fields(Fields::one(yaml_field(v, FIELD_NAME)?)),
                FIELDS_NAME => processor.with_fields(yaml_fields(v, FIELDS_NAME)?),
                PATTERN_NAME => {
                    let pattern: Pattern = yaml_parse_string(v, PATTERN_NAME)?;
                    processor.with_patterns(vec![pattern]);
                }
                PATTERNS_NAME => {
                    let patterns = yaml_parse_strings(v, PATTERNS_NAME)?;
                    processor.with_patterns(patterns);
                }
                IGNORE_MISSING_NAME => {
                    processor.with_ignore_missing(yaml_bool(v, IGNORE_MISSING_NAME)?)
                }
                APPEND_SEPARATOR_NAME => {
                    processor.with_append_separator(yaml_string(v, APPEND_SEPARATOR_NAME)?)
                }
                _ => {}
            }
        }
        processor.update_output_keys();
        Ok(processor)
    }
}

impl Processor for DissectProcessor {
    fn kind(&self) -> &str {
        PROCESSOR_DISSECT
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
        self.patterns
            .iter()
            .flat_map(|p| {
                p.parts
                    .iter()
                    .filter(|p| {
                        if let Part::Name(name) = p {
                            !name.is_empty()
                        } else {
                            false
                        }
                    })
                    .map(|p| {
                        if let Part::Name(name) = p {
                            name.to_string()
                        } else {
                            unreachable!()
                        }
                    })
            })
            .collect()
    }

    fn exec_field(&self, val: &Value, _field: &Field) -> Result<Map, String> {
        match val {
            Value::String(val) => match self.process(val) {
                Ok(map) => Ok(map),
                Err(e) => {
                    warn!("dissect processor: {}", e);
                    Ok(Map::default())
                }
            },
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
                Some(Value::String(val_str)) => match self.process(val_str) {
                    Ok(mut map) => {
                        field.output_fields.iter().for_each(|(k, output_index)| {
                            if let Some(v) = map.remove(k) {
                                val[*output_index] = v
                            }
                        });
                    }
                    Err(e) => {
                        warn!("dissect processor: {}", e);
                    }
                },
                _ => {}
            }
        }
        Ok(())
    }
}

fn is_valid_char(ch: char) -> bool {
    ch.is_alphanumeric() || ch == '_'
}

#[cfg(test)]
mod tests {
    use ahash::HashMap;

    use super::{DissectProcessor, EndModifier, Name, Part, Pattern, StartModifier};
    use crate::etl::value::{Map, Value};

    fn assert(pattern_str: &str, input: &str, expected: HashMap<String, Value>) {
        let chs = input.chars().collect::<Vec<char>>();
        let pattern = pattern_str.parse().unwrap();

        let processor = DissectProcessor::default();
        let map = processor.process_pattern(&chs, &pattern).unwrap();

        assert_eq!(map, Map::from(expected), "pattern: {}", pattern_str);
    }

    #[test]
    fn test_dissect_simple_pattern() {
        let cases = [(
            "%{clientip} %{ident} %{auth} [%{timestamp}] \"%{verb} %{request} HTTP/%{httpversion}\" %{status} %{size}",
            vec![
                Part::Name("clientip".into()),
                Part::Split(" ".into()),
                Part::Name("ident".into()),
                Part::Split(" ".into()),
                Part::Name("auth".into()),
                Part::Split(" [".into()),
                Part::Name("timestamp".into()),
                Part::Split("] \"".into()),
                Part::Name("verb".into()),
                Part::Split(" ".into()),
                Part::Name("request".into()),
                Part::Split(" HTTP/".into()),
                Part::Name("httpversion".into()),
                Part::Split("\" ".into()),
                Part::Name("status".into()),
                Part::Split(" ".into()),
                Part::Name("size".into()),
            ],
        )];

        for (pattern, expected) in cases.into_iter() {
            let p: Pattern = pattern.parse().unwrap();
            assert_eq!(p.parts, expected);
        }
    }

    #[test]
    fn test_dissect_modifier_pattern() {
        let cases = [
            (
                "%{} %{}",
                vec![
                    Part::Name(Name {
                        name: "".into(),
                        start_modifier: None,
                        end_modifier: None,
                    }),
                    Part::Split(" ".into()),
                    Part::Name(Name {
                        name: "".into(),
                        start_modifier: None,
                        end_modifier: None,
                    }),
                ],
            ),
            (
                "%{ts->} %{level}",
                vec![
                    Part::Name(Name {
                        name: "ts".into(),
                        start_modifier: None,
                        end_modifier: Some(EndModifier),
                    }),
                    Part::Split(" ".into()),
                    Part::Name("level".into()),
                ],
            ),
            (
                "[%{ts}]%{->}[%{level}]",
                vec![
                    Part::Split("[".into()),
                    Part::Name(Name {
                        name: "ts".into(),
                        start_modifier: None,
                        end_modifier: None,
                    }),
                    Part::Split("]".into()),
                    Part::Name(Name {
                        name: "".into(),
                        start_modifier: None,
                        end_modifier: Some(EndModifier),
                    }),
                    Part::Split("[".into()),
                    Part::Name(Name {
                        name: "level".into(),
                        start_modifier: None,
                        end_modifier: None,
                    }),
                    Part::Split("]".into()),
                ],
            ),
            (
                "%{+name} %{+name} %{+name} %{+name}",
                vec![
                    Part::Name(Name {
                        name: "name".into(),
                        start_modifier: Some(StartModifier::Append(None)),
                        end_modifier: None,
                    }),
                    Part::Split(" ".into()),
                    Part::Name(Name {
                        name: "name".into(),
                        start_modifier: Some(StartModifier::Append(None)),
                        end_modifier: None,
                    }),
                    Part::Split(" ".into()),
                    Part::Name(Name {
                        name: "name".into(),
                        start_modifier: Some(StartModifier::Append(None)),
                        end_modifier: None,
                    }),
                    Part::Split(" ".into()),
                    Part::Name(Name {
                        name: "name".into(),
                        start_modifier: Some(StartModifier::Append(None)),
                        end_modifier: None,
                    }),
                ],
            ),
            (
                "%{+name/2} %{+name/4} %{+name/3} %{+name/1}",
                vec![
                    Part::Name(Name {
                        name: "name".into(),
                        start_modifier: Some(StartModifier::Append(Some(2))),
                        end_modifier: None,
                    }),
                    Part::Split(" ".into()),
                    Part::Name(Name {
                        name: "name".into(),
                        start_modifier: Some(StartModifier::Append(Some(4))),
                        end_modifier: None,
                    }),
                    Part::Split(" ".into()),
                    Part::Name(Name {
                        name: "name".into(),
                        start_modifier: Some(StartModifier::Append(Some(3))),
                        end_modifier: None,
                    }),
                    Part::Split(" ".into()),
                    Part::Name(Name {
                        name: "name".into(),
                        start_modifier: Some(StartModifier::Append(Some(1))),
                        end_modifier: None,
                    }),
                ],
            ),
            (
                "%{clientip} %{?ident} %{?auth} [%{timestamp}]",
                vec![
                    Part::Name(Name {
                        name: "clientip".into(),
                        start_modifier: None,
                        end_modifier: None,
                    }),
                    Part::Split(" ".into()),
                    Part::Name(Name {
                        name: "ident".into(),
                        start_modifier: Some(StartModifier::NamedSkip),
                        end_modifier: None,
                    }),
                    Part::Split(" ".into()),
                    Part::Name(Name {
                        name: "auth".into(),
                        start_modifier: Some(StartModifier::NamedSkip),
                        end_modifier: None,
                    }),
                    Part::Split(" [".into()),
                    Part::Name(Name {
                        name: "timestamp".into(),
                        start_modifier: None,
                        end_modifier: None,
                    }),
                    Part::Split("]".into()),
                ],
            ),
            (
                "[%{ts}] [%{level}] %{*p1}:%{&p1} %{*p2}:%{&p2}",
                vec![
                    Part::Split("[".into()),
                    Part::Name(Name {
                        name: "ts".into(),
                        start_modifier: None,
                        end_modifier: None,
                    }),
                    Part::Split("] [".into()),
                    Part::Name(Name {
                        name: "level".into(),
                        start_modifier: None,
                        end_modifier: None,
                    }),
                    Part::Split("] ".into()),
                    Part::Name(Name {
                        name: "p1".into(),
                        start_modifier: Some(StartModifier::MapKey),
                        end_modifier: None,
                    }),
                    Part::Split(":".into()),
                    Part::Name(Name {
                        name: "p1".into(),
                        start_modifier: Some(StartModifier::MapVal),
                        end_modifier: None,
                    }),
                    Part::Split(" ".into()),
                    Part::Name(Name {
                        name: "p2".into(),
                        start_modifier: Some(StartModifier::MapKey),
                        end_modifier: None,
                    }),
                    Part::Split(":".into()),
                    Part::Name(Name {
                        name: "p2".into(),
                        start_modifier: Some(StartModifier::MapVal),
                        end_modifier: None,
                    }),
                ],
            ),
            (
                "%{&p1}:%{*p1}",
                vec![
                    Part::Name(Name {
                        name: "p1".into(),
                        start_modifier: Some(StartModifier::MapVal),
                        end_modifier: None,
                    }),
                    Part::Split(":".into()),
                    Part::Name(Name {
                        name: "p1".into(),
                        start_modifier: Some(StartModifier::MapKey),
                        end_modifier: None,
                    }),
                ],
            ),
        ];

        for (pattern, expected) in cases.into_iter() {
            let p: Pattern = pattern.parse().unwrap();
            assert_eq!(p.parts, expected);
        }
    }

    #[test]
    fn test_dissect_invalid_pattern() {
        let cases = [
            ("", "Empty pattern is not allowed"),
            (
                "%{name1}%{name2}",
                "Invalid Pattern: '%{name1}%{name2}'. consecutive names are not allowed: 'name1' 'name2'"
            ),
            (
                "%{} %{ident",
                "Invalid Pattern: '%{} %{ident'. 'ident' is not closed",
            ),
            (
                "%{->clientip} ",
                "Invalid Pattern: '%{->clientip} '. expected '}' after '->'",
            ),
            (
                "%{/clientip} ",
                "Invalid Pattern: '%{/clientip} '. Invalid '/'",
            ),
            (
                "%{+?clientip} ",
                "Invalid Pattern: '%{+?clientip} '. Invalid '?'",
            ),
            (
                "%{+clientip/} ",
                "Invalid Pattern: '%{+clientip/} '. Digit order must be set after '/'",
            ),
            (
                "%{+clientip/a} ",
                "Invalid Pattern: '%{+clientip/a} '. Digit order must be set after '/'",
            ),
            (
                "%{clientip/1} ",
                "Invalid Pattern: '%{clientip/1} '. Invalid '/' in 'clientip'",
            ),
            (
                "%{+clientip/1/2} ",
                "Append Order modifier is already set to '1', cannot be set to 2",
            ),
            (
                "%{+/1} ",
                "Invalid Pattern: '%{+/1} '. only '+/1' modifier is invalid",
            ),
            (
                "%{+} ",
                "Invalid Pattern: '%{+} '. only '+' modifier is invalid",
            ),
            (
                "%{?} ",
                "Invalid Pattern: '%{?} '. only '?' modifier is invalid",
            ),
            (
                "%{*} ",
                "Invalid Pattern: '%{*} '. only '*' modifier is invalid",
            ),
            (
                "%{&} ",
                "Invalid Pattern: '%{&} '. only '&' modifier is invalid",
            ),
            (
                "%{*ip}",
                "Invalid Pattern: '%{*ip}'. key and value not matched: 'ip'"
            ),
            (
                "%{*ip} %{*ip}",
                "Invalid Pattern: '%{*ip} %{*ip}'. Duplicate map key: 'ip'",
            ),
            (
                "%{*ip1} %{&ip2}",
                "Invalid Pattern: '%{*ip1} %{&ip2}'. key and value not matched: 'ip1,ip2'"
            ),
        ];

        for (pattern, expected) in cases.into_iter() {
            let err = pattern.parse::<Pattern>().unwrap_err();
            assert_eq!(err, expected);
        }
    }

    #[test]
    fn test_dissect_process() {
        let expected = [
            ("timestamp", "30/Apr/1998:22:00:52 +0000"),
            ("status", "200"),
            ("clientip", "1.2.3.4"),
            ("ident", "-"),
            ("size", "3171"),
            (
                "request",
                "/english/venues/cities/images/montpellier/18.gif",
            ),
            ("auth", "-"),
            ("verb", "GET"),
            ("httpversion", "1.0"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), Value::String(v.to_string())))
        .collect::<HashMap<String, Value>>();

        {
            // pattern start with Name
            let pattern_str = "%{clientip} %{ident} %{auth} [%{timestamp}] \"%{verb} %{request} HTTP/%{httpversion}\" %{status} %{size}";
            let input = "1.2.3.4 - - [30/Apr/1998:22:00:52 +0000] \"GET /english/venues/cities/images/montpellier/18.gif HTTP/1.0\" 200 3171";

            assert(pattern_str, input, expected.clone());
        }

        {
            // pattern start with Split
            let pattern_str = " %{clientip} %{ident} %{auth} [%{timestamp}] \"%{verb} %{request} HTTP/%{httpversion}\" %{status} %{size}";
            let input = " 1.2.3.4 - - [30/Apr/1998:22:00:52 +0000] \"GET /english/venues/cities/images/montpellier/18.gif HTTP/1.0\" 200 3171";

            assert(pattern_str, input, expected);
        }
    }

    #[test]
    fn test_dissect_right_padding_modifier() {
        let cases = [
            (
                "%{ts->} %{level}",
                "1998-08-10T17:15:42,466          WARN",
                [("ts", "1998-08-10T17:15:42,466"), ("level", "WARN")],
            ),
            (
                "[%{ts}]%{->}[%{level}]",
                "[1998-08-10T17:15:42,466]            [WARN]",
                [("ts", "1998-08-10T17:15:42,466"), ("level", "WARN")],
            ),
            (
                "[%{ts}]%{->}[%{level}]",
                "[1998-08-10T17:15:42,466]            [[[[WARN]",
                [("ts", "1998-08-10T17:15:42,466"), ("level", "WARN")],
            ),
        ]
        .into_iter()
        .map(|(pattern, input, expected)| {
            let map = expected
                .into_iter()
                .map(|(k, v)| (k.to_string(), Value::String(v.to_string())));
            (pattern, input, map)
        });

        for (pattern_str, input, expected) in cases {
            assert(
                pattern_str,
                input,
                expected.collect::<HashMap<String, Value>>(),
            );
        }
    }

    #[test]
    fn test_dissect_append_modifier() {
        let cases = [
            (
                "%{+name} %{+name} %{+name} %{+name}",
                "john jacob jingleheimer schmidt",
                [("name", "john jacob jingleheimer schmidt")],
            ),
            (
                "%{+name/2} %{+name/4} %{+name/3} %{+name/1}",
                "john jacob jingleheimer schmidt",
                [("name", "schmidt john jingleheimer jacob")],
            ),
        ]
        .into_iter()
        .map(|(pattern, input, expected)| {
            let map = expected
                .into_iter()
                .map(|(k, v)| (k.to_string(), Value::String(v.to_string())));
            (pattern, input, map)
        });

        for (pattern_str, input, expected) in cases {
            assert(
                pattern_str,
                input,
                expected.collect::<HashMap<String, Value>>(),
            );
        }
    }

    #[test]
    fn test_dissect_named_skip_modifier() {
        let cases = [(
            "%{clientip} %{?ident} %{?auth} [%{timestamp}]",
            "1.2.3.4 - - [30/Apr/1998:22:00:52 +0000]",
            [
                ("clientip", "1.2.3.4"),
                ("timestamp", "30/Apr/1998:22:00:52 +0000"),
            ],
        )]
        .into_iter()
        .map(|(pattern, input, expected)| {
            let map = expected
                .into_iter()
                .map(|(k, v)| (k.to_string(), Value::String(v.to_string())));
            (pattern, input, map)
        });

        for (pattern_str, input, expected) in cases {
            assert(
                pattern_str,
                input,
                expected.collect::<HashMap<String, Value>>(),
            );
        }
    }

    #[test]
    fn test_dissect_reference_keys() {
        let cases = [
            (
                "[%{ts}] [%{level}] %{*p1}:%{&p1} %{*p2}:%{&p2}",
                "[2018-08-10T17:15:42,466] [ERR] ip:1.2.3.4 error:REFUSED",
                [
                    ("ts", "2018-08-10T17:15:42,466"),
                    ("level", "ERR"),
                    ("ip", "1.2.3.4"),
                    ("error", "REFUSED"),
                ],
            ),
            (
                "[%{ts}] [%{level}] %{&p1}:%{*p1} %{*p2}:%{&p2}",
                "[2018-08-10T17:15:42,466] [ERR] ip:1.2.3.4 error:REFUSED",
                [
                    ("ts", "2018-08-10T17:15:42,466"),
                    ("level", "ERR"),
                    ("1.2.3.4", "ip"),
                    ("error", "REFUSED"),
                ],
            ),
        ]
        .into_iter()
        .map(|(pattern, input, expected)| {
            let map = expected
                .into_iter()
                .map(|(k, v)| (k.to_string(), Value::String(v.to_string())));
            (pattern, input, map)
        });

        for (pattern_str, input, expected) in cases {
            assert(
                pattern_str,
                input,
                expected.collect::<HashMap<String, Value>>(),
            );
        }
    }

    #[test]
    fn test_abc() {
        let pattern_str = r#"%{ip_address} - - [%{timestamp}] "%{http_method} %{request_line}" %{status_code} %{response_size} "-" "%{user_agent}""#;
        let input = r#"127.0.0.1 - - [25/May/2024:20:16:37 +0000] "GET /index.html HTTP/1.1" 200 612 "-" "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36""#;
        let chs = input.chars().collect::<Vec<char>>();
        let pattern: Pattern = pattern_str.parse().unwrap();
        println!("{:?}", pattern);
        let processor = DissectProcessor::default();
        let map = processor.process_pattern(&chs, &pattern).unwrap();
        println!("{:?}", map);
    }
}
