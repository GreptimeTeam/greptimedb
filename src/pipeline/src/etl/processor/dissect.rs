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

use std::ops::Deref;

use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use itertools::Itertools;

use crate::etl::field::{Fields, InputFieldInfo, OneInputMultiOutputField};
use crate::etl::find_key_index;
use crate::etl::processor::{
    yaml_bool, yaml_new_field, yaml_new_fields, yaml_parse_string, yaml_parse_strings, yaml_string,
    Processor, ProcessorBuilder, ProcessorKind, FIELDS_NAME, FIELD_NAME, IGNORE_MISSING_NAME,
    PATTERNS_NAME, PATTERN_NAME,
};
use crate::etl::value::Value;

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
struct NameInfo {
    name: String,
    start_modifier: Option<StartModifier>,
    end_modifier: Option<EndModifier>,
}

impl NameInfo {
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

impl std::fmt::Display for NameInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl From<&str> for NameInfo {
    fn from(value: &str) -> Self {
        NameInfo {
            name: value.to_string(),
            start_modifier: None,
            end_modifier: None,
        }
    }
}

#[derive(Debug, PartialEq, Default)]
struct Name {
    name: String,
    index: usize,
    start_modifier: Option<StartModifier>,
    end_modifier: Option<EndModifier>,
}

impl std::fmt::Display for Name {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl From<NameInfo> for Name {
    fn from(value: NameInfo) -> Self {
        Name {
            name: value.name,
            index: 0,
            start_modifier: value.start_modifier,
            end_modifier: value.end_modifier,
        }
    }
}

impl Name {
    fn is_name_empty(&self) -> bool {
        self.name.is_empty()
    }

    fn is_empty(&self) -> bool {
        self.name.is_empty() && self.start_modifier.is_none() && self.end_modifier.is_none()
    }

    fn is_end_modifier_set(&self) -> bool {
        self.end_modifier.is_some()
    }
}

#[derive(Debug, PartialEq)]
enum PartInfo {
    Split(String),
    Name(NameInfo),
}

impl PartInfo {
    fn is_empty(&self) -> bool {
        match self {
            PartInfo::Split(v) => v.is_empty(),
            PartInfo::Name(v) => v.is_empty(),
        }
    }

    fn empty_split() -> Self {
        PartInfo::Split(String::new())
    }

    fn empty_name() -> Self {
        PartInfo::Name(NameInfo::default())
    }

    fn push(&mut self, ch: char) {
        match self {
            PartInfo::Split(v) => v.push(ch),
            PartInfo::Name(v) => v.name.push(ch),
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
}

impl From<PartInfo> for Part {
    fn from(value: PartInfo) -> Self {
        match value {
            PartInfo::Split(v) => Part::Split(v),
            PartInfo::Name(v) => Part::Name(v.into()),
        }
    }
}

#[derive(Debug, Default)]
struct Pattern {
    origin: String,
    parts: Vec<Part>,
}

impl Deref for Pattern {
    type Target = Vec<Part>;

    fn deref(&self) -> &Self::Target {
        &self.parts
    }
}

impl From<PatternInfo> for Pattern {
    fn from(value: PatternInfo) -> Self {
        let parts = value.parts.into_iter().map(|x| x.into()).collect();
        Pattern {
            origin: value.origin,
            parts,
        }
    }
}

#[derive(Debug, Default)]
struct PatternInfo {
    origin: String,
    parts: Vec<PartInfo>,
}

impl std::ops::Deref for PatternInfo {
    type Target = Vec<PartInfo>;

    fn deref(&self) -> &Self::Target {
        &self.parts
    }
}

impl std::ops::DerefMut for PatternInfo {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.parts
    }
}

impl std::str::FromStr for PatternInfo {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = vec![];
        let mut cursor = PartInfo::empty_split();

        let origin = s.to_string();
        let chars: Vec<char> = origin.chars().collect();

        let mut pos = 0;
        while pos < chars.len() {
            let ch = chars[pos];
            match (ch, &mut cursor) {
                // if cursor is Split part, and found %{, then ready to start a Name part
                ('%', PartInfo::Split(_)) if matches!(chars.get(pos + 1), Some('{')) => {
                    if !cursor.is_empty() {
                        parts.push(cursor);
                    }

                    cursor = PartInfo::empty_name();
                    pos += 1; // skip '{'
                }
                // if cursor is Split part, and not found % or {, then continue the Split part
                (_, PartInfo::Split(_)) => {
                    cursor.push(ch);
                }
                // if cursor is Name part, and found }, then end the Name part, start the next Split part
                ('}', PartInfo::Name(_)) => {
                    parts.push(cursor);
                    cursor = PartInfo::empty_split();
                }
                ('+', PartInfo::Name(name)) if !name.is_start_modifier_set() => {
                    name.try_start_modifier(StartModifier::Append(None))?;
                }
                ('/', PartInfo::Name(name)) if name.is_append_modifier_set() => {
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
                ('?', PartInfo::Name(name)) if !name.is_start_modifier_set() => {
                    name.try_start_modifier(StartModifier::NamedSkip)?;
                }
                ('*', PartInfo::Name(name)) if !name.is_start_modifier_set() => {
                    name.try_start_modifier(StartModifier::MapKey)?;
                }
                ('&', PartInfo::Name(name)) if !name.is_start_modifier_set() => {
                    name.try_start_modifier(StartModifier::MapVal)?;
                }
                ('-', PartInfo::Name(name)) if !name.is_end_modifier_set() => {
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
                (_, PartInfo::Name(name)) if !is_valid_char(ch) => {
                    let tail: String = if name.is_name_empty() {
                        format!("Invalid '{ch}'")
                    } else {
                        format!("Invalid '{ch}' in '{name}'")
                    };
                    return Err(format!("Invalid Pattern: '{s}'. {tail}"));
                }
                (_, PartInfo::Name(_)) => {
                    cursor.push(ch);
                }
            }

            pos += 1;
        }

        match cursor {
            PartInfo::Split(ref split) if !split.is_empty() => parts.push(cursor),
            PartInfo::Name(name) if !name.is_empty() => {
                return Err(format!("Invalid Pattern: '{s}'. '{name}' is not closed"))
            }
            _ => {}
        }

        let pattern = Self { parts, origin };
        pattern.check()?;
        Ok(pattern)
    }
}

impl PatternInfo {
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
                (PartInfo::Split(split), _) if split.is_empty() => {
                    return Err(format!(
                        "Invalid Pattern: '{}'. Empty split is not allowed",
                        self.origin
                    ));
                }
                (PartInfo::Name(name1), Some(PartInfo::Name(name2))) => {
                    return Err(format!(
                        "Invalid Pattern: '{}'. consecutive names are not allowed: '{}' '{}'",
                        self.origin, name1, name2
                    ));
                }
                (PartInfo::Name(name), _) if name.is_name_empty() => {
                    if let Some(ref m) = name.start_modifier {
                        return Err(format!(
                            "Invalid Pattern: '{}'. only '{}' modifier is invalid",
                            self.origin, m
                        ));
                    }
                }
                (PartInfo::Name(name), _) => match name.start_modifier {
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

impl std::fmt::Display for PatternInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.origin)
    }
}

#[derive(Debug, Default)]
pub struct DissectProcessorBuilder {
    fields: Fields,
    patterns: Vec<PatternInfo>,
    ignore_missing: bool,
    append_separator: Option<String>,
    output_keys: HashSet<String>,
}

impl DissectProcessorBuilder {
    fn build_output_keys(patterns: &[PatternInfo]) -> HashSet<String> {
        patterns
            .iter()
            .flat_map(|pattern| pattern.iter())
            .filter_map(|p| match p {
                PartInfo::Name(name) => {
                    if !name.is_empty()
                        && (name.start_modifier.is_none()
                            || name
                                .start_modifier
                                .as_ref()
                                .is_some_and(|x| matches!(x, StartModifier::Append(_))))
                    {
                        Some(name.to_string())
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .collect()
    }

    fn part_info_to_part(
        part_info: PartInfo,
        intermediate_keys: &[String],
    ) -> Result<Part, String> {
        match part_info {
            PartInfo::Split(s) => Ok(Part::Split(s)),
            PartInfo::Name(n) => match n.start_modifier {
                None | Some(StartModifier::Append(_)) => {
                    let index = find_key_index(intermediate_keys, &n.name, "dissect")?;
                    Ok(Part::Name(Name {
                        name: n.name,
                        index,
                        start_modifier: n.start_modifier,
                        end_modifier: n.end_modifier,
                    }))
                }
                _ => Ok(Part::Name(Name {
                    name: n.name,
                    index: usize::MAX,
                    start_modifier: n.start_modifier,
                    end_modifier: n.end_modifier,
                })),
            },
        }
    }

    fn pattern_info_to_pattern(
        pattern_info: PatternInfo,
        intermediate_keys: &[String],
    ) -> Result<Pattern, String> {
        let original = pattern_info.origin;
        let pattern = pattern_info
            .parts
            .into_iter()
            .map(|part_info| Self::part_info_to_part(part_info, intermediate_keys))
            .collect::<Result<Vec<_>, String>>()?;
        Ok(Pattern {
            origin: original,
            parts: pattern,
        })
    }

    fn build_patterns_from_pattern_infos(
        patterns: Vec<PatternInfo>,
        intermediate_keys: &[String],
    ) -> Result<Vec<Pattern>, String> {
        patterns
            .into_iter()
            .map(|pattern_info| Self::pattern_info_to_pattern(pattern_info, intermediate_keys))
            .collect()
    }
}

impl ProcessorBuilder for DissectProcessorBuilder {
    fn output_keys(&self) -> HashSet<&str> {
        self.output_keys.iter().map(|s| s.as_str()).collect()
    }

    fn input_keys(&self) -> HashSet<&str> {
        self.fields.iter().map(|f| f.input_field()).collect()
    }

    fn build(self, intermediate_keys: &[String]) -> Result<ProcessorKind, String> {
        let mut real_fields = vec![];
        for field in self.fields.into_iter() {
            let input_index = find_key_index(intermediate_keys, field.input_field(), "dissect")?;

            let input_field_info = InputFieldInfo::new(field.input_field(), input_index);

            let real_field = OneInputMultiOutputField::new(input_field_info, field.target_field);
            real_fields.push(real_field);
        }
        let patterns = Self::build_patterns_from_pattern_infos(self.patterns, intermediate_keys)?;
        let processor = DissectProcessor {
            fields: real_fields,
            patterns,
            ignore_missing: self.ignore_missing,
            append_separator: self.append_separator,
        };
        Ok(ProcessorKind::Dissect(processor))
    }
}

#[derive(Debug, Default)]
pub struct DissectProcessor {
    fields: Vec<OneInputMultiOutputField>,
    patterns: Vec<Pattern>,
    ignore_missing: bool,

    // The character(s) that separate the appended fields. Default is an empty string.
    append_separator: Option<String>,
}

impl DissectProcessor {
    fn process_pattern(
        &self,
        chs: &[char],
        pattern: &Pattern,
    ) -> Result<Vec<(usize, Value)>, String> {
        let mut map = Vec::new();
        let mut pos = 0;

        let mut appends: HashMap<usize, Vec<(String, u32)>> = HashMap::new();
        // let mut maps: HashMap<usize, (String,String)> = HashMap::new();

        let mut process_name_value = |name: &Name, value: String| {
            let name_index = name.index;
            match name.start_modifier {
                Some(StartModifier::NamedSkip) => {
                    // do nothing, ignore this match
                }
                Some(StartModifier::Append(order)) => {
                    appends
                        .entry(name_index)
                        .or_default()
                        .push((value, order.unwrap_or_default()));
                }
                // Some(StartModifier::MapKey) => match maps.get(&name_index) {
                //     Some(map_val) => {
                //         map.insert(value, Value::String(map_val.to_string()));
                //     }
                //     None => {
                //         maps.insert(name_index, value);
                //     }
                // },
                // Some(StartModifier::MapVal) => match maps.get(&name_index) {
                //     Some(map_key) => {
                //         map.insert(map_key, Value::String(value));
                //     }
                //     None => {
                //         maps.insert(name_index, value);
                //     }
                // },
                Some(_) => {
                    // do nothing, ignore MapKey and MapVal
                    // because transform can know the key name
                }
                None => {
                    map.push((name_index, Value::String(value)));
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
                map.push((name, Value::String(value)));
            }
        }

        Ok(map)
    }

    fn process(&self, val: &str) -> Result<Vec<(usize, Value)>, String> {
        let chs = val.chars().collect::<Vec<char>>();

        for pattern in &self.patterns {
            if let Ok(map) = self.process_pattern(&chs, pattern) {
                return Ok(map);
            }
        }

        Err("No matching pattern found".to_string())
    }
}

impl TryFrom<&yaml_rust::yaml::Hash> for DissectProcessorBuilder {
    type Error = String;

    fn try_from(value: &yaml_rust::yaml::Hash) -> Result<Self, Self::Error> {
        let mut fields = Fields::default();
        let mut patterns = vec![];
        let mut ignore_missing = false;
        let mut append_separator = None;

        for (k, v) in value.iter() {
            let key = k
                .as_str()
                .ok_or(format!("key must be a string, but got '{k:?}'"))?;

            match key {
                FIELD_NAME => {
                    fields = Fields::one(yaml_new_field(v, FIELD_NAME)?);
                }
                FIELDS_NAME => {
                    fields = yaml_new_fields(v, FIELDS_NAME)?;
                }
                PATTERN_NAME => {
                    let pattern: PatternInfo = yaml_parse_string(v, PATTERN_NAME)?;
                    patterns = vec![pattern];
                }
                PATTERNS_NAME => {
                    patterns = yaml_parse_strings(v, PATTERNS_NAME)?;
                }
                IGNORE_MISSING_NAME => {
                    ignore_missing = yaml_bool(v, IGNORE_MISSING_NAME)?;
                }
                APPEND_SEPARATOR_NAME => {
                    append_separator = Some(yaml_string(v, APPEND_SEPARATOR_NAME)?);
                }
                _ => {}
            }
        }
        let output_keys = Self::build_output_keys(&patterns);
        let builder = DissectProcessorBuilder {
            fields,
            patterns,
            ignore_missing,
            append_separator,
            output_keys,
        };

        Ok(builder)
    }
}

impl Processor for DissectProcessor {
    fn kind(&self) -> &str {
        PROCESSOR_DISSECT
    }

    fn ignore_missing(&self) -> bool {
        self.ignore_missing
    }

    fn exec_mut(&self, val: &mut Vec<Value>) -> Result<(), String> {
        for field in self.fields.iter() {
            let index = field.input_index();
            match val.get(index) {
                Some(Value::String(val_str)) => {
                    let r = self.process(val_str)?;
                    for (k, v) in r {
                        val[k] = v;
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

    use super::{DissectProcessor, EndModifier, NameInfo, PartInfo, PatternInfo, StartModifier};
    use crate::etl::processor::dissect::DissectProcessorBuilder;
    use crate::etl::value::Value;

    fn assert(pattern_str: &str, input: &str, expected: HashMap<String, Value>) {
        let chs = input.chars().collect::<Vec<char>>();
        let pattern_infos: Vec<PatternInfo> = vec![pattern_str.parse().unwrap()];
        let output_keys: Vec<String> = DissectProcessorBuilder::build_output_keys(&pattern_infos)
            .into_iter()
            .collect();
        let pattern =
            DissectProcessorBuilder::build_patterns_from_pattern_infos(pattern_infos, &output_keys)
                .unwrap();

        let processor = DissectProcessor::default();
        let result: HashMap<String, Value> = processor
            .process_pattern(&chs, &pattern[0])
            .unwrap()
            .into_iter()
            .map(|(k, v)| (output_keys[k].to_string(), v))
            .collect();

        assert_eq!(result, expected, "pattern: {}", pattern_str);
    }

    #[test]
    fn test_dissect_simple_pattern() {
        let cases = [(
            "%{clientip} %{ident} %{auth} [%{timestamp}] \"%{verb} %{request} HTTP/%{httpversion}\" %{status} %{size}",
            vec![
                PartInfo::Name("clientip".into()),
                PartInfo::Split(" ".into()),
                PartInfo::Name("ident".into()),
                PartInfo::Split(" ".into()),
                PartInfo::Name("auth".into()),
                PartInfo::Split(" [".into()),
                PartInfo::Name("timestamp".into()),
                PartInfo::Split("] \"".into()),
                PartInfo::Name("verb".into()),
                PartInfo::Split(" ".into()),
                PartInfo::Name("request".into()),
                PartInfo::Split(" HTTP/".into()),
                PartInfo::Name("httpversion".into()),
                PartInfo::Split("\" ".into()),
                PartInfo::Name("status".into()),
                PartInfo::Split(" ".into()),
                PartInfo::Name("size".into()),
            ],
        )];

        for (pattern, expected) in cases.into_iter() {
            let p: PatternInfo = pattern.parse().unwrap();
            assert_eq!(p.parts, expected);
        }
    }

    #[test]
    fn test_dissect_modifier_pattern() {
        let cases = [
            (
                "%{} %{}",
                vec![
                    PartInfo::Name(NameInfo {
                        name: "".into(),
                        start_modifier: None,
                        end_modifier: None,
                    }),
                    PartInfo::Split(" ".into()),
                    PartInfo::Name(NameInfo {
                        name: "".into(),
                        start_modifier: None,
                        end_modifier: None,
                    }),
                ],
            ),
            (
                "%{ts->} %{level}",
                vec![
                    PartInfo::Name(NameInfo {
                        name: "ts".into(),
                        start_modifier: None,
                        end_modifier: Some(EndModifier),
                    }),
                    PartInfo::Split(" ".into()),
                    PartInfo::Name("level".into()),
                ],
            ),
            (
                "[%{ts}]%{->}[%{level}]",
                vec![
                    PartInfo::Split("[".into()),
                    PartInfo::Name(NameInfo {
                        name: "ts".into(),
                        start_modifier: None,
                        end_modifier: None,
                    }),
                    PartInfo::Split("]".into()),
                    PartInfo::Name(NameInfo {
                        name: "".into(),
                        start_modifier: None,
                        end_modifier: Some(EndModifier),
                    }),
                    PartInfo::Split("[".into()),
                    PartInfo::Name(NameInfo {
                        name: "level".into(),
                        start_modifier: None,
                        end_modifier: None,
                    }),
                    PartInfo::Split("]".into()),
                ],
            ),
            (
                "%{+name} %{+name} %{+name} %{+name}",
                vec![
                    PartInfo::Name(NameInfo {
                        name: "name".into(),
                        start_modifier: Some(StartModifier::Append(None)),
                        end_modifier: None,
                    }),
                    PartInfo::Split(" ".into()),
                    PartInfo::Name(NameInfo {
                        name: "name".into(),
                        start_modifier: Some(StartModifier::Append(None)),
                        end_modifier: None,
                    }),
                    PartInfo::Split(" ".into()),
                    PartInfo::Name(NameInfo {
                        name: "name".into(),
                        start_modifier: Some(StartModifier::Append(None)),
                        end_modifier: None,
                    }),
                    PartInfo::Split(" ".into()),
                    PartInfo::Name(NameInfo {
                        name: "name".into(),
                        start_modifier: Some(StartModifier::Append(None)),
                        end_modifier: None,
                    }),
                ],
            ),
            (
                "%{+name/2} %{+name/4} %{+name/3} %{+name/1}",
                vec![
                    PartInfo::Name(NameInfo {
                        name: "name".into(),
                        start_modifier: Some(StartModifier::Append(Some(2))),
                        end_modifier: None,
                    }),
                    PartInfo::Split(" ".into()),
                    PartInfo::Name(NameInfo {
                        name: "name".into(),
                        start_modifier: Some(StartModifier::Append(Some(4))),
                        end_modifier: None,
                    }),
                    PartInfo::Split(" ".into()),
                    PartInfo::Name(NameInfo {
                        name: "name".into(),
                        start_modifier: Some(StartModifier::Append(Some(3))),
                        end_modifier: None,
                    }),
                    PartInfo::Split(" ".into()),
                    PartInfo::Name(NameInfo {
                        name: "name".into(),
                        start_modifier: Some(StartModifier::Append(Some(1))),
                        end_modifier: None,
                    }),
                ],
            ),
            (
                "%{clientip} %{?ident} %{?auth} [%{timestamp}]",
                vec![
                    PartInfo::Name(NameInfo {
                        name: "clientip".into(),
                        start_modifier: None,
                        end_modifier: None,
                    }),
                    PartInfo::Split(" ".into()),
                    PartInfo::Name(NameInfo {
                        name: "ident".into(),
                        start_modifier: Some(StartModifier::NamedSkip),
                        end_modifier: None,
                    }),
                    PartInfo::Split(" ".into()),
                    PartInfo::Name(NameInfo {
                        name: "auth".into(),
                        start_modifier: Some(StartModifier::NamedSkip),
                        end_modifier: None,
                    }),
                    PartInfo::Split(" [".into()),
                    PartInfo::Name(NameInfo {
                        name: "timestamp".into(),
                        start_modifier: None,
                        end_modifier: None,
                    }),
                    PartInfo::Split("]".into()),
                ],
            ),
            (
                "[%{ts}] [%{level}] %{*p1}:%{&p1} %{*p2}:%{&p2}",
                vec![
                    PartInfo::Split("[".into()),
                    PartInfo::Name(NameInfo {
                        name: "ts".into(),
                        start_modifier: None,
                        end_modifier: None,
                    }),
                    PartInfo::Split("] [".into()),
                    PartInfo::Name(NameInfo {
                        name: "level".into(),
                        start_modifier: None,
                        end_modifier: None,
                    }),
                    PartInfo::Split("] ".into()),
                    PartInfo::Name(NameInfo {
                        name: "p1".into(),
                        start_modifier: Some(StartModifier::MapKey),
                        end_modifier: None,
                    }),
                    PartInfo::Split(":".into()),
                    PartInfo::Name(NameInfo {
                        name: "p1".into(),
                        start_modifier: Some(StartModifier::MapVal),
                        end_modifier: None,
                    }),
                    PartInfo::Split(" ".into()),
                    PartInfo::Name(NameInfo {
                        name: "p2".into(),
                        start_modifier: Some(StartModifier::MapKey),
                        end_modifier: None,
                    }),
                    PartInfo::Split(":".into()),
                    PartInfo::Name(NameInfo {
                        name: "p2".into(),
                        start_modifier: Some(StartModifier::MapVal),
                        end_modifier: None,
                    }),
                ],
            ),
            (
                "%{&p1}:%{*p1}",
                vec![
                    PartInfo::Name(NameInfo {
                        name: "p1".into(),
                        start_modifier: Some(StartModifier::MapVal),
                        end_modifier: None,
                    }),
                    PartInfo::Split(":".into()),
                    PartInfo::Name(NameInfo {
                        name: "p1".into(),
                        start_modifier: Some(StartModifier::MapKey),
                        end_modifier: None,
                    }),
                ],
            ),
        ];

        for (pattern, expected) in cases.into_iter() {
            let p: PatternInfo = pattern.parse().unwrap();
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
            let err = pattern.parse::<PatternInfo>().unwrap_err();
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
}
