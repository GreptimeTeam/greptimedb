// Copyright 2024 Greptime Team
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

use common_telemetry::warn;

use crate::etl::field::{Field, Fields};
use crate::etl::processor::{
    yaml_bool, yaml_field, yaml_fields, yaml_parse_strings, yaml_string, Processor, FIELDS_NAME,
    FIELD_NAME, IGNORE_MISSING_NAME, PATTERNS_NAME,
};
use crate::etl::value::{Map, Value};

pub(crate) const PROCESSOR_DISSECT: &str = "dissect";

const APPEND_SEPARATOR_NAME: &str = "append_separator";

#[derive(Debug, PartialEq)]
enum Part {
    Split(String),
    Key(String),
}

impl Part {
    fn is_empty(&self) -> bool {
        match self {
            Part::Split(v) => v.is_empty(),
            Part::Key(v) => v.is_empty(),
        }
    }

    fn empty_split() -> Self {
        Part::Split(String::new())
    }

    fn empty_key() -> Self {
        Part::Key(String::new())
    }
}

impl std::ops::Deref for Part {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        match self {
            Part::Split(v) => v,
            Part::Key(v) => v,
        }
    }
}

impl std::ops::DerefMut for Part {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Part::Split(v) => v,
            Part::Key(v) => v,
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
        let mut last_ch = None;
        let chars: Vec<char> = origin.chars().collect();

        for i in 0..chars.len() {
            let ch = chars[i];
            match (ch, &mut cursor) {
                // if cursor is Split part, and found %{, then ready to start a Key part
                ('%', Part::Split(_)) if i + 1 < chars.len() && chars[i + 1] == '{' => {}
                // if cursor is Split part, and found %{, then end the Split part, start the Key part
                ('{', Part::Split(_)) if last_ch == Some('%') => {
                    if !cursor.is_empty() {
                        parts.push(cursor);
                    }

                    cursor = Part::empty_key();
                }
                // if cursor is Split part, and not found % or {, then continue the Split part
                (_, Part::Split(_)) => {
                    cursor.push(ch);
                }
                // if cursor is Key part, and found }, then end the Key part, start the next Split part
                ('}', Part::Key(_)) => {
                    parts.push(cursor);
                    cursor = Part::empty_split();
                }
                (_, Part::Key(_)) if !is_valid_char(ch) => {
                    return Err(format!("Invalid character in key: '{ch}'"));
                }
                (_, Part::Key(_)) => {
                    cursor.push(ch);
                }
            }

            last_ch = Some(ch);
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

        for i in 0..self.len() {
            let this_part = &self[i];
            let next_part = self.get(i + 1);
            match (this_part, next_part) {
                (Part::Split(split), _) if split.is_empty() => {
                    return Err("Empty split is not allowed".to_string());
                }
                (Part::Key(key1), Some(Part::Key(key2))) => {
                    return Err(format!(
                        "consecutive keys are not allowed: '{key1}' '{key2}'"
                    ));
                }
                _ => {}
            }
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

    fn process_pattern(chs: &[char], pattern: &Pattern) -> Result<Map, String> {
        let mut map = Map::default();
        let mut pos = 0;

        for i in 0..pattern.len() {
            let this_part = &pattern[i];
            let next_part = pattern.get(i + 1);
            match (this_part, next_part) {
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
                (Part::Key(key), None) => {
                    let value = chs[pos..].iter().collect::<String>();
                    map.insert(key.clone(), Value::String(value));
                }

                (Part::Key(key), Some(Part::Split(split))) => match split.chars().next() {
                    None => return Err("Empty split is not allowed".to_string()),
                    Some(stop) => {
                        let mut end = pos;
                        while end < chs.len() && chs[end] != stop {
                            end += 1;
                        }

                        if end == chs.len() {
                            return Err("No matching split found".to_string());
                        }

                        let value = chs[pos..end].iter().collect::<String>();
                        map.insert(key.clone(), Value::String(value));
                        pos = end;
                    }
                },
                (Part::Key(key1), Some(Part::Key(key2))) => {
                    return Err(format!(
                        "consecutive keys are not allowed: '{key1}' '{key2}'"
                    ));
                }
            }
        }

        Ok(map)
    }

    fn process(&self, val: &str) -> Result<Map, String> {
        let chs = val.chars().collect::<Vec<char>>();

        for pattern in &self.patterns {
            if let Ok(map) = DissectProcessor::process_pattern(&chs, pattern) {
                return Ok(map);
            }
        }

        Err("No matching pattern found".to_string())
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
}

fn is_valid_char(ch: char) -> bool {
    ch.is_alphanumeric() || ch == '_'
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{DissectProcessor, Part, Pattern};
    use crate::etl::value::{Map, Value};

    #[test]
    fn test_pattern() {
        let cases = [(
            "%{clientip} %{ident} %{auth} [%{timestamp}] \"%{verb} %{request} HTTP/%{httpversion}\" %{status} %{size}",
            vec![
                Part::Key("clientip".chars().collect()),
                Part::Split(" ".chars().collect()),
                Part::Key("ident".chars().collect()),
                Part::Split(" ".chars().collect()),
                Part::Key("auth".chars().collect()),
                Part::Split(" [".chars().collect()),
                Part::Key("timestamp".chars().collect()),
                Part::Split("] \"".chars().collect()),
                Part::Key("verb".chars().collect()),
                Part::Split(" ".chars().collect()),
                Part::Key("request".chars().collect()),
                Part::Split(" HTTP/".chars().collect()),
                Part::Key("httpversion".chars().collect()),
                Part::Split("\" ".chars().collect()),
                Part::Key("status".chars().collect()),
                Part::Split(" ".chars().collect()),
                Part::Key("size".chars().collect()),
            ],
        )];

        for (pattern, expected) in cases.into_iter() {
            let p: Pattern = pattern.parse().unwrap();
            assert_eq!(p.parts, expected);
        }
    }

    #[test]
    fn test_process() {
        let assert = |pattern_str: &str, input: &str, expected: HashMap<String, Value>| {
            let chs = input.chars().collect::<Vec<char>>();
            let pattern = pattern_str.parse().unwrap();
            let map = DissectProcessor::process_pattern(&chs, &pattern).unwrap();

            assert_eq!(map, Map::from(expected));
        };

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
        .collect::<std::collections::HashMap<String, Value>>();

        {
            // pattern start with Key
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
}
