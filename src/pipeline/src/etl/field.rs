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
use std::str::FromStr;

use super::processor::find_key_index;
enum IndexInfo {
    Index(usize),
    NotSet,
}

#[derive(Debug, Default, Clone)]
pub struct InputFieldInfo {
    pub(crate) name: String,
    pub(crate) index: usize,
}

struct InputFieldInfoBuilder {
    name: String,
}

impl InputFieldInfo {
    pub(crate) fn new(field: impl Into<String>, index: usize) -> Self {
        InputFieldInfo {
            name: field.into(),
            index,
        }
    }

    pub(crate) fn name(field: impl Into<String>) -> Self {
        InputFieldInfo {
            name: field.into(),
            index: 0,
        }
    }
}
#[derive(Debug, Default, Clone)]
pub struct OneInputOneOutPutField {
    input: InputFieldInfo,
    output: Option<(String, usize)>,
}

impl OneInputOneOutPutField {
    pub(crate) fn new(input: InputFieldInfo, output: (String, usize)) -> Self {
        OneInputOneOutPutField {
            input,
            output: Some(output),
        }
    }

    pub(crate) fn build(
        processor_kind: &str,
        intermediate_keys: &[String],
        input_field: &str,
        target_field: &str,
    ) -> Result<Self, String> {
        let input_index = find_key_index(intermediate_keys, input_field, processor_kind)?;

        let input_field_info = InputFieldInfo::new(input_field, input_index);
        let output_index = find_key_index(intermediate_keys, target_field, processor_kind)?;
        Ok(OneInputOneOutPutField::new(
            input_field_info,
            (target_field.to_string(), output_index),
        ))
    }

    pub(crate) fn input(&self) -> &InputFieldInfo {
        &self.input
    }

    pub(crate) fn input_index(&self) -> usize {
        self.input.index
    }

    pub(crate) fn input_name(&self) -> &str {
        &self.input.name
    }

    pub(crate) fn output_index(&self) -> usize {
        *self.output().1
    }

    pub(crate) fn output_name(&self) -> &str {
        self.output().0
    }

    pub(crate) fn output(&self) -> (&String, &usize) {
        if let Some((name, index)) = &self.output {
            (name, index)
        } else {
            (&self.input.name, &self.input.index)
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct OneInputMultiOutputField {
    input: InputFieldInfo,
    prefix: Option<String>,
}

impl OneInputMultiOutputField {
    pub(crate) fn new(input: InputFieldInfo, prefix: Option<String>) -> Self {
        OneInputMultiOutputField { input, prefix }
    }

    pub(crate) fn input(&self) -> &InputFieldInfo {
        &self.input
    }

    pub(crate) fn input_index(&self) -> usize {
        self.input.index
    }

    pub(crate) fn input_name(&self) -> &str {
        &self.input.name
    }

    pub(crate) fn target_prefix(&self) -> &str {
        self.prefix.as_deref().unwrap_or(&self.input.name)
    }
}

#[derive(Debug, Default, Clone)]
pub struct Field {
    pub(crate) input_field: String,
    pub(crate) target_field: Option<String>,
}

impl FromStr for Field {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split(',');
        let input_field = parts
            .next()
            .ok_or("input field is missing")?
            .trim()
            .to_string();
        let target_field = parts.next().map(|x| x.trim().to_string());

        if input_field.is_empty() {
            return Err("input field is empty".to_string());
        }

        Ok(Field {
            input_field,
            target_field,
        })
    }
}

impl Field {
    pub(crate) fn new(input_field: impl Into<String>, target_field: Option<String>) -> Self {
        Field {
            input_field: input_field.into(),
            target_field,
        }
    }

    pub(crate) fn input_field(&self) -> &str {
        &self.input_field
    }

    pub(crate) fn target_field(&self) -> Option<&str> {
        self.target_field.as_deref()
    }

    pub(crate) fn target_or_input_field(&self) -> &str {
        self.target_field.as_deref().unwrap_or(&self.input_field)
    }
}

#[derive(Debug, Default, Clone)]
pub struct Fields(Vec<Field>);

impl Fields {
    pub(crate) fn new(fields: Vec<Field>) -> Self {
        Fields(fields)
    }

    pub(crate) fn one(field: Field) -> Self {
        Fields(vec![field])
    }
}

impl Deref for Fields {
    type Target = Vec<Field>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl IntoIterator for Fields {
    type Item = Field;
    type IntoIter = std::vec::IntoIter<Field>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use crate::etl::field::Field;

    #[test]
    fn test_parse_field() {
        let field: Result<Field, _> = " ".parse();
        assert!(field.is_err());

        let field: Result<Field, _> = ",".parse();
        assert!(field.is_err());

        let field: Result<Field, _> = ",field".parse();
        assert!(field.is_err());

        let cases = [
            // ("field", "field", None, None),
            ("field, target_field", "field", Some("target_field")),
            ("field", "field", None),
        ];

        for (s, field, target_field) in cases.into_iter() {
            let f: Field = s.parse().unwrap();
            assert_eq!(f.input_field(), field, "{s}");
            assert_eq!(f.target_field(), target_field, "{s}");
        }
    }
}
