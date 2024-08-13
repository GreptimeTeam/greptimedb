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

use ahash::{HashSet, HashSetExt};
use itertools::Itertools;

#[derive(Debug, Default, Clone)]
pub struct Fields(Vec<Field>);

impl Fields {
    pub(crate) fn new(fields: Vec<Field>) -> Result<Self, String> {
        let ff = Fields(fields);
        ff.check()
    }

    pub(crate) fn one(field: Field) -> Self {
        Fields(vec![field])
    }

    pub(crate) fn get_target_fields(&self) -> Vec<&str> {
        self.0.iter().map(|f| f.get_target_field()).collect()
    }

    fn check(self) -> Result<Self, String> {
        if self.0.is_empty() {
            return Err("fields must not be empty".to_string());
        }

        let mut set = HashSet::new();
        for f in self.0.iter() {
            if set.contains(&f.input_field.name) {
                return Err(format!(
                    "field name must be unique, but got duplicated: {}",
                    f.input_field.name
                ));
            }
            set.insert(&f.input_field.name);
        }

        Ok(self)
    }
}

impl std::fmt::Display for Fields {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let s = self.0.iter().map(|f| f.to_string()).join(";");
        write!(f, "{s}")
    }
}

impl std::ops::Deref for Fields {
    type Target = Vec<Field>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Fields {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug, Default, Clone)]
pub struct InputFieldInfo {
    pub(crate) name: String,
    pub(crate) index: usize,
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

/// Used to represent the input and output fields of a processor or transform.
#[derive(Debug, Default, Clone)]
pub struct Field {
    /// The input field name and index.
    pub input_field: InputFieldInfo,

    /// The output field name and index mapping.
    pub output_fields_index_mapping: BTreeMap<String, usize>,

    // rename
    pub target_field: Option<String>,

    // 1-to-many mapping
    // processors:
    //  - csv
    pub target_fields: Option<Vec<String>>,
}

impl Field {
    pub(crate) fn new(field: impl Into<String>) -> Self {
        Field {
            input_field: InputFieldInfo::name(field.into()),
            output_fields_index_mapping: BTreeMap::new(),
            target_field: None,
            target_fields: None,
        }
    }

    /// target column_name in processor or transform
    /// if target_field is None, return input field name
    pub(crate) fn get_target_field(&self) -> &str {
        self.target_field
            .as_deref()
            .unwrap_or(&self.input_field.name)
    }

    /// input column_name in processor or transform
    pub(crate) fn get_field_name(&self) -> &str {
        &self.input_field.name
    }

    /// set input column index in processor or transform
    pub(crate) fn set_input_index(&mut self, index: usize) {
        self.input_field.index = index;
    }

    pub(crate) fn set_output_index(&mut self, key: &str, index: usize) {
        if let Some(v) = self.output_fields_index_mapping.get_mut(key) {
            *v = index;
        }
    }

    pub(crate) fn insert_output_index(&mut self, key: String, index: usize) {
        self.output_fields_index_mapping.insert(key, index);
    }
}

impl std::str::FromStr for Field {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split(',');
        let field = parts.next().ok_or("field is missing")?.trim().to_string();

        if field.is_empty() {
            return Err("field is empty".to_string());
        }

        let renamed_field = match parts.next() {
            Some(s) if !s.trim().is_empty() => Some(s.trim().to_string()),
            _ => None,
        };

        // TODO(qtang): ???? what's this?
        // weird design? field: <field>,<target_field>,<target_fields>,<target_fields>....
        // and only use in csv processor
        let fields: Vec<_> = parts
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        let target_fields = if fields.is_empty() {
            None
        } else {
            Some(fields)
        };

        Ok(Field {
            input_field: InputFieldInfo::name(field),
            output_fields_index_mapping: BTreeMap::new(),
            target_field: renamed_field,
            target_fields,
        })
    }
}

impl std::fmt::Display for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match (&self.target_field, &self.target_fields) {
            (Some(target_field), None) => write!(f, "{}, {target_field}", self.input_field.name),
            (None, Some(target_fields)) => {
                write!(
                    f,
                    "{}, {}",
                    self.input_field.name,
                    target_fields.iter().join(",")
                )
            }
            _ => write!(f, "{}", self.input_field.name),
        }
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
            (
                "field, target_field",
                "field",
                Some("target_field".into()),
                None,
            ),
            (
                "field, target_field1, target_field2, target_field3",
                "field",
                Some("target_field1".into()),
                Some(vec!["target_field2".into(), "target_field3".into()]),
            ),
            (
                "field,, target_field1, target_field2, target_field3",
                "field",
                None,
                Some(vec![
                    "target_field1".into(),
                    "target_field2".into(),
                    "target_field3".into(),
                ]),
            ),
        ];

        for (s, field, target_field, target_fields) in cases.into_iter() {
            let f: Field = s.parse().unwrap();
            assert_eq!(f.get_field_name(), field, "{s}");
            assert_eq!(f.target_field, target_field, "{s}");
            assert_eq!(f.target_fields, target_fields, "{s}");
        }
    }
}
