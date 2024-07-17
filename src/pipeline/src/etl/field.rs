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
        self.0.iter().map(|f| f.get_renamed_field()).collect()
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
pub struct FieldInfo {
    pub(crate) name: String,
    pub(crate) index: usize,
}

impl FieldInfo {
    pub(crate) fn new(field: impl Into<String>, index: usize) -> Self {
        FieldInfo {
            name: field.into(),
            index,
        }
    }

    pub(crate) fn name(field: impl Into<String>) -> Self {
        FieldInfo {
            name: field.into(),
            index: 0,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct Field {
    pub input_field: FieldInfo,

    pub output_fields: BTreeMap<String, usize>,

    // rename
    pub renamed_field: Option<String>,

    // 1-to-many mapping
    // processors:
    //  - csv
    pub target_fields: Option<Vec<String>>,
}

impl Field {
    pub(crate) fn new(field: impl Into<String>) -> Self {
        Field {
            input_field: FieldInfo::name(field.into()),
            output_fields: BTreeMap::new(),
            renamed_field: None,
            target_fields: None,
        }
    }

    // column_name in transform
    pub(crate) fn get_renamed_field(&self) -> &str {
        self.renamed_field
            .as_deref()
            .unwrap_or(&self.input_field.name)
    }

    pub(crate) fn get_field_name(&self) -> &str {
        &self.input_field.name
    }

    pub(crate) fn set_input_index(&mut self, index: usize) {
        self.input_field.index = index;
    }

    pub(crate) fn set_output_index(&mut self, key: &str, index: usize) {
        self.output_fields.get_mut(key).map(|v| *v = index);
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
        let fields: Vec<_> = parts
            .filter(|s| !s.trim().is_empty())
            .map(|s| s.trim().to_string())
            .collect();
        let target_fields = if fields.is_empty() {
            None
        } else {
            Some(fields)
        };

        Ok(Field {
            input_field: FieldInfo::name(field),
            output_fields: BTreeMap::new(),
            renamed_field,
            target_fields,
        })
    }
}

impl std::fmt::Display for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match (&self.renamed_field, &self.target_fields) {
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
            assert_eq!(f.renamed_field, target_field, "{s}");
            assert_eq!(f.target_fields, target_fields, "{s}");
        }
    }
}
