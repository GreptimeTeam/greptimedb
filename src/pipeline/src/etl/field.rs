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

        let mut set = std::collections::HashSet::new();
        for f in self.0.iter() {
            if set.contains(&f.field) {
                return Err(format!(
                    "field name must be unique, but got duplicated: {}",
                    f.field
                ));
            }
            set.insert(&f.field);
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

#[derive(Debug, Default, Clone)]
pub struct Field {
    pub field: String,

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
            field: field.into(),
            target_field: None,
            target_fields: None,
        }
    }

    // column_name in transform
    pub(crate) fn get_target_field(&self) -> &str {
        self.target_field.as_deref().unwrap_or(&self.field)
    }

    pub(crate) fn get_field(&self) -> &str {
        &self.field
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

        let target_field = match parts.next() {
            Some(s) if !s.trim().is_empty() => Some(s.trim().to_string()),
            _ => None,
        };

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
            field,
            target_field,
            target_fields,
        })
    }
}

impl std::fmt::Display for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match (&self.target_field, &self.target_fields) {
            (Some(target_field), None) => write!(f, "{}, {target_field}", self.field),
            (None, Some(target_fields)) => {
                write!(f, "{}, {}", self.field, target_fields.iter().join(","))
            }
            _ => write!(f, "{}", self.field),
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
            assert_eq!(f.get_field(), field, "{s}");
            assert_eq!(f.target_field, target_field, "{s}");
            assert_eq!(f.target_fields, target_fields, "{s}");
        }
    }
}
