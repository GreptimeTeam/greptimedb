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

use std::ops::{Deref, DerefMut};
use std::str::FromStr;

use snafu::OptionExt;

use super::error::{EmptyInputFieldSnafu, MissingInputFieldSnafu};
use crate::etl::error::{Error, Result};

/// Raw processor-defined inputs and outputs
#[derive(Debug, Default, Clone)]
pub struct Field {
    input_field: String,
    target_field: Option<String>,
}

impl FromStr for Field {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let mut parts = s.split(',');
        let input_field = parts
            .next()
            .context(MissingInputFieldSnafu)?
            .trim()
            .to_string();
        let target_field = parts.next().map(|x| x.trim().to_string());

        if input_field.is_empty() {
            return EmptyInputFieldSnafu.fail();
        }

        Ok(Field {
            input_field,
            target_field,
        })
    }
}

impl Field {
    /// Create a new field with the given input and target fields.
    pub(crate) fn new(input_field: impl Into<String>, target_field: Option<String>) -> Self {
        Field {
            input_field: input_field.into(),
            target_field,
        }
    }

    /// Get the input field.
    pub(crate) fn input_field(&self) -> &str {
        &self.input_field
    }

    /// Get the target field.
    pub(crate) fn target_field(&self) -> Option<&str> {
        self.target_field.as_deref()
    }

    /// Get the target field or the input field if the target field is not set.
    pub(crate) fn target_or_input_field(&self) -> &str {
        self.target_field.as_deref().unwrap_or(&self.input_field)
    }

    pub(crate) fn set_target_field(&mut self, target_field: Option<String>) {
        self.target_field = target_field;
    }
}

/// A collection of fields.
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

impl DerefMut for Fields {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
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
