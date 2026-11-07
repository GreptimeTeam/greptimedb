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

//! Shared registration, parsing and application of public hints.

use std::collections::HashMap;

use crate::session_config::{Error, InvalidConfigValueSnafu};

/// Public value domains used by validation and discovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HintValueType {
    Boolean,
    Integer,
    Number,
    String,
    Enum(&'static [&'static str]),
}

impl HintValueType {
    /// Returns the scalar name used in discovery responses.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Boolean => "boolean",
            Self::Integer => "integer",
            Self::Number => "number",
            Self::String | Self::Enum(_) => "string",
        }
    }

    /// Returns the complete finite domain, if there is one.
    pub const fn allowed_values(self) -> Option<&'static [&'static str]> {
        match self {
            Self::Boolean => Some(&["true", "false"]),
            Self::Enum(values) => Some(values),
            _ => None,
        }
    }
}

/// Metadata shared by every adapter of a registered hint.
#[derive(Debug, Clone)]
pub struct HintMetadata {
    pub name: String,
    pub aliases: Vec<&'static str>,
    pub value_type: HintValueType,
    pub description: String,
    pub deprecated: Option<&'static str>,
    pub discoverable: bool,
    /// Matches concrete names in an open namespace. The display name is not
    /// itself accepted as an input key when this matcher is present.
    pub matches_pattern: Option<fn(&str) -> bool>,
}

impl HintMetadata {
    /// Creates metadata for an exact-name, publicly discoverable hint.
    pub fn new(
        name: impl Into<String>,
        value_type: HintValueType,
        description: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            aliases: Vec::new(),
            value_type,
            description: description.into(),
            deprecated: None,
            discoverable: true,
            matches_pattern: None,
        }
    }
}

/// Resolved omitted-hint behavior, independent of the discovery request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HintDefault {
    pub value: Option<String>,
    pub description: Option<&'static str>,
}

/// Purpose of a hint, shared by registration and discovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HintCategory {
    Query,
    Optimizer,
    Ingestion,
}

impl HintCategory {
    /// Returns the category's discovery name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Optimizer => "optimizer",
            Self::Ingestion => "ingestion",
        }
    }

    /// Describes the category's scope.
    pub const fn description(self) -> &'static str {
        match self {
            Self::Query => "Query behavior",
            Self::Optimizer => "Query optimizer options",
            Self::Ingestion => "Write requests and automatic table creation",
        }
    }
}

/// One hint's metadata and owner-supplied behavior. Parsing validates before
/// application, which must not fail or depend on a particular transport.
pub struct HintDefinition<T, D, P = ()> {
    pub metadata: HintMetadata,
    pub category: HintCategory,
    pub parse: fn(&str, &str, P) -> Result<String, Error>,
    pub apply: fn(&mut T, &str, String),
    pub default: fn(&D, &str) -> HintDefault,
    /// Rejects unequal normalized values for the same canonical name. Otherwise
    /// every occurrence is validated and applied in input order: the last wins.
    pub reject_conflicts: bool,
}

/// A normalized value and its registered application hook.
#[derive(Debug)]
pub struct ParsedHint<T> {
    name: String,
    value: String,
    category: HintCategory,
    apply: fn(&mut T, &str, String),
}

impl<T> Clone for ParsedHint<T> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            value: self.value.clone(),
            category: self.category,
            apply: self.apply,
        }
    }
}

impl<T> ParsedHint<T> {
    /// Returns the canonical name, or the concrete key for a pattern hint.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the validated representation.
    pub fn value(&self) -> &str {
        &self.value
    }

    /// Returns the registered category.
    pub fn category(&self) -> HintCategory {
        self.category
    }

    /// Applies this value using its owner's hook.
    pub fn apply(self, target: &mut T) {
        (self.apply)(target, &self.name, self.value);
    }
}

/// Validated hints in registration order, retaining original input positions.
pub struct ParsedBatch<T> {
    parsed: Vec<(usize, ParsedHint<T>)>,
    unmatched: Vec<(usize, (String, String))>,
}

impl<T> ParsedBatch<T> {
    /// Restores normalized and unrelated inputs to their original order.
    pub fn into_normalized(self) -> Vec<(String, String)> {
        let mut entries = self.unmatched;
        entries.extend(
            self.parsed
                .into_iter()
                .map(|(index, hint)| (index, (hint.name, hint.value))),
        );
        entries.sort_by_key(|(index, _)| *index);
        entries.into_iter().map(|(_, entry)| entry).collect()
    }

    /// Separates registered applications from inputs handled by the adapter.
    pub fn into_parts(self) -> (Vec<ParsedHint<T>>, Vec<(String, String)>) {
        (
            self.parsed.into_iter().map(|(_, hint)| hint).collect(),
            self.unmatched.into_iter().map(|(_, entry)| entry).collect(),
        )
    }
}

/// A registry whose definition order determines validation and application order.
pub struct HintRegistry<T, D, P = ()> {
    definitions: Vec<HintDefinition<T, D, P>>,
}

impl<T, D, P: Copy> HintRegistry<T, D, P> {
    /// Registers owner-provided definitions in execution order.
    pub fn new(definitions: Vec<HintDefinition<T, D, P>>) -> Self {
        Self { definitions }
    }

    /// Returns the registrations used by parsing and discovery.
    pub fn definitions(&self) -> &[HintDefinition<T, D, P>] {
        &self.definitions
    }

    /// Checks exact names and aliases for ambiguous registrations.
    pub fn validate_definitions(&self) -> Result<(), Error> {
        let mut names: Vec<&str> = Vec::new();
        for definition in &self.definitions {
            let metadata = &definition.metadata;
            if metadata.matches_pattern.is_some() && !metadata.aliases.is_empty() {
                return InvalidConfigValueSnafu {
                    name: &metadata.name,
                    value: &metadata.name,
                    hint: "Pattern registrations cannot declare exact-name aliases",
                }
                .fail();
            }
            for name in
                std::iter::once(metadata.name.as_str()).chain(metadata.aliases.iter().copied())
            {
                if names
                    .iter()
                    .any(|previous| previous.eq_ignore_ascii_case(name))
                {
                    return InvalidConfigValueSnafu {
                        name,
                        value: name,
                        hint: "Ambiguous registered hint name or alias",
                    }
                    .fail();
                }
                names.push(name);
            }
        }
        Ok(())
    }

    /// Finds an exact name or alias before trying registered patterns.
    pub fn find(&self, name: &str) -> Option<&HintDefinition<T, D, P>> {
        self.find_index(name).map(|index| &self.definitions[index])
    }

    fn find_index(&self, name: &str) -> Option<usize> {
        self.definitions
            .iter()
            .position(|definition| {
                let metadata = &definition.metadata;
                metadata.matches_pattern.is_none()
                    && std::iter::once(metadata.name.as_str())
                        .chain(metadata.aliases.iter().copied())
                        .any(|registered| registered.eq_ignore_ascii_case(name))
            })
            .or_else(|| {
                self.definitions.iter().position(|definition| {
                    definition
                        .metadata
                        .matches_pattern
                        .is_some_and(|matches| matches(name))
                })
            })
    }

    /// Parses a known hint; unknown names remain the adapter's concern.
    pub fn parse(
        &self,
        name: &str,
        value: &str,
        options: P,
    ) -> Result<Option<ParsedHint<T>>, Error> {
        self.find(name)
            .map(|definition| Self::parse_definition(definition, name, value, options))
            .transpose()
    }

    /// Validates in registration order, preserving input order for occurrences
    /// of each definition. No application runs before the entire batch succeeds.
    pub fn parse_grouped(
        &self,
        hints: impl IntoIterator<Item = (String, String)>,
        options: P,
    ) -> Result<ParsedBatch<T>, Error> {
        let mut hints = hints.into_iter().enumerate().peekable();
        if hints.peek().is_none() {
            return Ok(ParsedBatch {
                parsed: Vec::new(),
                unmatched: Vec::new(),
            });
        }
        let mut groups: Vec<Vec<_>> = (0..self.definitions.len()).map(|_| Vec::new()).collect();
        let mut unmatched = Vec::new();
        for (position, (name, value)) in hints {
            match self.find_index(&name) {
                Some(index) => groups[index].push((position, name, value)),
                None => unmatched.push((position, (name, value))),
            }
        }
        let mut parsed = Vec::new();
        for (definition, inputs) in self.definitions.iter().zip(groups) {
            let mut seen = HashMap::new();
            for (position, name, value) in inputs {
                let hint = Self::parse_definition(definition, &name, &value, options)?;
                if definition.reject_conflicts
                    && seen
                        .insert(hint.name.clone(), hint.value.clone())
                        .is_some_and(|previous| previous != hint.value)
                {
                    return InvalidConfigValueSnafu {
                        name: hint.name,
                        value: hint.value,
                        hint: "Conflicting values for hint",
                    }
                    .fail();
                }
                parsed.push((position, hint));
            }
        }
        Ok(ParsedBatch { parsed, unmatched })
    }

    /// Validates a batch containing only registered hints.
    pub fn parse_batch(
        &self,
        hints: impl IntoIterator<Item = (String, String)>,
        options: P,
    ) -> Result<Vec<ParsedHint<T>>, Error> {
        let (parsed, unmatched) = self.parse_grouped(hints, options)?.into_parts();
        if let Some((name, value)) = unmatched.into_iter().next() {
            return InvalidConfigValueSnafu {
                name,
                value,
                hint: "Unknown hint in this registry",
            }
            .fail();
        }
        Ok(parsed)
    }

    /// Applies a fully validated batch.
    pub fn apply(&self, target: &mut T, hints: Vec<ParsedHint<T>>) {
        for hint in hints {
            hint.apply(target);
        }
    }

    fn parse_definition(
        definition: &HintDefinition<T, D, P>,
        name: &str,
        value: &str,
        options: P,
    ) -> Result<ParsedHint<T>, Error> {
        let canonical = if definition.metadata.matches_pattern.is_some() {
            name
        } else {
            &definition.metadata.name
        };
        Ok(ParsedHint {
            name: canonical.to_string(),
            value: (definition.parse)(name, value, options)?,
            category: definition.category,
            apply: definition.apply,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type Target = HashMap<String, String>;

    fn definition(name: &str, reject_conflicts: bool) -> HintDefinition<Target, ()> {
        let metadata = HintMetadata::new(name, HintValueType::Boolean, "A test feature");
        HintDefinition {
            metadata,
            category: HintCategory::Query,
            parse: |name, value, _| match value.to_ascii_lowercase().as_str() {
                "true" => Ok("true".to_string()),
                "false" => Ok("false".to_string()),
                _ => InvalidConfigValueSnafu {
                    name,
                    value,
                    hint: "Expected true or false",
                }
                .fail(),
            },
            apply: |target, name, value| {
                target.insert(name.to_string(), value);
            },
            default: |_, _| HintDefault {
                value: Some("false".to_string()),
                description: None,
            },
            reject_conflicts,
        }
    }

    fn inputs(entries: &[(&str, &str)]) -> Vec<(String, String)> {
        entries
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn test_aliases_categories_and_duplicate_policies() {
        for reject_conflicts in [false, true] {
            let mut definition = definition("enabled", reject_conflicts);
            definition.metadata.aliases.push("enable");
            let registry = HintRegistry::new(vec![definition]);
            registry.validate_definitions().unwrap();
            let parsed = registry
                .parse_batch(inputs(&[("ENABLE", "TRUE"), ("enabled", "true")]), ())
                .unwrap();
            assert!(parsed.iter().all(|hint| hint.name() == "enabled"
                && hint.value() == "true"
                && hint.category() == HintCategory::Query));
            for duplicate in ["ENABLE", "enabled"] {
                let result =
                    registry.parse_batch(inputs(&[("enabled", "true"), (duplicate, "false")]), ());
                if reject_conflicts {
                    assert!(
                        result
                            .unwrap_err()
                            .to_string()
                            .contains("Conflicting values")
                    );
                } else {
                    let mut target = Target::new();
                    registry.apply(&mut target, result.unwrap());
                    assert_eq!(
                        target,
                        Target::from([("enabled".to_string(), "false".to_string())])
                    );
                }
            }
            // An overwritten value must still be valid.
            assert!(
                registry
                    .parse_batch(inputs(&[("enable", "invalid"), ("enabled", "true")]), ())
                    .is_err()
            );
        }
    }

    #[test]
    fn test_registration_order_and_atomic_application() {
        let registry =
            HintRegistry::new(vec![definition("first", true), definition("second", false)]);
        let mut target = Target::new();
        let result =
            registry.parse_batch(inputs(&[("second", "invalid"), ("first", "invalid")]), ());
        assert!(
            matches!(result.unwrap_err(), Error::InvalidConfigValue { name, .. } if name == "first")
        );
        let result = registry.parse_batch(inputs(&[("first", "true"), ("second", "invalid")]), ());
        if let Ok(parsed) = result {
            registry.apply(&mut target, parsed);
        }
        assert!(target.is_empty());
        let parsed = registry
            .parse_batch(inputs(&[("second", "true"), ("first", "true")]), ())
            .unwrap();
        assert_eq!(
            parsed.iter().map(ParsedHint::name).collect::<Vec<_>>(),
            ["first", "second"]
        );
    }

    #[test]
    fn test_pattern_concrete_names_and_exact_priority() {
        let mut pattern = definition("feature.<name>", true);
        pattern.metadata.matches_pattern = Some(|name| {
            name.strip_prefix("feature.")
                .is_some_and(|suffix| !suffix.is_empty())
        });
        let mut exact = definition("feature.special", false);
        exact.category = HintCategory::Ingestion;
        let registry = HintRegistry::new(vec![pattern, exact]);
        assert_eq!(
            registry.find("feature.special").unwrap().category,
            HintCategory::Ingestion
        );
        let parsed = registry
            .parse_batch(
                inputs(&[("feature.one", "true"), ("feature.two", "false")]),
                (),
            )
            .unwrap();
        let mut target = Target::new();
        registry.apply(&mut target, parsed);
        assert_eq!(
            target,
            inputs(&[("feature.one", "true"), ("feature.two", "false")])
                .into_iter()
                .collect::<Target>()
        );
    }

    #[test]
    fn test_unmatched_inputs_and_normalized_order() {
        let registry =
            HintRegistry::new(vec![definition("first", true), definition("second", false)]);
        let batch = registry
            .parse_grouped(
                inputs(&[("custom", "raw"), ("SECOND", "TRUE"), ("FIRST", "FALSE")]),
                (),
            )
            .unwrap();
        assert_eq!(
            batch.into_normalized(),
            inputs(&[("custom", "raw"), ("second", "true"), ("first", "false")])
        );
        assert!(
            registry
                .parse_batch(inputs(&[("custom", "raw")]), ())
                .is_err()
        );
    }

    #[test]
    fn test_registration_conflicts() {
        let mut first = definition("first", true);
        first.metadata.aliases.push("SECOND");
        let registry = HintRegistry::new(vec![first, definition("second", false)]);
        assert!(registry.validate_definitions().is_err());
        let mut pattern = definition("feature.<name>", true);
        pattern.metadata.matches_pattern = Some(|name| name.starts_with("feature."));
        pattern.metadata.aliases.push("alias");
        assert!(
            HintRegistry::new(vec![pattern])
                .validate_definitions()
                .is_err()
        );
    }
}
