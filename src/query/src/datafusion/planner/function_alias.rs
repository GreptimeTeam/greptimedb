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

use std::collections::HashMap;

use once_cell::sync::Lazy;

const SCALAR_ALIASES: &[(&str, &str)] = &[
    // SQL compat aliases.
    ("ucase", "upper"),
    ("lcase", "lower"),
    ("ceiling", "ceil"),
    ("mid", "substr"),
    // MySQL's RAND([seed]) accepts an optional seed argument, while DataFusion's `random()`
    // does not. We alias the name for `rand()` compatibility, and `rand(seed)` will error
    // due to mismatched arity.
    ("rand", "random"),
];

const AGGREGATE_ALIASES: &[(&str, &str)] = &[
    // MySQL compat aliases that don't override existing DataFusion aggregate names.
    //
    // NOTE: We intentionally do NOT alias `stddev` here, because DataFusion defines `stddev`
    // as sample standard deviation while MySQL's `STDDEV` is population standard deviation.
    ("std", "stddev_pop"),
    ("variance", "var_pop"),
];

static SCALAR_FUNCTION_ALIAS: Lazy<HashMap<&'static str, &'static str>> =
    Lazy::new(|| SCALAR_ALIASES.iter().copied().collect());

static AGGREGATE_FUNCTION_ALIAS: Lazy<HashMap<&'static str, &'static str>> =
    Lazy::new(|| AGGREGATE_ALIASES.iter().copied().collect());

pub fn resolve_scalar(name: &str) -> Option<&'static str> {
    let name = name.to_ascii_lowercase();
    SCALAR_FUNCTION_ALIAS.get(name.as_str()).copied()
}

pub fn resolve_aggregate(name: &str) -> Option<&'static str> {
    let name = name.to_ascii_lowercase();
    AGGREGATE_FUNCTION_ALIAS.get(name.as_str()).copied()
}

pub fn scalar_alias_names() -> impl Iterator<Item = &'static str> {
    SCALAR_ALIASES.iter().map(|(name, _)| *name)
}

pub fn aggregate_alias_names() -> impl Iterator<Item = &'static str> {
    AGGREGATE_ALIASES.iter().map(|(name, _)| *name)
}

#[cfg(test)]
mod tests {
    use super::{resolve_aggregate, resolve_scalar};

    #[test]
    fn resolves_scalar_aliases_case_insensitive() {
        assert_eq!(resolve_scalar("ucase"), Some("upper"));
        assert_eq!(resolve_scalar("UCASE"), Some("upper"));
        assert_eq!(resolve_scalar("lcase"), Some("lower"));
        assert_eq!(resolve_scalar("ceiling"), Some("ceil"));
        assert_eq!(resolve_scalar("MID"), Some("substr"));
        assert_eq!(resolve_scalar("RAND"), Some("random"));
        assert_eq!(resolve_scalar("not_a_real_alias"), None);
    }

    #[test]
    fn resolves_aggregate_aliases_case_insensitive() {
        assert_eq!(resolve_aggregate("std"), Some("stddev_pop"));
        assert_eq!(resolve_aggregate("variance"), Some("var_pop"));
        assert_eq!(resolve_aggregate("STDDEV"), None);
        assert_eq!(resolve_aggregate("not_a_real_alias"), None);
    }
}
