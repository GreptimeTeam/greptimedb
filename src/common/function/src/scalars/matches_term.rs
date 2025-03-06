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

use std::sync::Arc;
use std::{fmt, iter};

use common_query::error::{InvalidFuncArgsSnafu, Result};
use datafusion_expr::Volatility;
use datatypes::prelude::ConcreteDataType;
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::vectors::{BooleanVector, BooleanVectorBuilder, MutableVector, VectorRef};
use snafu::ensure;

use crate::function::{Function, FunctionContext};
use crate::function_registry::FunctionRegistry;

/// Exact term/phrase matching function for text columns.
///
/// This function checks if a text column contains exact term/phrase matches
/// with non-alphanumeric boundaries. Designed for:
/// - Whole-word matching (e.g. "cat" in "cat!" but not in "category")
/// - Phrase matching (e.g. "hello world" in "note:hello world!")
///
/// # Signature
/// `matches_term(text: String, term: String) -> Boolean`
///
/// # Arguments
/// * `text` - String column to search (VectorRef containing String elements)
/// * `term` - Search term/phrase (VectorRef containing String elements)
///
/// # Returns
/// BooleanVector where each element indicates if the corresponding text
/// contains an exact match of the term, following these rules:
/// 1. Exact substring match found
/// 2. Match boundaries are either:
///    - Start/end of text
///    - Non-alphanumeric characters
///
/// # Examples
/// ```
/// -- SQL examples --
/// -- Match phrase with space --
/// SELECT matches_term(column, 'hello world') FROM table;
/// -- Text: "warning:hello world!" => true
/// -- Text: "hello-world"          => false (hyphen instead of space)
/// -- Text: "hello world2023"      => false (ending with numbers)
///
/// -- Match multiple words with boundaries --
/// SELECT matches_term(column, 'critical error') FROM logs;
/// -- Match in: "ERROR:critical error!"
/// -- No match: "critical_errors"
pub(crate) struct MatchesTermFunction;

impl MatchesTermFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register(Arc::new(MatchesTermFunction));
    }
}

impl fmt::Display for MatchesTermFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MATCHES_TERM")
    }
}

impl Function for MatchesTermFunction {
    fn name(&self) -> &str {
        "matches_term"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::boolean_datatype())
    }

    fn signature(&self) -> common_query::prelude::Signature {
        common_query::prelude::Signature::exact(
            vec![
                ConcreteDataType::string_datatype(),
                ConcreteDataType::string_datatype(),
            ],
            Volatility::Immutable,
        )
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure!(
            columns.len() == 2,
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The length of the args is not correct, expect exactly 2, have: {}",
                    columns.len()
                ),
            }
        );

        let text_column = &columns[0];
        if text_column.is_empty() {
            return Ok(Arc::new(BooleanVector::from(Vec::<bool>::with_capacity(0))));
        }

        let term_column = &columns[1];
        let const_term = if term_column.is_const() {
            let term = term_column.get_ref(0).as_string().unwrap();
            match term {
                None => {
                    return Ok(Arc::new(BooleanVector::from_iter(
                        iter::repeat(None).take(text_column.len()),
                    )));
                }
                term => term,
            }
        } else {
            None
        };

        let len = text_column.len();
        let mut result = BooleanVectorBuilder::with_capacity(len);
        for i in 0..len {
            let text = text_column.get_ref(i).as_string().unwrap();
            let Some(text) = text else {
                result.push_null();
                continue;
            };

            let term = match const_term {
                Some(term) => term,
                None => match term_column.get_ref(i).as_string().unwrap() {
                    None => {
                        result.push_null();
                        continue;
                    }
                    Some(term) => term,
                },
            };

            let contains = contains_term(text, term);
            result.push(Some(contains));
        }

        Ok(result.to_vector())
    }
}

/// Checks if a text contains a term as a whole word/phrase with non-alphanumeric boundaries.
///
/// A term is considered matched when:
/// 1. The exact sequence appears in the text
/// 2. It is either:
///    - At the start/end of text with adjacent non-alphanumeric character
///    - Surrounded by non-alphanumeric characters
///
/// # Arguments
/// * `text` - The text to search in
/// * `term` - The term/phrase to search for
///
/// # Returns
/// * `true` if the term appears as a whole word/phrase, `false` otherwise
///
/// # Examples
/// ```
/// assert!(contains_term("cat!", "cat"));      // Term at end with punctuation
/// assert!(contains_term("dog,cat", "cat"));   // Term preceded by comma
/// assert!(!contains_term("category", "cat")); // Partial match rejected
/// assert!(contains_term("hello-world", "world")); // Hyphen boundary
/// ```
pub fn contains_term(text: &str, term: &str) -> bool {
    if term.is_empty() {
        return false;
    }

    for (i, _) in text.match_indices(term) {
        let term_len = term.len();

        // Check preceding character
        let prev_ok = text[..i]
            .chars()
            .last()
            .map(|c| !c.is_alphanumeric())
            .unwrap_or(true); // Beginning of text

        // Check following character
        let next_ok = text[i + term_len..]
            .chars()
            .next()
            .map(|c| !c.is_alphanumeric())
            .unwrap_or(true); // End of text

        if prev_ok && next_ok {
            return true;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    // Normal cases
    #[test]
    fn matches_term_with_punctuation() {
        assert!(contains_term("cat!", "cat"));
        assert!(contains_term("!dog", "dog"));
    }

    #[test]
    fn matches_phrase_with_boundaries() {
        assert!(contains_term("hello-world", "hello-world"));
        assert!(contains_term("test: 'foo bar'", "'foo bar'"));
    }

    #[test]
    fn matches_at_text_boundaries() {
        assert!(contains_term("start...", "start"));
        assert!(contains_term("...end", "end"));
    }

    // Negative cases
    #[test]
    fn rejects_partial_matches() {
        assert!(!contains_term("category", "cat"));
        assert!(!contains_term("rebooted", "boot"));
    }

    #[test]
    fn rejects_missing_term() {
        assert!(!contains_term("hello world", "foo"));
    }

    // Edge cases
    #[test]
    fn handles_empty_inputs() {
        assert!(!contains_term("", "test"));
        assert!(!contains_term("text", ""));
    }

    #[test]
    fn different_unicode_boundaries() {
        assert!(contains_term("café>", "café"));
        assert!(contains_term("русский!", "русский"));
    }

    #[test]
    fn case_sensitive_matching() {
        assert!(!contains_term("Cat", "cat"));
        assert!(contains_term("CaT", "CaT"));
    }

    #[test]
    fn numbers_in_term() {
        assert!(contains_term("v1.0!", "v1.0"));
        assert!(!contains_term("v1.0a", "v1.0"));
    }

    #[test]
    fn adjacent_alphanumeric_fails() {
        assert!(!contains_term("cat5", "cat"));
        assert!(!contains_term("dogcat", "cat"));
    }
}
