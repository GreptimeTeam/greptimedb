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

use std::fmt;
use std::iter::repeat_n;
use std::sync::Arc;

use common_query::error::{InvalidFuncArgsSnafu, Result};
use common_query::prelude::Volatility;
use datatypes::prelude::ConcreteDataType;
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::vectors::{BooleanVector, BooleanVectorBuilder, MutableVector, VectorRef};
use memchr::memmem;
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
/// * `text` - String column to search
/// * `term` - Search term/phrase
///
/// # Returns
/// BooleanVector where each element indicates if the corresponding text
/// contains an exact match of the term, following these rules:
/// 1. Exact substring match found (case-sensitive)
/// 2. Match boundaries are either:
///    - Start/end of text
///    - Any non-alphanumeric character (including spaces, hyphens, punctuation, etc.)
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
///
/// -- Empty string handling --
/// SELECT matches_term(column, '') FROM table;
/// -- Text: "" => true
/// -- Text: "any" => false
///
/// -- Case sensitivity --
/// SELECT matches_term(column, 'Cat') FROM table;
/// -- Text: "Cat" => true
/// -- Text: "cat" => false
/// ```
pub struct MatchesTermFunction;

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
        let compiled_finder = if term_column.is_const() {
            let term = term_column.get_ref(0).as_string().unwrap();
            match term {
                None => {
                    return Ok(Arc::new(BooleanVector::from_iter(repeat_n(
                        None,
                        text_column.len(),
                    ))));
                }
                Some(term) => Some(MatchesTermFinder::new(term)),
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

            let contains = match &compiled_finder {
                Some(finder) => finder.find(text),
                None => {
                    let term = match term_column.get_ref(i).as_string().unwrap() {
                        None => {
                            result.push_null();
                            continue;
                        }
                        Some(term) => term,
                    };
                    MatchesTermFinder::new(term).find(text)
                }
            };
            result.push(Some(contains));
        }

        Ok(result.to_vector())
    }
}

/// A compiled finder for `matches_term` function that holds the compiled term
/// and its metadata for efficient matching.
///
/// A term is considered matched when:
/// 1. The exact sequence appears in the text
/// 2. It is either:
///    - At the start/end of text with adjacent non-alphanumeric character
///    - Surrounded by non-alphanumeric characters
///
/// # Examples
/// ```
/// let finder = MatchesTermFinder::new("cat");
/// assert!(finder.find("cat!"));      // Term at end with punctuation
/// assert!(finder.find("dog,cat"));   // Term preceded by comma
/// assert!(!finder.find("category")); // Partial match rejected
///
/// let finder = MatchesTermFinder::new("world");
/// assert!(finder.find("hello-world")); // Hyphen boundary
/// ```
#[derive(Clone, Debug)]
pub struct MatchesTermFinder {
    finder: memmem::Finder<'static>,
    term: String,
    starts_with_non_alnum: bool,
    ends_with_non_alnum: bool,
}

impl MatchesTermFinder {
    /// Create a new `MatchesTermFinder` for the given term.
    pub fn new(term: &str) -> Self {
        let starts_with_non_alnum = term.chars().next().is_some_and(|c| !c.is_alphanumeric());
        let ends_with_non_alnum = term.chars().last().is_some_and(|c| !c.is_alphanumeric());

        Self {
            finder: memmem::Finder::new(term).into_owned(),
            term: term.to_string(),
            starts_with_non_alnum,
            ends_with_non_alnum,
        }
    }

    /// Find the term in the text.
    pub fn find(&self, text: &str) -> bool {
        if self.term.is_empty() {
            return text.is_empty();
        }

        if text.len() < self.term.len() {
            return false;
        }

        let mut pos = 0;
        while let Some(found_pos) = self.finder.find(&text.as_bytes()[pos..]) {
            let actual_pos = pos + found_pos;

            let prev_ok = self.starts_with_non_alnum
                || text[..actual_pos]
                    .chars()
                    .last()
                    .map(|c| !c.is_alphanumeric())
                    .unwrap_or(true);

            if prev_ok {
                let next_pos = actual_pos + self.finder.needle().len();
                let next_ok = self.ends_with_non_alnum
                    || text[next_pos..]
                        .chars()
                        .next()
                        .map(|c| !c.is_alphanumeric())
                        .unwrap_or(true);

                if next_ok {
                    return true;
                }
            }

            if let Some(next_char) = text[actual_pos..].chars().next() {
                pos = actual_pos + next_char.len_utf8();
            } else {
                break;
            }
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_term_example() {
        let finder = MatchesTermFinder::new("hello world");
        assert!(finder.find("warning:hello world!"));
        assert!(!finder.find("hello-world"));
        assert!(!finder.find("hello world2023"));

        let finder = MatchesTermFinder::new("critical error");
        assert!(finder.find("ERROR:critical error!"));
        assert!(!finder.find("critical_errors"));

        let finder = MatchesTermFinder::new("");
        assert!(finder.find(""));
        assert!(!finder.find("any"));

        let finder = MatchesTermFinder::new("Cat");
        assert!(finder.find("Cat"));
        assert!(!finder.find("cat"));
    }

    #[test]
    fn matches_term_with_punctuation() {
        assert!(MatchesTermFinder::new("cat").find("cat!"));
        assert!(MatchesTermFinder::new("dog").find("!dog"));
    }

    #[test]
    fn matches_phrase_with_boundaries() {
        assert!(MatchesTermFinder::new("hello-world").find("hello-world"));
        assert!(MatchesTermFinder::new("'foo bar'").find("test: 'foo bar'"));
    }

    #[test]
    fn matches_at_text_boundaries() {
        assert!(MatchesTermFinder::new("start").find("start..."));
        assert!(MatchesTermFinder::new("end").find("...end"));
    }

    // Negative cases
    #[test]
    fn rejects_partial_matches() {
        assert!(!MatchesTermFinder::new("cat").find("category"));
        assert!(!MatchesTermFinder::new("boot").find("rebooted"));
    }

    #[test]
    fn rejects_missing_term() {
        assert!(!MatchesTermFinder::new("foo").find("hello world"));
    }

    // Edge cases
    #[test]
    fn handles_empty_inputs() {
        assert!(!MatchesTermFinder::new("test").find(""));
        assert!(!MatchesTermFinder::new("").find("text"));
    }

    #[test]
    fn different_unicode_boundaries() {
        assert!(MatchesTermFinder::new("café").find("café>"));
        assert!(!MatchesTermFinder::new("café").find("口café>"));
        assert!(!MatchesTermFinder::new("café").find("café口"));
        assert!(!MatchesTermFinder::new("café").find("cafémore"));
        assert!(MatchesTermFinder::new("русский").find("русский!"));
        assert!(MatchesTermFinder::new("русский").find("русский！"));
    }

    #[test]
    fn case_sensitive_matching() {
        assert!(!MatchesTermFinder::new("cat").find("Cat"));
        assert!(MatchesTermFinder::new("CaT").find("CaT"));
    }

    #[test]
    fn numbers_in_term() {
        assert!(MatchesTermFinder::new("v1.0").find("v1.0!"));
        assert!(!MatchesTermFinder::new("v1.0").find("v1.0a"));
    }

    #[test]
    fn adjacent_alphanumeric_fails() {
        assert!(!MatchesTermFinder::new("cat").find("cat5"));
        assert!(!MatchesTermFinder::new("dog").find("dogcat"));
    }

    #[test]
    fn empty_term_text() {
        assert!(!MatchesTermFinder::new("").find("text"));
        assert!(MatchesTermFinder::new("").find(""));
        assert!(!MatchesTermFinder::new("text").find(""));
    }

    #[test]
    fn leading_non_alphanumeric() {
        assert!(MatchesTermFinder::new("/cat").find("dog/cat"));
        assert!(MatchesTermFinder::new("dog/").find("dog/cat"));
        assert!(MatchesTermFinder::new("dog/cat").find("dog/cat"));
    }

    #[test]
    fn continues_searching_after_boundary_mismatch() {
        assert!(!MatchesTermFinder::new("log").find("bloglog!"));
        assert!(MatchesTermFinder::new("log").find("bloglog log"));
        assert!(MatchesTermFinder::new("log").find("alogblog_log!"));

        assert!(MatchesTermFinder::new("error").find("errorlog_error_case"));
        assert!(MatchesTermFinder::new("test").find("atestbtestc_test_end"));
        assert!(MatchesTermFinder::new("data").find("database_data_store"));
        assert!(!MatchesTermFinder::new("data").find("database_datastore"));
        assert!(MatchesTermFinder::new("log.txt").find("catalog.txt_log.txt!"));
        assert!(!MatchesTermFinder::new("log.txt").find("catalog.txtlog.txt!"));
        assert!(MatchesTermFinder::new("data-set").find("bigdata-set_data-set!"));

        assert!(MatchesTermFinder::new("中文").find("这是中文测试，中文！"));
        assert!(MatchesTermFinder::new("error").find("错误errorerror日志_error!"));
    }
}
