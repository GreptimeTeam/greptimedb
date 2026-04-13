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
use std::sync::Arc;

use datafusion_common::arrow::array::{Array, AsArray, BooleanArray, BooleanBuilder};
use datafusion_common::arrow::compute;
use datafusion_common::arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};
use memchr::memmem;

use crate::function::Function;
use crate::function_registry::FunctionRegistry;

/// Exact term/phrase matching function for text columns.
///
/// This function uses script-aware matching rules:
/// - ASCII-only terms keep whole-word style boundary matching, like Whole-word matching (e.g. "cat" in "cat!" but not in "category")
/// - Phrase matching (e.g. "hello world" in "note:hello world!")
/// - Terms containing Han characters match as contiguous substrings
/// - Mixed-script identifiers and numeric terms remain searchable in Chinese text
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
/// 2. For ASCII-only terms, adjacent ASCII word characters block the match
/// 3. For Han-containing terms, contiguous substring match is sufficient
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
/// -- Chinese substring examples --
/// SELECT matches_term(column, '手机') FROM table;
/// -- Text: "登录手机号18888888888的动态key" => true
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
pub struct MatchesTermFunction {
    signature: Signature,
}

impl MatchesTermFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register_scalar(MatchesTermFunction::default());
    }
}

impl Default for MatchesTermFunction {
    fn default() -> Self {
        Self {
            signature: Signature::string(2, Volatility::Immutable),
        }
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

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [arg0, arg1] = datafusion_common::utils::take_function_args(self.name(), &args.args)?;

        fn as_str(v: &ScalarValue) -> Option<&str> {
            match v {
                ScalarValue::Utf8View(Some(x))
                | ScalarValue::Utf8(Some(x))
                | ScalarValue::LargeUtf8(Some(x)) => Some(x.as_str()),
                _ => None,
            }
        }

        if let (ColumnarValue::Scalar(text), ColumnarValue::Scalar(term)) = (arg0, arg1) {
            let text = as_str(text);
            let term = as_str(term);
            let result = match (text, term) {
                (Some(text), Some(term)) => Some(MatchesTermFinder::new(term).find(text)),
                _ => None,
            };
            return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(result)));
        };

        let v = match (arg0, arg1) {
            (ColumnarValue::Scalar(_), ColumnarValue::Scalar(_)) => {
                // Unreachable because we have checked this case above and returned if matched.
                unreachable!()
            }
            (ColumnarValue::Scalar(text), ColumnarValue::Array(terms)) => {
                let text = as_str(text);
                if let Some(text) = text {
                    let terms = compute::cast(terms, &DataType::Utf8View)?;
                    let terms = terms.as_string_view();

                    let mut builder = BooleanBuilder::with_capacity(terms.len());
                    terms.iter().for_each(|term| {
                        builder.append_option(term.map(|x| MatchesTermFinder::new(x).find(text)))
                    });
                    ColumnarValue::Array(Arc::new(builder.finish()))
                } else {
                    ColumnarValue::Array(Arc::new(BooleanArray::new_null(terms.len())))
                }
            }
            (ColumnarValue::Array(texts), ColumnarValue::Scalar(term)) => {
                let term = as_str(term);
                if let Some(term) = term {
                    let finder = MatchesTermFinder::new(term);

                    let texts = compute::cast(texts, &DataType::Utf8View)?;
                    let texts = texts.as_string_view();

                    let mut builder = BooleanBuilder::with_capacity(texts.len());
                    texts
                        .iter()
                        .for_each(|text| builder.append_option(text.map(|x| finder.find(x))));
                    ColumnarValue::Array(Arc::new(builder.finish()))
                } else {
                    ColumnarValue::Array(Arc::new(BooleanArray::new_null(texts.len())))
                }
            }
            (ColumnarValue::Array(texts), ColumnarValue::Array(terms)) => {
                let terms = compute::cast(terms, &DataType::Utf8View)?;
                let terms = terms.as_string_view();
                let texts = compute::cast(texts, &DataType::Utf8View)?;
                let texts = texts.as_string_view();

                let len = texts.len();
                if terms.len() != len {
                    return Err(DataFusionError::Internal(format!(
                        "input arrays have different lengths: {len}, {}",
                        terms.len()
                    )));
                }

                let mut builder = BooleanBuilder::with_capacity(len);
                for (text, term) in texts.iter().zip(terms.iter()) {
                    let result = match (text, term) {
                        (Some(text), Some(term)) => Some(MatchesTermFinder::new(term).find(text)),
                        _ => None,
                    };
                    builder.append_option(result);
                }
                ColumnarValue::Array(Arc::new(builder.finish()))
            }
        };
        Ok(v)
    }
}

/// A compiled finder for `matches_term` function that holds the compiled term
/// and its metadata for efficient matching.
///
/// A term is considered matched when:
/// 1. The exact sequence appears in the text
/// 2. ASCII-only terms are not adjacent to ASCII word characters
/// 3. Han-containing terms match as contiguous substrings
///
/// # Examples
/// ```
/// let finder = MatchesTermFinder::new("cat");
/// assert!(finder.find("cat!"));      // Term at end with punctuation
/// assert!(finder.find("dog,cat"));   // Term preceded by comma
/// assert!(!finder.find("category")); // Partial match rejected
///
/// let finder = MatchesTermFinder::new("手机");
/// assert!(finder.find("登录手机号18888888888的动态key"));
/// ```
#[derive(Clone, Debug)]
pub struct MatchesTermFinder {
    finder: memmem::Finder<'static>,
    term: String,
    term_kind: TermKind,
    starts_with_other: bool,
    ends_with_other: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CharClass {
    AsciiWord,
    Han,
    UnicodeWord,
    Other,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TermKind {
    AsciiLike,
    UnicodeWord,
    HanContaining,
}

fn classify_char(c: char) -> CharClass {
    if c.is_ascii_alphanumeric() {
        CharClass::AsciiWord
    } else if is_han(c) {
        CharClass::Han
    } else if c.is_alphanumeric() {
        CharClass::UnicodeWord
    } else {
        CharClass::Other
    }
}

fn is_han(c: char) -> bool {
    matches!(
        c as u32,
        0x3400..=0x4DBF
            | 0x4E00..=0x9FFF
            | 0xF900..=0xFAFF
            | 0x20000..=0x2A6DF
            | 0x2A700..=0x2B73F
            | 0x2B740..=0x2B81F
            | 0x2B820..=0x2CEAF
            | 0x2CEB0..=0x2EBEF
            | 0x30000..=0x3134F
    )
}

fn classify_term(term: &str) -> TermKind {
    let mut has_han = false;
    let mut has_unicode_word = false;
    for c in term.chars() {
        match classify_char(c) {
            CharClass::AsciiWord => {}
            CharClass::Han => has_han = true,
            CharClass::UnicodeWord => has_unicode_word = true,
            CharClass::Other => {}
        }
    }

    if has_han {
        TermKind::HanContaining
    } else if has_unicode_word {
        TermKind::UnicodeWord
    } else {
        TermKind::AsciiLike
    }
}

fn boundary_ok(term_kind: TermKind, neighbor: Option<char>, term_has_other_boundary: bool) -> bool {
    if term_has_other_boundary {
        return true;
    }

    match term_kind {
        TermKind::AsciiLike => !matches!(neighbor.map(classify_char), Some(CharClass::AsciiWord)),
        TermKind::UnicodeWord => !matches!(
            neighbor.map(classify_char),
            Some(CharClass::AsciiWord | CharClass::UnicodeWord | CharClass::Han)
        ),
        TermKind::HanContaining => true,
    }
}

impl MatchesTermFinder {
    /// Create a new `MatchesTermFinder` for the given term.
    pub fn new(term: &str) -> Self {
        let starts_with_other = term
            .chars()
            .next()
            .is_some_and(|c| classify_char(c) == CharClass::Other);
        let ends_with_other = term
            .chars()
            .last()
            .is_some_and(|c| classify_char(c) == CharClass::Other);
        Self {
            finder: memmem::Finder::new(term).into_owned(),
            term: term.to_string(),
            term_kind: classify_term(term),
            starts_with_other,
            ends_with_other,
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

            let prev = text[..actual_pos].chars().last();
            let prev_ok = self.starts_with_other || boundary_ok(self.term_kind, prev, false);

            if prev_ok {
                if self.term_kind == TermKind::HanContaining {
                    return true;
                }

                let next_pos = actual_pos + self.finder.needle().len();
                let next = text[next_pos..].chars().next();
                let next_ok = self.ends_with_other || boundary_ok(self.term_kind, next, false);

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
    fn mixed_script_terms_match_inside_chinese_context() {
        let text = "登录手机号18888888888的动态key";
        assert!(MatchesTermFinder::new("手机号").find(text));
        assert!(MatchesTermFinder::new("18888888888").find(text));
        assert!(MatchesTermFinder::new("手机").find(text));
        assert!(MatchesTermFinder::new("机号").find(text));
        assert!(MatchesTermFinder::new("机号1888").find(text));
        assert!(MatchesTermFinder::new("农业").find("中国农业银行"));
        assert!(MatchesTermFinder::new("error").find("错误error日志"));
    }

    #[test]
    fn underscore_still_counts_as_boundary_for_ascii_terms() {
        assert!(MatchesTermFinder::new("world").find("hello_world"));
        assert!(MatchesTermFinder::new("id").find("trace_id=abc"));
        assert!(!MatchesTermFinder::new("error").find("criticalerrors"));
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

    #[test]
    fn han_terms_match_as_contiguous_substrings() {
        assert!(MatchesTermFinder::new("行账号").find("中国农业银行账号"));
        assert!(MatchesTermFinder::new("登录").find("登录手机号18888888888的动态key"));
    }
}
