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

//! Word boundaries shared by `matches_term` and the fulltext bloom index.
//!
//! Bloom pruning is only sound if the index splits text exactly where
//! `matches_term` sees word boundaries, so both use [`classify_char`], and the
//! query side derives its probes with [`term_probes`] and [`like_probes`].

use std::ops::Range;

use icu_properties::props::Script;
use icu_properties::{CodePointMapData, CodePointMapDataBorrowed};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CharClass {
    AsciiWord,
    Han,
    UnicodeWord,
    Other,
}

pub fn classify_char(c: char) -> CharClass {
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

static HAN_SCRIPT_DATA: CodePointMapDataBorrowed<'static, Script> =
    CodePointMapData::<Script>::new();

pub fn is_han(c: char) -> bool {
    HAN_SCRIPT_DATA.get(c) == Script::Han
}

/// Returns the tokens the fulltext bloom index stores for `text`.
///
/// A token is a maximal run of ASCII alphanumerics or of non-Han alphanumerics;
/// `_` and other symbols only separate tokens. A run of Han characters is
/// emitted as its bigrams, or as itself when it has a single character, since
/// `matches_term` matches Han terms as plain substrings.
pub fn tokenize(text: &str) -> Vec<&str> {
    let mut tokens = Vec::new();
    for_each_word_run(text, |class, range| {
        let run = &text[range];
        if class == CharClass::Han {
            push_han_tokens(run, &mut tokens);
        } else {
            tokens.push(run);
        }
    });
    tokens
}

/// Calls `f` on every maximal run of word characters of the same class.
fn for_each_word_run(text: &str, mut f: impl FnMut(CharClass, Range<usize>)) {
    if text.is_ascii() {
        // ASCII text only has `AsciiWord` and `Other` characters, so scan bytes
        // instead of decoding chars. Most log lines take this path.
        let mut start = None;
        for (i, b) in text.bytes().enumerate() {
            match (b.is_ascii_alphanumeric(), start) {
                (true, None) => start = Some(i),
                (false, Some(s)) => {
                    f(CharClass::AsciiWord, s..i);
                    start = None;
                }
                _ => {}
            }
        }
        if let Some(s) = start {
            f(CharClass::AsciiWord, s..text.len());
        }
        return;
    }

    let mut run: Option<(CharClass, usize)> = None;
    for (i, c) in text.char_indices() {
        let class = classify_char(c);
        if let Some((run_class, start)) = run {
            if run_class == class {
                continue;
            }
            f(run_class, start..i);
            run = None;
        }
        if class != CharClass::Other {
            run = Some((class, i));
        }
    }
    if let Some((run_class, start)) = run {
        f(run_class, start..text.len());
    }
}

fn push_han_tokens<'a>(run: &'a str, tokens: &mut Vec<&'a str>) {
    let mut starts = run.char_indices().map(|(i, _)| i).skip(1);
    let Some(mut second) = starts.next() else {
        tokens.push(run);
        return;
    };
    let mut first = 0;
    for next in starts.chain(std::iter::once(run.len())) {
        tokens.push(&run[first..next]);
        first = second;
        second = next;
    }
}

/// Returns tokens that every text satisfying `matches_term(text, term)` contains,
/// as produced by [`tokenize`].
pub fn term_probes(term: &str) -> Vec<String> {
    // Non-Han terms only match between word boundaries that `ScriptTokenizer` also
    // splits at, so their edge runs are whole tokens. Han-containing terms match
    // as plain substrings and give no such guarantee at their edges.
    let bounded = !term.chars().any(|c| classify_char(c) == CharClass::Han);
    let mut probes = Vec::new();
    push_literal_probes(term, bounded, bounded, &mut probes);
    probes
}

/// Returns tokens that every text satisfying `text LIKE pattern` contains, as
/// produced by [`ScriptTokenizer`]. `\` escapes the next character, following
/// arrow's `like` kernel; a trailing `\` is a literal backslash.
pub fn like_probes(pattern: &str) -> Vec<String> {
    let mut probes = Vec::new();
    let mut literal = String::new();
    // The pattern start and end are anchors; a wildcard can stand for word
    // characters, so a run touching it may be part of a longer token.
    let mut anchored_left = true;
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        match c {
            '%' | '_' => {
                push_literal_probes(&literal, anchored_left, false, &mut probes);
                literal.clear();
                anchored_left = false;
            }
            '\\' => literal.push(chars.next().unwrap_or('\\')),
            c => literal.push(c),
        }
    }
    push_literal_probes(&literal, anchored_left, true, &mut probes);
    probes
}

/// Pushes the tokens of `literal` that stay whole tokens wherever `literal`
/// occurs. A side is bounded when the character beyond it can't extend the
/// adjacent run.
fn push_literal_probes(
    literal: &str,
    left_bounded: bool,
    right_bounded: bool,
    probes: &mut Vec<String>,
) {
    for_each_word_run(literal, |class, range| {
        let whole =
            (range.start > 0 || left_bounded) && (range.end < literal.len() || right_bounded);
        let run = &literal[range];
        if class == CharClass::Han && run.chars().nth(1).is_some() {
            // Bigrams inside the run occur in every text containing it.
            let mut tokens = Vec::new();
            push_han_tokens(run, &mut tokens);
            probes.extend(tokens.into_iter().map(str::to_string));
        } else if whole {
            probes.push(run.to_string());
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn han_detection_uses_script_not_all_cjk() {
        assert!(is_han('汉'));
        assert!(is_han('\u{30000}'));
        assert!(!is_han('あ'));
        assert!(!is_han('한'));
    }

    #[test]
    fn test_script_tokenizer() {
        let cases: [(&str, &[&str]); 7] = [
            ("hello_world __x__", &["hello", "world", "x"]),
            ("trace_id=abc", &["trace", "id", "abc"]),
            ("错误error日志", &["错误", "error", "日志"]),
            ("连接timeout.5次", &["连接", "timeout", "5", "次"]),
            ("用户user-123登录", &["用户", "user", "123", "登录"]),
            ("中国农业银行", &["中国", "国农", "农业", "业银", "银行"]),
            ("Größe日x", &["Gr", "öß", "e", "日", "x"]),
        ];
        for (text, expected) in cases {
            assert_eq!(tokenize(text), expected, "text: {text}");
        }
    }

    #[test]
    fn test_term_probes() {
        let cases: [(&str, &[&str]); 8] = [
            ("world", &["world"]),
            ("hello world", &["hello", "world"]),
            ("/start", &["start"]),
            ("naïve", &["na", "ï", "ve"]),
            // Han terms match as substrings: only bigrams and runs enclosed by
            // other characters of the term are safe.
            ("农业", &["农业"]),
            ("农", &[]),
            ("机号1888", &["机号"]),
            ("x错误y日z", &["错误", "y", "日"]),
        ];
        for (term, expected) in cases {
            assert_eq!(term_probes(term), expected, "term: {term}");
        }
    }

    #[test]
    fn test_like_probes() {
        let cases: [(&str, &[&str]); 11] = [
            ("%timeout%", &[]),
            ("% timeout %", &["timeout"]),
            ("timeout%", &[]),
            ("timeout: %", &["timeout"]),
            ("%: timeout", &["timeout"]),
            ("a_b c", &["c"]),
            (r"trace\_id=%", &["trace", "id"]),
            (r"100\% done", &["100", "done"]),
            (r"x\\y", &["x", "y"]),
            ("%错误error日%", &["错误", "error"]),
            ("%错误 x%", &["错误"]),
        ];
        for (pattern, expected) in cases {
            assert_eq!(like_probes(pattern), expected, "pattern: {pattern}");
        }
    }
}
