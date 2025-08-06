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

use crate::fulltext_index::error::Result;
use crate::Bytes;

lazy_static::lazy_static! {
    static ref JIEBA: jieba_rs::Jieba = jieba_rs::Jieba::new();
}

/// A-Z, a-z, 0-9, and '_' are true
const VALID_ASCII_TOKEN: [bool; 256] = [
    false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, true, true, true, true, true,
    true, true, true, true, true, false, false, false, false, false, false, false, true, true,
    true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, true, false, false, false, false, true, false, true,
    true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, true, true, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false,
];

/// `Tokenizer` tokenizes a text into a list of tokens.
pub trait Tokenizer: Send {
    fn tokenize<'a>(&self, text: &'a str) -> Vec<&'a str>;
}

/// `EnglishTokenizer` tokenizes an English text.
///
/// It splits the text by non-alphabetic characters.
#[derive(Debug, Default)]
pub struct EnglishTokenizer;

impl Tokenizer for EnglishTokenizer {
    fn tokenize<'a>(&self, text: &'a str) -> Vec<&'a str> {
        let mut tokens = Vec::new();
        let mut start = 0;
        for (i, &byte) in text.as_bytes().iter().enumerate() {
            if !VALID_ASCII_TOKEN[byte as usize] {
                if start < i {
                    tokens.push(&text[start..i]);
                }
                start = i + 1;
            }
        }

        if start < text.len() {
            tokens.push(&text[start..]);
        }

        tokens
    }
}

/// `ChineseTokenizer` tokenizes a Chinese text.
///
/// It uses the Jieba tokenizer to split the text into Chinese words.
#[derive(Debug, Default)]
pub struct ChineseTokenizer;

impl Tokenizer for ChineseTokenizer {
    fn tokenize<'a>(&self, text: &'a str) -> Vec<&'a str> {
        if text.is_ascii() {
            EnglishTokenizer {}.tokenize(text)
        } else {
            JIEBA.cut(text, false)
        }
    }
}

/// `Analyzer` analyzes a text into a list of tokens.
///
/// It uses a `Tokenizer` to tokenize the text and optionally lowercases the tokens.
pub struct Analyzer {
    tokenizer: Box<dyn Tokenizer>,
    case_sensitive: bool,
}

impl Analyzer {
    /// Creates a new `Analyzer` with the given `Tokenizer` and case sensitivity.
    pub fn new(tokenizer: Box<dyn Tokenizer>, case_sensitive: bool) -> Self {
        Self {
            tokenizer,
            case_sensitive,
        }
    }

    /// Analyzes the given text into a list of tokens.
    pub fn analyze_text(&self, text: &str) -> Result<Vec<Bytes>> {
        let res = self
            .tokenizer
            .tokenize(text)
            .iter()
            .map(|s| {
                if self.case_sensitive {
                    s.as_bytes().to_vec()
                } else {
                    s.to_lowercase().as_bytes().to_vec()
                }
            })
            .collect();
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_english_tokenizer() {
        let tokenizer = EnglishTokenizer;
        let text = "Hello, world!!! This is å ----++   test012345+67890";
        let tokens = tokenizer.tokenize(text);
        assert_eq!(
            tokens,
            vec!["Hello", "world", "This", "is", "test012345", "67890"]
        );
    }

    #[test]
    fn test_english_tokenizer_with_utf8() {
        let tokenizer = EnglishTokenizer;
        let text = "💸unfold the 纸巾😣and gently 清洁表😭面";
        let tokens = tokenizer.tokenize(text);
        assert_eq!(
            tokens,
            // Don't care what happens to non-ASCII characters.
            // It's kind of a misconfiguration to use EnglishTokenizer on non-ASCII text.
            vec!["unfold", "the", "and", "gently"]
        );
    }

    #[test]
    fn test_chinese_tokenizer() {
        let tokenizer = ChineseTokenizer;
        let text = "我喜欢苹果";
        let tokens = tokenizer.tokenize(text);
        assert_eq!(tokens, vec!["我", "喜欢", "苹果"]);
    }

    #[test]
    fn test_analyzer() {
        let tokenizer = EnglishTokenizer;
        let analyzer = Analyzer::new(Box::new(tokenizer), false);
        let text = "Hello, world! This is a test.";
        let tokens = analyzer.analyze_text(text).unwrap();
        assert_eq!(
            tokens,
            vec![
                b"hello".to_vec(),
                b"world".to_vec(),
                b"this".to_vec(),
                b"is".to_vec(),
                b"a".to_vec(),
                b"test".to_vec()
            ]
        );
    }
}
