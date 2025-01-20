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

use jieba_rs::Jieba;

use crate::fulltext_index::error::Result;
use crate::fulltext_index::Bytes;

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
        text.split(|c: char| !c.is_alphabetic())
            .filter(|s| !s.is_empty())
            .collect()
    }
}

/// `ChineseTokenizer` tokenizes a Chinese text.
/// 
/// It uses the Jieba tokenizer to split the text into Chinese words.
#[derive(Debug, Default)]
pub struct ChineseTokenizer;

impl Tokenizer for ChineseTokenizer {
    fn tokenize<'a>(&self, text: &'a str) -> Vec<&'a str> {
        let jieba = Jieba::new();
        jieba.cut(text, false)
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
        let text = "Hello, world! This is a test.";
        let tokens = tokenizer.tokenize(text);
        assert_eq!(tokens, vec!["Hello", "world", "This", "is", "a", "test"]);
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