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

use common_base::term_token;

use crate::Bytes;
use crate::bloom_filter::element_hash;
use crate::fulltext_index::error::Result;

/// `Tokenizer` tokenizes a text into a list of tokens.
pub trait Tokenizer: Send {
    fn tokenize<'a>(&self, text: &'a str) -> Vec<&'a str>;
}

/// `ScriptTokenizer` produces the tokens described in [`term_token::tokenize`].
#[derive(Debug, Default)]
pub struct ScriptTokenizer;

impl Tokenizer for ScriptTokenizer {
    fn tokenize<'a>(&self, text: &'a str) -> Vec<&'a str> {
        term_token::tokenize(text)
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

    /// Returns the bloom filter hash of each token in the given text.
    ///
    /// Equivalent to hashing every token returned by [`Analyzer::analyze_text`] with
    /// [`element_hash`]. Only case-insensitive non-ASCII tokens allocate, in `to_lowercase`;
    /// case-insensitive ASCII tokens are lowercased in `buf`.
    pub fn analyze_text_hashes<'a>(
        &self,
        text: &'a str,
        buf: &'a mut Vec<u8>,
    ) -> impl Iterator<Item = u64> + use<'a> {
        let case_sensitive = self.case_sensitive;
        self.tokenizer.tokenize(text).into_iter().map(move |token| {
            if case_sensitive {
                element_hash(token.as_bytes())
            } else if token.is_ascii() {
                buf.clear();
                buf.extend(token.bytes().map(|b| b.to_ascii_lowercase()));
                element_hash(buf)
            } else {
                element_hash(token.to_lowercase().as_bytes())
            }
        })
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
    fn test_analyze_text_hashes_matches_analyze_text() {
        let text = "Hello, WORLD ship_Ship 清洁表面 ÄÖÜ straße İstanbul x";
        for case_sensitive in [false, true] {
            let analyzer = Analyzer::new(Box::new(ScriptTokenizer), case_sensitive);
            let expected = analyzer
                .analyze_text(text)
                .unwrap()
                .iter()
                .map(|t| element_hash(t))
                .collect::<Vec<_>>();
            let hashes = analyzer
                .analyze_text_hashes(text, &mut Vec::new())
                .collect::<Vec<_>>();
            assert_eq!(expected, hashes);
        }
    }

    #[test]
    fn test_analyzer() {
        let analyzer = Analyzer::new(Box::new(ScriptTokenizer), false);
        let tokens = analyzer.analyze_text("Hello, World_Two").unwrap();
        assert_eq!(
            tokens,
            vec![b"hello".to_vec(), b"world".to_vec(), b"two".to_vec()]
        );
    }
}
