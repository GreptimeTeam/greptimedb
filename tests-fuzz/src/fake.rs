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

use std::collections::HashSet;
use std::marker::PhantomData;

use lazy_static::lazy_static;
use rand::prelude::IndexedRandom;
use rand::seq::{IteratorRandom, SliceRandom};
use rand::Rng;

use crate::generator::Random;
use crate::impl_random;
use crate::ir::Ident;

lazy_static! {
    pub static ref LOREM_WORDS: Vec<String> = include_str!("data/lorem_words")
        .lines()
        .map(String::from)
        .collect();
}

/// Modified from https://github.com/ucarion/faker_rand/blob/ea70c660e1ecd7320156eddb31d2830a511f8842/src/lib.rs
macro_rules! faker_impl_from_values {
    ($name: ident, $values: expr) => {
        impl rand::distr::Distribution<$name> for rand::distr::StandardUniform {
            fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> $name {
                $name($values[rng.random_range(0..$values.len())].clone())
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

pub struct Word(String);
faker_impl_from_values!(Word, LOREM_WORDS);
pub struct WordGenerator;
impl_random!(Ident, WordGenerator, LOREM_WORDS);

pub struct MappedGenerator<T, F, R, V>
where
    T: Random<V, R>,
    F: Fn(&mut R, V) -> V,
    R: Rng,
{
    base: T,
    map: F,
    _r: PhantomData<R>,
    _v: PhantomData<V>,
}

pub fn random_capitalize_map<R: Rng + 'static>(rng: &mut R, s: Ident) -> Ident {
    let mut v = s.value.chars().collect::<Vec<_>>();

    let str_len = s.value.len();
    let select = rng.random_range(0..str_len);
    for idx in (0..str_len).choose_multiple(rng, select) {
        v[idx] = v[idx].to_uppercase().next().unwrap();
    }

    Ident {
        quote_style: s.quote_style,
        value: v.into_iter().collect::<String>(),
    }
}

lazy_static! {
    static ref KEYWORDS_SET: HashSet<&'static str> = sqlparser::keywords::ALL_KEYWORDS
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
}

/// Returns true if it's a keyword.
pub fn is_keyword(word: impl AsRef<str>) -> bool {
    KEYWORDS_SET.contains(word.as_ref())
}

/// Returns true if it contains uppercase char.
pub fn contain_uppercase_char(s: &str) -> bool {
    s.chars().any(|c| c.is_uppercase())
}

/// Returns true if it's a keyword or contains uppercase char.
pub fn is_keyword_or_contain_uppercase(s: &str) -> bool {
    is_keyword(s.to_uppercase()) || contain_uppercase_char(s)
}

pub fn make_backtick_map<R: Rng + 'static, F: Fn(&str) -> bool>(
    f: F,
) -> impl Fn(&mut R, Ident) -> Ident {
    move |_rng, s| -> Ident {
        let need = f(&s.value);

        if need {
            Ident {
                value: s.value,
                quote_style: Some('`'),
            }
        } else {
            s
        }
    }
}

pub fn make_quote_map<R: Rng + 'static, F: Fn(&str) -> bool>(
    f: F,
) -> impl Fn(&mut R, Ident) -> Ident {
    move |_rng, s| -> Ident {
        let need = f(&s.value);

        if need {
            Ident {
                value: s.value,
                quote_style: Some('"'),
            }
        } else {
            s
        }
    }
}

/// Adds backticks if it contains uppercase chars.
pub fn auto_backtick_map<R: Rng + 'static>(_rng: &mut R, s: Ident) -> Ident {
    let need = s.value.chars().any(|c| c.is_uppercase());

    if need {
        Ident {
            value: s.value,
            quote_style: Some('`'),
        }
    } else {
        s
    }
}

/// Adds backticks if it contains uppercase chars.
pub fn uppercase_and_keyword_backtick_map<R: Rng + 'static>(rng: &mut R, s: Ident) -> Ident {
    make_backtick_map(is_keyword_or_contain_uppercase)(rng, s)
}

/// Adds quotes if it contains uppercase chars.
pub fn auto_quote_map<R: Rng + 'static>(rng: &mut R, s: Ident) -> Ident {
    make_quote_map(contain_uppercase_char)(rng, s)
}

/// Adds quotes if it contains uppercase chars.
pub fn uppercase_and_keyword_quote_map<R: Rng + 'static>(rng: &mut R, s: Ident) -> Ident {
    make_quote_map(is_keyword_or_contain_uppercase)(rng, s)
}

pub fn merge_two_word_map_fn<R: Rng>(
    f1: impl Fn(&mut R, Ident) -> Ident,
    f2: impl Fn(&mut R, Ident) -> Ident,
) -> impl Fn(&mut R, Ident) -> Ident {
    move |rng, s| -> Ident {
        let s = f1(rng, s);
        f2(rng, s)
    }
}

impl<T, F, R, V> MappedGenerator<T, F, R, V>
where
    T: Random<V, R>,
    F: Fn(&mut R, V) -> V,
    R: Rng,
{
    pub fn new(base: T, map: F) -> Self {
        Self {
            base,
            map,
            _r: Default::default(),
            _v: Default::default(),
        }
    }
}

impl<T, F, R, V> Random<V, R> for MappedGenerator<T, F, R, V>
where
    T: Random<V, R>,
    F: Fn(&mut R, V) -> V,
    R: Rng,
{
    fn choose(&self, rng: &mut R, amount: usize) -> Vec<V> {
        self.base
            .choose(rng, amount)
            .into_iter()
            .map(|s| (self.map)(rng, s))
            .collect()
    }
}
