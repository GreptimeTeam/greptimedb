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

use lazy_static::lazy_static;
use rand::seq::{IteratorRandom, SliceRandom};
use rand::Rng;

use crate::generator::Random;
use crate::impl_random;

lazy_static! {
    pub static ref LOREM_WORDS: Vec<String> = include_str!("data/lorem_words")
        .lines()
        .map(String::from)
        .collect();
}

/// Modified from https://github.com/ucarion/faker_rand/blob/ea70c660e1ecd7320156eddb31d2830a511f8842/src/lib.rs
macro_rules! faker_impl_from_values {
    ($name: ident, $values: expr) => {
        impl rand::distributions::Distribution<$name> for rand::distributions::Standard {
            fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> $name {
                $name($values[rng.gen_range(0..$values.len())].clone())
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
impl_random!(String, WordGenerator, LOREM_WORDS);

pub type WordMapFn<R> = Box<dyn Fn(&mut R, String) -> String>;

pub struct MapWordGenerator<R: Rng> {
    base: WordGenerator,
    map: WordMapFn<R>,
}

pub fn random_capitalize_map<R: Rng + 'static>(rng: &mut R, s: String) -> String {
    let mut v = s.chars().collect::<Vec<_>>();

    let select = rng.gen_range(0..s.len());
    for idx in (0..s.len()).choose_multiple(rng, select) {
        v[idx] = v[idx].to_uppercase().next().unwrap();
    }

    v.into_iter().collect::<String>()
}

/// Add backticks if it contains uppercase chars.
pub fn auto_backtick_map<R: Rng + 'static>(_rng: &mut R, s: String) -> String {
    let need_backtick = s.chars().any(|c| c.is_uppercase());

    if need_backtick {
        format!("`{s}`")
    } else {
        s
    }
}

pub fn merge_two_word_map_fn<R: Rng>(
    f1: impl Fn(&mut R, String) -> String,
    f2: impl Fn(&mut R, String) -> String,
) -> impl Fn(&mut R, String) -> String {
    move |rng, s| -> String {
        let s = f1(rng, s);
        f2(rng, s)
    }
}

impl<R: Rng> MapWordGenerator<R> {
    pub fn new(map: WordMapFn<R>) -> Self {
        Self {
            base: WordGenerator,
            map,
        }
    }
}

impl<R: Rng> Random<String, R> for MapWordGenerator<R> {
    fn choose(&self, rng: &mut R, amount: usize) -> Vec<String> {
        self.base
            .choose(rng, amount)
            .into_iter()
            .map(|s| (self.map)(rng, s))
            .collect()
    }
}
