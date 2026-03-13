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

use datatypes::value::Value;
use rand::Rng;

use crate::error::{self, Result};
use crate::generator::Random;
use crate::ir::Ident;

const READABLE_CHARSET: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

fn readable_token(index: usize) -> String {
    let base = READABLE_CHARSET.len();
    let mut n = index + 1;
    let mut buf = Vec::new();

    while n > 0 {
        let rem = (n - 1) % base;
        buf.push(READABLE_CHARSET[rem] as char);
        n = (n - 1) / base;
    }

    buf.iter().rev().collect()
}

pub fn generate_data_string_value<R: Rng>(
    rng: &mut R,
    random_str: Option<&dyn Random<Ident, R>>,
) -> Value {
    match random_str {
        Some(random) => Value::from(random.generate(rng).value),
        None => {
            let idx = rng.random_range(0..(READABLE_CHARSET.len() * READABLE_CHARSET.len() * 4));
            Value::from(readable_token(idx))
        }
    }
}

/// Generates ordered readable string bounds for partition expressions.
pub fn generate_partition_bounds(bounds: usize) -> Vec<Value> {
    let token_space = READABLE_CHARSET.len() * READABLE_CHARSET.len() * 1024;
    (1..=bounds)
        .map(|i| {
            let idx = i * token_space / (bounds + 1);
            Value::from(readable_token(idx))
        })
        .collect()
}

/// Picks a representative string value for the target partition range.
pub fn generate_partition_value(bounds: &[Value], bound_idx: usize) -> Value {
    let first = bounds.first().unwrap();
    let last = bounds.last().unwrap();
    let upper = match first {
        Value::String(v) => v.as_utf8(),
        _ => "",
    };

    if bound_idx == 0 {
        if upper <= "0" {
            Value::from("")
        } else {
            Value::from("0")
        }
    } else if bound_idx < bounds.len() {
        bounds[bound_idx - 1].clone()
    } else {
        last.clone()
    }
}

/// Generates a unique readable bound not present in existing bounds.
pub fn generate_unique_partition_bound<R: Rng>(rng: &mut R, bounds: &[Value]) -> Result<Value> {
    let search_space = READABLE_CHARSET.len() * READABLE_CHARSET.len() * 1024;
    let start = rng.random_range(0..search_space);
    for offset in 0..search_space {
        let idx = start + offset;
        let candidate = Value::from(readable_token(idx));
        if !bounds.contains(&candidate) {
            return Ok(candidate);
        }
    }

    error::UnexpectedSnafu {
        violated: "unable to generate unique string partition bound".to_string(),
    }
    .fail()
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;

    use super::*;

    #[test]
    fn test_readable_token_grows_length() {
        assert_eq!("0", readable_token(0));
        assert_eq!("9", readable_token(9));
        assert_eq!("A", readable_token(10));
        assert_eq!("z", readable_token(61));
        assert_eq!("00", readable_token(62));
    }

    #[test]
    fn test_generate_partition_bounds_are_readable_and_unique() {
        let bounds = generate_partition_bounds(8);
        assert_eq!(8, bounds.len());

        let mut values = bounds
            .iter()
            .map(|v| match v {
                Value::String(s) => s.as_utf8().to_string(),
                _ => panic!("expected string value"),
            })
            .collect::<Vec<_>>();
        let mut dedup = values.clone();
        dedup.sort();
        dedup.dedup();
        assert_eq!(values.len(), dedup.len());

        for s in values.drain(..) {
            assert!(s.chars().all(|c| c.is_ascii_alphanumeric()));
        }
    }

    #[test]
    fn test_generate_partition_value_for_string_bounds() {
        let bounds = vec![Value::from("A"), Value::from("M")];
        assert_eq!(Value::from("0"), generate_partition_value(&bounds, 0));
        assert_eq!(Value::from("A"), generate_partition_value(&bounds, 1));
        assert_eq!(Value::from("M"), generate_partition_value(&bounds, 2));
    }

    #[test]
    fn test_generate_unique_partition_bound_not_in_existing() {
        let mut rng = ChaCha8Rng::seed_from_u64(42);
        let bounds = vec![Value::from("0"), Value::from("1"), Value::from("2")];
        let candidate = generate_unique_partition_bound(&mut rng, &bounds).unwrap();
        assert!(!bounds.contains(&candidate));
        match candidate {
            Value::String(s) => {
                assert!(!s.as_utf8().is_empty());
                assert!(s.as_utf8().chars().all(|c| c.is_ascii_alphanumeric()));
            }
            _ => panic!("expected string value"),
        }
    }
}
