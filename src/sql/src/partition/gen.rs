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

use num_bigint::BigUint;
use num_traits::ToPrimitive;
use sqlparser::ast::{BinaryOperator, Expr, Ident, Value};

use crate::between_string;
use crate::error::{InvalidPartitionNumberSnafu, InvalidPartitionRangeSnafu, Result};

/// Merges a list of potentially overlapping or adjacent character ranges
/// into a sorted, unique list of characters, forming the custom alphabet.
fn get_custom_alphabet(ranges: &[(char, char)]) -> Vec<char> {
    let mut chars: Vec<char> = Vec::new();
    for &(start_char, end_char) in ranges {
        if start_char > end_char {
            continue;
        }
        for c in (start_char as u32)..=(end_char as u32) {
            // Ensure characters are visible ASCII, though the problem implies input is ASCII.
            // This filter is a safeguard if non-ASCII chars are accidentally passed.
            if (0x20..=0x7E).contains(&c) {
                // Visible ASCII range
                chars.push(std::char::from_u32(c).unwrap());
            }
        }
    }
    chars.sort_unstable();
    chars.dedup();
    chars
}

/// Converts a string to a BigUint based on a custom alphabet and fixed length.
/// The string is conceptually padded with the first character of the alphabet
/// or truncated to match `fixed_len`.
fn string_to_custom_biguint_fixed_len(
    s: &str,
    alphabet: &[char],
    base: &BigUint,
    fixed_len: usize,
) -> BigUint {
    if fixed_len == 0 {
        return BigUint::from(0u32);
    }

    let mut val = BigUint::from(0u32);
    let first_char_idx = 0; // Index of the first character in the sorted alphabet

    // Pad or truncate the string to fixed_len
    let chars: Vec<char> = s.chars().take(fixed_len).collect();

    for i in 0..fixed_len {
        let c = if i < chars.len() {
            chars[i]
        } else {
            // Pad with the first character of the alphabet
            alphabet[first_char_idx]
        };

        // unwrap checked by previous logic to ensure string is made up with all
        // available alphabets
        let char_idx = alphabet.iter().position(|&x| x == c).unwrap();
        let char_idx_biguint: BigUint = char_idx.into();

        val = val * base + char_idx_biguint;
    }
    val
}

/// Converts a BigUint back to a string of fixed length based on a custom alphabet.
fn custom_biguint_to_string_fixed_len(
    n: &BigUint,
    alphabet: &[char],
    base: &BigUint,
    fixed_len: usize,
) -> String {
    if fixed_len == 0 {
        return String::new();
    }
    if alphabet.is_empty() {
        return String::new();
    }

    let mut temp_n = n.clone();
    let mut result_chars = Vec::with_capacity(fixed_len);
    let first_char = alphabet[0];

    // Handle the case where n is 0 or very small
    if temp_n == BigUint::from(0u32) {
        return (0..fixed_len).map(|_| first_char).collect();
    }

    // Extract digits in base `base`
    for _ in 0..fixed_len {
        let char_idx = (&temp_n % base).to_usize().unwrap();
        temp_n /= base;
        result_chars.push(alphabet[char_idx]);
    }

    // The digits are extracted in reverse order (least significant first), so reverse them.
    result_chars.reverse();

    // Ensure the string is exactly fixed_len by padding if necessary (shouldn't be needed if logic is correct)
    // and truncating (also shouldn't be needed if logic is correct, but as a safeguard)
    let s: String = result_chars.into_iter().collect();

    // The string may be shorter than fixed_len if temp_n became zero early.
    // In this fixed-length context, we should pad with the first character of the alphabet.
    // Example: if fixed_len=3, base=2, alphabet=['a','b'], n=1 (which is 'b' in base 2)
    // it should be "aab"
    // The current loop fills `fixed_len` characters, so this padding logic is mostly for conceptual clarity.
    // If the BigUint represents a number that would result in a shorter string, it means leading zeros.
    // The loop `for _ in 0..fixed_len` already ensures `fixed_len` characters are pushed.

    s
}

/// Divides a range of string values into multiple segments.
///
/// This function treats strings as large numbers (base 256) to perform
/// arithmetic for segmentation. It can handle arbitrary string values.
///
/// # Arguments
/// * `start` - The starting string of the range (e.g., "a", "apple").
/// * `end` - The ending string of the range (e.g., "z", "orange").
/// * `num_segments` - The desired number of segments.
///   - If `num_segments` is 1, the output will be `[end]`.
///   - If `num_segments` is greater than 1, the output will be `num_segments - 1`
///     computed stops, excluding the `end` string itself.
/// * `hardstops` - An optional vector of string values. If provided, the
///   nearest computed stop will be replaced by the corresponding hardstop.
///   Each hardstop will replace at most one computed stop.
///
/// # Returns
/// A `Vec<String>` containing the computed segment stops, sorted lexicographically.
///
///
/// # Examples
/// ```
/// // Example 1: Simple character range, 3 segments (returns 2 stops)
/// let stops = divide_string_range("a", "z", 3, None);
/// // Expected: ["i", "q"] (approximate, depends on exact calculation)
/// println!("Stops for 'a' to 'z' (3 segments): {:?}", stops);
///
/// // Example 2: More complex string range, 2 segments (returns 1 stop)
/// let stops = divide_string_range("apple", "orange", 2, None);
/// // Expected: ["midpoint_string"]
/// println!("Stops for 'apple' to 'orange' (2 segments): {:?}", stops);
///
/// // Example 3: With hardstops, 3 segments (returns 2 stops)
/// let hardstops = Some(vec!["banana".to_string(), "grape".to_string()]);
/// let stops = divide_string_range("apple", "kiwi", 3, hardstops.as_ref());
/// // Expected: ["banana", "grape"] (order might vary based on nearest match)
/// println!("Stops for 'apple' to 'kiwi' (3 segments) with hardstops: {:?}", stops);
///
/// // Example 4: 1 segment (returns only the end string)
/// let stops = divide_string_range("a", "z", 1, None);
/// // Expected: ["z"]
/// println!("Stops for 'a' to 'z' (1 segment): {:?}", stops);
/// ```
pub fn divide_string_range(
    ranges: &[(char, char)],
    max_len: usize,
    num_segments: u32,
    hardstops: &[&str],
) -> Result<Vec<String>> {
    // Input validation
    if max_len == 0 {
        return Ok(Vec::new());
    }

    if num_segments < 2 {
        InvalidPartitionNumberSnafu {
            partition_num: num_segments,
        }
        .fail()?;
    }

    if ranges.is_empty() {
        return Ok(Vec::new());
    }

    // --- Step 1: Get the custom alphabet and its base ---
    let alphabet = get_custom_alphabet(ranges);
    if alphabet.is_empty() {
        return Ok(Vec::new());
    }
    let base: BigUint = alphabet.len().into();

    // --- Step 2: Calculate the total effective length (number of possible strings) ---
    // This is base^max_len, representing all strings of exactly max_len length.
    let total_effective_length = base.pow(max_len as u32);

    // Handle cases where total_effective_length is too small for num_segments
    let num_segments_biguint: BigUint = num_segments.into();
    if total_effective_length < num_segments_biguint {
        return Ok(Vec::new());
    }

    // Calculate the step size in the effective space.
    let segment_step_in_effective_space = &total_effective_length / &num_segments_biguint;

    let mut computed_stops_with_values: Vec<(BigUint, String)> =
        Vec::with_capacity(num_segments as usize);

    // --- Step 3: Generate intermediate stops ---
    if num_segments == 1 {
        // If only one segment, the stop is the lexicographically last string of max_len.
        let last_char_idx = alphabet.len() - 1;
        let last_char = alphabet[last_char_idx];
        let end_string: String = (0..max_len).map(|_| last_char).collect();
        let end_biguint =
            string_to_custom_biguint_fixed_len(&end_string, &alphabet, &base, max_len);
        computed_stops_with_values.push((end_biguint, end_string));
    } else {
        // Generate intermediate stops, excluding the very last one.
        // The loop goes from 1 to num_segments - 1.
        for i in 1..num_segments {
            let i_biguint: BigUint = i.into();
            let effective_pos_for_stop = segment_step_in_effective_space.clone() * i_biguint;
            let current_biguint = effective_pos_for_stop; // In this model, effective_pos is already the BigUint value

            let stop_str =
                custom_biguint_to_string_fixed_len(&current_biguint, &alphabet, &base, max_len);
            computed_stops_with_values.push((current_biguint, stop_str));
        }
    }

    // --- Step 4: Handle hardstops if provided ---

    let mut final_stops_with_values = computed_stops_with_values;
    // Keep track of which computed stops have been replaced by a hardstop
    let mut used_computed_indices: Vec<bool> = vec![false; final_stops_with_values.len()];

    for hardstop_str in hardstops.iter() {
        // Validate hardstop characters are in the alphabet
        for c in hardstop_str.chars() {
            if !alphabet.contains(&c) {
                InvalidPartitionRangeSnafu {
                    start: hardstop_str.to_string(),
                    end: "".to_string(),
                }
                .fail()?;
            }
        }

        let hardstop_biguint =
            string_to_custom_biguint_fixed_len(hardstop_str, &alphabet, &base, max_len);

        let mut min_diff: Option<BigUint> = None;
        let mut nearest_idx: Option<usize> = None;

        // Find the nearest *unused* computed stop
        for (idx, (computed_val, _)) in final_stops_with_values.iter().enumerate() {
            if !used_computed_indices[idx] {
                let diff = if computed_val >= &hardstop_biguint {
                    computed_val - &hardstop_biguint
                } else {
                    &hardstop_biguint - computed_val
                };

                if min_diff.is_none() || diff < *min_diff.as_ref().unwrap() {
                    min_diff = Some(diff);
                    nearest_idx = Some(idx);
                }
            }
        }

        // If a nearest unused computed stop is found, replace it
        if let Some(idx) = nearest_idx {
            final_stops_with_values[idx].1 = hardstop_str.to_string();
            final_stops_with_values[idx].0 = hardstop_biguint; // Update the BigUint value as well
            used_computed_indices[idx] = true;
        }
    }

    // Re-sort the final stops based on their (potentially updated) BigUint values
    final_stops_with_values.sort_by(|a, b| a.0.cmp(&b.0));

    // Extract and return only the string values
    Ok(final_stops_with_values
        .into_iter()
        .map(|(_, s)| s)
        .collect())
}

pub fn partition_rule_for_range(
    field_name: &str,
    char_ranges: &[(char, char)],
    max_len: usize,
    num_partitions: u32,
    hardstops: &[&str],
) -> Result<Vec<Expr>> {
    let stops = divide_string_range(char_ranges, max_len, num_partitions, hardstops)?;

    let ident_expr = Expr::Identifier(Ident::new(field_name).clone());
    let mut last_stop: Option<String> = None;
    Ok(stops
        .into_iter()
        .enumerate()
        .map(|(i, stop)| {
            let rule = if i == 0 {
                Expr::BinaryOp {
                    left: Box::new(ident_expr.clone()),
                    op: BinaryOperator::Lt,
                    right: Box::new(Expr::Value(Value::SingleQuotedString(stop.clone()))),
                }
            } else if i == num_partitions as usize - 2 {
                Expr::BinaryOp {
                    left: Box::new(ident_expr.clone()),
                    op: BinaryOperator::GtEq,
                    right: Box::new(Expr::Value(Value::SingleQuotedString(stop.clone()))),
                }
            } else {
                // last_stop is not empty guaranteed by previous logic
                let last_stop_str = last_stop.clone().unwrap();
                between_string!(ident_expr, last_stop_str, stop.clone())
            };
            last_stop = Some(stop);
            rule
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_divide_string_range() {
        assert!(divide_string_range(&[('_', '_')], 20, 10, &[])
            .unwrap()
            .is_empty());
        assert!(divide_string_range(&[('b', 'a')], 20, 10, &[])
            .unwrap()
            .is_empty());
        assert!(divide_string_range(&[('a', 'z')], 20, 0, &[]).is_err());
        assert!(divide_string_range(&[('a', 'z')], 20, 1, &[]).is_err());

        let stops =
            divide_string_range(&[('a', 'z')], 16, 10, &[]).expect("failed to divide string range");
        assert_eq!(
            stops,
            vec![
                "cppppppppppppppp",
                "fffffffffffffffe",
                "huuuuuuuuuuuuuut",
                "kkkkkkkkkkkkkkki",
                "mzzzzzzzzzzzzzzx",
                "pppppppppppppppm",
                "sffffffffffffffb",
                "uuuuuuuuuuuuuuuq",
                "xkkkkkkkkkkkkkkf"
            ]
        );

        let stops = divide_string_range(&[('0', '9'), ('-', '-'), ('a', 'z')], 16, 4, &[])
            .expect("failed to divide string range");
        assert_eq!(
            stops,
            vec!["8888888888888888", "hhhhhhhhhhhhhhhh", "qqqqqqqqqqqqqqqq"]
        );

        let stops = divide_string_range(
            &[('0', '9'), ('-', '-'), ('a', 'z')],
            16,
            10,
            &["eu-central-1", "us-east-1"],
        )
        .expect("failed to divide string range");
        assert_eq!(
            stops,
            vec![
                "2owa2owa2owa2owa",
                "6dsl6dsl6dsl6dsl",
                "a2owa2owa2owa2ow",
                "eu-central-1",
                "hhhhhhhhhhhhhhhh",
                "l6dsl6dsl6dsl6ds",
                "owa2owa2owa2owa2",
                "sl6dsl6dsl6dsl6d",
                "us-east-1"
            ]
        );

        let stops = divide_string_range(&[('0', '9'), ('a', 'f')], 32, 16, &[])
            .expect("failed to divide string range");
        assert_eq!(
            stops,
            vec![
                "10000000000000000000000000000000",
                "20000000000000000000000000000000",
                "30000000000000000000000000000000",
                "40000000000000000000000000000000",
                "50000000000000000000000000000000",
                "60000000000000000000000000000000",
                "70000000000000000000000000000000",
                "80000000000000000000000000000000",
                "90000000000000000000000000000000",
                "a0000000000000000000000000000000",
                "b0000000000000000000000000000000",
                "c0000000000000000000000000000000",
                "d0000000000000000000000000000000",
                "e0000000000000000000000000000000",
                "f0000000000000000000000000000000"
            ]
        );
    }

    #[test]
    fn test_generate_partition_expr() {
        let rules = partition_rule_for_range(
            "ns",
            &[('0', '9'), ('-', '-'), ('a', 'z')],
            16,
            10,
            &["eu-central-1", "us-east-1"],
        )
        .expect("failed to divide string range");
        assert_eq!(
            rules.iter().map(|e| e.to_string()).collect::<Vec<String>>(),
            vec![
                "ns < '2owa2owa2owa2owa'",
                "ns >= '2owa2owa2owa2owa' AND ns < '6dsl6dsl6dsl6dsl'",
                "ns >= '6dsl6dsl6dsl6dsl' AND ns < 'a2owa2owa2owa2ow'",
                "ns >= 'a2owa2owa2owa2ow' AND ns < 'eu-central-1'",
                "ns >= 'eu-central-1' AND ns < 'hhhhhhhhhhhhhhhh'",
                "ns >= 'hhhhhhhhhhhhhhhh' AND ns < 'l6dsl6dsl6dsl6ds'",
                "ns >= 'l6dsl6dsl6dsl6ds' AND ns < 'owa2owa2owa2owa2'",
                "ns >= 'owa2owa2owa2owa2' AND ns < 'sl6dsl6dsl6dsl6d'",
                "ns >= 'us-east-1'"
            ]
        );
    }
}
