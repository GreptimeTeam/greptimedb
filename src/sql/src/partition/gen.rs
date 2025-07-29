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

use std::cmp::max;

use num_bigint::{BigUint, ToBigUint};
use sqlparser::ast::{BinaryOperator, Expr, Ident, Value};

use crate::between_string;
use crate::error::{InvalidPartitionNumberSnafu, InvalidPartitionRangeSnafu, Result};

/// Converts a string to a BigUint by treating its bytes as a big-endian number.
/// The string is optionally padded with null bytes (0x00) to a specified length
/// before conversion, ensuring consistent numerical representation for comparison.
fn string_to_biguint_padded(s: &str, min_len: usize) -> BigUint {
    let mut bytes = s.as_bytes().to_vec();
    // Pad with null bytes to ensure a consistent length for numerical comparison
    // This is crucial for lexicographical ordering when converting back and forth.
    while bytes.len() < min_len {
        bytes.push(0x00);
    }
    BigUint::from_bytes_be(&bytes)
}

/// Converts a BigUint back to a string, ensuring all characters are visible ASCII.
/// It converts the BigUint to a byte array, trims trailing null bytes (0x00),
/// and then collects bytes only until the first non-visible ASCII character is encountered.
/// The resulting byte array is then converted to a String using lossy UTF-8 conversion.
fn biguint_to_string_trimmed(n: &BigUint) -> String {
    let mut bytes = n.to_bytes_be();
    // Trim trailing null bytes to get the original string length or closest representation
    while let Some(&0x00) = bytes.last() {
        bytes.pop();
    }

    // Collect bytes only as long as they are visible ASCII characters (0x20 to 0x7E).
    // The string is truncated at the first non-visible character.
    let mut visible_ascii_bytes: Vec<u8> = Vec::new();
    for &b in bytes.iter() {
        if b >= 0x28 && b <= 0x7E {
            visible_ascii_bytes.push(b);
        } else {
            // Stop collecting if a non-visible ASCII character is found
            break;
        }
    }

    // Convert the collected bytes to a String.
    String::from_utf8_lossy(&visible_ascii_bytes).into_owned()
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
/// # Panics
/// This function will panic if `start` or `end` strings are empty,
/// or if `num_segments` is zero and `start` is not equal to `end`.
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
    start: &str,
    end: &str,
    num_segments: u32,
    hardstops: &[&str],
) -> Result<Vec<String>> {
    // Input validation
    if start.is_empty() || end.is_empty() {
        InvalidPartitionRangeSnafu {
            start: start.to_string(),
            end: end.to_string(),
        }
        .fail()?;
    }
    if num_segments < 2 {
        InvalidPartitionNumberSnafu {
            partition_num: num_segments,
        }
        .fail()?;
    }

    // Determine the maximum length for consistent padding during BigUint conversion.
    // This ensures that "a" and "aa" are treated differently, and "a" is padded to "a\0"
    // if compared to "aa".
    let mut max_len = max(start.len(), end.len());
    for hs in hardstops {
        max_len = max(max_len, hs.len());
    }

    let start_biguint = string_to_biguint_padded(start, max_len);
    let end_biguint = string_to_biguint_padded(end, max_len);

    // Handle invalid range
    if start_biguint >= end_biguint {
        InvalidPartitionRangeSnafu {
            start: start.to_string(),
            end: end.to_string(),
        }
        .fail()?;
    }

    let range = &end_biguint - &start_biguint;
    let num_segments_biguint = num_segments.to_biguint().unwrap();

    // Calculate the step size. If range is smaller than num_segments, step might be 0.
    let step = range / num_segments_biguint;

    let mut computed_stops_with_values: Vec<(BigUint, String)> =
        Vec::with_capacity(num_segments as usize);

    // Generate intermediate stops based on the new logic.

    // Generate intermediate stops, excluding the very last one (end).
    // The loop goes from 1 to num_segments - 1.
    for i in 1..num_segments {
        // Note: `..num_segments` excludes `num_segments`
        let current_biguint = &start_biguint + &step * i.to_biguint().unwrap();
        let stop_str = biguint_to_string_trimmed(&current_biguint);
        computed_stops_with_values.push((current_biguint, stop_str));
    }

    // Handle hardstops if provided

    let mut final_stops_with_values = computed_stops_with_values.clone();
    // Keep track of which computed stops have been replaced by a hardstop
    let mut used_computed_indices: Vec<bool> = vec![false; final_stops_with_values.len()];

    for hardstop_str in hardstops.iter() {
        let hardstop_biguint = string_to_biguint_padded(hardstop_str, max_len);

        let mut min_diff: Option<BigUint> = None;
        let mut nearest_idx: Option<usize> = None;

        // Find the nearest *unused* computed stop
        for (idx, (computed_val, _)) in computed_stops_with_values.iter().enumerate() {
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
    start: &str,
    end: &str,
    num_partitions: u32,
    hardstops: &[&str],
) -> Result<Vec<Expr>> {
    let stops = divide_string_range(start, end, num_partitions, hardstops)?;

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
        assert!(divide_string_range("", "", 10, &[]).is_err());
        assert!(divide_string_range("a", "a", 10, &[]).is_err());
        assert!(divide_string_range("z", "a", 10, &[]).is_err());
        assert!(divide_string_range("a", "b", 0, &[]).is_err());
        assert!(divide_string_range("a", "b", 1, &[]).is_err());

        let stops = divide_string_range("a", "z", 10, &[]).expect("failed to divide string range");
        assert_eq!(stops, vec!["c", "e", "g", "i", "k", "m", "o", "q", "s"]);

        let stops = divide_string_range("ap-southeast-1", "us-west-2", 4, &[])
            .expect("failed to divide string range");
        assert_eq!(stops, vec!["fp", "kq", "prmvg"]);

        let stops = divide_string_range(
            "ap-southeast-1",
            "us-west-2",
            10,
            &["eu-central-1", "us-east-1"],
        )
        .expect("failed to divide string range");
        assert_eq!(
            stops,
            vec![
                "cpz@",
                "eu-central-1",
                "gq",
                "iq`",
                "kq",
                "mq",
                "orG",
                "qr",
                "us-east-1"
            ]
        );
    }

    #[test]
    fn test_generate_partition_expr() {
        let rules = partition_rule_for_range(
            "ns",
            "ap-southeast-1",
            "us-west-2",
            10,
            &["eu-central-1", "us-east-1"],
        )
        .expect("failed to divide string range");
        assert_eq!(
            rules.iter().map(|e| e.to_string()).collect::<Vec<String>>(),
            vec![
                "ns < 'cpz@'",
                "ns >= 'cpz@' AND ns < 'eu-central-1'",
                "ns >= 'eu-central-1' AND ns < 'gq'",
                "ns >= 'gq' AND ns < 'iq`'",
                "ns >= 'iq`' AND ns < 'kq'",
                "ns >= 'kq' AND ns < 'mq'",
                "ns >= 'mq' AND ns < 'orG'",
                "ns >= 'orG' AND ns < 'qr'",
                "ns >= 'us-east-1'"
            ]
        );
    }
}
