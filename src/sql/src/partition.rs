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

use snafu::ensure;
use sqlparser::ast::{BinaryOperator, Expr, Ident, Value};

use crate::error::{InvalidPartitionNumberSnafu, Result};
use crate::statements::create::Partitions;

/// The default number of partitions for OpenTelemetry traces.
const DEFAULT_PARTITION_NUM_FOR_TRACES: u32 = 16;

/// The maximum number of partitions for OpenTelemetry traces.
const MAX_PARTITION_NUM_FOR_TRACES: u32 = 65536;

macro_rules! between_string {
    ($col: expr, $left_incl: expr, $right_excl: expr) => {
        Expr::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expr::BinaryOp {
                op: BinaryOperator::GtEq,
                left: Box::new($col.clone()),
                right: Box::new(Expr::Value(
                    Value::SingleQuotedString($left_incl.to_string()).into(),
                )),
            }),
            right: Box::new(Expr::BinaryOp {
                op: BinaryOperator::Lt,
                left: Box::new($col.clone()),
                right: Box::new(Expr::Value(
                    Value::SingleQuotedString($right_excl.to_string()).into(),
                )),
            }),
        }
    };
}

pub fn partition_rule_for_hexstring(ident: &str) -> Result<Partitions> {
    Ok(Partitions {
        column_list: vec![Ident::new(ident)],
        exprs: partition_rules_for_uuid(DEFAULT_PARTITION_NUM_FOR_TRACES, ident)?,
    })
}

// partition_rules_for_uuid can creates partition rules up to 65536 partitions.
fn partition_rules_for_uuid(partition_num: u32, ident: &str) -> Result<Vec<Expr>> {
    ensure!(
        partition_num.is_power_of_two() && (2..=65536).contains(&partition_num),
        InvalidPartitionNumberSnafu { partition_num }
    );

    let ident_expr = Expr::Identifier(Ident::new(ident).clone());

    let (total_partitions, hex_length) = {
        match partition_num {
            2..=16 => (16, 1),
            17..=256 => (256, 2),
            257..=4096 => (4096, 3),
            4097..=MAX_PARTITION_NUM_FOR_TRACES => (MAX_PARTITION_NUM_FOR_TRACES, 4),
            _ => unreachable!(),
        }
    };

    let partition_size = total_partitions / partition_num;
    let remainder = total_partitions % partition_num;

    let mut rules = Vec::new();
    let mut current_boundary = 0;
    for i in 0..partition_num {
        let mut size = partition_size;
        if i < remainder {
            size += 1;
        }
        let start = current_boundary;
        let end = current_boundary + size;

        if i == 0 {
            // Create the leftmost rule, for example: trace_id < '1'.
            rules.push(Expr::BinaryOp {
                left: Box::new(ident_expr.clone()),
                op: BinaryOperator::Lt,
                right: Box::new(Expr::Value(
                    Value::SingleQuotedString(format!("{:0hex_length$x}", end)).into(),
                )),
            });
        } else if i == partition_num - 1 {
            // Create the rightmost rule, for example: trace_id >= 'f'.
            rules.push(Expr::BinaryOp {
                left: Box::new(ident_expr.clone()),
                op: BinaryOperator::GtEq,
                right: Box::new(Expr::Value(
                    Value::SingleQuotedString(format!("{:0hex_length$x}", start)).into(),
                )),
            });
        } else {
            // Create the middle rules, for example: trace_id >= '1' AND trace_id < '2'.
            rules.push(between_string!(
                ident_expr,
                format!("{:0hex_length$x}", start),
                format!("{:0hex_length$x}", end)
            ));
        }

        current_boundary = end;
    }

    Ok(rules)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use sqlparser::ast::{Expr, ValueWithSpan};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use uuid::Uuid;

    use super::*;

    #[test]
    fn test_partition_rules_for_uuid() {
        // NOTE: We only test a subset of partitions to keep the test execution time reasonable.
        // As the number of partitions increases, we need to increase the number of test samples to ensure uniform distribution.
        assert!(check_distribution(2, 10_000)); // 2^1
        assert!(check_distribution(4, 10_000)); // 2^2
        assert!(check_distribution(8, 10_000)); // 2^3
        assert!(check_distribution(16, 10_000)); // 2^4
        assert!(check_distribution(32, 10_000)); // 2^5
        assert!(check_distribution(64, 100_000)); // 2^6
        assert!(check_distribution(128, 100_000)); // 2^7
        assert!(check_distribution(256, 100_000)); // 2^8
    }

    #[test]
    fn test_rules() {
        let expr = vec![
            "trace_id < '1'",
            "trace_id >= '1' AND trace_id < '2'",
            "trace_id >= '2' AND trace_id < '3'",
            "trace_id >= '3' AND trace_id < '4'",
            "trace_id >= '4' AND trace_id < '5'",
            "trace_id >= '5' AND trace_id < '6'",
            "trace_id >= '6' AND trace_id < '7'",
            "trace_id >= '7' AND trace_id < '8'",
            "trace_id >= '8' AND trace_id < '9'",
            "trace_id >= '9' AND trace_id < 'a'",
            "trace_id >= 'a' AND trace_id < 'b'",
            "trace_id >= 'b' AND trace_id < 'c'",
            "trace_id >= 'c' AND trace_id < 'd'",
            "trace_id >= 'd' AND trace_id < 'e'",
            "trace_id >= 'e' AND trace_id < 'f'",
            "trace_id >= 'f'",
        ];

        let dialect = GenericDialect {};
        let results = expr
            .into_iter()
            .map(|s| {
                let mut parser = Parser::new(&dialect).try_with_sql(s).unwrap();
                parser.parse_expr().unwrap()
            })
            .collect::<Vec<Expr>>();

        assert_eq!(
            results,
            partition_rule_for_hexstring("trace_id").unwrap().exprs
        );
    }

    fn check_distribution(test_partition: u32, test_uuid_num: usize) -> bool {
        // Generate test_uuid_num random uuids.
        let uuids = (0..test_uuid_num)
            .map(|_| Uuid::new_v4().to_string().replace("-", "").to_lowercase())
            .collect::<Vec<String>>();

        // Generate the partition rules.
        let rules = partition_rules_for_uuid(test_partition, "test_trace_id").unwrap();

        // Collect the number of partitions for each uuid.
        let mut stats = HashMap::new();
        for uuid in uuids {
            let partition = allocate_partition_for_uuid(uuid.clone(), &rules);
            // Count the number of uuids in each partition.
            *stats.entry(partition).or_insert(0) += 1;
        }

        // Check if the partition distribution is uniform.
        let expected_ratio = 100.0 / test_partition as f64;

        // tolerance is the allowed deviation from the expected ratio.
        let tolerance = 100.0 / test_partition as f64 * 0.30;

        // For each partition, its ratio should be as close as possible to the expected ratio.
        for (_, count) in stats {
            let ratio = (count as f64 / test_uuid_num as f64) * 100.0;
            if (ratio - expected_ratio).abs() >= tolerance {
                return false;
            }
        }

        true
    }

    fn allocate_partition_for_uuid(uuid: String, rules: &[Expr]) -> usize {
        for (i, rule) in rules.iter().enumerate() {
            if let Expr::BinaryOp { left, op: _, right } = rule {
                if i == 0 {
                    // Hit the leftmost rule.
                    if let Expr::Value(ValueWithSpan {
                        value: Value::SingleQuotedString(leftmost),
                        ..
                    }) = *right.clone()
                    {
                        if uuid < leftmost {
                            return i;
                        }
                    }
                } else if i == rules.len() - 1 {
                    // Hit the rightmost rule.
                    if let Expr::Value(ValueWithSpan {
                        value: Value::SingleQuotedString(rightmost),
                        ..
                    }) = *right.clone()
                    {
                        if uuid >= rightmost {
                            return i;
                        }
                    }
                } else {
                    // Hit the middle rules.
                    if let Expr::BinaryOp {
                        left: _,
                        op: _,
                        right: inner_right,
                    } = *left.clone()
                    {
                        if let Expr::Value(ValueWithSpan {
                            value: Value::SingleQuotedString(lower),
                            ..
                        }) = *inner_right.clone()
                        {
                            if let Expr::BinaryOp {
                                left: _,
                                op: _,
                                right: inner_right,
                            } = *right.clone()
                            {
                                if let Expr::Value(ValueWithSpan {
                                    value: Value::SingleQuotedString(upper),
                                    ..
                                }) = *inner_right.clone()
                                {
                                    if uuid >= lower && uuid < upper {
                                        return i;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        panic!("No partition found for uuid: {}, rules: {:?}", uuid, rules);
    }
}
