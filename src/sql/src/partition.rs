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

use sqlparser::ast::{BinaryOperator, Expr, Ident, Value};

use crate::statements::create::Partitions;

const DEFAULT_PARTITION_NUM_FOR_TRACES: u32 = 16;

macro_rules! between_string {
    ($col: expr, $left_incl: expr, $right_excl: expr) => {
        Expr::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expr::BinaryOp {
                op: BinaryOperator::GtEq,
                left: Box::new($col.clone()),
                right: Box::new(Expr::Value(Value::SingleQuotedString(
                    $left_incl.to_string(),
                ))),
            }),
            right: Box::new(Expr::BinaryOp {
                op: BinaryOperator::Lt,
                left: Box::new($col.clone()),
                right: Box::new(Expr::Value(Value::SingleQuotedString(
                    $right_excl.to_string(),
                ))),
            }),
        }
    };
}

pub fn partition_rule_for_hexstring(ident: &str) -> Partitions {
    Partitions {
        column_list: vec![Ident::new(ident)],
        exprs: partition_rules_for_uuid(DEFAULT_PARTITION_NUM_FOR_TRACES, ident),
    }
}

// partition_rules_for_uuid can creates partition rules up to 256 partitions.
fn partition_rules_for_uuid(partition_num: u32, ident: &str) -> Vec<Expr> {
    let ident_expr = Expr::Identifier(Ident::new(ident).clone());

    let total_partitions = 256;
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
            // Create the leftmost rule, for example: trace_id < '10'.
            rules.push(Expr::BinaryOp {
                left: Box::new(ident_expr.clone()),
                op: BinaryOperator::Lt,
                right: Box::new(Expr::Value(Value::SingleQuotedString(format!(
                    "{:02x}",
                    end
                )))),
            });
        } else if i == partition_num - 1 {
            // Create the rightmost rule, for example: trace_id >= 'f0'.
            rules.push(Expr::BinaryOp {
                left: Box::new(ident_expr.clone()),
                op: BinaryOperator::GtEq,
                right: Box::new(Expr::Value(Value::SingleQuotedString(format!(
                    "{:02x}",
                    start
                )))),
            });
        } else {
            // Create the middle rules, for example: trace_id >= '10' AND trace_id < '20'.
            rules.push(between_string!(
                ident_expr,
                format!("{:02x}", start),
                format!("{:02x}", end)
            ));
        }

        current_boundary = end;
    }

    rules
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use sqlparser::ast::Expr;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use uuid::Uuid;

    use super::*;

    #[test]
    fn test_partition_rules_for_uuid() {
        assert!(check_distribution(16, 10000));
        assert!(check_distribution(32, 10000));
        assert!(check_distribution(64, 10000));
        assert!(check_distribution(128, 10000));
        assert!(check_distribution(256, 10000));
    }

    #[test]
    fn test_rules() {
        let expr = vec![
            "trace_id < '10'",
            "trace_id >= '10' AND trace_id < '20'",
            "trace_id >= '20' AND trace_id < '30'",
            "trace_id >= '30' AND trace_id < '40'",
            "trace_id >= '40' AND trace_id < '50'",
            "trace_id >= '50' AND trace_id < '60'",
            "trace_id >= '60' AND trace_id < '70'",
            "trace_id >= '70' AND trace_id < '80'",
            "trace_id >= '80' AND trace_id < '90'",
            "trace_id >= '90' AND trace_id < 'a0'",
            "trace_id >= 'a0' AND trace_id < 'b0'",
            "trace_id >= 'b0' AND trace_id < 'c0'",
            "trace_id >= 'c0' AND trace_id < 'd0'",
            "trace_id >= 'd0' AND trace_id < 'e0'",
            "trace_id >= 'e0' AND trace_id < 'f0'",
            "trace_id >= 'f0'",
        ];

        let dialect = GenericDialect {};
        let results = expr
            .into_iter()
            .map(|s| {
                let mut parser = Parser::new(&dialect).try_with_sql(s).unwrap();
                parser.parse_expr().unwrap()
            })
            .collect::<Vec<Expr>>();

        assert_eq!(results, partition_rule_for_hexstring("trace_id").exprs);
    }

    fn check_distribution(test_partition: u32, test_uuid_num: usize) -> bool {
        // Generate test_uuid_num random uuids.
        let uuids = (0..test_uuid_num)
            .map(|_| Uuid::new_v4().to_string().replace("-", ""))
            .collect::<Vec<String>>();

        // Generate the partition rules.
        let rules = partition_rules_for_uuid(test_partition, "test_trace_id");

        // Collect the number of partitions for each uuid.
        let mut stats = HashMap::new();
        for uuid in uuids {
            let partition = allocate_partition_for_uuid(uuid.clone(), &rules);
            // Count the number of uuids in each partition.
            *stats.entry(partition).or_insert(0) += 1;
        }

        // Check if the partition distribution is uniform.
        let expected_ratio = 100.0 / test_partition as f64;

        // delta is the allowed deviation from the expected ratio.
        let delta = 0.9;

        // For each partition, its ratio should be as close as possible to the expected ratio.
        for (_, count) in stats {
            let ratio = (count as f64 / test_uuid_num as f64) * 100.0;
            if (ratio - expected_ratio).abs() >= delta {
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
                    if let Expr::Value(Value::SingleQuotedString(leftmost)) = *right.clone() {
                        if uuid < leftmost {
                            return i;
                        }
                    }
                } else if i == rules.len() - 1 {
                    // Hit the rightmost rule.
                    if let Expr::Value(Value::SingleQuotedString(rightmost)) = *right.clone() {
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
                        if let Expr::Value(Value::SingleQuotedString(lower)) = *inner_right.clone()
                        {
                            if let Expr::BinaryOp {
                                left: _,
                                op: _,
                                right: inner_right,
                            } = *right.clone()
                            {
                                if let Expr::Value(Value::SingleQuotedString(upper)) =
                                    *inner_right.clone()
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

        panic!("No partition found for uuid: {}", uuid);
    }
}
