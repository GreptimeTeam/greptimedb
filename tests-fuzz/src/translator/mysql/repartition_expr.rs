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

use partition::expr::PartitionExpr;

use crate::error::Result;
use crate::ir::create_expr::PartitionDef;
use crate::ir::repartition_expr::{
    AlterTablePartitionsExpr, MergePartitionExpr, RepartitionExpr, SplitPartitionExpr,
};
use crate::translator::DslTranslator;

pub struct RepartitionExprTranslator;

impl DslTranslator<RepartitionExpr, String> for RepartitionExprTranslator {
    type Error = crate::error::Error;

    fn translate(&self, input: &RepartitionExpr) -> Result<String> {
        match input {
            RepartitionExpr::Split(SplitPartitionExpr {
                table_name,
                target,
                into,
                wait,
            }) => {
                let target_expr = format_partition_expr_sql(target);
                let into_exprs = into
                    .iter()
                    .map(format_partition_expr_sql)
                    .collect::<Vec<_>>()
                    .join(",\n  ");
                let wait_clause = format_wait_clause(*wait);
                Ok(format!(
                    "ALTER TABLE {} SPLIT PARTITION (\n  {}\n) INTO (\n  {}\n){};",
                    table_name, target_expr, into_exprs, wait_clause
                ))
            }
            RepartitionExpr::Merge(MergePartitionExpr {
                table_name,
                targets,
                wait,
            }) => {
                let merge_exprs = targets
                    .iter()
                    .map(format_partition_expr_sql)
                    .collect::<Vec<_>>()
                    .join(",\n  ");
                let wait_clause = format_wait_clause(*wait);
                Ok(format!(
                    "ALTER TABLE {} MERGE PARTITION (\n  {}\n){};",
                    table_name, merge_exprs, wait_clause
                ))
            }
            RepartitionExpr::AlterPartitions(AlterTablePartitionsExpr {
                table_name,
                partition,
                wait,
            }) => {
                let partition_clause = format_partition_clause(partition);
                let wait_clause = format_wait_clause(*wait);
                Ok(format!(
                    "ALTER TABLE {} {}{};",
                    table_name, partition_clause, wait_clause
                ))
            }
        }
    }
}

fn format_partition_clause(partition: &PartitionDef) -> String {
    let columns = partition
        .columns
        .iter()
        .map(|column| column.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let exprs = partition
        .exprs
        .iter()
        .map(format_partition_expr_sql)
        .collect::<Vec<_>>()
        .join(",\n  ");
    format!("PARTITION ON COLUMNS ({columns}) (\n  {exprs}\n)")
}

fn format_partition_expr_sql(expr: &PartitionExpr) -> String {
    expr.to_parser_expr().to_string()
}

fn format_wait_clause(wait: bool) -> String {
    if wait {
        String::new()
    } else {
        " WITH (\n  WAIT = false\n)".to_string()
    }
}

#[cfg(test)]
mod tests {
    use datatypes::value::Value;
    use partition::expr::col;
    use sql::dialect::GreptimeDbDialect;
    use sql::parser::{ParseOptions, ParserContext};

    use super::RepartitionExprTranslator;
    use crate::ir::Ident;
    use crate::ir::create_expr::PartitionDef;
    use crate::ir::repartition_expr::{
        AlterTablePartitionsExpr, MergePartitionExpr, RepartitionExpr, SplitPartitionExpr,
    };
    use crate::translator::DslTranslator;

    #[test]
    fn test_translate_split_expr() {
        let expr = RepartitionExpr::Split(SplitPartitionExpr {
            table_name: "demo".into(),
            target: col("id").lt(Value::Int32(10)),
            into: vec![
                col("id").lt(Value::Int32(5)),
                col("id")
                    .gt_eq(Value::Int32(5))
                    .and(col("id").lt(Value::Int32(10))),
            ],
            wait: true,
        });
        let sql = RepartitionExprTranslator.translate(&expr).unwrap();
        let expected = r#"ALTER TABLE demo SPLIT PARTITION (
  id < 10
) INTO (
  id < 5,
  id >= 5 AND id < 10
);"#;
        assert_eq!(sql, expected);
    }

    #[test]
    fn test_translate_merge_expr() {
        let expr = RepartitionExpr::Merge(MergePartitionExpr {
            table_name: "demo".into(),
            targets: vec![
                col("id").gt_eq(Value::Int32(10)),
                col("id").gt_eq(Value::Int32(20)),
            ],
            wait: true,
        });
        let sql = RepartitionExprTranslator.translate(&expr).unwrap();
        let expected = r#"ALTER TABLE demo MERGE PARTITION (
  id >= 10,
  id >= 20
);"#;
        assert_eq!(sql, expected);
    }

    #[test]
    fn test_translate_split_expr_wait_false() {
        let expr = RepartitionExpr::Split(SplitPartitionExpr {
            table_name: "demo".into(),
            target: col("id").lt(Value::Int32(10)),
            into: vec![
                col("id").lt(Value::Int32(5)),
                col("id")
                    .gt_eq(Value::Int32(5))
                    .and(col("id").lt(Value::Int32(10))),
            ],
            wait: false,
        });
        let sql = RepartitionExprTranslator.translate(&expr).unwrap();
        let expected = r#"ALTER TABLE demo SPLIT PARTITION (
  id < 10
) INTO (
  id < 5,
  id >= 5 AND id < 10
) WITH (
  WAIT = false
);"#;
        assert_eq!(sql, expected);
    }

    #[test]
    fn test_translate_alter_table_partitions_expr() {
        let expr = RepartitionExpr::AlterPartitions(AlterTablePartitionsExpr {
            table_name: "demo".into(),
            partition: PartitionDef {
                columns: vec![Ident::new("id")],
                exprs: vec![
                    col("id").lt(Value::Int32(10)),
                    col("id")
                        .gt_eq(Value::Int32(10))
                        .and(col("id").lt(Value::Int32(20))),
                    col("id").gt_eq(Value::Int32(20)),
                ],
            },
            wait: true,
        });
        let sql = RepartitionExprTranslator.translate(&expr).unwrap();
        let expected = r#"ALTER TABLE demo PARTITION ON COLUMNS (id) (
  id < 10,
  id >= 10 AND id < 20,
  id >= 20
);"#;
        assert_eq!(sql, expected);
        assert_repartition_sql_parseable(&sql);
    }

    #[test]
    fn test_translate_alter_table_partitions_expr_wait_false() {
        let expr = RepartitionExpr::AlterPartitions(AlterTablePartitionsExpr {
            table_name: "demo".into(),
            partition: PartitionDef {
                columns: vec![Ident::new("host")],
                exprs: vec![
                    col("host").lt(Value::from("m")),
                    col("host").gt_eq(Value::from("m")),
                ],
            },
            wait: false,
        });
        let sql = RepartitionExprTranslator.translate(&expr).unwrap();
        let expected = r#"ALTER TABLE demo PARTITION ON COLUMNS (host) (
  host < 'm',
  host >= 'm'
) WITH (
  WAIT = false
);"#;
        assert_eq!(sql, expected);
        assert_repartition_sql_parseable(&sql);
    }

    fn assert_repartition_sql_parseable(sql: &str) {
        let statements =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(statements.len(), 1);
    }
}
