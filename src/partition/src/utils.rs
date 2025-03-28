use api::helper::ColumnDataTypeWrapper;
use api::v1::{column_def, CreateTableExpr};
use common_meta::rpc::router::Partition as MetaPartition;
use common_time::Timezone;
use datafusion_common::HashMap;
use datatypes::prelude::ConcreteDataType;
use datatypes::value::Value;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};
use sql::error::ParseSqlValueSnafu;
use sql::statements::create::Partitions;
use sql::statements::sql_value_to_value;
use sqlparser::ast::{Expr, Ident, UnaryOperator, Value as ParserValue};

use crate::error::{self, Result};
use crate::expr::{Operand, PartitionExpr, RestrictedOp};
use crate::multi_dim::MultiDimPartitionRule;
use crate::partition::{PartitionBound, PartitionDef};

/// Parse [Partitions] into a group of partition entries.
///
/// Returns a list of [PartitionBound], each of which defines a partition.
fn find_partition_bounds(
    create_table: &CreateTableExpr,
    partitions: &Option<Partitions>,
    partition_columns: &[String],
    query_ctx: &QueryContextRef,
) -> Result<Vec<Vec<PartitionBound>>> {
    let entries = if let Some(partitions) = partitions {
        // extract concrete data type of partition columns
        let column_defs = partition_columns
            .iter()
            .map(|pc| {
                create_table
                    .column_defs
                    .iter()
                    .find(|c| &c.name == pc)
                    // unwrap is safe here because we have checked that partition columns are defined
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let mut column_name_and_type = HashMap::with_capacity(column_defs.len());
        for column in column_defs {
            let column_name = &column.name;
            let data_type = ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(column.data_type, column.datatype_extension)
                    .unwrap(),
            );
            column_name_and_type.insert(column_name, data_type);
        }

        // Transform parser expr to partition expr
        let mut partition_exprs = Vec::with_capacity(partitions.exprs.len());
        for partition in &partitions.exprs {
            let partition_expr =
                convert_one_expr(partition, &column_name_and_type, &query_ctx.timezone())?;
            partition_exprs.push(vec![PartitionBound::Expr(partition_expr)]);
        }

        // fallback for no expr
        if partition_exprs.is_empty() {
            partition_exprs.push(vec![PartitionBound::MaxValue]);
        }

        partition_exprs
    } else {
        vec![vec![PartitionBound::MaxValue]]
    };
    Ok(entries)
}

fn find_partition_columns(partitions: &Option<Partitions>) -> Result<Vec<String>> {
    let columns = if let Some(partitions) = partitions {
        partitions
            .column_list
            .iter()
            .map(|x| x.value.clone())
            .collect::<Vec<_>>()
    } else {
        vec![]
    };
    Ok(columns)
}

fn parse_partition_columns_and_exprs(
    create_table: &CreateTableExpr,
    partitions: Option<Partitions>,
    query_ctx: &QueryContextRef,
) -> Result<(Vec<String>, Vec<Vec<PartitionBound>>, Vec<PartitionExpr>)> {
    // If partitions are not defined by user, use the timestamp column (which has to be existed) as
    // the partition column, and create only one partition.
    let partition_columns = find_partition_columns(&partitions)?;
    let partition_bounds =
        find_partition_bounds(create_table, &partitions, &partition_columns, query_ctx)?;

    // Validates partition
    let mut exprs = vec![];
    for partition in &partition_bounds {
        for bound in partition {
            if let PartitionBound::Expr(expr) = bound {
                exprs.push(expr.clone());
            }
        }
    }
    Ok((partition_columns, partition_bounds, exprs))
}

/// Parse partition statement [Partitions] into [MetaPartition] and partition columns.
pub fn parse_partitions(
    create_table: &CreateTableExpr,
    partitions: Option<Partitions>,
    query_ctx: &QueryContextRef,
) -> Result<(Vec<MetaPartition>, Vec<String>)> {
    let (partition_columns, partition_bounds, exprs) =
        parse_partition_columns_and_exprs(create_table, partitions, query_ctx)?;

    // Check if the partition rule is valid
    MultiDimPartitionRule::try_new(partition_columns.clone(), vec![], exprs)?;

    Ok((
        partition_bounds
            .into_iter()
            .map(|x| MetaPartition::try_from(PartitionDef::new(partition_columns.clone(), x)))
            .collect::<std::result::Result<_, _>>()?,
        partition_columns,
    ))
}

// #[cfg(test)]
// pub(crate) fn parsr_sql_to_partition_rule(
//     sql: &str,
// ) -> Result<(Vec<String>, Vec<Vec<PartitionBound>>, Vec<PartitionExpr>)> {
//     use session::context::QueryContextBuilder;
//     use sql::dialect::GreptimeDbDialect;
//     use sql::parser::{ParseOptions, ParserContext};
//     use sql::statements::statement::Statement;

//     let ctx = QueryContextBuilder::default().build().into();
//     let result =
//         ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
//             .unwrap();
//     match &result[0] {
//         Statement::CreateTable(c) => {
//             let expr = expr_helper::create_to_expr(c, &QueryContext::arc()).unwrap();
//             let (partitions, _) = parse_partitions(&expr, c.partitions.clone(), &ctx).unwrap();
//         }
//         _ => unreachable!(),
//     }

//     parse_partition_columns_and_exprs(create_table, partitions, query_ctx)
// }

fn convert_one_expr(
    expr: &Expr,
    column_name_and_type: &HashMap<&String, ConcreteDataType>,
    timezone: &Timezone,
) -> Result<PartitionExpr> {
    let Expr::BinaryOp { left, op, right } = expr else {
        return error::InvalidPartitionRuleSnafu {
            reason: "partition rule must be a binary expression",
        }
        .fail();
    };

    let op = RestrictedOp::try_from_parser(&op.clone()).with_context(|| {
        error::InvalidPartitionRuleSnafu {
            reason: format!("unsupported operator in partition expr {op}"),
        }
    })?;

    // convert leaf node.
    let (lhs, op, rhs) = match (left.as_ref(), right.as_ref()) {
        // col, val
        (Expr::Identifier(ident), Expr::Value(value)) => {
            let (column_name, data_type) = convert_identifier(ident, column_name_and_type)?;
            let value = convert_value(value, data_type, timezone, None)?;
            (Operand::Column(column_name), op, Operand::Value(value))
        }
        (Expr::Identifier(ident), Expr::UnaryOp { op: unary_op, expr })
            if let Expr::Value(v) = &**expr =>
        {
            let (column_name, data_type) = convert_identifier(ident, column_name_and_type)?;
            let value = convert_value(v, data_type, timezone, Some(*unary_op))?;
            (Operand::Column(column_name), op, Operand::Value(value))
        }
        // val, col
        (Expr::Value(value), Expr::Identifier(ident)) => {
            let (column_name, data_type) = convert_identifier(ident, column_name_and_type)?;
            let value = convert_value(value, data_type, timezone, None)?;
            (Operand::Value(value), op, Operand::Column(column_name))
        }
        (Expr::UnaryOp { op: unary_op, expr }, Expr::Identifier(ident))
            if let Expr::Value(v) = &**expr =>
        {
            let (column_name, data_type) = convert_identifier(ident, column_name_and_type)?;
            let value = convert_value(v, data_type, timezone, Some(*unary_op))?;
            (Operand::Value(value), op, Operand::Column(column_name))
        }
        (Expr::BinaryOp { .. }, Expr::BinaryOp { .. }) => {
            // sub-expr must against another sub-expr
            let lhs = convert_one_expr(left, column_name_and_type, timezone)?;
            let rhs = convert_one_expr(right, column_name_and_type, timezone)?;
            (Operand::Expr(lhs), op, Operand::Expr(rhs))
        }
        _ => {
            return error::InvalidPartitionRuleSnafu {
                reason: format!("invalid partition expr {expr}"),
            }
            .fail();
        }
    };

    Ok(PartitionExpr::new(lhs, op, rhs))
}

fn convert_identifier(
    ident: &Ident,
    column_name_and_type: &HashMap<&String, ConcreteDataType>,
) -> Result<(String, ConcreteDataType)> {
    let column_name = ident.value.clone();
    let data_type = column_name_and_type
        .get(&column_name)
        .cloned()
        .with_context(|| error::ColumnNotFoundSnafu {
            column: column_name.clone(),
        })?;
    Ok((column_name, data_type))
}

fn convert_value(
    value: &ParserValue,
    data_type: ConcreteDataType,
    timezone: &Timezone,
    unary_op: Option<UnaryOperator>,
) -> Result<Value> {
    sql_value_to_value("<NONAME>", &data_type, value, Some(timezone), unary_op)
        .context(error::ParseSqlValueSnafu)
}
