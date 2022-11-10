use std::cmp::Ordering;

use itertools::Itertools;
use once_cell::sync::Lazy;
use snafu::ResultExt;
use snafu::{ensure, OptionExt};
use sqlparser::ast::Value;
use sqlparser::dialect::keywords::Keyword;
use sqlparser::parser::IsOptional::Mandatory;
use sqlparser::tokenizer::{Token, Word};
use table_engine::engine;

use crate::ast::{ColumnDef, Ident, TableConstraint, Value as SqlValue};
use crate::error::{self, InvalidTimeIndexSnafu, Result, SyntaxSnafu};
use crate::parser::ParserContext;
use crate::statements::create::{
    CreateDatabase, CreateTable, PartitionEntry, Partitions, TIME_INDEX,
};
use crate::statements::statement::Statement;
use crate::statements::{sql_data_type_to_concrete_data_type, sql_value_to_value};

const ENGINE: &str = "ENGINE";
const MAXVALUE: &str = "MAXVALUE";

static LESS: Lazy<Token> = Lazy::new(|| Token::make_keyword("LESS"));
static THAN: Lazy<Token> = Lazy::new(|| Token::make_keyword("THAN"));

/// Parses create [table] statement
impl<'a> ParserContext<'a> {
    pub(crate) fn parse_create(&mut self) -> Result<Statement> {
        match self.parser.peek_token() {
            Token::Word(w) => match w.keyword {
                Keyword::TABLE => self.parse_create_table(),

                Keyword::DATABASE => self.parse_create_database(),

                _ => self.unsupported(w.to_string()),
            },
            unexpected => self.unsupported(unexpected.to_string()),
        }
    }

    fn parse_create_database(&mut self) -> Result<Statement> {
        self.parser.next_token();

        let database_name = self
            .parser
            .parse_object_name()
            .context(error::UnexpectedSnafu {
                sql: self.sql,
                expected: "a database name",
                actual: self.peek_token_as_string(),
            })?;

        Ok(Statement::CreateDatabase(CreateDatabase {
            name: database_name,
        }))
    }

    fn parse_create_table(&mut self) -> Result<Statement> {
        self.parser.next_token();
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

        let table_name = self
            .parser
            .parse_object_name()
            .context(error::UnexpectedSnafu {
                sql: self.sql,
                expected: "a table name",
                actual: self.peek_token_as_string(),
            })?;

        let (columns, constraints) = self.parse_columns()?;

        let partitions = self.parse_partitions()?;

        let engine = self.parse_table_engine()?;
        let options = self
            .parser
            .parse_options(Keyword::WITH)
            .context(error::SyntaxSnafu { sql: self.sql })?;

        let create_table = CreateTable {
            if_not_exists,
            name: table_name,
            columns,
            engine,
            constraints,
            options,
            table_id: 0, // table id is assigned by catalog manager
            partitions,
        };
        validate_create(&create_table)?;

        Ok(Statement::CreateTable(create_table))
    }

    // "PARTITION BY ..." syntax:
    // https://dev.mysql.com/doc/refman/8.0/en/partitioning-columns-range.html
    fn parse_partitions(&mut self) -> Result<Option<Partitions>> {
        if !self.parser.parse_keyword(Keyword::PARTITION) {
            return Ok(None);
        }
        self.parser
            .expect_keywords(&[Keyword::BY, Keyword::RANGE, Keyword::COLUMNS])
            .context(error::UnexpectedSnafu {
                sql: self.sql,
                expected: "BY, RANGE, COLUMNS",
                actual: self.peek_token_as_string(),
            })?;

        let column_list = self
            .parser
            .parse_parenthesized_column_list(Mandatory)
            .context(error::SyntaxSnafu { sql: self.sql })?;

        let entries = self.parse_comma_separated(Self::parse_partition_entry)?;

        Ok(Some(Partitions {
            column_list,
            entries,
        }))
    }

    fn parse_partition_entry(&mut self) -> Result<PartitionEntry> {
        self.parser
            .expect_keyword(Keyword::PARTITION)
            .context(error::UnexpectedSnafu {
                sql: self.sql,
                expected: "PARTITION",
                actual: self.peek_token_as_string(),
            })?;

        let name = self
            .parser
            .parse_identifier()
            .context(error::SyntaxSnafu { sql: self.sql })?;

        self.parser
            .expect_keyword(Keyword::VALUES)
            .and_then(|_| self.parser.expect_token(&LESS))
            .and_then(|_| self.parser.expect_token(&THAN))
            .context(error::SyntaxSnafu { sql: self.sql })?;

        let value_list = self.parse_comma_separated(Self::parse_value_list)?;

        Ok(PartitionEntry { name, value_list })
    }

    fn parse_value_list(&mut self) -> Result<SqlValue> {
        let token = self.parser.peek_token();
        let value = match token {
            Token::Word(Word { value, .. }) if value == MAXVALUE => {
                let _ = self.parser.next_token();
                SqlValue::Number(MAXVALUE.to_string(), false)
            }
            _ => self
                .parser
                .parse_value()
                .context(error::SyntaxSnafu { sql: self.sql })?,
        };
        Ok(value)
    }

    /// Parse a comma-separated list wrapped by "()", and of which all items accepted by `F`
    fn parse_comma_separated<T, F>(&mut self, mut f: F) -> Result<Vec<T>>
    where
        F: FnMut(&mut ParserContext<'a>) -> Result<T>,
    {
        self.parser
            .expect_token(&Token::LParen)
            .context(error::UnexpectedSnafu {
                sql: self.sql,
                expected: "(",
                actual: self.peek_token_as_string(),
            })?;

        let mut values = vec![];
        while self.parser.peek_token() != Token::RParen {
            values.push(f(self)?);
            if !self.parser.consume_token(&Token::Comma) {
                break;
            }
        }

        self.parser
            .expect_token(&Token::RParen)
            .context(error::UnexpectedSnafu {
                sql: self.sql,
                expected: ")",
                actual: self.peek_token_as_string(),
            })?;

        Ok(values)
    }

    fn parse_columns(&mut self) -> Result<(Vec<ColumnDef>, Vec<TableConstraint>)> {
        let mut columns = vec![];
        let mut constraints = vec![];
        if !self.parser.consume_token(&Token::LParen) || self.parser.consume_token(&Token::RParen) {
            return Ok((columns, constraints));
        }

        loop {
            if let Some(constraint) = self.parse_optional_table_constraint()? {
                constraints.push(constraint);
            } else if let Token::Word(_) = self.parser.peek_token() {
                columns.push(
                    self.parser
                        .parse_column_def()
                        .context(SyntaxSnafu { sql: self.sql })?,
                );
            } else {
                return self.expected(
                    "column name or constraint definition",
                    self.parser.peek_token(),
                );
            }
            let comma = self.parser.consume_token(&Token::Comma);
            if self.parser.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected(
                    "',' or ')' after column definition",
                    self.parser.peek_token(),
                );
            }
        }

        Ok((columns, constraints))
    }

    // Copy from sqlparser by boyan
    fn parse_optional_table_constraint(&mut self) -> Result<Option<TableConstraint>> {
        let name = if self.parser.parse_keyword(Keyword::CONSTRAINT) {
            Some(
                self.parser
                    .parse_identifier()
                    .context(error::SyntaxSnafu { sql: self.sql })?,
            )
        } else {
            None
        };
        match self.parser.next_token() {
            Token::Word(w) if w.keyword == Keyword::PRIMARY => {
                self.parser
                    .expect_keyword(Keyword::KEY)
                    .context(error::UnexpectedSnafu {
                        sql: self.sql,
                        expected: "KEY",
                        actual: self.peek_token_as_string(),
                    })?;
                let columns = self
                    .parser
                    .parse_parenthesized_column_list(Mandatory)
                    .context(error::SyntaxSnafu { sql: self.sql })?;
                Ok(Some(TableConstraint::Unique {
                    name,
                    columns,
                    is_primary: true,
                }))
            }
            Token::Word(w) if w.keyword == Keyword::TIME => {
                self.parser
                    .expect_keyword(Keyword::INDEX)
                    .context(error::UnexpectedSnafu {
                        sql: self.sql,
                        expected: "INDEX",
                        actual: self.peek_token_as_string(),
                    })?;

                let columns = self
                    .parser
                    .parse_parenthesized_column_list(Mandatory)
                    .context(error::SyntaxSnafu { sql: self.sql })?;

                ensure!(columns.len() == 1, InvalidTimeIndexSnafu { sql: self.sql });

                // TODO(dennis): TableConstraint doesn't support dialect right now,
                // so we use unique constraint with special key to represent TIME INDEX.
                Ok(Some(TableConstraint::Unique {
                    name: Some(Ident {
                        value: TIME_INDEX.to_owned(),
                        quote_style: None,
                    }),
                    columns,
                    is_primary: false,
                }))
            }
            unexpected => {
                if name.is_some() {
                    self.expected("PRIMARY, TIME", unexpected)
                } else {
                    self.parser.prev_token();
                    Ok(None)
                }
            }
        }
    }

    /// Parses the set of valid formats
    fn parse_table_engine(&mut self) -> Result<String> {
        if !self.consume_token(ENGINE) {
            return Ok(engine::MITO_ENGINE.to_string());
        }

        self.parser
            .expect_token(&Token::Eq)
            .context(error::UnexpectedSnafu {
                sql: self.sql,
                expected: "=",
                actual: self.peek_token_as_string(),
            })?;

        match self.parser.next_token() {
            Token::Word(w) => Ok(w.value),
            unexpected => self.expected("Engine is missing", unexpected),
        }
    }
}

fn validate_create(create_table: &CreateTable) -> Result<()> {
    if let Some(partitions) = &create_table.partitions {
        validate_partitions(&create_table.columns, partitions)?;
    }
    Ok(())
}

fn validate_partitions(columns: &[ColumnDef], partitions: &Partitions) -> Result<()> {
    let partition_columns = ensure_partition_columns_defined(columns, partitions)?;

    ensure_partition_names_no_duplicate(partitions)?;

    ensure_value_list_len_matches_columns(partitions, &partition_columns)?;

    let value_lists = ensure_value_lists_strictly_increased(partitions, partition_columns)?;

    ensure_value_lists_bounded_by_maxvalue(value_lists)?;

    Ok(())
}

/// Ensure that partition ranges fully cover all values.
// Simply check the last partition is bounded by "MAXVALUE"s.
// MySQL does not have this restriction. However, I think we'd better have it because:
//   - It might save user from adding more partitions in the future by hand, which is often
//     a tedious task. Why not provide an extra partition at the beginning and leave all
//     other partition related jobs to us? I think it's a reasonable argument to user.
//   - It might save us from some ugly designs and codings. The "MAXVALUE" bound is natural
//     in dealing with values that are unspecified upfront. Without it, we have to store
//     and use the user defined max bound everywhere, starting from calculating regions by
//     partition rule in Frontend, to automatically split and merge regions in Meta.
fn ensure_value_lists_bounded_by_maxvalue(value_lists: Vec<&Vec<Value>>) -> Result<()> {
    let is_maxvalue_bound = value_lists.last().map(|v| {
        v.iter()
            .all(|x| matches!(x, SqlValue::Number(s, _) if s == MAXVALUE))
    });
    ensure!(
        matches!(is_maxvalue_bound, Some(true)),
        error::InvalidSqlSnafu {
            msg: "Please provide an extra partition that is bounded by 'MAXVALUE'."
        }
    );
    Ok(())
}

/// Ensure that value lists of partitions are strictly increasing.
fn ensure_value_lists_strictly_increased<'a>(
    partitions: &'a Partitions,
    partition_columns: Vec<&'a ColumnDef>,
) -> Result<Vec<&'a Vec<Value>>> {
    let value_lists = partitions
        .entries
        .iter()
        .map(|x| &x.value_list)
        .collect::<Vec<_>>();
    for i in 1..value_lists.len() {
        let mut equal_tuples = 0;
        for (n, (x, y)) in value_lists[i - 1]
            .iter()
            .zip(value_lists[i].iter())
            .enumerate()
        {
            let column = partition_columns[n];
            let is_x_maxvalue = matches!(x, SqlValue::Number(s, _) if s == MAXVALUE);
            let is_y_maxvalue = matches!(y, SqlValue::Number(s, _) if s == MAXVALUE);
            match (is_x_maxvalue, is_y_maxvalue) {
                (true, true) => {
                    equal_tuples += 1;
                }
                (false, false) => {
                    let column_name = &column.name.value;
                    let cdt = sql_data_type_to_concrete_data_type(&column.data_type)?;
                    let x = sql_value_to_value(column_name, &cdt, x)?;
                    let y = sql_value_to_value(column_name, &cdt, y)?;
                    match x.cmp(&y) {
                        Ordering::Less => break,
                        Ordering::Equal => equal_tuples += 1,
                        Ordering::Greater => return error::InvalidSqlSnafu {
                            msg: "VALUES LESS THAN value must be strictly increasing for each partition.",
                        }.fail()
                    }
                }
                (true, false) => return error::InvalidSqlSnafu {
                    msg: "VALUES LESS THAN value must be strictly increasing for each partition.",
                }
                .fail(),
                (false, true) => break,
            }
        }
        ensure!(
            equal_tuples < partition_columns.len(),
            error::InvalidSqlSnafu {
                msg: "VALUES LESS THAN value must be strictly increasing for each partition.",
            }
        );
    }
    Ok(value_lists)
}

/// Ensure that value list's length matches the column list.
fn ensure_value_list_len_matches_columns(
    partitions: &Partitions,
    partition_columns: &Vec<&ColumnDef>,
) -> Result<()> {
    for entry in partitions.entries.iter() {
        ensure!(
            entry.value_list.len() == partition_columns.len(),
            error::InvalidSqlSnafu {
                msg: "Partition value list does not match column list.",
            }
        );
    }
    Ok(())
}

/// Ensure that all columns used in "PARTITION BY RANGE COLUMNS" are defined in create table.
fn ensure_partition_columns_defined<'a>(
    columns: &'a [ColumnDef],
    partitions: &'a Partitions,
) -> Result<Vec<&'a ColumnDef>> {
    partitions
        .column_list
        .iter()
        .map(|x| {
            // Normally the columns in "create table" won't be too many,
            // a linear search to find the target every time is fine.
            columns
                .iter()
                .find(|c| &c.name == x)
                .context(error::InvalidSqlSnafu {
                    msg: format!("Partition column {:?} not defined!", x.value),
                })
        })
        .collect::<Result<Vec<&ColumnDef>>>()
}

/// Ensure that partition names do not duplicate.
fn ensure_partition_names_no_duplicate(partitions: &Partitions) -> Result<()> {
    let partition_names = partitions
        .entries
        .iter()
        .map(|x| &x.name.value)
        .sorted()
        .collect::<Vec<&String>>();
    for w in partition_names.windows(2) {
        ensure!(
            w[0] != w[1],
            error::InvalidSqlSnafu {
                msg: format!("Duplicate partition names: {}", w[0]),
            }
        )
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use sqlparser::dialect::GenericDialect;

    use super::*;

    #[test]
    fn test_parse_create_database() {
        let sql = "create database";
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {});
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unexpected token while parsing SQL statement"));

        let sql = "create database prometheus";
        let stmts = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();

        assert_eq!(1, stmts.len());
        match &stmts[0] {
            Statement::CreateDatabase(c) => {
                assert_eq!(c.name.to_string(), "prometheus");
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_validate_create() {
        let sql = r"
CREATE TABLE rcx ( a INT, b STRING, c INT )
PARTITION BY RANGE COLUMNS(b, a) (
  PARTITION r0 VALUES LESS THAN ('hz', 1000),
  PARTITION r1 VALUES LESS THAN ('sh', 2000),
  PARTITION r3 VALUES LESS THAN (MAXVALUE, MAXVALUE),
)
ENGINE=mito";
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {});
        assert!(result.is_ok());

        let sql = r"
CREATE TABLE rcx ( a INT, b STRING, c INT )
PARTITION BY RANGE COLUMNS(b, x) (
  PARTITION r0 VALUES LESS THAN ('hz', 1000),
  PARTITION r1 VALUES LESS THAN ('sh', 2000),
  PARTITION r3 VALUES LESS THAN (MAXVALUE, MAXVALUE),
)
ENGINE=mito";
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {});
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Partition column \"x\" not defined!"));

        let sql = r"
CREATE TABLE rcx ( a INT, b STRING, c INT )
PARTITION BY RANGE COLUMNS(b, a) (
  PARTITION r0 VALUES LESS THAN ('hz', 1000),
  PARTITION r1 VALUES LESS THAN ('sh', 2000),
  PARTITION r2 VALUES LESS THAN ('sz', 3000),
  PARTITION r1 VALUES LESS THAN (MAXVALUE, MAXVALUE),
)
ENGINE=mito";
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {});
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Duplicate partition names: r1"));

        let sql = r"
CREATE TABLE rcx ( a INT, b STRING, c INT )
PARTITION BY RANGE COLUMNS(b, a) (
  PARTITION r0 VALUES LESS THAN ('hz', 1000),
  PARTITION r1 VALUES LESS THAN ('sh'),
  PARTITION r3 VALUES LESS THAN (MAXVALUE, MAXVALUE),
)
ENGINE=mito";
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {});
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Partition value list does not match column list"));

        let cases = vec![
            r"
CREATE TABLE rcx ( a INT, b STRING, c INT )
PARTITION BY RANGE COLUMNS(b, a) (
  PARTITION r0 VALUES LESS THAN ('sh', 1000),
  PARTITION r1 VALUES LESS THAN ('hz', 2000),
  PARTITION r3 VALUES LESS THAN (MAXVALUE, MAXVALUE),
)
ENGINE=mito",
            r"
CREATE TABLE rcx ( a INT, b STRING, c INT )
PARTITION BY RANGE COLUMNS(b, a) (
  PARTITION r0 VALUES LESS THAN ('hz', 2000),
  PARTITION r1 VALUES LESS THAN ('hz', 1000),
  PARTITION r3 VALUES LESS THAN (MAXVALUE, MAXVALUE),
)
ENGINE=mito",
            r"
CREATE TABLE rcx ( a INT, b STRING, c INT )
PARTITION BY RANGE COLUMNS(b, a) (
  PARTITION r0 VALUES LESS THAN ('hz', 1000),
  PARTITION r1 VALUES LESS THAN ('hz', 1000),
  PARTITION r3 VALUES LESS THAN (MAXVALUE, MAXVALUE),
)
ENGINE=mito",
            r"
CREATE TABLE rcx ( a INT, b STRING, c INT )
PARTITION BY RANGE COLUMNS(b, a) (
  PARTITION r0 VALUES LESS THAN ('hz', 1000),
  PARTITION r3 VALUES LESS THAN (MAXVALUE, 2000),
  PARTITION r1 VALUES LESS THAN ('sh', 3000),
)
ENGINE=mito",
        ];
        for sql in cases {
            let result = ParserContext::create_with_dialect(sql, &GenericDialect {});
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("VALUES LESS THAN value must be strictly increasing for each partition"));
        }

        let sql = r"
CREATE TABLE rcx ( a INT, b STRING, c INT )
PARTITION BY RANGE COLUMNS(b, a) (
  PARTITION r0 VALUES LESS THAN ('hz', 1000),
  PARTITION r1 VALUES LESS THAN ('sh', 2000),
  PARTITION r3 VALUES LESS THAN (MAXVALUE, 9999),
)
ENGINE=mito";
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {});
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Please provide an extra partition that is bounded by 'MAXVALUE'."));
    }

    #[test]
    fn test_parse_create_table_with_partitions() {
        let sql = r"
CREATE TABLE monitor (
  host_id    INT,
  idc        STRING,
  ts         TIMESTAMP,
  cpu        DOUBLE DEFAULT 0,
  memory     DOUBLE,
  TIME INDEX (ts),
  PRIMARY KEY (host),
)
PARTITION BY RANGE COLUMNS(idc, host_id) (
  PARTITION r0 VALUES LESS THAN ('hz', 1000),
  PARTITION r1 VALUES LESS THAN ('sh', 2000),
  PARTITION r2 VALUES LESS THAN ('sh', 3000),
  PARTITION r3 VALUES LESS THAN (MAXVALUE, MAXVALUE),
)
ENGINE=mito";
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();
        assert_eq!(result.len(), 1);
        match &result[0] {
            Statement::CreateTable(c) => {
                assert!(c.partitions.is_some());

                let partitions = c.partitions.as_ref().unwrap();
                let column_list = partitions
                    .column_list
                    .iter()
                    .map(|x| &x.value)
                    .collect::<Vec<&String>>();
                assert_eq!(column_list, vec!["idc", "host_id"]);

                let entries = &partitions.entries;
                let partition_names = entries
                    .iter()
                    .map(|x| &x.name.value)
                    .collect::<Vec<&String>>();
                assert_eq!(partition_names, vec!["r0", "r1", "r2", "r3"]);

                assert_eq!(
                    entries[0].value_list,
                    vec![
                        SqlValue::SingleQuotedString("hz".to_string()),
                        SqlValue::Number("1000".to_string(), false)
                    ]
                );
                assert_eq!(
                    entries[1].value_list,
                    vec![
                        SqlValue::SingleQuotedString("sh".to_string()),
                        SqlValue::Number("2000".to_string(), false)
                    ]
                );
                assert_eq!(
                    entries[2].value_list,
                    vec![
                        SqlValue::SingleQuotedString("sh".to_string()),
                        SqlValue::Number("3000".to_string(), false)
                    ]
                );
                assert_eq!(
                    entries[3].value_list,
                    vec![
                        SqlValue::Number(MAXVALUE.to_string(), false),
                        SqlValue::Number(MAXVALUE.to_string(), false)
                    ]
                );
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_partitions_with_error_syntax() {
        let sql = r"
CREATE TABLE rcx ( a INT, b STRING, c INT )
PARTITION RANGE COLUMNS(b, a) (
  PARTITION r0 VALUES LESS THAN ('hz', 1000),
  PARTITION r1 VALUES LESS THAN ('sh', 2000),
  PARTITION r3 VALUES LESS THAN (MAXVALUE, MAXVALUE),
)
ENGINE=mito";
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {});
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("sql parser error: Expected BY, found: RANGE"));

        let sql = r"
CREATE TABLE rcx ( a INT, b STRING, c INT )
PARTITION BY RANGE COLUMNS(b, a) (
  PARTITION r0 VALUES THAN ('hz', 1000),
  PARTITION r1 VALUES LESS THAN ('sh', 2000),
  PARTITION r3 VALUES LESS THAN (MAXVALUE, MAXVALUE),
)
ENGINE=mito";
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {});
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("sql parser error: Expected LESS, found: THAN"));

        let sql = r"
CREATE TABLE rcx ( a INT, b STRING, c INT )
PARTITION BY RANGE COLUMNS(b, a) (
  PARTITION r0 VALUES LESS THAN ('hz', 1000),
  PARTITION r1 VALUES LESS THAN ('sh', 2000),
  PARTITION r3 VALUES LESS THAN (MAXVALUE, MAXVALU),
)
ENGINE=mito";
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {});
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("sql parser error: Expected a concrete value, found: MAXVALU"));
    }

    fn assert_column_def(column: &ColumnDef, name: &str, data_type: &str) {
        assert_eq!(column.name.to_string(), name);
        assert_eq!(column.data_type.to_string(), data_type);
    }

    #[test]
    pub fn test_parse_create_table() {
        let sql = r"create table demo(
                             host string,
                             ts int64,
                             cpu float64 default 0,
                             memory float64,
                             TIME INDEX (ts),
                             PRIMARY KEY(ts, host)) engine=mito
                             with(regions=1);
         ";
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();
        assert_eq!(1, result.len());
        match &result[0] {
            Statement::CreateTable(c) => {
                assert!(!c.if_not_exists);
                assert_eq!("demo", c.name.to_string());
                assert_eq!("mito", c.engine);
                assert_eq!(4, c.columns.len());
                let columns = &c.columns;
                assert_column_def(&columns[0], "host", "STRING");
                assert_column_def(&columns[1], "ts", "int64");
                assert_column_def(&columns[2], "cpu", "float64");
                assert_column_def(&columns[3], "memory", "float64");
                let constraints = &c.constraints;
                assert_matches!(
                    &constraints[0],
                    TableConstraint::Unique {
                        is_primary: false,
                        ..
                    }
                );
                assert_matches!(
                    &constraints[1],
                    TableConstraint::Unique {
                        is_primary: true,
                        ..
                    }
                );
                let options = &c.options;
                assert_eq!(1, options.len());
                assert_eq!("regions", &options[0].name.to_string());
                assert_eq!("1", &options[0].value.to_string());
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_invalid_index_keys() {
        let sql = r"create table demo(
                             host string,
                             ts int64,
                             cpu float64 default 0,
                             memory float64,
                             TIME INDEX (ts, host),
                             PRIMARY KEY(ts, host)) engine=mito
                             with(regions=1);
         ";
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {});
        assert!(result.is_err());
        assert_matches!(result, Err(crate::error::Error::InvalidTimeIndex { .. }));
    }
}
