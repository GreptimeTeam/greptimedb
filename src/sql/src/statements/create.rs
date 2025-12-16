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

use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};

use common_catalog::consts::FILE_ENGINE;
use datatypes::json::JsonStructureSettings;
use datatypes::schema::{
    FulltextOptions, SkippingIndexOptions, VectorDistanceMetric, VectorIndexEngineType,
    VectorIndexOptions,
};
use itertools::Itertools;
use serde::Serialize;
use snafu::ResultExt;
use sqlparser::ast::{ColumnOptionDef, DataType, Expr, Query};
use sqlparser_derive::{Visit, VisitMut};

use crate::ast::{ColumnDef, Ident, ObjectName, Value as SqlValue};
use crate::error::{
    InvalidFlowQuerySnafu, InvalidSqlSnafu, Result, SetFulltextOptionSnafu,
    SetSkippingIndexOptionSnafu,
};
use crate::statements::OptionMap;
use crate::statements::statement::Statement;
use crate::statements::tql::Tql;
use crate::util::OptionValue;

const LINE_SEP: &str = ",\n";
const COMMA_SEP: &str = ", ";
const INDENT: usize = 2;
pub const VECTOR_OPT_DIM: &str = "dim";

pub const JSON_OPT_UNSTRUCTURED_KEYS: &str = "unstructured_keys";
pub const JSON_OPT_FORMAT: &str = "format";
pub const JSON_FORMAT_FULL_STRUCTURED: &str = "structured";
pub const JSON_FORMAT_RAW: &str = "raw";
pub const JSON_FORMAT_PARTIAL: &str = "partial";

macro_rules! format_indent {
    ($fmt: expr, $arg: expr) => {
        format!($fmt, format_args!("{: >1$}", "", INDENT), $arg)
    };
    ($arg: expr) => {
        format_indent!("{}{}", $arg)
    };
}

macro_rules! format_list_indent {
    ($list: expr) => {
        $list.iter().map(|e| format_indent!(e)).join(LINE_SEP)
    };
}

macro_rules! format_list_comma {
    ($list: expr) => {
        $list.iter().map(|e| format!("{}", e)).join(COMMA_SEP)
    };
}

#[cfg(feature = "enterprise")]
pub mod trigger;

fn format_table_constraint(constraints: &[TableConstraint]) -> String {
    constraints.iter().map(|c| format_indent!(c)).join(LINE_SEP)
}

/// Table constraint for create table statement.
#[derive(Debug, PartialEq, Eq, Clone, Visit, VisitMut, Serialize)]
pub enum TableConstraint {
    /// Primary key constraint.
    PrimaryKey { columns: Vec<Ident> },
    /// Time index constraint.
    TimeIndex { column: Ident },
}

impl Display for TableConstraint {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TableConstraint::PrimaryKey { columns } => {
                write!(f, "PRIMARY KEY ({})", format_list_comma!(columns))
            }
            TableConstraint::TimeIndex { column } => {
                write!(f, "TIME INDEX ({})", column)
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Visit, VisitMut, Serialize)]
pub struct CreateTable {
    /// Create if not exists
    pub if_not_exists: bool,
    pub table_id: u32,
    /// Table name
    pub name: ObjectName,
    pub columns: Vec<Column>,
    pub engine: String,
    pub constraints: Vec<TableConstraint>,
    /// Table options in `WITH`. All keys are lowercase.
    pub options: OptionMap,
    pub partitions: Option<Partitions>,
}

/// Column definition in `CREATE TABLE` statement.
#[derive(Debug, PartialEq, Eq, Clone, Visit, VisitMut, Serialize)]
pub struct Column {
    /// `ColumnDef` from `sqlparser::ast`
    pub column_def: ColumnDef,
    /// Column extensions for greptimedb dialect.
    pub extensions: ColumnExtensions,
}

/// Column extensions for greptimedb dialect.
#[derive(Debug, PartialEq, Eq, Clone, Visit, VisitMut, Default, Serialize)]
pub struct ColumnExtensions {
    /// Vector type options.
    pub vector_options: Option<OptionMap>,

    /// Fulltext index options.
    pub fulltext_index_options: Option<OptionMap>,
    /// Skipping index options.
    pub skipping_index_options: Option<OptionMap>,
    /// Inverted index options.
    ///
    /// Inverted index doesn't have options at present. There won't be any options in that map.
    pub inverted_index_options: Option<OptionMap>,
    /// Vector index options for HNSW-based vector similarity search.
    pub vector_index_options: Option<OptionMap>,
    pub json_datatype_options: Option<OptionMap>,
}

impl Column {
    pub fn name(&self) -> &Ident {
        &self.column_def.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.column_def.data_type
    }

    pub fn mut_data_type(&mut self) -> &mut DataType {
        &mut self.column_def.data_type
    }

    pub fn options(&self) -> &[ColumnOptionDef] {
        &self.column_def.options
    }

    pub fn mut_options(&mut self) -> &mut Vec<ColumnOptionDef> {
        &mut self.column_def.options
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(vector_options) = &self.extensions.vector_options
            && let Some(dim) = vector_options.get(VECTOR_OPT_DIM)
        {
            write!(f, "{} VECTOR({})", self.column_def.name, dim)?;
            return Ok(());
        }

        write!(f, "{} {}", self.column_def.name, self.column_def.data_type)?;
        if let Some(options) = &self.extensions.json_datatype_options {
            write!(
                f,
                "({})",
                options
                    .entries()
                    .map(|(k, v)| format!("{k} = {v}"))
                    .join(COMMA_SEP)
            )?;
        }
        for option in &self.column_def.options {
            write!(f, " {option}")?;
        }

        if let Some(fulltext_options) = &self.extensions.fulltext_index_options {
            if !fulltext_options.is_empty() {
                let options = fulltext_options.kv_pairs();
                write!(f, " FULLTEXT INDEX WITH({})", format_list_comma!(options))?;
            } else {
                write!(f, " FULLTEXT INDEX")?;
            }
        }

        if let Some(skipping_index_options) = &self.extensions.skipping_index_options {
            if !skipping_index_options.is_empty() {
                let options = skipping_index_options.kv_pairs();
                write!(f, " SKIPPING INDEX WITH({})", format_list_comma!(options))?;
            } else {
                write!(f, " SKIPPING INDEX")?;
            }
        }

        if let Some(inverted_index_options) = &self.extensions.inverted_index_options {
            if !inverted_index_options.is_empty() {
                let options = inverted_index_options.kv_pairs();
                write!(f, " INVERTED INDEX WITH({})", format_list_comma!(options))?;
            } else {
                write!(f, " INVERTED INDEX")?;
            }
        }

        if let Some(vector_index_options) = &self.extensions.vector_index_options {
            if !vector_index_options.is_empty() {
                let options = vector_index_options.kv_pairs();
                write!(f, " VECTOR INDEX WITH({})", format_list_comma!(options))?;
            } else {
                write!(f, " VECTOR INDEX")?;
            }
        }
        Ok(())
    }
}

impl ColumnExtensions {
    pub fn build_fulltext_options(&self) -> Result<Option<FulltextOptions>> {
        let Some(options) = self.fulltext_index_options.as_ref() else {
            return Ok(None);
        };

        let options: HashMap<String, String> = options.clone().into_map();
        Ok(Some(options.try_into().context(SetFulltextOptionSnafu)?))
    }

    pub fn build_skipping_index_options(&self) -> Result<Option<SkippingIndexOptions>> {
        let Some(options) = self.skipping_index_options.as_ref() else {
            return Ok(None);
        };

        let options: HashMap<String, String> = options.clone().into_map();
        Ok(Some(
            options.try_into().context(SetSkippingIndexOptionSnafu)?,
        ))
    }

    pub fn build_vector_index_options(&self) -> Result<Option<VectorIndexOptions>> {
        let Some(options) = self.vector_index_options.as_ref() else {
            return Ok(None);
        };

        let options_map: HashMap<String, String> = options.clone().into_map();
        let mut result = VectorIndexOptions::default();

        if let Some(s) = options_map.get("engine") {
            result.engine = s.parse::<VectorIndexEngineType>().map_err(|e| {
                InvalidSqlSnafu {
                    msg: format!("invalid VECTOR INDEX engine: {e}"),
                }
                .build()
            })?;
        }

        if let Some(s) = options_map.get("metric") {
            result.metric = s.parse::<VectorDistanceMetric>().map_err(|e| {
                InvalidSqlSnafu {
                    msg: format!("invalid VECTOR INDEX metric: {e}"),
                }
                .build()
            })?;
        }

        if let Some(s) = options_map.get("connectivity") {
            let value = s.parse::<u32>().map_err(|_| {
                InvalidSqlSnafu {
                    msg: format!(
                        "invalid VECTOR INDEX connectivity: {s}, expected positive integer"
                    ),
                }
                .build()
            })?;
            if !(2..=2048).contains(&value) {
                return InvalidSqlSnafu {
                    msg: "VECTOR INDEX connectivity must be in the range [2, 2048].".to_string(),
                }
                .fail();
            }
            result.connectivity = value;
        }

        if let Some(s) = options_map.get("expansion_add") {
            let value = s.parse::<u32>().map_err(|_| {
                InvalidSqlSnafu {
                    msg: format!(
                        "invalid VECTOR INDEX expansion_add: {s}, expected positive integer"
                    ),
                }
                .build()
            })?;
            if value == 0 {
                return InvalidSqlSnafu {
                    msg: "VECTOR INDEX expansion_add must be greater than 0".to_string(),
                }
                .fail();
            }
            result.expansion_add = value;
        }

        if let Some(s) = options_map.get("expansion_search") {
            let value = s.parse::<u32>().map_err(|_| {
                InvalidSqlSnafu {
                    msg: format!(
                        "invalid VECTOR INDEX expansion_search: {s}, expected positive integer"
                    ),
                }
                .build()
            })?;
            if value == 0 {
                return InvalidSqlSnafu {
                    msg: "VECTOR INDEX expansion_search must be greater than 0".to_string(),
                }
                .fail();
            }
            result.expansion_search = value;
        }

        Ok(Some(result))
    }

    pub fn build_json_structure_settings(&self) -> Result<Option<JsonStructureSettings>> {
        let Some(options) = self.json_datatype_options.as_ref() else {
            return Ok(None);
        };

        let unstructured_keys = options
            .value(JSON_OPT_UNSTRUCTURED_KEYS)
            .and_then(|v| {
                v.as_list().map(|x| {
                    x.into_iter()
                        .map(|x| x.to_string())
                        .collect::<HashSet<String>>()
                })
            })
            .unwrap_or_default();

        options
            .get(JSON_OPT_FORMAT)
            .map(|format| match format {
                JSON_FORMAT_FULL_STRUCTURED => Ok(JsonStructureSettings::Structured(None)),
                JSON_FORMAT_PARTIAL => Ok(JsonStructureSettings::PartialUnstructuredByKey {
                    fields: None,
                    unstructured_keys,
                }),
                JSON_FORMAT_RAW => Ok(JsonStructureSettings::UnstructuredRaw),
                _ => InvalidSqlSnafu {
                    msg: format!("unknown JSON datatype 'format': {format}"),
                }
                .fail(),
            })
            .transpose()
    }

    pub fn set_json_structure_settings(&mut self, settings: JsonStructureSettings) {
        let mut map = OptionMap::default();

        let format = match settings {
            JsonStructureSettings::Structured(_) => JSON_FORMAT_FULL_STRUCTURED,
            JsonStructureSettings::PartialUnstructuredByKey { .. } => JSON_FORMAT_PARTIAL,
            JsonStructureSettings::UnstructuredRaw => JSON_FORMAT_RAW,
        };
        map.insert(JSON_OPT_FORMAT.to_string(), format.to_string());

        if let JsonStructureSettings::PartialUnstructuredByKey {
            fields: _,
            unstructured_keys,
        } = settings
        {
            let value = OptionValue::from(
                unstructured_keys
                    .iter()
                    .map(|x| x.as_str())
                    .sorted()
                    .collect::<Vec<_>>(),
            );
            map.insert_options(JSON_OPT_UNSTRUCTURED_KEYS, value);
        }

        self.json_datatype_options = Some(map);
    }
}

/// Partition on columns or values.
///
/// - `column_list` is the list of columns in `PARTITION ON COLUMNS` clause.
/// - `exprs` is the list of expressions in `PARTITION ON VALUES` clause, like
///   `host <= 'host1'`, `host > 'host1' and host <= 'host2'` or `host > 'host2'`.
///   Each expression stands for a partition.
#[derive(Debug, PartialEq, Eq, Clone, Visit, VisitMut, Serialize)]
pub struct Partitions {
    pub column_list: Vec<Ident>,
    pub exprs: Vec<Expr>,
}

impl Partitions {
    /// set quotes to all [Ident]s from column list
    pub fn set_quote(&mut self, quote_style: char) {
        self.column_list
            .iter_mut()
            .for_each(|c| c.quote_style = Some(quote_style));
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Visit, VisitMut)]
pub struct PartitionEntry {
    pub name: Ident,
    pub value_list: Vec<SqlValue>,
}

impl Display for PartitionEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PARTITION {} VALUES LESS THAN ({})",
            self.name,
            format_list_comma!(self.value_list),
        )
    }
}

impl Display for Partitions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if !self.column_list.is_empty() {
            write!(
                f,
                "PARTITION ON COLUMNS ({}) (\n{}\n)",
                format_list_comma!(self.column_list),
                format_list_indent!(self.exprs),
            )?;
        }
        Ok(())
    }
}

impl Display for CreateTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE ")?;
        if self.engine == FILE_ENGINE {
            write!(f, "EXTERNAL ")?;
        }
        write!(f, "TABLE ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        writeln!(f, "{} (", &self.name)?;
        writeln!(f, "{},", format_list_indent!(self.columns))?;
        writeln!(f, "{}", format_table_constraint(&self.constraints))?;
        writeln!(f, ")")?;
        if let Some(partitions) = &self.partitions {
            writeln!(f, "{partitions}")?;
        }
        writeln!(f, "ENGINE={}", &self.engine)?;
        if !self.options.is_empty() {
            let options = self.options.kv_pairs();
            write!(f, "WITH(\n{}\n)", format_list_indent!(options))?;
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Visit, VisitMut, Serialize)]
pub struct CreateDatabase {
    pub name: ObjectName,
    /// Create if not exists
    pub if_not_exists: bool,
    pub options: OptionMap,
}

impl CreateDatabase {
    /// Creates a statement for `CREATE DATABASE`
    pub fn new(name: ObjectName, if_not_exists: bool, options: OptionMap) -> Self {
        Self {
            name,
            if_not_exists,
            options,
        }
    }
}

impl Display for CreateDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE DATABASE ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(f, "{}", &self.name)?;
        if !self.options.is_empty() {
            let options = self.options.kv_pairs();
            write!(f, "\nWITH(\n{}\n)", format_list_indent!(options))?;
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Visit, VisitMut, Serialize)]
pub struct CreateExternalTable {
    /// Table name
    pub name: ObjectName,
    pub columns: Vec<Column>,
    pub constraints: Vec<TableConstraint>,
    /// Table options in `WITH`. All keys are lowercase.
    pub options: OptionMap,
    pub if_not_exists: bool,
    pub engine: String,
}

impl Display for CreateExternalTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE EXTERNAL TABLE ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        writeln!(f, "{} (", &self.name)?;
        writeln!(f, "{},", format_list_indent!(self.columns))?;
        writeln!(f, "{}", format_table_constraint(&self.constraints))?;
        writeln!(f, ")")?;
        writeln!(f, "ENGINE={}", &self.engine)?;
        if !self.options.is_empty() {
            let options = self.options.kv_pairs();
            write!(f, "WITH(\n{}\n)", format_list_indent!(options))?;
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Visit, VisitMut, Serialize)]
pub struct CreateTableLike {
    /// Table name
    pub table_name: ObjectName,
    /// The table that is designated to be imitated by `Like`
    pub source_name: ObjectName,
}

impl Display for CreateTableLike {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let table_name = &self.table_name;
        let source_name = &self.source_name;
        write!(f, r#"CREATE TABLE {table_name} LIKE {source_name}"#)
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Visit, VisitMut, Serialize)]
pub struct CreateFlow {
    /// Flow name
    pub flow_name: ObjectName,
    /// Output (sink) table name
    pub sink_table_name: ObjectName,
    /// Whether to replace existing task
    pub or_replace: bool,
    /// Create if not exist
    pub if_not_exists: bool,
    /// `EXPIRE AFTER`
    /// Duration in second as `i64`
    pub expire_after: Option<i64>,
    /// Duration for flow evaluation interval
    /// Duration in seconds as `i64`
    /// If not set, flow will be evaluated based on time window size and other args.
    pub eval_interval: Option<i64>,
    /// Comment string
    pub comment: Option<String>,
    /// SQL statement
    pub query: Box<SqlOrTql>,
}

/// Either a sql query or a tql query
#[derive(Debug, PartialEq, Eq, Clone, Visit, VisitMut, Serialize)]
pub enum SqlOrTql {
    Sql(Query, String),
    Tql(Tql, String),
}

impl std::fmt::Display for SqlOrTql {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sql(_, s) => write!(f, "{}", s),
            Self::Tql(_, s) => write!(f, "{}", s),
        }
    }
}

impl SqlOrTql {
    pub fn try_from_statement(
        value: Statement,
        original_query: &str,
    ) -> std::result::Result<Self, crate::error::Error> {
        match value {
            Statement::Query(query) => {
                Ok(Self::Sql((*query).try_into()?, original_query.to_string()))
            }
            Statement::Tql(tql) => Ok(Self::Tql(tql, original_query.to_string())),
            _ => InvalidFlowQuerySnafu {
                reason: format!("Expect either sql query or promql query, found {:?}", value),
            }
            .fail(),
        }
    }
}

impl Display for CreateFlow {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE ")?;
        if self.or_replace {
            write!(f, "OR REPLACE ")?;
        }
        write!(f, "FLOW ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        writeln!(f, "{}", &self.flow_name)?;
        writeln!(f, "SINK TO {}", &self.sink_table_name)?;
        if let Some(expire_after) = &self.expire_after {
            writeln!(f, "EXPIRE AFTER '{} s'", expire_after)?;
        }
        if let Some(eval_interval) = &self.eval_interval {
            writeln!(f, "EVAL INTERVAL '{} s'", eval_interval)?;
        }
        if let Some(comment) = &self.comment {
            writeln!(f, "COMMENT '{}'", comment)?;
        }
        write!(f, "AS {}", &self.query)
    }
}

/// Create SQL view statement.
#[derive(Debug, PartialEq, Eq, Clone, Visit, VisitMut, Serialize)]
pub struct CreateView {
    /// View name
    pub name: ObjectName,
    /// An optional list of names to be used for columns of the view
    pub columns: Vec<Ident>,
    /// The clause after `As` that defines the VIEW.
    /// Can only be either [Statement::Query] or [Statement::Tql].
    pub query: Box<Statement>,
    /// Whether to replace existing VIEW
    pub or_replace: bool,
    /// Create VIEW only when it doesn't exists
    pub if_not_exists: bool,
}

impl Display for CreateView {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE ")?;
        if self.or_replace {
            write!(f, "OR REPLACE ")?;
        }
        write!(f, "VIEW ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(f, "{} ", &self.name)?;
        if !self.columns.is_empty() {
            write!(f, "({}) ", format_list_comma!(self.columns))?;
        }
        write!(f, "AS {}", &self.query)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use crate::dialect::GreptimeDbDialect;
    use crate::error::Error;
    use crate::parser::{ParseOptions, ParserContext};
    use crate::statements::statement::Statement;

    #[test]
    fn test_display_create_table() {
        let sql = r"create table if not exists demo(
                             host string,
                             ts timestamp,
                             cpu double default 0,
                             memory double,
                             TIME INDEX (ts),
                             PRIMARY KEY(host)
                       )
                       PARTITION ON COLUMNS (host) (
                            host = 'a',
                            host > 'a',
                       )
                       engine=mito
                       with(ttl='7d', storage='File');
         ";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());

        match &result[0] {
            Statement::CreateTable(c) => {
                let new_sql = format!("\n{}", c);
                assert_eq!(
                    r#"
CREATE TABLE IF NOT EXISTS demo (
  host STRING,
  ts TIMESTAMP,
  cpu DOUBLE DEFAULT 0,
  memory DOUBLE,
  TIME INDEX (ts),
  PRIMARY KEY (host)
)
PARTITION ON COLUMNS (host) (
  host = 'a',
  host > 'a'
)
ENGINE=mito
WITH(
  storage = 'File',
  ttl = '7d'
)"#,
                    &new_sql
                );

                let new_result = ParserContext::create_with_dialect(
                    &new_sql,
                    &GreptimeDbDialect {},
                    ParseOptions::default(),
                )
                .unwrap();
                assert_eq!(result, new_result);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_display_empty_partition_column() {
        let sql = r"create table if not exists demo(
            host string,
            ts timestamp,
            cpu double default 0,
            memory double,
            TIME INDEX (ts),
            PRIMARY KEY(ts, host)
            );
        ";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());

        match &result[0] {
            Statement::CreateTable(c) => {
                let new_sql = format!("\n{}", c);
                assert_eq!(
                    r#"
CREATE TABLE IF NOT EXISTS demo (
  host STRING,
  ts TIMESTAMP,
  cpu DOUBLE DEFAULT 0,
  memory DOUBLE,
  TIME INDEX (ts),
  PRIMARY KEY (ts, host)
)
ENGINE=mito
"#,
                    &new_sql
                );

                let new_result = ParserContext::create_with_dialect(
                    &new_sql,
                    &GreptimeDbDialect {},
                    ParseOptions::default(),
                )
                .unwrap();
                assert_eq!(result, new_result);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_validate_table_options() {
        let sql = r"create table if not exists demo(
            host string,
            ts timestamp,
            cpu double default 0,
            memory double,
            TIME INDEX (ts),
            PRIMARY KEY(host)
      )
      PARTITION ON COLUMNS (host) ()
      engine=mito
      with(ttl='7d', 'compaction.type'='world');
";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        match &result[0] {
            Statement::CreateTable(c) => {
                assert_eq!(2, c.options.len());
            }
            _ => unreachable!(),
        }

        let sql = r"create table if not exists demo(
            host string,
            ts timestamp,
            cpu double default 0,
            memory double,
            TIME INDEX (ts),
            PRIMARY KEY(host)
      )
      PARTITION ON COLUMNS (host) ()
      engine=mito
      with(ttl='7d', hello='world');
";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert_matches!(result, Err(Error::InvalidTableOption { .. }))
    }

    #[test]
    fn test_display_create_database() {
        let sql = r"create database test;";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::CreateDatabase { .. });

        match &stmts[0] {
            Statement::CreateDatabase(set) => {
                let new_sql = format!("\n{}", set);
                assert_eq!(
                    r#"
CREATE DATABASE test"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }

        let sql = r"create database if not exists test;";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::CreateDatabase { .. });

        match &stmts[0] {
            Statement::CreateDatabase(set) => {
                let new_sql = format!("\n{}", set);
                assert_eq!(
                    r#"
CREATE DATABASE IF NOT EXISTS test"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }

        let sql = r#"CREATE DATABASE IF NOT EXISTS test WITH (ttl='1h');"#;
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::CreateDatabase { .. });

        match &stmts[0] {
            Statement::CreateDatabase(set) => {
                let new_sql = format!("\n{}", set);
                assert_eq!(
                    r#"
CREATE DATABASE IF NOT EXISTS test
WITH(
  ttl = '1h'
)"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }
    }

    #[test]
    fn test_display_create_table_like() {
        let sql = r"create table t2 like t1;";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::CreateTableLike { .. });

        match &stmts[0] {
            Statement::CreateTableLike(create) => {
                let new_sql = format!("\n{}", create);
                assert_eq!(
                    r#"
CREATE TABLE t2 LIKE t1"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }
    }

    #[test]
    fn test_display_create_external_table() {
        let sql = r#"CREATE EXTERNAL TABLE city (
            host string,
            ts timestamp,
            cpu float64 default 0,
            memory float64,
            TIME INDEX (ts),
            PRIMARY KEY(host)
) WITH (location='/var/data/city.csv', format='csv');"#;
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::CreateExternalTable { .. });

        match &stmts[0] {
            Statement::CreateExternalTable(create) => {
                let new_sql = format!("\n{}", create);
                assert_eq!(
                    r#"
CREATE EXTERNAL TABLE city (
  host STRING,
  ts TIMESTAMP,
  cpu DOUBLE DEFAULT 0,
  memory DOUBLE,
  TIME INDEX (ts),
  PRIMARY KEY (host)
)
ENGINE=file
WITH(
  format = 'csv',
  location = '/var/data/city.csv'
)"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }
    }

    #[test]
    fn test_display_create_flow() {
        let sql = r"CREATE FLOW filter_numbers
            SINK TO out_num_cnt
            AS SELECT number FROM numbers_input where number > 10;";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());

        match &result[0] {
            Statement::CreateFlow(c) => {
                let new_sql = format!("\n{}", c);
                assert_eq!(
                    r#"
CREATE FLOW filter_numbers
SINK TO out_num_cnt
AS SELECT number FROM numbers_input where number > 10"#,
                    &new_sql
                );

                let new_result = ParserContext::create_with_dialect(
                    &new_sql,
                    &GreptimeDbDialect {},
                    ParseOptions::default(),
                )
                .unwrap();
                assert_eq!(result, new_result);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_vector_index_options_validation() {
        use super::{ColumnExtensions, OptionMap};

        // Test zero connectivity should fail
        let extensions = ColumnExtensions {
            fulltext_index_options: None,
            vector_options: None,
            skipping_index_options: None,
            inverted_index_options: None,
            json_datatype_options: None,
            vector_index_options: Some(OptionMap::from([(
                "connectivity".to_string(),
                "0".to_string(),
            )])),
        };
        let result = extensions.build_vector_index_options();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("connectivity must be in the range [2, 2048]")
        );

        // Test zero expansion_add should fail
        let extensions = ColumnExtensions {
            fulltext_index_options: None,
            vector_options: None,
            skipping_index_options: None,
            inverted_index_options: None,
            json_datatype_options: None,
            vector_index_options: Some(OptionMap::from([(
                "expansion_add".to_string(),
                "0".to_string(),
            )])),
        };
        let result = extensions.build_vector_index_options();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("expansion_add must be greater than 0")
        );

        // Test zero expansion_search should fail
        let extensions = ColumnExtensions {
            fulltext_index_options: None,
            vector_options: None,
            skipping_index_options: None,
            inverted_index_options: None,
            json_datatype_options: None,
            vector_index_options: Some(OptionMap::from([(
                "expansion_search".to_string(),
                "0".to_string(),
            )])),
        };
        let result = extensions.build_vector_index_options();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("expansion_search must be greater than 0")
        );

        // Test valid values should succeed
        let extensions = ColumnExtensions {
            fulltext_index_options: None,
            vector_options: None,
            skipping_index_options: None,
            inverted_index_options: None,
            json_datatype_options: None,
            vector_index_options: Some(OptionMap::from([
                ("connectivity".to_string(), "32".to_string()),
                ("expansion_add".to_string(), "200".to_string()),
                ("expansion_search".to_string(), "100".to_string()),
            ])),
        };
        let result = extensions.build_vector_index_options();
        assert!(result.is_ok());
        let options = result.unwrap().unwrap();
        assert_eq!(options.connectivity, 32);
        assert_eq!(options.expansion_add, 200);
        assert_eq!(options.expansion_search, 100);
    }
}
