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

use std::collections::HashMap;
use std::fmt::{Display, Formatter, Write};

use common_catalog::consts::FILE_ENGINE;
use datatypes::json::{JSON2_DEFAULT_MAX_AUTO_EXPANDED_PATHS, JsonSettings};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{FulltextOptions, SkippingIndexOptions};
use itertools::Itertools;
use serde::Serialize;
use snafu::ResultExt;
use sqlparser::ast::{ColumnOptionDef, DataType, Expr};
use sqlparser_derive::{Visit, VisitMut};

use crate::ast::{ColumnDef, Ident, ObjectName};
use crate::error::{
    InvalidFlowQuerySnafu, InvalidSqlSnafu, Result, SetFulltextOptionSnafu,
    SetSkippingIndexOptionSnafu,
};
use crate::statements::query::Query as GtQuery;
use crate::statements::statement::Statement;
use crate::statements::tql::Tql;
use crate::statements::{OptionMap, sql_data_type_to_concrete_data_type};

const LINE_SEP: &str = ",\n";
const COMMA_SEP: &str = ", ";
const INDENT: usize = 2;
pub const VECTOR_OPT_DIM: &str = "dim";

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

fn write_list<T: Display>(
    f: &mut dyn Write,
    values: &[T],
    separator: &str,
    prefix: &str,
) -> std::fmt::Result {
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            f.write_str(separator)?;
        }
        write!(f, "{prefix}{value}")?;
    }
    Ok(())
}

// sqlparser's Custom Display joins all modifiers into an intermediate String.
fn write_data_type(f: &mut dyn Write, data_type: &DataType) -> std::fmt::Result {
    if let DataType::Custom(name, modifiers) = data_type {
        write!(f, "{name}")?;
        if !modifiers.is_empty() {
            f.write_char('(')?;
            write_list(f, modifiers, COMMA_SEP, "")?;
            f.write_char(')')?;
        }
        Ok(())
    } else {
        write!(f, "{data_type}")
    }
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
                f.write_str("PRIMARY KEY (")?;
                write_list(f, columns, COMMA_SEP, "")?;
                f.write_str(")")
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
    /// JSON2-specific column options.
    pub json2_options: Option<Json2Options>,
}

/// JSON2-specific options represented in the SQL AST.
#[derive(Debug, PartialEq, Eq, Clone, Visit, VisitMut, Default, Serialize)]
pub struct Json2Options {
    /// Maximum number of unhinted JSON2 paths expanded into Arrow fields.
    pub(crate) max_auto_expanded_paths: Option<u32>,
    /// Paths stored as explicitly typed JSON2 fields.
    pub(crate) type_hints: Vec<JsonTypeHint>,
}

impl Display for Json2Options {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("(\n    ")?;
        if let Some(max) = self.max_auto_expanded_paths {
            write!(f, "max_auto_expanded_paths = {max}")?;
        }
        for (index, hint) in self.type_hints.iter().enumerate() {
            if index > 0 || self.max_auto_expanded_paths.is_some() {
                f.write_str(",\n    ")?;
            }
            for (index, segment) in hint.path.iter().enumerate() {
                if index > 0 {
                    f.write_char('.')?;
                }
                f.write_char('"')?;
                for part in segment.split_inclusive('"') {
                    f.write_str(part)?;
                    if part.ends_with('"') {
                        f.write_char('"')?;
                    }
                }
                f.write_char('"')?;
            }
            f.write_char(' ')?;
            write_data_type(f, &hint.data_type)?;
            if hint.inverted_index {
                f.write_str(" INVERTED INDEX")?;
            }
        }
        f.write_str("\n  )")
    }
}

impl Json2Options {
    pub fn build_json_settings(&self) -> Result<JsonSettings> {
        let type_hints = self
            .type_hints
            .iter()
            .map(|hint| {
                Ok(datatypes::json::JsonTypeHint {
                    path: hint.path.clone(),
                    data_type: sql_data_type_to_concrete_data_type(&hint.data_type)?,
                    inverted_index: hint.inverted_index,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let max_auto_expanded_paths = self
            .max_auto_expanded_paths
            .or(Some(JSON2_DEFAULT_MAX_AUTO_EXPANDED_PATHS));
        JsonSettings::try_new(type_hints, max_auto_expanded_paths).map_err(Into::into)
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Visit, VisitMut, Serialize)]
pub struct JsonTypeHint {
    pub path: Vec<String>,
    pub data_type: DataType,
    pub inverted_index: bool,
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
        self.write_sql(f, false)
    }
}

impl Column {
    fn write_sql(&self, f: &mut dyn Write, complete: bool) -> std::fmt::Result {
        if !complete
            && let Some(vector_options) = &self.extensions.vector_options
            && let Some(dim) = vector_options.get(VECTOR_OPT_DIM)
        {
            return write!(f, "{} VECTOR({})", self.column_def.name, dim);
        }
        write!(f, "{} ", self.column_def.name)?;
        write_data_type(f, &self.column_def.data_type)?;
        if let Some(options) = &self.extensions.json2_options {
            write!(f, "{options}")?;
        }
        for option in &self.column_def.options {
            write!(f, " {option}")?;
        }

        for (kind, options) in [
            ("FULLTEXT", &self.extensions.fulltext_index_options),
            ("SKIPPING", &self.extensions.skipping_index_options),
            ("INVERTED", &self.extensions.inverted_index_options),
        ] {
            if let Some(options) = options {
                write!(f, " {kind} INDEX")?;
                if !options.is_empty() {
                    f.write_str(" WITH(")?;
                    options.write_sql(f, COMMA_SEP, "", complete)?;
                    f.write_str(")")?;
                }
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

    pub fn build_json_settings(&self) -> Result<Option<JsonSettings>> {
        let Some(options) = &self.json2_options else {
            return Ok(None);
        };

        options.build_json_settings().map(Some)
    }

    pub fn set_json_settings(&mut self, settings: JsonSettings) -> Result<()> {
        let (type_hints, max_auto_expanded_paths) = settings.into_parts();
        let type_hints = type_hints
            .into_iter()
            .map(|hint| {
                let data_type = json_type_hint_sql_data_type(&hint.data_type)?;
                Ok(JsonTypeHint {
                    path: hint.path,
                    data_type,
                    inverted_index: hint.inverted_index,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        self.json2_options = (max_auto_expanded_paths.is_some() || !type_hints.is_empty())
            .then_some(Json2Options {
                max_auto_expanded_paths,
                type_hints,
            });
        Ok(())
    }
}

fn json_type_hint_sql_data_type(data_type: &ConcreteDataType) -> Result<DataType> {
    let sql_type = match data_type {
        ConcreteDataType::String(_) => DataType::String(None),
        ConcreteDataType::Int64(_) => DataType::BigInt(None),
        ConcreteDataType::UInt64(_) => DataType::BigIntUnsigned(None),
        ConcreteDataType::Float64(_) => DataType::Double(sqlparser::ast::ExactNumberInfo::None),
        ConcreteDataType::Boolean(_) => DataType::Boolean,
        _ => {
            return InvalidSqlSnafu {
                msg: format!("unsupported JSON2 type hint data type: {data_type}"),
            }
            .fail();
        }
    };
    Ok(sql_type)
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

impl Display for Partitions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if !self.column_list.is_empty() {
            f.write_str("PARTITION ON COLUMNS (")?;
            write_list(f, &self.column_list, COMMA_SEP, "")?;
            f.write_str(") (\n")?;
            write_list(f, &self.exprs, LINE_SEP, "  ")?;
            f.write_str("\n)")?;
        }
        Ok(())
    }
}

impl Display for CreateTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.write_sql(f, false)
    }
}

impl CreateTable {
    /// Debits the SQL size of a rewritten AST, including unredacted and non-scalar
    /// options. Returns false on exhaustion or a shape hidden by normal Display.
    /// No SQL or secret value is returned. Delegated sqlparser formatters retain
    /// their own allocation behavior; this is not a general AST memory limit.
    pub fn consume_sql_size_budget(&self, remaining: &mut usize) -> bool {
        struct Budget<'a>(&'a mut usize);
        impl Write for Budget<'_> {
            fn write_str(&mut self, value: &str) -> std::fmt::Result {
                *self.0 = self.0.checked_sub(value.len()).ok_or(std::fmt::Error)?;
                Ok(())
            }
        }
        if self
            .partitions
            .as_ref()
            .is_some_and(|p| p.column_list.is_empty() && !p.exprs.is_empty())
        {
            return false;
        }
        for column in &self.columns {
            if let Some(options) = &column.extensions.vector_options {
                let DataType::Custom(name, tokens) = &column.column_def.data_type else {
                    return false;
                };
                if options.len() != 1
                    || name.0.len() != 1
                    || tokens.len() != 1
                    || !name.0[0]
                        .as_ident()
                        .is_some_and(|name| name.value.eq_ignore_ascii_case("VECTOR"))
                    || !tokens[0].parse::<u32>().ok().is_some_and(|dim| {
                        options.get(VECTOR_OPT_DIM) == Some(dim.to_string().as_str())
                    })
                {
                    return false;
                }
            }
        }
        self.write_sql(&mut Budget(remaining), true).is_ok()
    }

    fn write_sql(&self, f: &mut dyn Write, complete: bool) -> std::fmt::Result {
        write!(f, "CREATE ")?;
        if self.engine == FILE_ENGINE {
            write!(f, "EXTERNAL ")?;
        }
        write!(f, "TABLE ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        writeln!(f, "{} (", &self.name)?;
        for (index, column) in self.columns.iter().enumerate() {
            if index > 0 {
                f.write_str(LINE_SEP)?;
            }
            f.write_str("  ")?;
            column.write_sql(f, complete)?;
        }
        f.write_str(",\n")?;
        write_list(f, &self.constraints, LINE_SEP, "  ")?;
        f.write_char('\n')?;
        writeln!(f, ")")?;
        if let Some(partitions) = &self.partitions {
            writeln!(f, "{partitions}")?;
        }
        writeln!(f, "ENGINE={}", &self.engine)?;
        if !self.options.is_empty() {
            f.write_str("WITH(\n")?;
            self.options.write_sql(f, LINE_SEP, "  ", complete)?;
            f.write_str("\n)")?;
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
    /// Phase offset of the flow evaluation schedule within `eval_interval`.
    /// Duration in seconds as `i64`.
    /// Must be in range `[0, eval_interval)`. Only legal together with
    /// `eval_interval`. A value of zero (the default) means the schedule is
    /// anchored to the Unix epoch, i.e. phases at `k * eval_interval`.
    pub eval_offset: Option<i64>,
    /// Comment string
    pub comment: Option<String>,
    /// Flow creation options from `WITH (...)`
    pub flow_options: OptionMap,
    /// SQL statement
    pub query: Box<SqlOrTql>,
}

/// Either a sql query or a tql query
#[derive(Debug, PartialEq, Eq, Clone, Visit, VisitMut, Serialize)]
pub enum SqlOrTql {
    Sql(GtQuery, String),
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
            Statement::Query(query) => Ok(Self::Sql(*query, original_query.to_string())),
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
        // Canonical display: omit a zero offset (equivalent to the default
        // epoch-anchored schedule). Non-zero offsets are always emitted.
        if let Some(eval_offset) = &self.eval_offset
            && *eval_offset != 0
        {
            writeln!(f, "EVAL OFFSET '{} s'", eval_offset)?;
        }
        if let Some(comment) = &self.comment {
            writeln!(f, "COMMENT '{}'", comment)?;
        }
        if !self.flow_options.is_empty() {
            let options = self.flow_options.kv_pairs();
            writeln!(f, "WITH ({})", format_list_comma!(options))?;
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
    use std::assert_matches;

    use datatypes::json::{JsonSettings, JsonTypeHint as DatatypeJsonTypeHint};
    use datatypes::prelude::ConcreteDataType;

    use super::*;
    use crate::dialect::GreptimeDbDialect;
    use crate::error::Error;
    use crate::parser::{ParseOptions, ParserContext};
    use crate::statements::statement::Statement;

    #[test]
    fn test_rewritten_create_sql_budget() {
        let parse = |sql| {
            let mut statements = ParserContext::create_with_dialect(
                sql,
                &GreptimeDbDialect {},
                ParseOptions::default(),
            )
            .unwrap();
            let Statement::CreateTable(create) = statements.remove(0) else {
                panic!("CREATE expected")
            };
            create
        };
        let create = parse(
            "CREATE TABLE demo (ts TIMESTAMP TIME INDEX, host STRING) ENGINE=metric WITH (on_physical_table='phy')",
        );
        let size = create.to_string().len();
        let mut remaining = size;
        assert!(create.consume_sql_size_budget(&mut remaining));
        assert_eq!(remaining, 0);
        assert!(!create.consume_sql_size_budget(&mut (size - 1)));

        for location in ["table", "fulltext", "skipping", "inverted"] {
            for value_type in ["secret", "array", "struct"] {
                for length in [10, 8192] {
                    let mut create = create.clone();
                    let extensions = &mut create.columns[1].extensions;
                    let map = match location {
                        "table" => &mut create.options,
                        "fulltext" => extensions.fulltext_index_options.get_or_insert_default(),
                        "skipping" => extensions.skipping_index_options.get_or_insert_default(),
                        _ => extensions.inverted_index_options.get_or_insert_default(),
                    };
                    let value = "private-value".repeat(length);
                    match value_type {
                        "secret" => map.insert("secret_access_key".into(), value.clone()),
                        "array" => map.insert_options("payload", vec![value.as_str()].into()),
                        _ => map.insert_options(
                            "payload",
                            crate::util::OptionValue::try_new(Expr::Struct {
                                values: vec![Expr::Value(
                                    sqlparser::ast::Value::SingleQuotedString(value.clone()).into(),
                                )],
                                fields: vec![],
                            })
                            .unwrap(),
                        ),
                    }
                    assert!(!create.to_string().contains(&value));
                    assert_eq!(
                        create.consume_sql_size_budget(&mut 4096),
                        length == 10,
                        "{location}/{value_type}"
                    );
                }
            }
        }
        let mut vector = parse("CREATE TABLE demo (ts TIMESTAMP TIME INDEX, v VECTOR(3))");
        vector.columns[1].column_def.options.push(ColumnOptionDef {
            name: None,
            option: sqlparser::ast::ColumnOption::Comment("private-value".repeat(8192)),
        });
        assert!(!vector.to_string().contains("private-value"));
        assert!(!vector.consume_sql_size_budget(&mut 4096));
        vector.columns[1].column_def.options.clear();
        assert!(vector.consume_sql_size_budget(&mut 4096));
        if let DataType::Custom(_, tokens) = &mut vector.columns[1].column_def.data_type {
            tokens[0] = format!("{}3", "0".repeat(8192));
        }
        assert!(!vector.consume_sql_size_budget(&mut 4096));
        vector.columns[1]
            .extensions
            .vector_options
            .as_mut()
            .unwrap()
            .insert("extra".into(), "x".into());
        assert!(!vector.consume_sql_size_budget(&mut 4096));

        let mut create = create;
        create.columns[1].extensions.json2_options = Some(Json2Options {
            max_auto_expanded_paths: Some(1),
            type_hints: vec![JsonTypeHint {
                path: vec!["x".repeat(8192)],
                data_type: DataType::Text,
                inverted_index: true,
            }],
        });
        assert!(!create.consume_sql_size_budget(&mut 4096));
        create.columns[1].extensions.json2_options = None;
        create.partitions = Some(Partitions {
            column_list: vec![],
            exprs: vec![Expr::Identifier(Ident::new("hidden"))],
        });
        assert!(!create.consume_sql_size_budget(&mut 4096));
    }

    #[test]
    fn test_json2_streamed_quote_escaping() {
        let path = "a\\\"\"b";
        let options = Json2Options {
            max_auto_expanded_paths: None,
            type_hints: vec![JsonTypeHint {
                path: vec![path.into()],
                data_type: DataType::Text,
                inverted_index: false,
            }],
        };
        assert_eq!(
            options.to_string(),
            format!("(\n    \"{}\" TEXT\n  )", path.replace('"', "\"\""))
        );
    }

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
        assert_matches!(result, Err(Error::InvalidTableOption { .. }));

        // A whitelisted semantic key with an in-domain value is accepted.
        let semantic = |with: &str| {
            let sql =
                format!("create table demo(host string, ts timestamp time index) with({with});");
            ParserContext::create_with_dialect(&sql, &GreptimeDbDialect {}, ParseOptions::default())
        };
        assert!(semantic("'greptime.semantic.signal_type'='metric'").is_ok());
        // An out-of-domain value is rejected.
        assert_matches!(
            semantic("'greptime.semantic.signal_type'='spans'"),
            Err(Error::InvalidTableOption { .. })
        );
        // An unknown key under the semantic prefix is rejected.
        assert_matches!(
            semantic("'greptime.semantic.bogus'='x'"),
            Err(Error::InvalidTableOption { .. })
        );
    }

    #[test]
    fn test_display_json2_type_hints_quotes_path_segments() {
        let sql = r#"CREATE TABLE traces (
            log_json_data JSON2 (
                "service.name" STRING,
                "a.b"."c" BIGINT,
                a."b.c" STRING
            ),
            ts TIMESTAMP TIME INDEX
        )"#;
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();

        match &result[0] {
            Statement::CreateTable(c) => {
                let new_sql = format!("\n{}", c);
                assert_eq!(
                    r#"
CREATE TABLE traces (
  log_json_data JSON2(
    "service.name" STRING,
    "a.b"."c" BIGINT,
    "a"."b.c" STRING
  ),
  ts TIMESTAMP NOT NULL,
  TIME INDEX (ts)
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
    fn test_parse_json2_max_auto_expanded_paths_option() -> Result<()> {
        let sql = r#"CREATE TABLE traces (
            log_json_data JSON2 (
                status_code BIGINT,
                max_auto_expanded_paths = 1
            ),
            ts TIMESTAMP TIME INDEX
        )"#;
        let result = ParserContext::create_with_dialect(
            sql,
            &GreptimeDbDialect {},
            ParseOptions::default(),
        )?;
        let Statement::CreateTable(create_table) = &result[0] else {
            unreachable!()
        };
        let settings = create_table.columns[0]
            .extensions
            .build_json_settings()?
            .unwrap();
        assert_eq!(settings.max_auto_expanded_paths(), Some(1));
        Ok(())
    }

    #[test]
    fn test_display_json2_type_hints_quotes_numeric_segments() {
        let sql = r#"CREATE TABLE traces (
            log_json_data JSON2 (
                "1abc" STRING,
                a."2b" BIGINT
            ),
            ts TIMESTAMP TIME INDEX
        )"#;
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();

        match &result[0] {
            Statement::CreateTable(c) => {
                let new_sql = format!("\n{}", c);
                assert_eq!(
                    r#"
CREATE TABLE traces (
  log_json_data JSON2(
    "1abc" STRING,
    "a"."2b" BIGINT
  ),
  ts TIMESTAMP NOT NULL,
  TIME INDEX (ts)
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
    fn test_json2_type_hint_rejects_default() {
        let sql = r#"CREATE TABLE traces (
            log_json_data JSON2 (
                status_code BIGINT DEFAULT -5,
                duration DOUBLE DEFAULT +1.5,
                error BOOLEAN DEFAULT false,
                message STRING DEFAULT 'unknown'
            ),
            ts TIMESTAMP TIME INDEX
        )"#;
        let err =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap_err();
        assert!(err.to_string().contains("DEFAULT is not supported"));
    }

    #[test]
    fn test_json2_type_hint_rejects_not_null() {
        let sql = r#"CREATE TABLE traces (
            log_json_data JSON2 (
                status_code BIGINT NOT NULL DEFAULT NULL
            ),
            ts TIMESTAMP TIME INDEX
        )"#;
        let err =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap_err();
        assert!(err.to_string().contains("NULL/NOT NULL is not supported"));
    }

    #[test]
    fn test_set_json_settings_preserves_type_hint_sql_types() -> Result<()> {
        let mut extensions = super::ColumnExtensions::default();
        let settings = JsonSettings::try_new(
            vec![
                DatatypeJsonTypeHint {
                    path: vec!["i".to_string()],
                    data_type: ConcreteDataType::int64_datatype(),
                    inverted_index: false,
                },
                DatatypeJsonTypeHint {
                    path: vec!["f".to_string()],
                    data_type: ConcreteDataType::float64_datatype(),
                    inverted_index: false,
                },
                DatatypeJsonTypeHint {
                    path: vec!["u".to_string()],
                    data_type: ConcreteDataType::uint64_datatype(),
                    inverted_index: false,
                },
                DatatypeJsonTypeHint {
                    path: vec!["s".to_string()],
                    data_type: ConcreteDataType::string_datatype(),
                    inverted_index: false,
                },
                DatatypeJsonTypeHint {
                    path: vec!["b".to_string()],
                    data_type: ConcreteDataType::boolean_datatype(),
                    inverted_index: false,
                },
            ],
            None,
        )?;
        extensions.set_json_settings(settings)?;

        assert_eq!(
            extensions
                .json2_options
                .unwrap()
                .type_hints
                .iter()
                .map(|hint| hint.data_type.to_string())
                .collect::<Vec<_>>(),
            vec!["BIGINT", "DOUBLE", "BIGINT UNSIGNED", "STRING", "BOOLEAN"]
        );
        Ok(())
    }

    #[test]
    fn test_set_json_settings_rejects_unsupported_type_hint_type() -> Result<()> {
        let err = JsonSettings::try_new(
            vec![DatatypeJsonTypeHint {
                path: vec!["u".to_string()],
                data_type: ConcreteDataType::date_datatype(),
                inverted_index: false,
            }],
            None,
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("unsupported JSON2 type hint data type")
        );
        Ok(())
    }

    #[test]
    fn test_set_empty_json_settings_omits_json2_options() -> Result<()> {
        let mut extensions = ColumnExtensions::default();
        extensions.set_json_settings(JsonSettings::default())?;
        assert!(extensions.json2_options.is_none());
        Ok(())
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
}
