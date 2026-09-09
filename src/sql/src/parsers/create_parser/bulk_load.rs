// Copyright 2023-2026 GrepTime Inc.
//
// This file is part of the GreptimeDB Enterprise Edition and is licensed under
// the GreptimeDB Enterprise License. You may not use this file except in
// compliance with that license. A copy of the license is available at the root
// of this repository in the file LICENSE-ENTERPRISE.
//
// Unless required by applicable law or agreed to in writing, this software is
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.

use std::collections::HashSet;

use snafu::{ResultExt, ensure};
use sqlparser::keywords::Keyword;

use crate::error::{InvalidSqlSnafu, Result, SyntaxSnafu};
use crate::parser::ParserContext;
use crate::statements::OptionMap;
use crate::statements::create::bulk_load::CreateBulkLoad;
use crate::statements::statement::Statement;
use crate::util::parse_option_string;

const OPTIONS: &[&str] = &[
    "format",
    "max_retries_per_task",
    "priority",
    "timestamp_offset",
    "frontend_group",
];

impl ParserContext<'_> {
    pub(crate) fn parse_create_bulk_load(&mut self) -> Result<Statement> {
        ensure!(
            self.consume_token("LOAD"),
            InvalidSqlSnafu {
                msg: "Expected LOAD after CREATE BULK".to_string()
            }
        );
        let job_id =
            Self::canonicalize_identifier(self.parser.parse_identifier().context(SyntaxSnafu)?);
        self.parser
            .expect_keyword(Keyword::INTO)
            .context(SyntaxSnafu)?;
        let table_name = self.intern_parse_table_name()?;
        self.parser
            .expect_keyword(Keyword::FROM)
            .context(SyntaxSnafu)?;
        let staging_uri = self.parser.parse_literal_string().context(SyntaxSnafu)?;

        let raw_options = self
            .parser
            .parse_options(Keyword::WITH)
            .context(SyntaxSnafu)?;
        let mut seen = HashSet::with_capacity(raw_options.len());
        let mut options = OptionMap::default();
        for option in raw_options {
            let (key, value) = parse_option_string(option)?;
            ensure!(
                OPTIONS.contains(&key.as_str()),
                InvalidSqlSnafu {
                    msg: format!("Unknown CREATE BULK LOAD option '{key}'")
                }
            );
            ensure!(
                seen.insert(key.clone()),
                InvalidSqlSnafu {
                    msg: format!("Duplicate CREATE BULK LOAD option '{key}'")
                }
            );
            options.insert_options(&key, value);
        }

        Ok(Statement::CreateBulkLoad(CreateBulkLoad {
            job_id,
            table_name,
            staging_uri,
            options,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialect::GreptimeDbDialect;
    use crate::parser::ParseOptions;

    fn parse(sql: &str) -> Result<Statement> {
        Ok(
            ParserContext::create_with_dialect(
                sql,
                &GreptimeDbDialect {},
                ParseOptions::default(),
            )?
            .remove(0),
        )
    }

    #[test]
    fn parses_and_round_trips_bulk_load() {
        let sql = "CREATE BULK LOAD job_1 INTO catalog.schema.metrics FROM 'staging/job_1/' WITH (format = 'parquet', max_retries_per_task = '5', priority = 'low', timestamp_offset = '+8h', frontend_group = 'ingest')";
        let statement = parse(sql).unwrap();
        let reparsed = parse(&statement.to_string()).unwrap();
        assert_eq!(statement, reparsed);

        let Statement::CreateBulkLoad(create) = statement else {
            panic!("expected CREATE BULK LOAD")
        };
        assert_eq!(create.job_id.value, "job_1");
        assert_eq!(create.table_name.to_string(), "catalog.schema.metrics");
        assert_eq!(create.options.get("frontend_group"), Some("ingest"));
    }

    #[test]
    fn rejects_unknown_and_duplicate_options() {
        for sql in [
            "CREATE BULK LOAD j INTO t FROM 'x/' WITH (unknown = 'x')",
            "CREATE BULK LOAD j INTO t FROM 'x/' WITH (format = 'parquet', FORMAT = 'parquet')",
        ] {
            assert!(parse(sql).is_err(), "{sql}");
        }
    }
}
