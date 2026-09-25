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

use datatypes::extension::json::JSON2_REMAINDER_FIELD_NAME;
use datatypes::json::JSON2_MAX_STRUCTURED_DEPTH;
use snafu::{ResultExt, ensure};
use sqlparser::ast::{DataType, ExactNumberInfo, ObjectName};
use sqlparser::dialect::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use crate::ast::Ident;
use crate::dialect::GreptimeDbDialect;
use crate::error::{InvalidSqlSnafu, Result, SyntaxSnafu};
use crate::parsers::create_parser::{INVERTED, SKIPPING};
use crate::statements::create::{Json2Options, JsonTypeHint};

const JSON2_TYPE_NAME: &str = "JSON2";
const MAX_AUTO_EXPANDED_PATHS: &str = "max_auto_expanded_paths";

/// Parses a JSON2 type hint path with the same grammar used by `CREATE TABLE`.
pub fn parse_json2_type_hint_path(path: &str) -> Result<Vec<String>> {
    let dialect = GreptimeDbDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(path)
        .context(SyntaxSnafu)?;
    let path = parse_json2_path(&mut parser)?;
    ensure!(
        parser.peek_token().token == Token::EOF,
        InvalidSqlSnafu {
            msg: format!(
                "unexpected token '{}' in JSON2 type hint path",
                parser.peek_token()
            )
        }
    );
    Ok(path)
}

pub(crate) fn parse_json2_type_and_options(
    parser: &mut Parser<'_>,
) -> Result<Option<(DataType, Option<Json2Options>)>> {
    let token = parser.peek_token();
    let Token::Word(word) = &token.token else {
        return Ok(None);
    };

    if !word.value.eq_ignore_ascii_case(JSON2_TYPE_NAME) || word.quote_style.is_some() {
        return Ok(None);
    }

    parser.next_token();
    let data_type = DataType::Custom(ObjectName::from(vec![Ident::new(JSON2_TYPE_NAME)]), vec![]);
    let options = if parser.consume_token(&Token::LParen) {
        parse_json2_options(parser)?
    } else {
        None
    };

    Ok(Some((data_type, options)))
}

fn parse_json2_options(parser: &mut Parser<'_>) -> Result<Option<Json2Options>> {
    if parser.consume_token(&Token::RParen) {
        return Ok(None);
    }

    let mut max_auto_expanded_paths = None;
    let mut type_hints = Vec::new();
    loop {
        let token = parser.peek_token();
        let is_max_auto_expanded_paths = matches!(
            &token.token,
            Token::Word(word)
                if word.quote_style.is_none()
                    && word.value.eq_ignore_ascii_case(MAX_AUTO_EXPANDED_PATHS)
        );
        if is_max_auto_expanded_paths {
            parser.next_token();
            ensure!(
                max_auto_expanded_paths.is_none(),
                InvalidSqlSnafu {
                    msg: format!("duplicated JSON2 option '{MAX_AUTO_EXPANDED_PATHS}'")
                }
            );
            parser.expect_token(&Token::Eq).context(SyntaxSnafu)?;

            let token = parser.next_token();
            let Token::Number(value, _) = token.token else {
                return InvalidSqlSnafu {
                    msg: format!(
                        "JSON2 option '{MAX_AUTO_EXPANDED_PATHS}' expects a non-negative integer"
                    ),
                }
                .fail();
            };
            max_auto_expanded_paths = Some(value.parse::<u32>().map_err(|_| {
                InvalidSqlSnafu {
                    msg: format!(
                        "JSON2 option '{MAX_AUTO_EXPANDED_PATHS}' expects a non-negative integer"
                    ),
                }
                .build()
            })?);
        } else {
            let hint = parse_json2_type_hint(parser)?;
            ensure_no_path_conflict(&type_hints, &hint.path)?;
            type_hints.push(hint);
        }

        if parser.consume_token(&Token::Comma) {
            if parser.consume_token(&Token::RParen) {
                break;
            }
        } else {
            parser.expect_token(&Token::RParen).context(SyntaxSnafu)?;
            break;
        }
    }

    Ok(Some(Json2Options {
        max_auto_expanded_paths,
        type_hints,
    }))
}

fn parse_json2_type_hint(parser: &mut Parser<'_>) -> Result<JsonTypeHint> {
    let path = parse_json2_path(parser)?;
    ensure!(
        path.first().is_none_or(|x| x != JSON2_REMAINDER_FIELD_NAME),
        InvalidSqlSnafu {
            msg: format!(
                "JSON2 type hint path cannot be rooted at reserved field '{JSON2_REMAINDER_FIELD_NAME}'"
            )
        }
    );
    ensure!(
        path.len() <= JSON2_MAX_STRUCTURED_DEPTH,
        InvalidSqlSnafu {
            msg: format!(
                "JSON2 type hint path cannot exceed {JSON2_MAX_STRUCTURED_DEPTH} segments"
            ),
        }
    );
    let data_type = parser.parse_data_type().context(SyntaxSnafu)?;
    let data_type = validate_json2_type_hint_type(data_type)?;

    let mut inverted_index = false;

    loop {
        if parser.parse_keywords(&[Keyword::NOT, Keyword::NULL])
            || parser.parse_keyword(Keyword::NULL)
        {
            return InvalidSqlSnafu {
                msg: "JSON2 type hint NULL/NOT NULL is not supported; hinted fields are always nullable"
                    .to_string(),
            }
            .fail();
        } else if parser.parse_keyword(Keyword::DEFAULT) {
            return InvalidSqlSnafu {
                msg: "JSON2 type hint DEFAULT is not supported".to_string(),
            }
            .fail();
        } else if let Token::Word(word) = parser.peek_token().token
            && word.value.eq_ignore_ascii_case(INVERTED)
        {
            parser.next_token();
            ensure!(
                parser.parse_keyword(Keyword::INDEX),
                InvalidSqlSnafu {
                    msg: format!(
                        "expect INDEX after INVERTED keyword for JSON2 type hint '{}'",
                        path.join(".")
                    )
                }
            );
            ensure!(
                !inverted_index,
                InvalidSqlSnafu {
                    msg: format!(
                        "duplicated INVERTED INDEX option for JSON2 type hint '{}'",
                        path.join(".")
                    )
                }
            );
            ensure!(
                !path
                    .iter()
                    .any(|segment| segment == JSON2_REMAINDER_FIELD_NAME),
                InvalidSqlSnafu {
                    msg: format!(
                        "JSON2 indexed type hint path cannot contain reserved field '{JSON2_REMAINDER_FIELD_NAME}'"
                    )
                }
            );
            inverted_index = true;
        } else if let Token::Word(word) = parser.peek_token().token
            && word.value.eq_ignore_ascii_case(SKIPPING)
        {
            return InvalidSqlSnafu {
                msg: "JSON2 type hint SKIPPING INDEX is not supported yet".to_string(),
            }
            .fail();
        } else if matches!(parser.peek_token().token, Token::Comma | Token::RParen) {
            break;
        } else {
            return parser
                .expected("JSON2 type hint option", parser.peek_token())
                .context(SyntaxSnafu);
        }
    }

    Ok(JsonTypeHint {
        path,
        data_type,
        inverted_index,
    })
}

fn parse_json2_path(parser: &mut Parser<'_>) -> Result<Vec<String>> {
    let first = parser.parse_identifier().context(SyntaxSnafu)?;
    let mut path = vec![first.value];

    while parser.consume_token(&Token::Period) {
        let segment = parser.parse_identifier().context(SyntaxSnafu)?;
        path.push(segment.value);
    }

    ensure!(
        !path.iter().any(|segment| segment.is_empty()),
        InvalidSqlSnafu {
            msg: "JSON2 type hint path segment cannot be empty".to_string(),
        }
    );

    Ok(path)
}

fn validate_json2_type_hint_type(data_type: DataType) -> Result<DataType> {
    match data_type {
        DataType::Int64
        | DataType::UInt64
        | DataType::Float64
        | DataType::String(None)
        | DataType::BigInt(None)
        | DataType::BigIntUnsigned(None)
        | DataType::Double(ExactNumberInfo::None)
        | DataType::Boolean => Ok(data_type),
        _ => InvalidSqlSnafu {
            msg: format!(
                "unsupported JSON2 type hint data type: {data_type}; supported types: STRING, BIGINT, BIGINT UNSIGNED, DOUBLE, BOOLEAN; supported aliases: INT64, UINT64, FLOAT64"
            ),
        }
        .fail(),
    }
}

fn ensure_no_path_conflict(hints: &[JsonTypeHint], path: &[String]) -> Result<()> {
    for hint in hints {
        ensure!(
            hint.path != path,
            InvalidSqlSnafu {
                msg: format!("duplicated JSON2 type hint path '{}'", path.join("."))
            }
        );
        ensure!(
            !hint.path.starts_with(path) && !path.starts_with(&hint.path),
            InvalidSqlSnafu {
                msg: format!(
                    "JSON2 type hint path '{}' conflicts with '{}'",
                    path.join("."),
                    hint.path.join(".")
                )
            }
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{DataType, ExactNumberInfo};

    use super::parse_json2_type_hint_path;
    use crate::dialect::GreptimeDbDialect;
    use crate::parser::{ParseOptions, ParserContext};
    use crate::statements::create::Column;
    use crate::statements::statement::Statement;

    fn parse_json2_column(sql: &str) -> Column {
        let Statement::CreateTable(mut create_table) =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap()
                .remove(0)
        else {
            unreachable!()
        };

        create_table.columns.remove(0)
    }

    #[test]
    fn test_parse_json2_type_hint_path() {
        assert_eq!(
            parse_json2_type_hint_path(r#"attrs."http.status_code""#).unwrap(),
            vec!["attrs", "http.status_code"]
        );
        assert!(parse_json2_type_hint_path("user.id trailing").is_err());
    }

    #[test]
    fn test_parse_json2_type_hints() {
        let column = parse_json2_column(
            r#"
CREATE TABLE traces (
    log_json_data JSON2 (
        "service.name" STRING INVERTED INDEX,
        http.method STRING,
        status_code BIGINT,
        comment STRING,
    ),
    ts TIMESTAMP TIME INDEX,
)"#,
        );

        assert!(matches!(
            column.column_def.data_type,
            DataType::Custom(_, _)
        ));
        let hints = column.extensions.json2_options.unwrap().type_hints;
        assert_eq!(hints.len(), 4);

        assert_eq!(hints[0].path, vec!["service.name"]);
        assert_eq!(hints[0].data_type, DataType::String(None));
        assert!(hints[0].inverted_index);

        assert_eq!(hints[1].path, vec!["http", "method"]);
        assert_eq!(hints[1].data_type, DataType::String(None));
        assert!(!hints[1].inverted_index);

        assert_eq!(hints[2].path, vec!["status_code"]);
        assert_eq!(hints[2].data_type, DataType::BigInt(None));

        assert_eq!(hints[3].path, vec!["comment"]);
        assert_eq!(hints[3].data_type, DataType::String(None));
    }

    #[test]
    fn test_parse_json2_max_auto_expanded_paths() {
        let column = parse_json2_column(
            r#"
CREATE TABLE traces (
    log_json_data JSON2 (
        http.method STRING,
        max_auto_expanded_paths = 0
    ),
    ts TIMESTAMP TIME INDEX,
)"#,
        );

        let options = column.extensions.json2_options.unwrap();
        assert_eq!(options.max_auto_expanded_paths, Some(0));
        assert_eq!(options.type_hints.len(), 1);

        let empty = parse_json2_column(
            r#"
CREATE TABLE traces (
    log_json_data JSON2 (),
    ts TIMESTAMP TIME INDEX,
)"#,
        );
        assert!(empty.extensions.json2_options.is_none());

        let quoted = parse_json2_column(
            r#"
CREATE TABLE traces (
    log_json_data JSON2 (
        "max_auto_expanded_paths" STRING,
        nested."!__remainder__!" STRING
    ),
    ts TIMESTAMP TIME INDEX,
)"#,
        );
        let options = quoted.extensions.json2_options.unwrap();
        assert_eq!(options.max_auto_expanded_paths, None);
        assert_eq!(options.type_hints.len(), 2);
    }

    #[test]
    fn test_parse_json2_max_auto_expanded_paths_rejects_invalid_options() {
        for options in [
            "max_auto_expanded_paths = 0, max_auto_expanded_paths = 1",
            "max_auto_expanded_paths = -1",
            "max_auto_expanded_paths = 1.5",
            "max_auto_expanded_paths = 4294967296",
            r#""!__remainder__!".value STRING"#,
        ] {
            let sql = format!(
                "CREATE TABLE traces (log_json_data JSON2 ({options}), ts TIMESTAMP TIME INDEX)"
            );
            assert!(
                ParserContext::create_with_dialect(
                    &sql,
                    &GreptimeDbDialect {},
                    ParseOptions::default()
                )
                .is_err(),
                "{options}"
            );
        }
    }

    #[test]
    fn test_parse_json2_type_hint_defaults_to_nullable() {
        let column = parse_json2_column(
            r#"
CREATE TABLE traces (
    log_json_data JSON2 (http.method STRING),
    ts TIMESTAMP TIME INDEX,
)"#,
        );

        let hints = column.extensions.json2_options.unwrap().type_hints;
        assert_eq!(hints.len(), 1);
        assert_eq!(hints[0].data_type, DataType::String(None));
    }

    #[test]
    fn test_parse_json2_type_hint_quoted_path_segments() {
        let column = parse_json2_column(
            r#"
CREATE TABLE traces (
    log_json_data JSON2 (
        "a".b STRING,
        "x"."y" STRING,
        "a.b"."c" STRING,
        a."b.c" STRING
    ),
    ts TIMESTAMP TIME INDEX,
)"#,
        );

        let hints = column.extensions.json2_options.unwrap().type_hints;
        assert_eq!(hints.len(), 4);
        assert_eq!(hints[0].path, vec!["a", "b"]);
        assert_eq!(hints[1].path, vec!["x", "y"]);
        assert_eq!(hints[2].path, vec!["a.b", "c"]);
        assert_eq!(hints[3].path, vec!["a", "b.c"]);
    }

    #[test]
    fn test_parse_json2_type_hint_supported_types() {
        for (sql_type, expected) in [
            ("STRING", DataType::String(None)),
            ("BIGINT", DataType::BigInt(None)),
            ("BIGINT UNSIGNED", DataType::BigIntUnsigned(None)),
            ("DOUBLE", DataType::Double(ExactNumberInfo::None)),
            ("BOOLEAN", DataType::Boolean),
            ("INT64", DataType::Int64),
            ("UINT64", DataType::UInt64),
            ("FLOAT64", DataType::Float64),
        ] {
            for sql_type in [sql_type.to_string(), sql_type.to_lowercase()] {
                let column = parse_json2_column(&format!(
                    "CREATE TABLE traces (j JSON2 (value {sql_type}), ts TIMESTAMP TIME INDEX)"
                ));
                let options = column.extensions.json2_options.unwrap();
                let settings = options.build_json_settings().unwrap();
                assert_eq!(settings.type_hints().len(), 1);
                let hints = options.type_hints;
                assert_eq!(hints[0].data_type, expected);
            }
        }
    }

    #[test]
    fn test_parse_json2_type_hint_rejects_unsupported_types() {
        for sql_type in [
            "INT2",
            "INT4",
            "INT8",
            "INT16",
            "INT32",
            "TINYINT",
            "SMALLINT",
            "INT",
            "INTEGER",
            "UINT8",
            "UINT16",
            "UINT32",
            "TINYINT UNSIGNED",
            "SMALLINT UNSIGNED",
            "INT UNSIGNED",
            "FLOAT",
            "REAL",
            "FLOAT4",
            "FLOAT8",
            "FLOAT32",
            "BOOL",
            "TEXT",
            "VARCHAR(10)",
            "CHAR(10)",
            "TIMESTAMP",
            "DECIMAL(10, 2)",
            "STRING(10)",
            "BIGINT(10)",
        ] {
            for sql in [
                format!(
                    "CREATE TABLE traces (j JSON2 (value {sql_type}), ts TIMESTAMP TIME INDEX)"
                ),
                format!("ALTER TABLE traces MODIFY COLUMN j JSON2 (value {sql_type})"),
            ] {
                let err = ParserContext::create_with_dialect(
                    &sql,
                    &GreptimeDbDialect {},
                    ParseOptions::default(),
                )
                .unwrap_err();
                assert!(
                    err.to_string().contains(
                        "supported types: STRING, BIGINT, BIGINT UNSIGNED, DOUBLE, BOOLEAN; supported aliases: INT64, UINT64, FLOAT64"
                    ),
                    "{sql}: {err}"
                );
            }
        }
    }

    #[test]
    fn test_parse_json2_type_hint_rejects_default() {
        for default in ["-5", "abs(-1)"] {
            let sql = format!(
                "CREATE TABLE traces (log_json_data JSON2 (status_code BIGINT DEFAULT {default}), ts TIMESTAMP TIME INDEX)"
            );
            let err = ParserContext::create_with_dialect(
                &sql,
                &GreptimeDbDialect {},
                ParseOptions::default(),
            )
            .unwrap_err();
            assert!(err.to_string().contains("DEFAULT is not supported"));
        }
    }

    #[test]
    fn test_parse_json2_type_hint_rejects_duplicate_path() {
        let result = ParserContext::create_with_dialect(
            r#"
CREATE TABLE traces (
    log_json_data JSON2 (a.b STRING, a.b BIGINT),
    ts TIMESTAMP TIME INDEX,
)"#,
            &GreptimeDbDialect {},
            ParseOptions::default(),
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("duplicated"));
    }

    #[test]
    fn test_parse_json2_type_hint_rejects_parent_child_path() {
        let result = ParserContext::create_with_dialect(
            r#"
CREATE TABLE traces (
    log_json_data JSON2 (a STRING, a.b BIGINT),
    ts TIMESTAMP TIME INDEX,
)"#,
            &GreptimeDbDialect {},
            ParseOptions::default(),
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("conflicts"));
    }

    #[test]
    fn test_parse_json2_type_hint_rejects_nullability() {
        for sql in [
            r#"
CREATE TABLE traces (
    log_json_data JSON2 (a STRING NULL NULL),
    ts TIMESTAMP TIME INDEX,
)"#,
            r#"
CREATE TABLE traces (
    log_json_data JSON2 (a STRING NOT NULL NOT NULL),
    ts TIMESTAMP TIME INDEX,
)"#,
            r#"
CREATE TABLE traces (
    log_json_data JSON2 (a STRING NOT NULL NULL),
    ts TIMESTAMP TIME INDEX,
)"#,
            r#"
CREATE TABLE traces (
    log_json_data JSON2 (a STRING NULL NOT NULL),
    ts TIMESTAMP TIME INDEX,
)"#,
        ] {
            let result = ParserContext::create_with_dialect(
                sql,
                &GreptimeDbDialect {},
                ParseOptions::default(),
            );

            assert!(result.is_err());
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("NULL/NOT NULL is not supported")
            );
        }
    }
}
