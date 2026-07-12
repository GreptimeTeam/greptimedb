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

use snafu::{ResultExt, ensure};
use sqlparser::ast::{DataType, ExactNumberInfo, Expr, ObjectName, UnaryOperator};
use sqlparser::dialect::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use crate::ast::Ident;
use crate::error::{InvalidSqlSnafu, Result, SyntaxSnafu};
use crate::parsers::create_parser::{INVERTED, SKIPPING};
use crate::statements::create::JsonTypeHint;
use crate::statements::transform::type_alias::get_type_by_alias;

const JSON2_TYPE_NAME: &str = "JSON2";

pub(super) fn parse_json2_type_and_hints(
    parser: &mut Parser<'_>,
) -> Result<Option<(DataType, Vec<JsonTypeHint>)>> {
    let token = parser.peek_token();
    let Token::Word(word) = &token.token else {
        return Ok(None);
    };

    if !word.value.eq_ignore_ascii_case(JSON2_TYPE_NAME) || word.quote_style.is_some() {
        return Ok(None);
    }

    parser.next_token();
    let data_type = DataType::Custom(ObjectName::from(vec![Ident::new(JSON2_TYPE_NAME)]), vec![]);
    let type_hints = if parser.consume_token(&Token::LParen) {
        parse_json2_type_hints(parser)?
    } else {
        vec![]
    };

    Ok(Some((data_type, type_hints)))
}

fn parse_json2_type_hints(parser: &mut Parser<'_>) -> Result<Vec<JsonTypeHint>> {
    let mut hints = Vec::new();

    if parser.consume_token(&Token::RParen) {
        return Ok(hints);
    }

    loop {
        let hint = parse_json2_type_hint(parser)?;
        ensure_no_path_conflict(&hints, &hint.path)?;
        hints.push(hint);

        if parser.consume_token(&Token::Comma) {
            if parser.consume_token(&Token::RParen) {
                break;
            }
        } else {
            parser.expect_token(&Token::RParen).context(SyntaxSnafu)?;
            break;
        }
    }

    Ok(hints)
}

fn parse_json2_type_hint(parser: &mut Parser<'_>) -> Result<JsonTypeHint> {
    let path = parse_json2_path(parser)?;
    let data_type = parser.parse_data_type().context(SyntaxSnafu)?;
    let data_type = normalize_json2_type_hint_type(data_type)?;

    let mut nullable = true;
    let mut nullable_set = false;
    let mut default = None;
    let mut inverted_index = false;

    loop {
        if parser.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
            ensure!(
                !nullable_set,
                InvalidSqlSnafu {
                    msg: format!(
                        "NULL/NOT NULL option already specified for JSON2 type hint '{}'",
                        path.join(".")
                    )
                }
            );
            nullable = false;
            nullable_set = true;
        } else if parser.parse_keyword(Keyword::NULL) {
            ensure!(
                !nullable_set,
                InvalidSqlSnafu {
                    msg: format!(
                        "NULL/NOT NULL option already specified for JSON2 type hint '{}'",
                        path.join(".")
                    )
                }
            );
            nullable = true;
            nullable_set = true;
        } else if parser.parse_keyword(Keyword::DEFAULT) {
            ensure!(
                default.is_none(),
                InvalidSqlSnafu {
                    msg: format!(
                        "duplicated DEFAULT option for JSON2 type hint '{}'",
                        path.join(".")
                    )
                }
            );
            let expr = parser.parse_expr().context(SyntaxSnafu)?;
            ensure_json2_default_expr_is_literal(&expr)?;
            default = Some(expr);
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
        nullable,
        default,
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

fn normalize_json2_type_hint_type(data_type: DataType) -> Result<DataType> {
    let data_type = get_type_by_alias(&data_type).unwrap_or(data_type);
    let normalized = match data_type {
        DataType::String(_) | DataType::Text | DataType::Varchar(_) | DataType::Char(_) => {
            DataType::String(None)
        }
        DataType::TinyInt(_)
        | DataType::SmallInt(_)
        | DataType::Int(_)
        | DataType::Integer(_)
        | DataType::BigInt(_) => DataType::BigInt(None),
        DataType::TinyIntUnsigned(_)
        | DataType::SmallIntUnsigned(_)
        | DataType::IntUnsigned(_)
        | DataType::UnsignedInteger
        | DataType::BigIntUnsigned(_) => DataType::BigIntUnsigned(None),
        DataType::Float(_) | DataType::Real | DataType::Double(_) => {
            DataType::Double(ExactNumberInfo::None)
        }
        DataType::Boolean => DataType::Boolean,
        _ => {
            return InvalidSqlSnafu {
                msg: format!("unsupported JSON2 type hint data type: {data_type}"),
            }
            .fail();
        }
    };

    Ok(normalized)
}

fn ensure_json2_default_expr_is_literal(expr: &Expr) -> Result<()> {
    let is_literal = match expr {
        Expr::Value(_) => true,
        Expr::UnaryOp { op, expr } => {
            matches!(op, UnaryOperator::Plus | UnaryOperator::Minus)
                && matches!(expr.as_ref(), Expr::Value(_))
        }
        _ => false,
    };
    ensure!(
        is_literal,
        InvalidSqlSnafu {
            msg: "JSON2 type hint DEFAULT only supports literal values",
        }
    );
    Ok(())
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
    fn test_parse_json2_type_hints() {
        let column = parse_json2_column(
            r#"
CREATE TABLE traces (
    log_json_data JSON2 (
        "service.name" STRING NOT NULL DEFAULT 'null' INVERTED INDEX,
        http.method STRING NOT NULL,
        status_code INT64 NOT NULL,
        comment STRING NULL,
    ),
    ts TIMESTAMP TIME INDEX,
)"#,
        );

        assert!(matches!(
            column.column_def.data_type,
            DataType::Custom(_, _)
        ));
        let hints = column.extensions.json_type_hints;
        assert_eq!(hints.len(), 4);

        assert_eq!(hints[0].path, vec!["service.name"]);
        assert_eq!(hints[0].data_type, DataType::String(None));
        assert!(!hints[0].nullable);
        assert_eq!(
            hints[0]
                .default
                .as_ref()
                .map(|expr| expr.to_string())
                .as_deref(),
            Some("'null'")
        );
        assert!(hints[0].inverted_index);

        assert_eq!(hints[1].path, vec!["http", "method"]);
        assert_eq!(hints[1].data_type, DataType::String(None));
        assert!(!hints[1].nullable);
        assert!(!hints[1].inverted_index);

        assert_eq!(hints[2].path, vec!["status_code"]);
        assert_eq!(hints[2].data_type, DataType::BigInt(None));
        assert!(!hints[2].nullable);

        assert_eq!(hints[3].path, vec!["comment"]);
        assert_eq!(hints[3].data_type, DataType::String(None));
        assert!(hints[3].nullable);
    }

    #[test]
    fn test_parse_json2_type_hint_default_nullable() {
        let column = parse_json2_column(
            r#"
CREATE TABLE traces (
    log_json_data JSON2 (http.method STRING),
    ts TIMESTAMP TIME INDEX,
)"#,
        );

        let hints = column.extensions.json_type_hints;
        assert_eq!(hints.len(), 1);
        assert!(hints[0].nullable);
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

        let hints = column.extensions.json_type_hints;
        assert_eq!(hints.len(), 4);
        assert_eq!(hints[0].path, vec!["a", "b"]);
        assert_eq!(hints[1].path, vec!["x", "y"]);
        assert_eq!(hints[2].path, vec!["a.b", "c"]);
        assert_eq!(hints[3].path, vec!["a", "b.c"]);
    }

    #[test]
    fn test_parse_json2_type_hint_normalizes_numeric_types() {
        let column = parse_json2_column(
            r#"
CREATE TABLE traces (
    log_json_data JSON2 (
        tinyint_value TINYINT,
        smallint_value SMALLINT,
        int_value INT,
        integer_value INTEGER,
        bigint_value BIGINT,
        int64_value INT64,
        tinyuint_value TINYINT UNSIGNED,
        smalluint_value SMALLINT UNSIGNED,
        uint_value INT UNSIGNED,
        uint64_value UINT64,
        float_value FLOAT,
        real_value REAL,
        double_value DOUBLE,
        float64_value FLOAT64
    ),
    ts TIMESTAMP TIME INDEX,
)"#,
        );

        let hints = column.extensions.json_type_hints;
        assert_eq!(hints.len(), 14);
        for hint in hints.iter().take(6) {
            assert_eq!(hint.data_type, DataType::BigInt(None));
        }
        for hint in hints.iter().skip(6).take(4) {
            assert_eq!(hint.data_type, DataType::BigIntUnsigned(None));
        }
        for hint in hints.iter().skip(10) {
            assert_eq!(hint.data_type, DataType::Double(ExactNumberInfo::None));
        }
    }

    #[test]
    fn test_parse_json2_type_hint_default_accepts_signed_literals() {
        let column = parse_json2_column(
            r#"
CREATE TABLE traces (
    log_json_data JSON2 (
        negative_int INT64 DEFAULT -5,
        positive_float FLOAT64 DEFAULT +1.5
    ),
    ts TIMESTAMP TIME INDEX,
)"#,
        );

        let hints = column.extensions.json_type_hints;
        assert_eq!(hints.len(), 2);
        assert_eq!(
            hints[0]
                .default
                .as_ref()
                .map(|expr| expr.to_string())
                .as_deref(),
            Some("-5")
        );
        assert_eq!(
            hints[1]
                .default
                .as_ref()
                .map(|expr| expr.to_string())
                .as_deref(),
            Some("+1.5")
        );
    }

    #[test]
    fn test_parse_json2_type_hint_default_rejects_function() {
        let result = ParserContext::create_with_dialect(
            r#"
CREATE TABLE traces (
    log_json_data JSON2 (status_code INT64 DEFAULT abs(-1)),
    ts TIMESTAMP TIME INDEX,
)"#,
            &GreptimeDbDialect {},
            ParseOptions::default(),
        );

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("DEFAULT only supports literal values")
        );
    }

    #[test]
    fn test_parse_json2_type_hint_rejects_duplicate_path() {
        let result = ParserContext::create_with_dialect(
            r#"
CREATE TABLE traces (
    log_json_data JSON2 (a.b STRING, a.b INT64),
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
    log_json_data JSON2 (a STRING, a.b INT64),
    ts TIMESTAMP TIME INDEX,
)"#,
            &GreptimeDbDialect {},
            ParseOptions::default(),
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("conflicts"));
    }

    #[test]
    fn test_parse_json2_type_hint_rejects_duplicated_nullability() {
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
                    .contains("NULL/NOT NULL option already specified")
            );
        }
    }
}
