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

use snafu::ResultExt;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use crate::error::{Result, SyntaxSnafu};
use crate::statements::OptionMap;
use crate::util;

pub(super) fn parse_json_datatype_options(parser: &mut Parser<'_>) -> Result<Option<OptionMap>> {
    if parser.consume_token(&Token::LParen) {
        let result = parser
            .parse_comma_separated0(Parser::parse_sql_option, Token::RParen)
            .context(SyntaxSnafu)
            .and_then(|options| {
                options
                    .into_iter()
                    .map(util::parse_option_string)
                    .collect::<Result<Vec<_>>>()
            })?;
        parser.expect_token(&Token::RParen).context(SyntaxSnafu)?;
        Ok(Some(OptionMap::new(result)))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::DataType;

    use crate::dialect::GreptimeDbDialect;
    use crate::parser::{ParseOptions, ParserContext};
    use crate::statements::OptionMap;
    use crate::statements::create::{
        Column, JSON_FORMAT_FULL_STRUCTURED, JSON_FORMAT_PARTIAL, JSON_FORMAT_RAW, JSON_OPT_FORMAT,
        JSON_OPT_UNSTRUCTURED_KEYS,
    };
    use crate::statements::statement::Statement;

    #[test]
    fn test_parse_json_datatype_options() {
        fn parse(sql: &str) -> Option<OptionMap> {
            let Statement::CreateTable(mut create_table) = ParserContext::create_with_dialect(
                sql,
                &GreptimeDbDialect {},
                ParseOptions::default(),
            )
            .unwrap()
            .remove(0) else {
                unreachable!()
            };

            let Column {
                column_def,
                extensions,
            } = create_table.columns.remove(0);
            assert_eq!(column_def.name.to_string(), "my_json");
            assert_eq!(column_def.data_type, DataType::JSON);
            assert!(column_def.options.is_empty());

            extensions.json_datatype_options
        }

        let sql = r#"
CREATE TABLE json_data (
    my_json JSON(format = "partial", unstructured_keys = ["k", "foo.bar", "a.b.c"]),
    ts TIMESTAMP TIME INDEX,
)"#;
        let options = parse(sql).unwrap();
        assert_eq!(options.len(), 2);
        assert_eq!(
            options.value(JSON_OPT_FORMAT).and_then(|x| x.as_string()),
            Some(JSON_FORMAT_PARTIAL)
        );
        let expected = vec!["k", "foo.bar", "a.b.c"];
        assert_eq!(
            options
                .value(JSON_OPT_UNSTRUCTURED_KEYS)
                .and_then(|x| x.as_list()),
            Some(expected)
        );

        let sql = r#"
CREATE TABLE json_data (
    my_json JSON(format = "structured"),
    ts TIMESTAMP TIME INDEX,
)"#;
        let options = parse(sql).unwrap();
        assert_eq!(options.len(), 1);
        assert_eq!(
            options.value(JSON_OPT_FORMAT).and_then(|x| x.as_string()),
            Some(JSON_FORMAT_FULL_STRUCTURED)
        );

        let sql = r#"
CREATE TABLE json_data (
    my_json JSON(format = "raw"),
    ts TIMESTAMP TIME INDEX,
)"#;
        let options = parse(sql).unwrap();
        assert_eq!(options.len(), 1);
        assert_eq!(
            options.value(JSON_OPT_FORMAT).and_then(|x| x.as_string()),
            Some(JSON_FORMAT_RAW)
        );

        let sql = r#"
CREATE TABLE json_data (
    my_json JSON(),
    ts TIMESTAMP TIME INDEX,
)"#;
        let options = parse(sql).unwrap();
        assert!(options.is_empty());

        let sql = r#"
CREATE TABLE json_data (
    my_json JSON,
    ts TIMESTAMP TIME INDEX,
)"#;
        let options = parse(sql);
        assert!(options.is_none());
    }
}
