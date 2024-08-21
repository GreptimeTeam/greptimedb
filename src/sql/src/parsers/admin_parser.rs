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
use sqlparser::tokenizer::Token;

use crate::ast::{Expr, Function};
use crate::error::{Result, SyntaxSnafu};
use crate::parser::ParserContext;
use crate::statements::admin::Admin;
use crate::statements::statement::Statement;

/// `admin` extension parser: `admin function(arg1, arg2, ...)`
/// or `admin function`
impl<'a> ParserContext<'a> {
    /// Parse `admin function(arg1, arg2, ...)` or `admin function` statement
    pub(crate) fn parse_admin_command(&mut self) -> Result<Statement> {
        let _token = self.parser.next_token();

        let object_name = self.parser.parse_object_name(false).context(SyntaxSnafu)?;

        let func = if self.parser.peek_token().token == Token::LParen {
            match self
                .parser
                .parse_function(object_name)
                .context(SyntaxSnafu)?
            {
                Expr::Function(f) => f,
                _ => {
                    return self.unsupported(self.peek_token_as_string());
                }
            }
        } else {
            Function {
                name: object_name,
                args: vec![],
                over: None,
                filter: None,
                null_treatment: None,
                distinct: false,
                special: false,
                order_by: vec![],
            }
        };

        Ok(Statement::Admin(Admin::Func(func)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Expr, FunctionArg, FunctionArgExpr, Value};
    use crate::dialect::GreptimeDbDialect;
    use crate::parser::ParseOptions;

    #[test]
    fn test_parse_admin_function() {
        let sql = "ADMIN flush_table('test')";

        let mut result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());
        let stmt = result.remove(0);
        match &stmt {
            Statement::Admin(Admin::Func(Function { name, args, .. })) => {
                assert_eq!("flush_table", name.to_string());
                assert_eq!(args.len(), 1);
                assert!(matches!(&args[0],
                                 FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                     Expr::Value(Value::SingleQuotedString(s))
                                 )) if s == "test"));
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_admin_function_without_args() {
        let sql = "ADMIN test";

        let mut result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());
        let stmt = result.remove(0);
        match &stmt {
            Statement::Admin(Admin::Func(Function { name, args, .. })) => {
                assert_eq!("test", name.to_string());
                assert_eq!(args.len(), 0);
            }
            _ => unreachable!(),
        }
    }
}
