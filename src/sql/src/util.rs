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

use std::fmt::{Display, Formatter};

use sqlparser::ast::{Expr, ObjectName, SqlOption, Value};

use crate::error::{InvalidTableOptionValueSnafu, Result};

/// Format an [ObjectName] without any quote of its idents.
pub fn format_raw_object_name(name: &ObjectName) -> String {
    struct Inner<'a> {
        name: &'a ObjectName,
    }

    impl<'a> Display for Inner<'a> {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            let mut delim = "";
            for ident in self.name.0.iter() {
                write!(f, "{delim}")?;
                delim = ".";
                write!(f, "{}", ident.value)?;
            }
            Ok(())
        }
    }

    format!("{}", Inner { name })
}

pub fn parse_option_string(option: SqlOption) -> Result<(String, String)> {
    let (key, value) = (option.name, option.value);
    let v = match value {
        Expr::Value(Value::SingleQuotedString(v)) | Expr::Value(Value::DoubleQuotedString(v)) => v,
        Expr::Identifier(v) => v.value,
        Expr::Value(Value::Number(v, _)) => v.to_string(),
        value => return InvalidTableOptionValueSnafu { key, value }.fail(),
    };
    let k = key.value.to_lowercase();
    Ok((k, v))
}
