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

pub use sqlparser::ast::{
    BinaryOperator, ColumnDef, ColumnOption, ColumnOptionDef, DataType, Expr, Function,
    FunctionArg, FunctionArgExpr, FunctionArguments, Ident, ObjectName, ObjectNamePart, SqlOption,
    TableConstraint, TimezoneInfo, Value, ValueWithSpan, Visit, VisitMut, Visitor, VisitorMut,
    visit_expressions_mut, visit_statements_mut,
};

/// Normalizes an identifier using ASCII-only case folding when unquoted,
/// preserving exact casing for quoted identifiers (matching DataFusion's
/// `IdentNormalizer` with `enable_ident_normalization = true`).
pub fn normalize_ident(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    }
}

pub trait ObjectNamePartExt {
    fn to_string_unquoted(&self) -> String;

    /// Returns the identifier string normalized with ASCII-only lowercase when
    /// unquoted, preserving original casing for quoted identifiers (matching
    /// DataFusion's SQL planner with `enable_ident_normalization = true`).
    fn to_string_normalized(&self) -> String;
}

impl ObjectNamePartExt for ObjectNamePart {
    fn to_string_unquoted(&self) -> String {
        let ObjectNamePart::Identifier(ident) = self else {
            // If it's not an ident, just return it as a string.
            return self.to_string();
        };
        ident.value.clone()
    }

    fn to_string_normalized(&self) -> String {
        let ObjectNamePart::Identifier(ident) = self else {
            return self.to_string();
        };
        normalize_ident(ident)
    }
}

/// Formats an object name by joining its identifier values without quotes.
pub fn object_name_to_string_unquoted(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(ObjectNamePartExt::to_string_unquoted)
        .collect::<Vec<_>>()
        .join(".")
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};

    use super::{ObjectNamePartExt, object_name_to_string_unquoted};

    #[test]
    fn test_object_name_to_string_unquoted() {
        let quoted = ObjectName(vec![ObjectNamePart::Identifier(Ident::with_quote(
            '"',
            "query.parallelism",
        ))]);
        let unquoted = ObjectName(vec![
            ObjectNamePart::Identifier(Ident::new("query")),
            ObjectNamePart::Identifier(Ident::new("parallelism")),
        ]);

        assert_eq!("query.parallelism", object_name_to_string_unquoted(&quoted));
        assert_eq!(
            object_name_to_string_unquoted(&unquoted),
            object_name_to_string_unquoted(&quoted)
        );
    }

    #[test]
    fn test_object_name_part_to_string_normalized() {
        let unquoted = ObjectNamePart::Identifier(Ident::new("Readings"));
        let double_quoted = ObjectNamePart::Identifier(Ident::with_quote('"', "Readings"));
        let backtick_quoted = ObjectNamePart::Identifier(Ident::with_quote('`', "Readings"));
        let kelvin_unquoted = ObjectNamePart::Identifier(Ident::new("\u{212A}PI"));
        let kelvin_quoted = ObjectNamePart::Identifier(Ident::with_quote('"', "\u{212A}PI"));

        assert_eq!("readings", unquoted.to_string_normalized());
        assert_eq!("Readings", double_quoted.to_string_normalized());
        assert_eq!("Readings", backtick_quoted.to_string_normalized());
        assert_eq!("\u{212A}pi", kelvin_unquoted.to_string_normalized());
        assert_eq!("\u{212A}PI", kelvin_quoted.to_string_normalized());
    }
}
