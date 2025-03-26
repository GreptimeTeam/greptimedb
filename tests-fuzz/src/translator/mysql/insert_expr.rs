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

use crate::error::{Error, Result};
use crate::ir::insert_expr::InsertIntoExpr;
use crate::translator::DslTranslator;

pub struct InsertIntoExprTranslator;

impl DslTranslator<InsertIntoExpr, String> for InsertIntoExprTranslator {
    type Error = Error;

    fn translate(&self, input: &InsertIntoExpr) -> Result<String> {
        Ok(format!(
            "INSERT INTO {} {} VALUES\n{};",
            input.table_name,
            Self::format_columns(input),
            Self::format_values(input)
        ))
    }
}

impl InsertIntoExprTranslator {
    fn format_columns(input: &InsertIntoExpr) -> String {
        if input.omit_column_list {
            "".to_string()
        } else {
            let list = input
                .columns
                .iter()
                .map(|c| c.name.to_string())
                .collect::<Vec<_>>()
                .join(", ")
                .to_string();

            format!("({})", list)
        }
    }

    fn format_values(input: &InsertIntoExpr) -> String {
        input
            .values_list
            .iter()
            .map(|value| {
                format!(
                    "({})",
                    value
                        .iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })
            .collect::<Vec<_>>()
            .join(",\n")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rand::{Rng, SeedableRng};

    use super::*;
    use crate::generator::insert_expr::InsertExprGeneratorBuilder;
    use crate::generator::Generator;
    use crate::test_utils;
    use crate::translator::DslTranslator;

    #[test]
    fn test_insert_into_translator() {
        let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(0);
        let omit_column_list = rng.random_bool(0.2);

        let test_ctx = test_utils::new_test_ctx();
        let insert_expr_generator = InsertExprGeneratorBuilder::default()
            .table_ctx(Arc::new(test_ctx))
            .omit_column_list(omit_column_list)
            .rows(2)
            .build()
            .unwrap();

        let insert_expr = insert_expr_generator.generate(&mut rng).unwrap();

        let output = InsertIntoExprTranslator.translate(&insert_expr).unwrap();
        let expected = r#"INSERT INTO test (cpu_util, ts, host) VALUES
(0.494276426950336, '+210328-02-20 15:44:23.848+0000', 'aut'),
(0.5240550121500691, '-78231-02-16 05:32:41.400+0000', 'in');"#;
        assert_eq!(output, expected);

        let insert_expr = insert_expr_generator.generate(&mut rng).unwrap();
        let output = InsertIntoExprTranslator.translate(&insert_expr).unwrap();
        let expected = r#"INSERT INTO test (ts, host) VALUES
('+137972-11-29 18:23:19.505+0000', 'repellendus'),
('-237884-01-11 09:44:43.491+0000', 'a');"#;
        assert_eq!(output, expected);

        let insert_expr = insert_expr_generator.generate(&mut rng).unwrap();
        let output = InsertIntoExprTranslator.translate(&insert_expr).unwrap();
        let expected = r#"INSERT INTO test (disk_util, ts) VALUES
(0.399415030703252, '+154545-01-21 09:38:13.768+0000'),
(NULL, '-227688-03-19 14:23:24.582+0000');"#;
        assert_eq!(output, expected);
    }
}
