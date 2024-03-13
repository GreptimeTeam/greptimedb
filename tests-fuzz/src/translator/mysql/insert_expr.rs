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
        if input.columns.is_empty() {
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

    use rand::SeedableRng;

    use super::*;
    use crate::generator::insert_expr::InsertExprGeneratorBuilder;
    use crate::generator::Generator;
    use crate::test_utils;
    use crate::translator::DslTranslator;

    #[test]
    fn test_insert_into_translator() {
        let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(0);

        let test_ctx = test_utils::new_test_ctx();
        let insert_expr_generator = InsertExprGeneratorBuilder::default()
            .table_ctx(Arc::new(test_ctx))
            .rows(2)
            .build()
            .unwrap();

        let insert_expr = insert_expr_generator.generate(&mut rng).unwrap();

        let output = InsertIntoExprTranslator.translate(&insert_expr).unwrap();
        let expected = r#"INSERT INTO test (ts, host, cpu_util) VALUES
('+199601-11-07 21:32:56.695+0000', 'corrupti', 0.051130243193075464),
('+40822-03-25 02:17:34.328+0000', NULL, 0.6552502332327004);"#;
        assert_eq!(output, expected);

        let insert_expr = insert_expr_generator.generate(&mut rng).unwrap();
        let output = InsertIntoExprTranslator.translate(&insert_expr).unwrap();
        let expected = r#"INSERT INTO test (cpu_util, disk_util, ts) VALUES
(0.7074194466620976, 0.661288102315126, '-47252-05-08 07:33:49.567+0000'),
(0.8266101224213618, 0.7947724277743285, '-224292-12-07 02:51:53.371+0000');"#;
        assert_eq!(output, expected);

        let insert_expr = insert_expr_generator.generate(&mut rng).unwrap();
        let output = InsertIntoExprTranslator.translate(&insert_expr).unwrap();
        let expected = r#"INSERT INTO test  VALUES
('odio', NULL, 0.48809950435391647, 0.5228925709595407, 0.9091528874275897, '+241156-12-16 20:52:15.185+0000'),
('dignissimos', 'labore', NULL, 0.12983559048685023, 0.6362040919831425, '-30691-06-17 23:41:09.938+0000');"#;
        assert_eq!(output, expected);
    }
}
