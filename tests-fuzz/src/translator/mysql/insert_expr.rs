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
        let columns = input
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
            .to_string();

        Ok(format!(
            "INSERT INTO {} ({})\nVALUES\n{};",
            input.name,
            columns,
            Self::format_values(input)
        ))
    }
}

impl InsertIntoExprTranslator {
    fn format_values(input: &InsertIntoExpr) -> String {
        input
            .rows
            .iter()
            .map(|row| {
                format!(
                    "({})",
                    row.iter()
                        .map(|v| format!("'{v}'"))
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

    use super::InsertIntoExprTranslator;
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
        let expected = r#"INSERT INTO test (host, idc, memory_util, ts, cpu_util, disk_util)
VALUES
('adipisci', 'debitis', '0.5495312687894465', '15292064470292927036', '0.9354265029131291', '0.8037816422279636'),
('ut', 'sequi', '0.8807117723618908', '14214208091261382505', '0.5240550121500691', '0.350785883750684');"#;
        assert_eq!(output, expected);
    }
}
