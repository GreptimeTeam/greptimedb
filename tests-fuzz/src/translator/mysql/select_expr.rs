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
use crate::ir::select_expr::SelectExpr;
use crate::translator::DslTranslator;

pub struct SelectExprTranslator;

impl DslTranslator<SelectExpr, String> for SelectExprTranslator {
    type Error = Error;

    fn translate(&self, input: &SelectExpr) -> Result<String> {
        let columns = input
            .columns
            .iter()
            .map(|c| c.name.to_string())
            .collect::<Vec<_>>()
            .join(", ")
            .to_string();

        let order_by = input
            .order_by
            .iter()
            .map(|c| c.as_str())
            .collect::<Vec<_>>()
            .join(", ")
            .to_string();

        Ok(format!(
            "SELECT {} FROM {} ORDER BY {} {};",
            columns, input.table_name, order_by, input.direction,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rand::SeedableRng;

    use super::SelectExprTranslator;
    use crate::generator::select_expr::SelectExprGeneratorBuilder;
    use crate::generator::Generator;
    use crate::test_utils;
    use crate::translator::DslTranslator;

    #[test]
    fn test_select_expr_translator() {
        let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(0);

        let test_ctx = test_utils::new_test_ctx();
        let select_expr_generator = SelectExprGeneratorBuilder::default()
            .table_ctx(Arc::new(test_ctx))
            .build()
            .unwrap();

        let select_expr = select_expr_generator.generate(&mut rng).unwrap();
        let output = SelectExprTranslator.translate(&select_expr).unwrap();
        let expected = r#"SELECT ts, memory_util, cpu_util, disk_util FROM test ORDER BY disk_util, ts DESC;"#;
        assert_eq!(output, expected);
    }
}
