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
use crate::ir::delete_expr::DeleteExpr;
use crate::translator::DslTranslator;

pub struct DeleteExprTranslator;

impl DslTranslator<DeleteExpr, String> for DeleteExprTranslator {
    type Error = Error;

    fn translate(&self, input: &DeleteExpr) -> Result<String> {
        // Generating WHERE clause if exists
        let where_clause = if !input.where_clause.is_empty() {
            input
                .where_clause
                .iter()
                .map(|where_expr| format!("{} = '{}'", where_expr.column, where_expr.value))
                .collect::<Vec<_>>()
                .join(" AND ")
        } else {
            "1".to_string()
        };

        Ok(format!(
            "DELETE FROM {} WHERE {};",
            input.table_name, where_clause,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rand::SeedableRng;

    use super::DeleteExprTranslator;
    use crate::generator::delete_expr::DeleteExprGeneratorBuilder;
    use crate::generator::Generator;
    use crate::test_utils;
    use crate::translator::DslTranslator;

    #[test]
    fn test_select_expr_translator() {
        let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(0);

        let test_ctx = test_utils::new_test_ctx();
        let delete_expr_generator = DeleteExprGeneratorBuilder::default()
            .table_ctx(Arc::new(test_ctx))
            .build()
            .unwrap();

        let delete_expr = delete_expr_generator.generate(&mut rng).unwrap();
        let output = DeleteExprTranslator.translate(&delete_expr).unwrap();

        let expected_output = "DELETE FROM test WHERE memory_util = '0.4147731206727985' AND ts = '+169626-08-17 05:35:46.714+0000' AND cpu_util = '0.494276426950336' AND disk_util = '0.9011706134313209';";
        assert_eq!(output, expected_output);
    }
}
