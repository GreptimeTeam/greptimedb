use std::marker::PhantomData;

use derive_builder::Builder;
use rand::seq::SliceRandom;
use rand::Rng;

use crate::context::TableContextRef;
use crate::error::{Error, Result};
use crate::generator::Generator;
use crate::ir::delete_expr::{DeleteExpr, WhereExpr};
use crate::ir::generate_random_value;
use crate::ir::insert_expr::RowValue;

#[derive(Builder)]
#[builder(pattern = "owned")]
pub struct DeleteExprGenerator<R: Rng + 'static> {
    table_ctx: TableContextRef,
    #[builder(default)]
    _phantom: PhantomData<R>,
}

impl<R: Rng + 'static> Generator<DeleteExpr, R> for DeleteExprGenerator<R> {
    type Error = Error;

    fn generate(&self, rng: &mut R) -> Result<DeleteExpr> {
        let filter_columns = self
            .table_ctx
            .columns
            .iter()
            .filter(|col| !col.is_nullable() && !col.has_default_value())
            .cloned()
            .collect::<Vec<_>>();
        if filter_columns.is_empty() {
            return Ok(DeleteExpr::default());
        }
        let selection = if filter_columns.len() > 1 {
            rng.gen_range(1..filter_columns.len())
        } else {
            1
        };
        let mut selected_columns = filter_columns
            .choose_multiple(rng, selection)
            .cloned()
            .collect::<Vec<_>>();
        selected_columns.shuffle(rng);

        let mut where_clause = Vec::with_capacity(selected_columns.len());

        for column in selected_columns.iter() {
            let value = generate_random_value(rng, &column.column_type, None);
            let condition = WhereExpr {
                column: column.name.to_string(),
                value: RowValue::Value(value),
            };
            where_clause.push(condition);
        }

        Ok(DeleteExpr {
            table_name: self.table_ctx.name.to_string(),
            columns: selected_columns,
            where_clause,
        })
    }
}
