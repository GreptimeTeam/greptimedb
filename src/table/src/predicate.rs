mod stats;

use std::sync::Arc;

use common_query::logical_plan::Expr;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datatypes::arrow::datatypes::Schema as ArrowSchema;
use datatypes::arrow::io::parquet::read::RowGroupMetaData;

use crate::predicate::stats::RowGroupPruningStatistics;

#[derive(Default, Clone)]
pub struct Predicate {
    exprs: Vec<Expr>,
}

impl Predicate {
    pub fn new(exprs: Vec<Expr>) -> Self {
        Self { exprs }
    }

    pub fn empty() -> Self {
        Self { exprs: vec![] }
    }

    pub fn prune_row_groups(
        &self,
        schema: Arc<ArrowSchema>,
        row_groups: &[RowGroupMetaData],
    ) -> Vec<bool> {
        let mut res = vec![true; row_groups.len()];
        for expr in &self.exprs {
            match PruningPredicate::try_new(expr.df_expr().clone(), schema.clone()) {
                Ok(p) => {
                    let stat = RowGroupPruningStatistics {
                        schema: &schema,
                        meta_data: row_groups,
                    };

                    let r = p.prune(&stat).unwrap();
                    for (curr_val, res) in r.into_iter().zip(res.iter_mut()) {
                        *res = curr_val & &*res
                    }
                }
                Err(e) => {}
            }
        }
        res
    }
}
