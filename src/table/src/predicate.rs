mod stats;

use common_query::logical_plan::Expr;
use common_telemetry::{error, warn};
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datatypes::arrow::io::parquet::read::RowGroupMetaData;
use datatypes::schema::SchemaRef;

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
        schema: SchemaRef,
        row_groups: &[RowGroupMetaData],
    ) -> Vec<bool> {
        let mut res = vec![true; row_groups.len()];
        for expr in &self.exprs {
            match PruningPredicate::try_new(expr.df_expr().clone(), schema.arrow_schema().clone()) {
                Ok(p) => {
                    let stat = RowGroupPruningStatistics::new(row_groups, &schema);
                    match p.prune(&stat) {
                        Ok(r) => {
                            for (curr_val, res) in r.into_iter().zip(res.iter_mut()) {
                                *res &= curr_val
                            }
                        }
                        Err(e) => {
                            warn!("Failed to prune row groups, error: {:?}", e);
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to create predicate for expr, error: {:?}", e);
                }
            }
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    pub use datafusion::parquet::schema::types::{BasicTypeInfo, PhysicalType};
    use datafusion_common::Column;
    use datafusion_expr::{Expr, Literal, Operator};
    use datatypes::arrow::array::{Int32Array, Utf8Array};
    use datatypes::arrow::chunk::Chunk;
    use datatypes::arrow::datatypes::{DataType, Field, Schema};
    use datatypes::arrow::io::parquet::read::FileReader;
    use datatypes::arrow::io::parquet::write::{
        Compression, Encoding, FileSink, Version, WriteOptions,
    };
    use futures::{AsyncWriteExt, SinkExt};
    use tempdir::TempDir;
    use tokio_util::compat::TokioAsyncWriteCompatExt;

    use super::*;

    async fn gen_test_parquet_file(dir: &TempDir, cnt: usize) -> (String, Arc<Schema>) {
        let path = dir
            .path()
            .join("test-prune.parquet")
            .to_string_lossy()
            .to_string();

        let name_field = Field::new("name", DataType::Utf8, true);
        let count_field = Field::new("cnt", DataType::Int32, true);

        let schema = Schema::from(vec![name_field, count_field]);

        // now all physical types use plain encoding, maybe let caller to choose encoding for each type.
        let encodings = vec![Encoding::Plain].repeat(schema.fields.len());

        let mut writer = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path)
            .await
            .unwrap()
            .compat_write();

        let mut sink = FileSink::try_new(
            &mut writer,
            schema.clone(),
            encodings,
            WriteOptions {
                write_statistics: true,
                compression: Compression::Gzip,
                version: Version::V2,
            },
        )
        .unwrap();

        for i in (0..cnt).step_by(10) {
            let name_array = Utf8Array::<i64>::from(
                &(i..(i + 10).min(cnt))
                    .map(|i| Some(i.to_string()))
                    .collect::<Vec<_>>(),
            );
            let count_array = Int32Array::from(
                &(i..(i + 10).min(cnt))
                    .map(|i| Some(i as i32))
                    .collect::<Vec<_>>(),
            );

            sink.send(Chunk::new(vec![
                Arc::new(name_array),
                Arc::new(count_array),
            ]))
            .await
            .unwrap();
        }
        sink.close().await.unwrap();

        drop(sink);
        writer.flush().await.unwrap();

        (path, Arc::new(schema))
    }

    async fn assert_prune(array_cnt: usize, predicate: Predicate, expect: Vec<bool>) {
        let dir = TempDir::new("prune_parquet").unwrap();
        let (path, schema) = gen_test_parquet_file(&dir, array_cnt).await;
        let file_reader =
            FileReader::try_new(std::fs::File::open(path).unwrap(), None, None, None, None)
                .unwrap();

        let schema = Arc::new(datatypes::schema::Schema::try_from(schema).unwrap());

        let vec = file_reader.metadata().row_groups.clone();
        let res = predicate.prune_row_groups(schema, &vec);
        assert_eq!(expect, res);
    }

    fn gen_predicate(max_val: i32, op: Operator) -> Predicate {
        Predicate::new(vec![Expr::BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("cnt".to_string()))),
            op,
            right: Box::new(max_val.lit()),
        }
        .into()])
    }

    #[tokio::test]
    async fn test_prune_empty() {
        assert_prune(3, Predicate::empty(), vec![true]).await;
    }

    #[tokio::test]
    async fn test_prune_all_match() {
        let p = gen_predicate(3, Operator::Gt);
        assert_prune(2, p, vec![false]).await;
    }

    #[tokio::test]
    async fn test_prune_gt() {
        let p = gen_predicate(29, Operator::Gt);
        assert_prune(
            100,
            p,
            vec![
                false, false, false, true, true, true, true, true, true, true,
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_prune_eq_expr() {
        let p = gen_predicate(30, Operator::Eq);
        assert_prune(40, p, vec![false, false, false, true]).await;
    }

    #[tokio::test]
    async fn test_prune_neq_expr() {
        let p = gen_predicate(30, Operator::NotEq);
        assert_prune(40, p, vec![true, true, true, true]).await;
    }

    #[tokio::test]
    async fn test_prune_gteq_expr() {
        let p = gen_predicate(29, Operator::GtEq);
        assert_prune(40, p, vec![false, false, true, true]).await;
    }

    #[tokio::test]
    async fn test_prune_lt_expr() {
        let p = gen_predicate(30, Operator::Lt);
        assert_prune(40, p, vec![true, true, true, false]).await;
    }

    #[tokio::test]
    async fn test_prune_lteq_expr() {
        let p = gen_predicate(30, Operator::LtEq);
        assert_prune(40, p, vec![true, true, true, true]).await;
    }

    #[tokio::test]
    async fn test_prune_between_expr() {
        let p = gen_predicate(30, Operator::LtEq);
        assert_prune(40, p, vec![true, true, true, true]).await;
    }

    #[tokio::test]
    async fn test_or() {
        // cnt > 30 or cnt < 20
        let e = Expr::Column(Column::from_name("cnt"))
            .gt(30.lit())
            .or(Expr::Column(Column::from_name("cnt")).lt(20.lit()));
        let p = Predicate::new(vec![e.into()]);
        assert_prune(40, p, vec![true, true, false, true]).await;
    }
}
