use std::sync::Arc;

use servers::error::Result;
use servers::query_handler::{ScriptHandler, SqlQueryHandler};
use session::context::QueryContext;
use table::test_util::MemTable;

use crate::create_testing_instance;

#[tokio::test]
async fn test_insert_udf_and_query() -> Result<()> {
    let query_ctx = Arc::new(QueryContext::new());
    let table = MemTable::default_numbers_table();

    let instance = create_testing_instance(table);
    let src = r#"
@coprocessor(args=["uint32s"], returns = ["ret"])
def double_that(col)->vector[u32]:
    return col*2
    "#;
    instance.insert_script("double_that", src).await?;
    let res = instance
        .do_query("select double_that(uint32s) from numbers", query_ctx)
        .await
        .remove(0)
        .unwrap();
    match res {
        common_query::Output::AffectedRows(_) => (),
        common_query::Output::RecordBatches(_) => {
            unreachable!()
        }
        common_query::Output::Stream(s) => {
            let batches = common_recordbatch::util::collect_batches(s).await.unwrap();
            assert_eq!(batches.iter().count(), 1);
            let first = batches.iter().next().unwrap();
            let col = first.column(0);
            let val = col.get(1);
            assert_eq!(val, datatypes::value::Value::UInt32(2));
            let pretty_print = batches.pretty_print().unwrap();
            println!("{pretty_print}");
        }
    }
    Ok(())
}
