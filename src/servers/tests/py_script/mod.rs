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

use std::collections::HashMap;
use std::sync::Arc;

use common_query::Output;
use common_recordbatch::RecordBatch;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::{StringVector, VectorRef};
use servers::error::Result;
use servers::query_handler::sql::SqlQueryHandler;
use servers::query_handler::ScriptHandler;
use session::context::QueryContextBuilder;
use table::test_util::MemTable;

use crate::create_testing_instance;

#[tokio::test]
async fn test_insert_py_udf_and_query() -> Result<()> {
    let catalog = "greptime";
    let schema = "test";
    let name = "hello";
    let script = r#"
@copr(returns=['n'])
def hello() -> vector[str]:
    return 'hello';
"#;

    let column_schemas = vec![
        ColumnSchema::new("script", ConcreteDataType::string_datatype(), false),
        ColumnSchema::new("schema", ConcreteDataType::string_datatype(), false),
        ColumnSchema::new("name", ConcreteDataType::string_datatype(), false),
    ];

    let columns: Vec<VectorRef> = vec![
        Arc::new(StringVector::from(vec![script])),
        Arc::new(StringVector::from(vec![schema])),
        Arc::new(StringVector::from(vec![name])),
    ];

    let raw_schema = Arc::new(Schema::new(column_schemas));
    let recordbatch = RecordBatch::new(raw_schema, columns).unwrap();

    let table = MemTable::table("scripts", recordbatch);

    let query_ctx = QueryContextBuilder::default()
        .current_catalog(catalog.to_string())
        .current_schema(schema.to_string())
        .build();

    let instance = create_testing_instance(table);
    instance
        .insert_script(query_ctx.clone(), name, script)
        .await?;

    let output = instance
        .execute_script(query_ctx.clone(), name, HashMap::new())
        .await?;

    match output {
        Output::RecordBatches(batches) => {
            let expected = "\
+-------+
| n     |
+-------+
| hello |
+-------+";
            assert_eq!(expected, batches.pretty_print().unwrap());
        }
        _ => unreachable!(),
    }

    let res = instance
        .do_query("select hello()", query_ctx)
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
            let expected = "\
+---------+
| hello() |
+---------+
| hello   |
+---------+";

            assert_eq!(expected, batches.pretty_print().unwrap());
        }
    }

    Ok(())
}
