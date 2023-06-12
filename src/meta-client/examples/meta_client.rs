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
use std::time::Duration;

use api::v1::meta::{HeartbeatRequest, Peer, Role};
use chrono::DateTime;
use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
use common_meta::rpc::router::{CreateRequest, Partition};
use common_meta::rpc::store::{
    BatchDeleteRequest, BatchGetRequest, BatchPutRequest, CompareAndPutRequest, DeleteRangeRequest,
    PutRequest, RangeRequest,
};
use common_meta::table_name::TableName;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, RawSchema};
use meta_client::client::MetaClientBuilder;
use table::metadata::{RawTableInfo, RawTableMeta, TableIdent, TableType};
use table::requests::TableOptions;
use tracing::{event, subscriber, Level};
use tracing_subscriber::FmtSubscriber;

fn main() {
    subscriber::set_global_default(FmtSubscriber::builder().finish()).unwrap();
    run();
}

#[tokio::main]
async fn run() {
    let id = (1000u64, 2000u64);
    let config = ChannelConfig::new()
        .timeout(Duration::from_secs(3))
        .connect_timeout(Duration::from_secs(5))
        .tcp_nodelay(true);
    let channel_manager = ChannelManager::with_config(config);
    let mut meta_client = MetaClientBuilder::new(id.0, id.1, Role::Datanode)
        .enable_heartbeat()
        .enable_router()
        .enable_store()
        .channel_manager(channel_manager)
        .build();
    meta_client.start(&["127.0.0.1:3002"]).await.unwrap();
    // required only when the heartbeat_client is enabled
    meta_client.ask_leader().await.unwrap();

    let (sender, mut receiver) = meta_client.heartbeat().await.unwrap();

    // send heartbeats
    tokio::spawn(async move {
        for _ in 0..5 {
            let req = HeartbeatRequest {
                peer: Some(Peer {
                    id: 1,
                    addr: "meta_client_peer".to_string(),
                }),
                ..Default::default()
            };
            sender.send(req).await.unwrap();
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    });

    tokio::spawn(async move {
        while let Some(res) = receiver.message().await.unwrap() {
            event!(Level::TRACE, "heartbeat response: {:#?}", res);
        }
    });

    let p1 = Partition {
        column_list: vec![b"col_1".to_vec(), b"col_2".to_vec()],
        value_list: vec![b"k1".to_vec(), b"k2".to_vec()],
    };

    let p2 = Partition {
        column_list: vec![b"col_1".to_vec(), b"col_2".to_vec()],
        value_list: vec![b"Max1".to_vec(), b"Max2".to_vec()],
    };

    let table_name = TableName::new("test_catalog", "test_schema", "test_table");
    let table_info = new_table_info();
    let create_req = CreateRequest::new(table_name, &table_info)
        .add_partition(p1)
        .add_partition(p2);

    let res = meta_client.create_route(create_req).await;
    event!(Level::INFO, "create_route result: {:#?}", res);

    // put
    let put = PutRequest::new()
        .with_key(b"key1".to_vec())
        .with_value(b"value1".to_vec())
        .with_prev_kv();
    let res = meta_client.put(put).await.unwrap();
    event!(Level::INFO, "put result: {:#?}", res);

    // get
    let range = RangeRequest::new().with_key(b"key1".to_vec());
    let res = meta_client.range(range.clone()).await.unwrap();
    event!(Level::INFO, "get range result: {:#?}", res);

    // get prefix
    let range2 = RangeRequest::new().with_prefix(b"key1".to_vec());
    let res = meta_client.range(range2.clone()).await.unwrap();
    event!(Level::INFO, "get prefix result: {:#?}", res);

    // batch put
    let batch_put = BatchPutRequest::new()
        .add_kv(b"batch_put1".to_vec(), b"batch_put_v1".to_vec())
        .add_kv(b"batch_put2".to_vec(), b"batch_put_v2".to_vec())
        .with_prev_kv();
    let res = meta_client.batch_put(batch_put).await.unwrap();
    event!(Level::INFO, "batch put result: {:#?}", res);

    // cas
    let cas = CompareAndPutRequest::new()
        .with_key(b"batch_put1".to_vec())
        .with_expect(b"batch_put_v_fail".to_vec())
        .with_value(b"batch_put_v111".to_vec());

    let res = meta_client.compare_and_put(cas).await.unwrap();
    event!(Level::INFO, "cas 0 result: {:#?}", res);

    let cas = CompareAndPutRequest::new()
        .with_key(b"batch_put1".to_vec())
        .with_expect(b"batch_put_v1".to_vec())
        .with_value(b"batch_put_v111".to_vec());

    let res = meta_client.compare_and_put(cas).await.unwrap();
    event!(Level::INFO, "cas 1 result: {:#?}", res);

    // delete
    let delete_range = DeleteRangeRequest::new().with_key(b"key1".to_vec());
    let res = meta_client.delete_range(delete_range).await.unwrap();
    event!(Level::INFO, "delete range result: {:#?}", res);

    // get none
    let res = meta_client.range(range).await.unwrap();
    event!(Level::INFO, "get range result: {:#?}", res);

    // batch delete
    // put two
    let batch_put = BatchPutRequest::new()
        .add_kv(b"batch_put1".to_vec(), b"batch_put_v1".to_vec())
        .add_kv(b"batch_put2".to_vec(), b"batch_put_v2".to_vec())
        .with_prev_kv();
    let res = meta_client.batch_put(batch_put).await.unwrap();
    event!(Level::INFO, "batch put result: {:#?}", res);

    // delete one
    let batch_delete = BatchDeleteRequest::new()
        .add_key(b"batch_put1".to_vec())
        .with_prev_kv();
    let res = meta_client.batch_delete(batch_delete).await.unwrap();
    event!(Level::INFO, "batch delete result: {:#?}", res);

    // get other one
    let batch_get = BatchGetRequest::new()
        .add_key(b"batch_put1".to_vec())
        .add_key(b"batch_put2".to_vec());

    let res = meta_client.batch_get(batch_get).await.unwrap();
    event!(Level::INFO, "batch get result: {:#?}", res);
}

fn new_table_info() -> RawTableInfo {
    RawTableInfo {
        ident: TableIdent {
            table_id: 0,
            version: 0,
        },
        name: "test_table".to_string(),
        desc: None,
        catalog_name: "test_catalog".to_string(),
        schema_name: "test_schema".to_string(),
        meta: RawTableMeta {
            schema: RawSchema {
                column_schemas: vec![
                    ColumnSchema::new(
                        "ts",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    ),
                    ColumnSchema::new("col1", ConcreteDataType::string_datatype(), true),
                    ColumnSchema::new("col2", ConcreteDataType::string_datatype(), true),
                ],
                timestamp_index: Some(0),
                version: 0,
            },
            primary_key_indices: vec![],
            value_indices: vec![],
            engine: "mito".to_string(),
            next_column_id: 0,
            region_numbers: vec![],
            engine_options: HashMap::new(),
            options: TableOptions::default(),
            created_on: DateTime::default(),
        },
        table_type: TableType::Base,
    }
}
