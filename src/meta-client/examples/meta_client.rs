use std::time::Duration;

use api::v1::meta::region::Partition;
use api::v1::meta::CreateRequest;
use api::v1::meta::DeleteRangeRequest;
use api::v1::meta::PutRequest;
use api::v1::meta::RangeRequest;
use api::v1::meta::Region;
use api::v1::meta::RequestHeader;
use api::v1::meta::TableName;
use common_grpc::channel_manager::ChannelConfig;
use common_grpc::channel_manager::ChannelManager;
use meta_client::client::MetaClientBuilder;
use tracing::event;
use tracing::subscriber;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

fn main() {
    subscriber::set_global_default(FmtSubscriber::builder().finish()).unwrap();
    run();
}

#[tokio::main]
async fn run() {
    let config = ChannelConfig::new()
        .timeout(Duration::from_secs(3))
        .connect_timeout(Duration::from_secs(5))
        .tcp_nodelay(true);
    let channel_manager = ChannelManager::with_config(config);
    let mut meta_client = MetaClientBuilder::new()
        .heartbeat_client(true)
        .router_client(true)
        .store_client(true)
        .channel_manager(channel_manager)
        .build();
    meta_client.start(&["127.0.0.1:3002"]).await.unwrap();
    // required only when the heartbeat_client is enabled
    meta_client.ask_leader().await.unwrap();

    let header = RequestHeader::new(0, 0);

    let p1 = Partition::new()
        .column_list(vec![b"col_1".to_vec(), b"col_2".to_vec()])
        .value_list(vec![b"k1".to_vec(), b"k2".to_vec()]);

    let p2 = Partition::new()
        .column_list(vec![b"col_1".to_vec(), b"col_2".to_vec()])
        .value_list(vec![b"Max1".to_vec(), b"Max2".to_vec()]);

    let table_name = TableName::new("test_catlog", "test_schema", "test_table");

    let create_req = CreateRequest::new(header, table_name)
        .add_region(Region::new(0, "test_region1", p1))
        .add_region(Region::new(1, "test_region2", p2));

    let res = meta_client.create_route(create_req).await.unwrap();
    event!(Level::INFO, "create_route result: {:#?}", res);

    // put
    let put_req = PutRequest {
        key: b"key1".to_vec(),
        value: b"value1".to_vec(),
        prev_kv: true,
        ..Default::default()
    };
    let res = meta_client.put(put_req).await.unwrap();
    event!(Level::INFO, "put result: {:#?}", res);

    // get
    let range_req = RangeRequest {
        key: b"key1".to_vec(),
        ..Default::default()
    };
    let res = meta_client.range(range_req.clone()).await.unwrap();
    event!(Level::INFO, "get range result: {:#?}", res);

    // delete
    let delete_range_req = DeleteRangeRequest {
        key: b"key1".to_vec(),
        ..Default::default()
    };
    let res = meta_client.delete_range(delete_range_req).await.unwrap();
    event!(Level::INFO, "delete range result: {:#?}", res);

    // get none
    let res = meta_client.range(range_req).await;
    event!(Level::INFO, "get range result: {:#?}", res);
}
