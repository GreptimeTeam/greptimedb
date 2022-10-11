use api::v1::meta::CreateRequest;
use api::v1::meta::DeleteRangeRequest;
use api::v1::meta::Peer;
use api::v1::meta::PutRequest;
use api::v1::meta::RangeRequest;
use api::v1::meta::Region;
use common_grpc::channel_manager::ChannelManager;
use meta_client::client::MetaClient;
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
    let channel_manager = ChannelManager::default();
    let mut meta_client = MetaClient::new(channel_manager);
    meta_client.start(&["127.0.0.1:3002"]).await.unwrap();

    let create_req = CreateRequest {
        db_name: "test_db".to_string(),
        regions: vec![
            Region {
                id: 0,
                name: "test_region1".to_string(),
                peer: Some(Peer {
                    id: 0,
                    ..Default::default()
                }),
                ..Default::default()
            },
            Region {
                id: 1,
                name: "test_region2".to_string(),
                peer: Some(Peer {
                    id: 1,
                    ..Default::default()
                }),
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    let res = meta_client.create_route(create_req).await.unwrap();
    event!(Level::INFO, "create_route result: {:#?}", res);

    // put
    let put_req = PutRequest {
        key: b"key1".to_vec(),
        value: b"value1".to_vec(),
        prev_kv: true,
        ..Default::default()
    };
    let res = meta_client.put(put_req).await;
    event!(Level::INFO, "put result: {:#?}", res);

    // get
    let range_req = RangeRequest {
        key: b"key1".to_vec(),
        ..Default::default()
    };
    let res = meta_client.range(range_req.clone()).await;
    event!(Level::INFO, "get range result: {:#?}", res);

    // delete
    let delete_range_req = DeleteRangeRequest {
        key: b"key1".to_vec(),
        ..Default::default()
    };
    let res = meta_client.delete_range(delete_range_req).await;
    event!(Level::INFO, "delete range result: {:#?}", res);

    // get none
    let res = meta_client.range(range_req).await;
    event!(Level::INFO, "get range result: {:#?}", res);
}
