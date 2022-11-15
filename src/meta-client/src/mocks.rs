use meta_srv::metasrv::SelectorRef;
use meta_srv::mocks as server_mock;
use meta_srv::mocks::MockInfo;

use crate::client::{MetaClient, MetaClientBuilder};

pub async fn mock_client_with_memstore() -> MetaClient {
    let mock_info = server_mock::mock_with_memstore().await;
    mock_client_by(mock_info).await
}

#[allow(dead_code)]
pub async fn mock_client_with_etcdstore(addr: &str) -> MetaClient {
    let mock_info = server_mock::mock_with_etcdstore(addr).await;
    mock_client_by(mock_info).await
}

pub async fn mock_client_with_memorystore_and_selector(selector: SelectorRef) -> MetaClient {
    let mock_info = server_mock::mock_with_memstore_and_selector(selector).await;
    mock_client_by(mock_info).await
}

pub async fn mock_client_by(mock_info: MockInfo) -> MetaClient {
    let MockInfo {
        server_addr,
        channel_manager,
    } = mock_info;

    let id = (1000u64, 2000u64);
    let mut meta_client = MetaClientBuilder::new(id.0, id.1)
        .enable_heartbeat()
        .enable_router()
        .enable_store()
        .channel_manager(channel_manager)
        .build();
    meta_client.start(&[&server_addr]).await.unwrap();
    // required only when the heartbeat_client is enabled
    meta_client.ask_leader().await.unwrap();

    meta_client
}
