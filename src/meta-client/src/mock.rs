use std::sync::Arc;

use meta_srv::metasrv::MetaSrvOptions;
use meta_srv::metasrv::SelectorRef;
use meta_srv::service::store::kv::KvStoreRef;
use meta_srv::service::store::noop::NoopKvStore;
use mock_meta::MockInfo;

use crate::client::MetaClient;
use crate::client::MetaClientBuilder;

pub async fn create_metaclient_with_noop_store() -> MetaClient {
    let kv_store = Arc::new(NoopKvStore {});
    create_metaclient(Default::default(), kv_store, None).await
}

pub async fn create_metaclient_with_selector(selector: SelectorRef) -> MetaClient {
    let kv_store = Arc::new(NoopKvStore {});
    create_metaclient(Default::default(), kv_store, Some(selector)).await
}

pub async fn create_metaclient(
    opts: MetaSrvOptions,
    kv_store: KvStoreRef,
    selector: Option<SelectorRef>,
) -> MetaClient {
    let mock_info = mock_meta::create_meta_client(opts, kv_store, selector).await;
    create_metaclient_by(mock_info).await
}

pub async fn create_metaclient_by(mock_info: MockInfo) -> MetaClient {
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
