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

mod activate_region;
mod deactivate_region;
mod failover_end;
mod failover_start;
mod invalidate_cache;
mod update_metadata;

use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use common_meta::key::datanode_table::DatanodeTableKey;
use common_meta::key::TableMetadataManagerRef;
use common_meta::kv_backend::ResettableKvBackendRef;
use common_meta::{ClusterId, RegionIdent};
use common_procedure::error::{
    Error as ProcedureError, FromJsonSnafu, Result as ProcedureResult, ToJsonSnafu,
};
use common_procedure::{
    watcher, Context as ProcedureContext, LockKey, Procedure, ProcedureManagerRef, ProcedureWithId,
    Status,
};
use common_telemetry::{error, info, warn};
use failover_start::RegionFailoverStart;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::storage::RegionNumber;
use table::metadata::TableId;

use crate::error::{Error, RegisterProcedureLoaderSnafu, Result, TableMetadataManagerSnafu};
use crate::lock::DistLockRef;
use crate::metasrv::{SelectorContext, SelectorRef};
use crate::procedure::utils::region_lock_key;
use crate::service::mailbox::MailboxRef;

const OPEN_REGION_MESSAGE_TIMEOUT: Duration = Duration::from_secs(30);

/// A key for the preventing running multiple failover procedures for the same region.
#[derive(PartialEq, Eq, Hash, Clone)]
pub(crate) struct RegionFailoverKey {
    pub(crate) cluster_id: ClusterId,
    pub(crate) table_id: TableId,
    pub(crate) region_number: RegionNumber,
}

impl From<RegionIdent> for RegionFailoverKey {
    fn from(region_ident: RegionIdent) -> Self {
        Self {
            cluster_id: region_ident.cluster_id,
            table_id: region_ident.table_id,
            region_number: region_ident.region_number,
        }
    }
}

pub(crate) struct RegionFailoverManager {
    region_lease_secs: u64,
    in_memory: ResettableKvBackendRef,
    mailbox: MailboxRef,
    procedure_manager: ProcedureManagerRef,
    selector: SelectorRef,
    selector_ctx: SelectorContext,
    dist_lock: DistLockRef,
    running_procedures: Arc<RwLock<HashSet<RegionFailoverKey>>>,
    table_metadata_manager: TableMetadataManagerRef,
}

struct FailoverProcedureGuard {
    running_procedures: Arc<RwLock<HashSet<RegionFailoverKey>>>,
    key: RegionFailoverKey,
}

impl Drop for FailoverProcedureGuard {
    fn drop(&mut self) {
        let _ = self.running_procedures.write().unwrap().remove(&self.key);
    }
}

impl RegionFailoverManager {
    pub(crate) fn new(
        region_lease_secs: u64,
        in_memory: ResettableKvBackendRef,
        mailbox: MailboxRef,
        procedure_manager: ProcedureManagerRef,
        (selector, selector_ctx): (SelectorRef, SelectorContext),
        dist_lock: DistLockRef,
        table_metadata_manager: TableMetadataManagerRef,
    ) -> Self {
        Self {
            region_lease_secs,
            in_memory,
            mailbox,
            procedure_manager,
            selector,
            selector_ctx,
            dist_lock,
            running_procedures: Arc::new(RwLock::new(HashSet::new())),
            table_metadata_manager,
        }
    }

    pub(crate) fn create_context(&self) -> RegionFailoverContext {
        RegionFailoverContext {
            region_lease_secs: self.region_lease_secs,
            in_memory: self.in_memory.clone(),
            mailbox: self.mailbox.clone(),
            selector: self.selector.clone(),
            selector_ctx: self.selector_ctx.clone(),
            dist_lock: self.dist_lock.clone(),
            table_metadata_manager: self.table_metadata_manager.clone(),
        }
    }

    pub(crate) fn try_start(&self) -> Result<()> {
        let context = self.create_context();
        self.procedure_manager
            .register_loader(
                RegionFailoverProcedure::TYPE_NAME,
                Box::new(move |json| {
                    let context = context.clone();
                    RegionFailoverProcedure::from_json(json, context).map(|p| Box::new(p) as _)
                }),
            )
            .context(RegisterProcedureLoaderSnafu {
                type_name: RegionFailoverProcedure::TYPE_NAME,
            })
    }

    fn insert_running_procedures(
        &self,
        failed_region: &RegionIdent,
    ) -> Option<FailoverProcedureGuard> {
        let key = RegionFailoverKey::from(failed_region.clone());
        let mut procedures = self.running_procedures.write().unwrap();
        if procedures.insert(key.clone()) {
            Some(FailoverProcedureGuard {
                running_procedures: self.running_procedures.clone(),
                key,
            })
        } else {
            None
        }
    }

    pub(crate) async fn do_region_failover(&self, failed_region: &RegionIdent) -> Result<()> {
        let Some(guard) = self.insert_running_procedures(failed_region) else {
            warn!("Region failover procedure for region {failed_region} is already running!");
            return Ok(());
        };

        if !self.table_exists(failed_region).await? {
            // The table could be dropped before the failure detector knows it. Then the region
            // failover is not needed.
            // Or the table could be renamed. But we will have a new region ident to detect failure.
            // So the region failover here is not needed either.
            return Ok(());
        }

        if !self.failed_region_exists(failed_region).await? {
            // The failed region could be failover by another procedure.
            return Ok(());
        }

        let context = self.create_context();
        let procedure = RegionFailoverProcedure::new(failed_region.clone(), context);
        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));
        let procedure_id = procedure_with_id.id;
        info!("Starting region failover procedure {procedure_id} for region {failed_region:?}");

        let procedure_manager = self.procedure_manager.clone();
        let failed_region = failed_region.clone();
        let _handle = common_runtime::spawn_bg(async move {
            let _ = guard;

            let watcher = &mut match procedure_manager.submit(procedure_with_id).await {
                Ok(watcher) => watcher,
                Err(e) => {
                    error!(e; "Failed to submit region failover procedure {procedure_id} for region {failed_region}");
                    return;
                }
            };

            if let Err(e) = watcher::wait(watcher).await {
                error!(e; "Failed to wait region failover procedure {procedure_id} for region {failed_region}");
                return;
            }

            info!("Region failover procedure {procedure_id} for region {failed_region} is finished successfully!");
        });
        Ok(())
    }

    async fn table_exists(&self, failed_region: &RegionIdent) -> Result<bool> {
        Ok(self
            .table_metadata_manager
            .table_route_manager()
            .get_region_distribution(failed_region.table_id)
            .await
            .context(TableMetadataManagerSnafu)?
            .is_some())
    }

    async fn failed_region_exists(&self, failed_region: &RegionIdent) -> Result<bool> {
        let table_id = failed_region.table_id;
        let datanode_id = failed_region.datanode_id;

        let value = self
            .table_metadata_manager
            .datanode_table_manager()
            .get(&DatanodeTableKey::new(datanode_id, table_id))
            .await
            .context(TableMetadataManagerSnafu)?;

        Ok(value
            .map(|value| {
                value
                    .regions
                    .iter()
                    .any(|region| *region == failed_region.region_number)
            })
            .unwrap_or_default())
    }
}

/// A "Node" in the state machine of region failover procedure.
/// Contains the current state and the data.
#[derive(Serialize, Deserialize, Debug)]
struct Node {
    failed_region: RegionIdent,
    state: Box<dyn State>,
}

/// The "Context" of region failover procedure state machine.
#[derive(Clone)]
pub struct RegionFailoverContext {
    pub region_lease_secs: u64,
    pub in_memory: ResettableKvBackendRef,
    pub mailbox: MailboxRef,
    pub selector: SelectorRef,
    pub selector_ctx: SelectorContext,
    pub dist_lock: DistLockRef,
    pub table_metadata_manager: TableMetadataManagerRef,
}

/// The state machine of region failover procedure. Driven by the call to `next`.
#[async_trait]
#[typetag::serde(tag = "region_failover_state")]
trait State: Sync + Send + Debug {
    async fn next(
        &mut self,
        ctx: &RegionFailoverContext,
        failed_region: &RegionIdent,
    ) -> Result<Box<dyn State>>;

    fn status(&self) -> Status {
        Status::executing(true)
    }
}

/// The states transition of region failover procedure:
///
/// ```text
///                       ┌───────────────────┐
///                       │RegionFailoverStart│
///                       └─────────┬─────────┘
///                                 │
///                                 │ Selects a candidate(Datanode)
///                  ┌─────────┐    │ to place the failed region
///                  │         │    │
///  If replied with │     ┌───▼────▼───────┐
///  "Close region   │     │DeactivateRegion│
///  failed"         │     └───┬────┬───────┘
///                  │         │    │
///                  └─────────┘    │ Sends "Close Region" request
///                                 │ to the failed Datanode, and
///                                 | wait for the Region lease expiry
///                  ┌─────────┐    │ seconds
///                  │         │    │
///                  │      ┌──▼────▼──────┐
/// Wait candidate   │      │ActivateRegion◄───────────────────────┐
/// response timeout │      └──┬────┬──────┘                       │
///                  │         │    │                              │
///                  └─────────┘    │ Sends "Open Region" request  │
///                                 │ to the candidate Datanode,   │
///                                 │ and wait for 30 seconds      │
///                                 │                              │
///                                 │ Check Datanode returns       │
///                                 │                              │
///                         success ├──────────────────────────────┘
///                                 │                       failed
///                       ┌─────────▼──────────┐
///                       │UpdateRegionMetadata│
///                       └─────────┬──────────┘
///                                 │
///                                 │ Updates the Region
///                                 │ placement metadata
///                                 │
///                         ┌───────▼───────┐
///                         │InvalidateCache│
///                         └───────┬───────┘
///                                 │
///                                 │ Broadcast Invalidate Table
///                                 │ Cache
///                                 │
///                        ┌────────▼────────┐
///                        │RegionFailoverEnd│
///                        └─────────────────┘
/// ```
pub struct RegionFailoverProcedure {
    node: Node,
    context: RegionFailoverContext,
}

impl RegionFailoverProcedure {
    const TYPE_NAME: &'static str = "metasrv-procedure::RegionFailover";

    pub fn new(failed_region: RegionIdent, context: RegionFailoverContext) -> Self {
        let state = RegionFailoverStart::new();
        let node = Node {
            failed_region,
            state: Box::new(state),
        };
        Self { node, context }
    }

    fn from_json(json: &str, context: RegionFailoverContext) -> ProcedureResult<Self> {
        let node: Node = serde_json::from_str(json).context(FromJsonSnafu)?;
        Ok(Self { node, context })
    }
}

#[async_trait]
impl Procedure for RegionFailoverProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = &mut self.node.state;
        *state = state
            .next(&self.context, &self.node.failed_region)
            .await
            .map_err(|e| {
                if matches!(e, Error::RetryLater { .. }) {
                    ProcedureError::retry_later(e)
                } else {
                    ProcedureError::external(e)
                }
            })?;
        Ok(state.status())
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.node).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let region_ident = &self.node.failed_region;
        let region_key = region_lock_key(region_ident.table_id, region_ident.region_number);
        LockKey::single_exclusive(region_key)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use api::v1::meta::mailbox_message::Payload;
    use api::v1::meta::{HeartbeatResponse, MailboxMessage, RequestHeader};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_meta::ddl::utils::region_storage_path;
    use common_meta::instruction::{Instruction, InstructionReply, OpenRegion, SimpleReply};
    use common_meta::key::TableMetadataManager;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::peer::Peer;
    use common_meta::sequence::SequenceBuilder;
    use common_meta::DatanodeId;
    use common_procedure::{BoxedProcedure, ProcedureId};
    use common_procedure_test::MockContextProvider;
    use rand::prelude::SliceRandom;
    use tokio::sync::mpsc::Receiver;

    use super::*;
    use crate::cluster::MetaPeerClientBuilder;
    use crate::handler::{HeartbeatMailbox, Pusher, Pushers};
    use crate::lock::memory::MemLock;
    use crate::selector::{Namespace, Selector, SelectorOptions};
    use crate::service::mailbox::Channel;
    use crate::test_util;

    struct RandomNodeSelector {
        nodes: Vec<Peer>,
    }

    #[async_trait]
    impl Selector for RandomNodeSelector {
        type Context = SelectorContext;
        type Output = Vec<Peer>;

        async fn select(
            &self,
            _ns: Namespace,
            _ctx: &Self::Context,
            _opts: SelectorOptions,
        ) -> Result<Self::Output> {
            let mut rng = rand::thread_rng();
            let mut nodes = self.nodes.clone();
            nodes.shuffle(&mut rng);
            Ok(nodes)
        }
    }

    pub struct TestingEnv {
        pub context: RegionFailoverContext,
        pub heartbeat_receivers: HashMap<DatanodeId, Receiver<tonic::Result<HeartbeatResponse>>>,
        pub pushers: Pushers,
        pub path: String,
    }

    impl TestingEnv {
        pub async fn failed_region(&self, region_number: u32) -> RegionIdent {
            let region_distribution = self
                .context
                .table_metadata_manager
                .table_route_manager()
                .get_region_distribution(1)
                .await
                .unwrap()
                .unwrap();
            let failed_datanode = region_distribution
                .iter()
                .find_map(|(&datanode_id, regions)| {
                    if regions.contains(&region_number) {
                        Some(datanode_id)
                    } else {
                        None
                    }
                })
                .unwrap();
            RegionIdent {
                cluster_id: 0,
                region_number,
                datanode_id: failed_datanode,
                table_id: 1,
                engine: "mito2".to_string(),
            }
        }
    }

    pub struct TestingEnvBuilder {
        selector: Option<SelectorRef>,
    }

    impl TestingEnvBuilder {
        pub fn new() -> Self {
            Self { selector: None }
        }

        fn with_selector(mut self, selector: SelectorRef) -> Self {
            self.selector = Some(selector);
            self
        }

        pub async fn build(self) -> TestingEnv {
            let in_memory = Arc::new(MemoryKvBackend::new());
            let kv_backend = Arc::new(MemoryKvBackend::new());
            let meta_peer_client = MetaPeerClientBuilder::default()
                .election(None)
                .in_memory(Arc::new(MemoryKvBackend::new()))
                .build()
                .map(Arc::new)
                // Safety: all required fields set at initialization
                .unwrap();

            let table_id = 1;
            let table = "my_table";
            let table_metadata_manager = Arc::new(TableMetadataManager::new(kv_backend.clone()));
            test_util::prepare_table_region_and_info_value(&table_metadata_manager, table).await;
            let region_distribution = table_metadata_manager
                .table_route_manager()
                .get_region_distribution(1)
                .await
                .unwrap()
                .unwrap();

            let pushers = Pushers::default();
            let mut heartbeat_receivers = HashMap::with_capacity(3);
            for datanode_id in 1..=3 {
                let (tx, rx) = tokio::sync::mpsc::channel(1);

                let pusher_id = Channel::Datanode(datanode_id).pusher_id();
                let pusher = Pusher::new(tx, &RequestHeader::default());
                let _ = pushers.insert(pusher_id, pusher).await;

                let _ = heartbeat_receivers.insert(datanode_id, rx);
            }

            let mailbox_sequence =
                SequenceBuilder::new("test_heartbeat_mailbox", kv_backend.clone())
                    .initial(0)
                    .step(100)
                    .build();
            let mailbox = HeartbeatMailbox::create(pushers.clone(), mailbox_sequence);

            let selector = self.selector.unwrap_or_else(|| {
                let nodes = (1..=region_distribution.len())
                    .map(|id| Peer {
                        id: id as u64,
                        addr: "".to_string(),
                    })
                    .collect();
                Arc::new(RandomNodeSelector { nodes })
            });
            let selector_ctx = SelectorContext {
                datanode_lease_secs: 10,
                server_addr: "127.0.0.1:3002".to_string(),
                kv_backend: kv_backend.clone(),
                meta_peer_client,
                table_id: Some(table_id),
            };

            TestingEnv {
                context: RegionFailoverContext {
                    region_lease_secs: 10,
                    in_memory,
                    mailbox,
                    selector,
                    selector_ctx,
                    dist_lock: Arc::new(MemLock::default()),
                    table_metadata_manager,
                },
                pushers,
                heartbeat_receivers,
                path: region_storage_path(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME).to_string(),
            }
        }
    }

    #[tokio::test]
    async fn test_region_failover_procedure() {
        let mut env = TestingEnvBuilder::new().build().await;
        let failed_region = env.failed_region(1).await;

        let mut procedure = Box::new(RegionFailoverProcedure::new(
            failed_region.clone(),
            env.context.clone(),
        )) as BoxedProcedure;

        let mut failed_datanode = env
            .heartbeat_receivers
            .remove(&failed_region.datanode_id)
            .unwrap();
        let mailbox_clone = env.context.mailbox.clone();
        let failed_region_clone = failed_region.clone();
        let _handle = common_runtime::spawn_bg(async move {
            let resp = failed_datanode.recv().await.unwrap().unwrap();
            let received = &resp.mailbox_message.unwrap();
            assert_eq!(
                received.payload,
                Some(Payload::Json(
                    serde_json::to_string(&Instruction::CloseRegion(failed_region_clone.clone()))
                        .unwrap(),
                ))
            );

            // simulating response from Datanode
            mailbox_clone
                .on_recv(
                    1,
                    Ok(MailboxMessage {
                        id: 1,
                        subject: "Deactivate Region".to_string(),
                        from: format!("Datanode-{}", failed_region.datanode_id),
                        to: "Metasrv".to_string(),
                        timestamp_millis: common_time::util::current_time_millis(),
                        payload: Some(Payload::Json(
                            serde_json::to_string(&InstructionReply::CloseRegion(SimpleReply {
                                result: true,
                                error: None,
                            }))
                            .unwrap(),
                        )),
                    }),
                )
                .await
                .unwrap();
        });

        let (candidate_tx, mut candidate_rx) = tokio::sync::mpsc::channel(1);
        for (datanode_id, mut recv) in env.heartbeat_receivers.into_iter() {
            let mailbox_clone = env.context.mailbox.clone();
            let opening_region = RegionIdent {
                datanode_id,
                ..failed_region.clone()
            };
            let path = env.path.to_string();
            let candidate_tx = candidate_tx.clone();
            let _handle = common_runtime::spawn_bg(async move {
                let resp = recv.recv().await.unwrap().unwrap();
                let received = &resp.mailbox_message.unwrap();
                assert_eq!(
                    received.payload,
                    Some(Payload::Json(
                        serde_json::to_string(&Instruction::OpenRegion(OpenRegion::new(
                            opening_region,
                            &path,
                            HashMap::new(),
                            HashMap::new(),
                            false
                        )))
                        .unwrap(),
                    ))
                );

                candidate_tx.send(datanode_id).await.unwrap();

                // simulating response from Datanode
                mailbox_clone
                    .on_recv(
                        // Very tricky here:
                        // the procedure only sends two messages in sequence, the second one is
                        // "Activate Region", and its message id is 2.
                        2,
                        Ok(MailboxMessage {
                            id: 2,
                            subject: "Activate Region".to_string(),
                            from: format!("Datanode-{datanode_id}"),
                            to: "Metasrv".to_string(),
                            timestamp_millis: common_time::util::current_time_millis(),
                            payload: Some(Payload::Json(
                                serde_json::to_string(&InstructionReply::OpenRegion(SimpleReply {
                                    result: true,
                                    error: None,
                                }))
                                .unwrap(),
                            )),
                        }),
                    )
                    .await
                    .unwrap();
            });
        }

        common_procedure_test::execute_procedure_until_done(&mut procedure).await;

        assert_eq!(
            procedure.dump().unwrap(),
            r#"{"failed_region":{"cluster_id":0,"datanode_id":1,"table_id":1,"region_number":1,"engine":"mito2"},"state":{"region_failover_state":"RegionFailoverEnd"}}"#
        );

        // Verifies that the failed region (region 1) is moved from failed datanode (datanode 1) to the candidate datanode.
        let region_distribution = env
            .context
            .table_metadata_manager
            .table_route_manager()
            .get_region_distribution(1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            region_distribution.get(&failed_region.datanode_id).unwrap(),
            &vec![2]
        );
        assert!(region_distribution
            .get(&candidate_rx.recv().await.unwrap())
            .unwrap()
            .contains(&1));
    }

    #[tokio::test]
    async fn test_state_serde() {
        let env = TestingEnvBuilder::new().build().await;
        let failed_region = env.failed_region(1).await;

        let state = RegionFailoverStart::new();
        let node = Node {
            failed_region,
            state: Box::new(state),
        };
        let procedure = RegionFailoverProcedure {
            node,
            context: env.context,
        };

        let s = procedure.dump().unwrap();
        assert_eq!(
            s,
            r#"{"failed_region":{"cluster_id":0,"datanode_id":1,"table_id":1,"region_number":1,"engine":"mito2"},"state":{"region_failover_state":"RegionFailoverStart","failover_candidate":null}}"#
        );
        let n: Node = serde_json::from_str(&s).unwrap();
        assert_eq!(
            format!("{n:?}"),
            r#"Node { failed_region: RegionIdent { cluster_id: 0, datanode_id: 1, table_id: 1, region_number: 1, engine: "mito2" }, state: RegionFailoverStart { failover_candidate: None } }"#
        );
    }

    #[tokio::test]
    async fn test_state_not_changed_upon_failure() {
        struct MySelector {
            peers: Arc<Mutex<Vec<Option<Peer>>>>,
        }

        #[async_trait]
        impl Selector for MySelector {
            type Context = SelectorContext;
            type Output = Vec<Peer>;

            async fn select(
                &self,
                _ns: Namespace,
                _ctx: &Self::Context,
                _opts: SelectorOptions,
            ) -> Result<Self::Output> {
                let mut peers = self.peers.lock().unwrap();
                Ok(if let Some(Some(peer)) = peers.pop() {
                    vec![peer]
                } else {
                    vec![]
                })
            }
        }

        // Returns a valid peer the second time called "select".
        let selector = MySelector {
            peers: Arc::new(Mutex::new(vec![
                Some(Peer {
                    id: 42,
                    addr: "".to_string(),
                }),
                None,
            ])),
        };

        let env = TestingEnvBuilder::new()
            .with_selector(Arc::new(selector))
            .build()
            .await;
        let failed_region = env.failed_region(1).await;

        let state = RegionFailoverStart::new();
        let node = Node {
            failed_region,
            state: Box::new(state),
        };
        let mut procedure = RegionFailoverProcedure {
            node,
            context: env.context,
        };

        let ctx = ProcedureContext {
            procedure_id: ProcedureId::random(),
            provider: Arc::new(MockContextProvider::default()),
        };

        let result = procedure.execute(&ctx).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_retry_later());
        assert_eq!(
            r#"{"region_failover_state":"RegionFailoverStart","failover_candidate":null}"#,
            serde_json::to_string(&procedure.node.state).unwrap()
        );

        let result = procedure.execute(&ctx).await;
        assert!(matches!(result, Ok(Status::Executing { persist: true })));
        assert_eq!(
            r#"{"region_failover_state":"DeactivateRegion","candidate":{"id":42,"addr":""}}"#,
            serde_json::to_string(&procedure.node.state).unwrap()
        );
    }
}
