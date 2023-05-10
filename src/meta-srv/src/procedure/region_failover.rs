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
mod update_metadata;

use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use common_meta::RegionIdent;
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
use tokio::sync::Mutex;

use crate::error::{Error, RegisterProcedureLoaderSnafu, Result};
use crate::metasrv::{SelectorContext, SelectorRef};
use crate::service::mailbox::MailboxRef;

pub(crate) struct RegionFailoverManager {
    mailbox: MailboxRef,
    procedure_manager: ProcedureManagerRef,
    selector: SelectorRef,
    selector_ctx: SelectorContext,
    running_procedures: Arc<Mutex<HashSet<RegionIdent>>>,
}

impl RegionFailoverManager {
    pub(crate) fn new(
        mailbox: MailboxRef,
        procedure_manager: ProcedureManagerRef,
        selector: SelectorRef,
        selector_ctx: SelectorContext,
    ) -> Self {
        Self {
            mailbox,
            procedure_manager,
            selector,
            selector_ctx,
            running_procedures: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub(crate) fn try_start(&self) -> Result<()> {
        let mailbox = self.mailbox.clone();
        let selector = self.selector.clone();
        let selector_ctx = self.selector_ctx.clone();
        self.procedure_manager
            .register_loader(
                RegionFailoverProcedure::TYPE_NAME,
                Box::new(move |json| {
                    RegionFailoverProcedure::from_json(
                        json,
                        RegionFailoverContext {
                            mailbox: mailbox.clone(),
                            selector: selector.clone(),
                            selector_ctx: selector_ctx.clone(),
                        },
                    )
                    .map(|p| Box::new(p) as _)
                }),
            )
            .context(RegisterProcedureLoaderSnafu {
                type_name: RegionFailoverProcedure::TYPE_NAME,
            })
    }

    async fn insert_running_procedures(&self, failed_region: &RegionIdent) -> bool {
        let mut procedures = self.running_procedures.lock().await;
        if procedures.contains(failed_region) {
            return false;
        }
        procedures.insert(failed_region.clone())
    }

    pub(crate) async fn fire_region_failover(&self, failed_region: RegionIdent) {
        if !self.insert_running_procedures(&failed_region).await {
            warn!("Region failover procedure for region {failed_region} is already running!");
            return;
        }

        let procedure = RegionFailoverProcedure::new(
            failed_region.clone(),
            RegionFailoverContext {
                mailbox: self.mailbox.clone(),
                selector: self.selector.clone(),
                selector_ctx: self.selector_ctx.clone(),
            },
        );
        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));
        let procedure_id = procedure_with_id.id;
        info!("Starting region failover procedure {procedure_id} for region {failed_region:?}");

        let procedure_manager = self.procedure_manager.clone();
        let running_procedures = self.running_procedures.clone();
        common_runtime::spawn_bg(async move {
            let watcher = &mut match procedure_manager.submit(procedure_with_id).await {
                Ok(watcher) => watcher,
                Err(e) => {
                    error!(e; "Failed to submit region failover procedure {procedure_id} for region {failed_region:?}");
                    running_procedures.lock().await.remove(&failed_region);
                    return;
                }
            };

            if let Err(e) = watcher::wait(watcher).await {
                error!(e; "Failed to wait region failover procedure {procedure_id} for region {failed_region:?}");
                running_procedures.lock().await.remove(&failed_region);
                return;
            }

            running_procedures.lock().await.remove(&failed_region);
            info!("Region failover procedure {procedure_id} for region {failed_region:?} is finished!")
        });
    }
}

/// A "Node" in the state machine of region failover procedure.
/// Contains the current state and the data.
#[derive(Serialize, Deserialize, Debug)]
struct Node {
    failed_region: RegionIdent,
    state: Option<Box<dyn State>>,
}

/// The "Context" of region failover procedure state machine.
#[derive(Clone)]
pub struct RegionFailoverContext {
    pub mailbox: MailboxRef,
    pub selector: SelectorRef,
    pub selector_ctx: SelectorContext,
}

/// The state machine of region failover procedure. Driven by the call to `next`.
#[async_trait]
#[typetag::serde(tag = "region_failover_state")]
trait State: Sync + Send + Debug {
    async fn next(
        mut self: Box<Self>,
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
///                  ┌─────────┐    │ wait for 2 seconds
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
            state: Some(Box::new(state)),
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
        if let Some(state) = self.node.state.take() {
            let next_state = state
                .next(&self.context, &self.node.failed_region)
                .await
                .map_err(|e| {
                    if matches!(e, Error::RetryLater { .. }) {
                        ProcedureError::retry_later(e)
                    } else {
                        ProcedureError::external(e)
                    }
                })?;
            self.node.state = Some(next_state);
        }
        Ok(self
            .node
            .state
            .as_ref()
            .map(|s| s.status())
            .unwrap_or(Status::Done))
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.node).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let region_ident = &self.node.failed_region;
        let key = format!(
            "{}/region-{}",
            common_catalog::format_full_table_name(
                &region_ident.catalog,
                &region_ident.schema,
                &region_ident.table
            ),
            region_ident.region_number
        );
        LockKey::single(key)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use api::v1::meta::mailbox_message::Payload;
    use api::v1::meta::{HeartbeatResponse, MailboxMessage, Peer, RequestHeader};
    use catalog::helper::TableGlobalKey;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, MITO_ENGINE};
    use common_meta::instruction::{Instruction, InstructionReply, SimpleReply};
    use common_meta::DatanodeId;
    use common_procedure::BoxedProcedure;
    use rand::prelude::SliceRandom;
    use tokio::sync::mpsc::Receiver;

    use super::*;
    use crate::handler::{HeartbeatMailbox, Pusher, Pushers};
    use crate::selector::{Namespace, Selector};
    use crate::sequence::Sequence;
    use crate::service::mailbox::Channel;
    use crate::service::store::memory::MemStore;
    use crate::table_routes;

    struct RandomNodeSelector {
        nodes: Vec<Peer>,
    }

    #[async_trait]
    impl Selector for RandomNodeSelector {
        type Context = SelectorContext;
        type Output = Vec<Peer>;

        async fn select(&self, _ns: Namespace, _ctx: &Self::Context) -> Result<Self::Output> {
            let mut rng = rand::thread_rng();
            let mut nodes = self.nodes.clone();
            nodes.shuffle(&mut rng);
            Ok(nodes)
        }
    }

    // The "foreign" means the Datanode is not containing any regions to the table before.
    pub struct ForeignNodeSelector {
        pub foreign: Peer,
    }

    #[async_trait]
    impl Selector for ForeignNodeSelector {
        type Context = SelectorContext;
        type Output = Vec<Peer>;

        async fn select(&self, _ns: Namespace, _ctx: &Self::Context) -> Result<Self::Output> {
            Ok(vec![self.foreign.clone()])
        }
    }

    pub struct TestingEnv {
        pub context: RegionFailoverContext,
        pub failed_region: RegionIdent,
        pub heartbeat_receivers: HashMap<DatanodeId, Receiver<tonic::Result<HeartbeatResponse>>>,
    }

    pub struct TestingEnvBuilder {
        selector: Option<SelectorRef>,
        failed_region: Option<u32>,
    }

    impl TestingEnvBuilder {
        pub fn new() -> Self {
            Self {
                selector: None,
                failed_region: None,
            }
        }

        #[allow(unused)]
        pub fn with_selector(mut self, selector: SelectorRef) -> Self {
            self.selector = Some(selector);
            self
        }

        pub fn with_failed_region(mut self, failed_region: u32) -> Self {
            self.failed_region = Some(failed_region);
            self
        }

        pub async fn build(self) -> TestingEnv {
            let kv_store = Arc::new(MemStore::new()) as _;

            let table = "my_table";
            let (_, table_global_value) =
                table_routes::tests::prepare_table_global_value(&kv_store, table).await;

            table_routes::tests::prepare_table_route_value(&kv_store, table).await;

            let pushers = Pushers::default();
            let mut heartbeat_receivers = HashMap::with_capacity(3);
            for datanode_id in 1..=3 {
                let (tx, rx) = tokio::sync::mpsc::channel(1);

                let pusher_id = Channel::Datanode(datanode_id).pusher_id();
                let pusher = Pusher::new(tx, &RequestHeader::default());
                let _ = pushers.insert(pusher_id, pusher).await;

                heartbeat_receivers.insert(datanode_id, rx);
            }

            let mailbox_sequence =
                Sequence::new("test_heartbeat_mailbox", 0, 100, kv_store.clone());
            let mailbox = HeartbeatMailbox::create(pushers, mailbox_sequence);

            let failed_region = self.failed_region.unwrap_or(1);
            let failed_datanode = table_global_value
                .regions_id_map
                .iter()
                .find_map(|(datanode_id, regions)| {
                    if regions.contains(&failed_region) {
                        Some(*datanode_id)
                    } else {
                        None
                    }
                })
                .unwrap();
            let failed_region = RegionIdent {
                cluster_id: 0,
                datanode_id: failed_datanode,
                table_id: 1,
                engine: MITO_ENGINE.to_string(),
                region_number: failed_region,
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table: table.to_string(),
            };

            let selector = self.selector.unwrap_or_else(|| {
                let nodes = (1..=table_global_value.regions_id_map.len())
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
                kv_store,
                catalog: None,
                schema: None,
            };

            TestingEnv {
                context: RegionFailoverContext {
                    mailbox,
                    selector,
                    selector_ctx,
                },
                failed_region,
                heartbeat_receivers,
            }
        }
    }

    #[tokio::test]
    async fn test_region_failover_procedure() {
        common_telemetry::init_default_ut_logging();

        let TestingEnv {
            context,
            failed_region,
            mut heartbeat_receivers,
        } = TestingEnvBuilder::new().build().await;

        let mut procedure = Box::new(RegionFailoverProcedure::new(
            failed_region.clone(),
            context.clone(),
        )) as BoxedProcedure;

        let mut failed_datanode = heartbeat_receivers
            .remove(&failed_region.datanode_id)
            .unwrap();
        let mailbox_clone = context.mailbox.clone();
        let failed_region_clone = failed_region.clone();
        common_runtime::spawn_bg(async move {
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
        for (datanode_id, mut recv) in heartbeat_receivers.into_iter() {
            let mailbox_clone = context.mailbox.clone();
            let failed_region_clone = failed_region.clone();
            let candidate_tx = candidate_tx.clone();
            common_runtime::spawn_bg(async move {
                let resp = recv.recv().await.unwrap().unwrap();
                let received = &resp.mailbox_message.unwrap();
                assert_eq!(
                    received.payload,
                    Some(Payload::Json(
                        serde_json::to_string(&Instruction::OpenRegion(
                            failed_region_clone.clone()
                        ))
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
            r#"{"failed_region":{"cluster_id":0,"datanode_id":1,"catalog":"greptime","schema":"public","table":"my_table","table_id":1,"engine":"mito","region_number":1},"state":{"region_failover_state":"RegionFailoverEnd"}}"#
        );

        // Verifies that the failed region (region 1) is moved from failed datanode (datanode 1) to the candidate datanode.
        let key = TableGlobalKey {
            catalog_name: failed_region.catalog.clone(),
            schema_name: failed_region.schema.clone(),
            table_name: failed_region.table.clone(),
        };
        let value = table_routes::get_table_global_value(&context.selector_ctx.kv_store, &key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            value
                .regions_id_map
                .get(&failed_region.datanode_id)
                .unwrap(),
            &vec![2]
        );
        assert!(value
            .regions_id_map
            .get(&candidate_rx.recv().await.unwrap())
            .unwrap()
            .contains(&1));
    }

    #[tokio::test]
    async fn test_state_serde() {
        let TestingEnv {
            context,
            failed_region,
            heartbeat_receivers: _,
        } = TestingEnvBuilder::new().build().await;

        let state = RegionFailoverStart::new();
        let node = Node {
            failed_region,
            state: Some(Box::new(state)),
        };
        let procedure = RegionFailoverProcedure { node, context };

        let s = procedure.dump().unwrap();
        assert_eq!(
            s,
            r#"{"failed_region":{"cluster_id":0,"datanode_id":1,"catalog":"greptime","schema":"public","table":"my_table","table_id":1,"engine":"mito","region_number":1},"state":{"region_failover_state":"RegionFailoverStart","failover_candidate":null}}"#
        );
        let n: Node = serde_json::from_str(&s).unwrap();
        assert_eq!(
            format!("{n:?}"),
            r#"Node { failed_region: RegionIdent { cluster_id: 0, datanode_id: 1, catalog: "greptime", schema: "public", table: "my_table", table_id: 1, engine: "mito", region_number: 1 }, state: Some(RegionFailoverStart { failover_candidate: None }) }"#
        );
    }
}
