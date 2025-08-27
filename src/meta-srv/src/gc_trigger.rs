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

// TODO(discord9): use it
#![allow(unused)]
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use api::v1::meta::MailboxMessage;
use common_meta::instruction::{CollectFileRefs, GcRegions, Instruction};
use common_meta::key::table_route::PhysicalTableRouteValue;
use common_meta::key::TableMetadataManagerRef;
use common_meta::peer::Peer;
use common_meta::rpc::router::RegionRoute;
use common_telemetry::{debug, error, info};
use futures::stream::BoxStream;
use futures::TryStreamExt;
use snafu::{OptionExt as _, ResultExt};
use store_api::storage::RegionId;
use table::metadata::TableId;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::error::{
    self, RegionRouteNotFoundSnafu, Result, TableMetadataManagerSnafu, TableRouteNotFoundSnafu,
    UnexpectedSnafu,
};
use crate::service::mailbox::{Channel, MailboxReceiver, MailboxRef};
use crate::{define_ticker, metrics};

/// The interval of the gc ticker.
const TICKER_INTERVAL: Duration = Duration::from_secs(60 * 5);

/// [`Event`] represents various types of events that can be processed by the gc ticker.
///
/// Variants:
/// - `Tick`: This event is used to trigger gc periodically.
pub(crate) enum Event {
    Tick,
}

pub(crate) type GcTickerRef = Arc<GcTicker>;

define_ticker!(
    /// [GcTicker] is used to trigger gc periodically.
    GcTicker,
    event_type = Event,
    event_value = Event::Tick
);

/// [`GcTrigger`] is used to periodically trigger garbage collection on datanodes.
pub struct GcTrigger {
    /// The metadata manager.
    table_metadata_manager: TableMetadataManagerRef,
    /// The mailbox to send messages.
    mailbox: MailboxRef,
    /// The server address.
    server_addr: String,
    /// The receiver of events.
    receiver: Receiver<Event>,
}

impl GcTrigger {
    /// Creates a new [`GcTrigger`].
    pub(crate) fn new(
        table_metadata_manager: TableMetadataManagerRef,
        mailbox: MailboxRef,
        server_addr: String,
    ) -> (Self, GcTicker) {
        let (tx, rx) = Self::channel();
        let gc_ticker = GcTicker::new(TICKER_INTERVAL, tx);
        let gc_trigger = Self {
            table_metadata_manager,
            mailbox,
            server_addr,
            receiver: rx,
        };
        (gc_trigger, gc_ticker)
    }

    fn channel() -> (Sender<Event>, Receiver<Event>) {
        tokio::sync::mpsc::channel(8)
    }

    /// Starts the gc trigger.
    pub fn try_start(mut self) -> Result<()> {
        common_runtime::spawn_global(async move { self.run().await });
        info!("GC trigger started");
        Ok(())
    }

    async fn run(&mut self) {
        // trigger a gc immediately after started
        let _ = self.trigger_gc().await;
        while let Some(event) = self.receiver.recv().await {
            match event {
                Event::Tick => {
                    info!("Received gc tick");
                    self.handle_tick().await
                }
            }
        }
    }

    async fn handle_tick(&self) {
        info!("Start to trigger gc");
        if let Err(e) = self.trigger_gc().await {
            error!(e; "Failed to trigger gc");
        }
    }

    /// Iterate through all physical tables and trigger gc for each table.
    /// TODO(discord9): Poc impl, will parallelize later.
    async fn trigger_gc(&self) -> Result<()> {
        info!("Triggering gc");
        // TODO: trigger gc based on statistics, e.g. number of deleted files per table.
        let mut tables: BoxStream<
            'static,
            common_meta::error::Result<(TableId, PhysicalTableRouteValue)>,
        > = self
            .table_metadata_manager
            .table_route_manager()
            .physical_table_values();

        while let Some((table_id, phy_table_val)) = tables
            .as_mut()
            .try_next()
            .await
            .context(TableMetadataManagerSnafu)?
        {
            info!("Triggering gc for table {}", table_id);
            let phy_table_val: PhysicalTableRouteValue = phy_table_val;
            let mut region_ids: Vec<RegionId> = phy_table_val
                .region_routes
                .iter()
                .map(|r: &RegionRoute| r.region.id)
                .collect::<Vec<_>>();

            region_ids.sort_by_key(|f| f.region_number());

            // send instruction to first region id's datanode
            let (first_region_id, first_region_peer) = phy_table_val
                .region_routes
                .first()
                .and_then(|r| r.leader_peer.as_ref().map(|p| (r.region.id, p.clone())))
                .context({ TableRouteNotFoundSnafu { table_id } })?;

            let all_peers = phy_table_val
                .region_routes
                .iter()
                .filter_map(|r| r.leader_peer.clone())
                .collect::<Vec<_>>();

            // only need to trigger gc for one region per datanode
            let peers_to_region_ids: HashMap<Peer, RegionId> = phy_table_val
                .region_routes
                .iter()
                .filter_map(|p| {
                    p.leader_peer
                        .as_ref()
                        .map(|peer| (peer.clone(), p.region.id))
                })
                .collect::<HashMap<_, _>>();

            let now_millis = common_time::util::current_time_millis();

            self.send_upload_ref_instructions(&peers_to_region_ids, now_millis)
                .await?;

            self.send_gc_instruction(first_region_peer.clone(), region_ids.clone(), now_millis)
                .await?;
            info!(
                "Sent gc instruction to datanode {} for table {} with regions {:?}",
                first_region_peer, table_id, region_ids
            );
        }

        Ok(())
    }

    /// Ask all the datanode that have at least one region of the table to upload table reference.
    ///
    /// If any datanode fails to reply the instruction within a timeout, then the entire gc operation
    /// is considered failed.
    async fn send_upload_ref_instructions(
        &self,
        peers_to_region_ids: &HashMap<Peer, RegionId>,
        now_millis: i64,
    ) -> Result<()> {
        let mut wait_for_replies = Vec::with_capacity(peers_to_region_ids.len());
        for (peer, region_id) in peers_to_region_ids {
            info!(
                "Sending upload reference instruction to datanode {} for region {}",
                peer, region_id
            );
            let instruction = Instruction::CollectFileRefs(CollectFileRefs {
                region_id: *region_id,
                ts_millis: now_millis,
            });
            let msg = MailboxMessage::json_message(
                &format!("Upload table reference: {}", instruction),
                &format!("Metasrv@{}", self.server_addr),
                &format!("Datanode-{}@{}", peer.id, peer.addr),
                common_time::util::current_time_millis(),
                &instruction,
            )
            .with_context(|_| error::SerializeToJsonSnafu {
                input: instruction.to_string(),
            })?;

            let mailbox_rx: MailboxReceiver = self
                .mailbox
                .send(&Channel::Datanode(peer.id), msg, Duration::from_secs(60))
                .await?;

            wait_for_replies.push((peer, mailbox_rx));
        }

        // wait for all replies
        for (peer, mailbox_rx) in wait_for_replies {
            match mailbox_rx.await {
                Ok(msg) => continue,
                Err(e) => {
                    error!(e; "Failed to receive upload reference reply from datanode {}", peer);
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    async fn send_gc_instruction(
        &self,
        peer: Peer,
        region_ids: Vec<RegionId>,
        now_millis: i64,
    ) -> Result<()> {
        info!(
            "Sending gc instruction to datanode {} with regions {:?}",
            peer, region_ids
        );
        let instruction = Instruction::GcRegions(GcRegions {
            region_ids,
            ts_millis: now_millis,
        });
        let msg = MailboxMessage::json_message(
            &format!("GC regions: {}", instruction),
            &format!("Metasrv@{}", self.server_addr),
            &format!("Datanode-{}@{}", peer.id, peer.addr),
            common_time::util::current_time_millis(),
            &instruction,
        )
        .with_context(|_| error::SerializeToJsonSnafu {
            input: instruction.to_string(),
        })?;

        if let Err(e) = self
            .mailbox
            .send_oneway(&Channel::Datanode(peer.id), msg)
            .await
        {
            error!(e; "Failed to send gc instruction to datanode {}", peer);
        } else {
            info!("Successfully sent gc instruction to datanode {}", peer);
        }

        metrics::METRIC_META_TRIGGERED_GC_TOTAL.inc();

        Ok(())
    }
}
