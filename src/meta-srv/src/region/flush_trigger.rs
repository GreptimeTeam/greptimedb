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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use api::v1::meta::MailboxMessage;
use common_base::readable_size::ReadableSize;
use common_meta::instruction::{FlushRegions, Instruction};
use common_meta::key::TableMetadataManagerRef;
use common_meta::peer::Peer;
use common_meta::region_registry::LeaderRegionRegistryRef;
use common_meta::stats::topic::TopicStatsRegistryRef;
use common_telemetry::{debug, error, info};
use common_time::util::current_time_millis;
use itertools::Itertools;
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::error::{self, Result};
use crate::service::mailbox::{Channel, MailboxRef};
use crate::{define_ticker, metrics};

/// The interval of the region flush ticker.
const TICKER_INTERVAL: Duration = Duration::from_secs(60);

/// The duration of the recent period.
const RECENT_DURATION: Duration = Duration::from_secs(300);

/// [`Event`] represents various types of events that can be processed by the region flush ticker.
///
/// Variants:
/// - `Tick`: This event is used to trigger region flush trigger periodically.
pub(crate) enum Event {
    Tick,
}

pub(crate) type RegionFlushTickerRef = Arc<RegionFlushTicker>;

define_ticker!(
    /// [RegionFlushTicker] is used to trigger region flush trigger periodically.
    RegionFlushTicker,
    event_type = Event,
    event_value = Event::Tick
);

/// [`RegionFlushTrigger`] is used to ensure that the estimated WAL replay size
/// stays below a certain threshold by triggering a region flush when the estimated
/// WAL replay size exceeds that threshold. This helps improve datanode startup
/// speed and reduce the overall startup time.
///
/// The estimated WAL replay size is calculated as:
/// `(latest_entry_id - flushed_entry_id) * avg_record_size`
pub struct RegionFlushTrigger {
    /// The metadata manager.
    table_metadata_manager: TableMetadataManagerRef,
    /// The leader region registry.
    leader_region_registry: LeaderRegionRegistryRef,
    /// The topic stats registry.
    topic_stats_registry: TopicStatsRegistryRef,
    /// The mailbox to send messages.
    mailbox: MailboxRef,
    /// The server address.
    server_addr: String,
    /// The flush trigger size.
    flush_trigger_size: ReadableSize,
    /// The receiver of events.
    receiver: Receiver<Event>,
}

impl RegionFlushTrigger {
    /// Creates a new [`RegionFlushTrigger`].
    pub(crate) fn new(
        table_metadata_manager: TableMetadataManagerRef,
        leader_region_registry: LeaderRegionRegistryRef,
        topic_stats_registry: TopicStatsRegistryRef,
        mailbox: MailboxRef,
        server_addr: String,
        flush_trigger_size: ReadableSize,
    ) -> (Self, RegionFlushTicker) {
        let (tx, rx) = Self::channel();
        let region_flush_ticker = RegionFlushTicker::new(TICKER_INTERVAL, tx);
        let region_flush_trigger = Self {
            table_metadata_manager,
            leader_region_registry,
            topic_stats_registry,
            mailbox,
            server_addr,
            flush_trigger_size,
            receiver: rx,
        };
        (region_flush_trigger, region_flush_ticker)
    }

    fn channel() -> (Sender<Event>, Receiver<Event>) {
        tokio::sync::mpsc::channel(8)
    }

    /// Starts the region flush trigger.
    pub fn try_start(mut self) -> Result<()> {
        common_runtime::spawn_global(async move { self.run().await });
        info!("Region flush trigger started");
        Ok(())
    }

    async fn run(&mut self) {
        while let Some(event) = self.receiver.recv().await {
            match event {
                Event::Tick => self.handle_tick().await,
            }
        }
    }

    async fn handle_tick(&self) {
        if let Err(e) = self.trigger_flush().await {
            error!(e; "Failed to trigger flush");
        }
    }

    async fn trigger_flush(&self) -> Result<()> {
        let now = Instant::now();
        let topics = self
            .table_metadata_manager
            .topic_name_manager()
            .range()
            .await
            .context(error::TableMetadataManagerSnafu)?;

        for topic in &topics {
            let Some((latest_entry_id, avg_record_size)) = self.retrieve_topic_stat(topic) else {
                continue;
            };
            if let Err(e) = self
                .flush_regions_in_topic(topic, latest_entry_id, avg_record_size)
                .await
            {
                error!(e; "Failed to flush regions in topic: {}", topic);
            }
        }

        debug!(
            "Triggered flush for {} topics in {:?}",
            topics.len(),
            now.elapsed()
        );
        Ok(())
    }

    /// Retrieves the latest entry id and average record size of a topic.
    ///
    /// Returns `None` if the topic is not found or the latest entry id is not recent.
    fn retrieve_topic_stat(&self, topic: &str) -> Option<(u64, usize)> {
        let Some((latest_entry_id, timestamp)) =
            self.topic_stats_registry.get_latest_entry_id(topic)
        else {
            debug!("No latest entry id found for topic: {}", topic);
            return None;
        };

        let Some(stat) = self
            .topic_stats_registry
            .get_calculated_topic_stat(topic, TICKER_INTERVAL)
        else {
            debug!("No topic stat found for topic: {}", topic);
            return None;
        };

        let now = current_time_millis();
        if !is_recent(timestamp, now, RECENT_DURATION) {
            debug!(
                "Latest entry id of topic '{}': is not recent (now: {}, stat timestamp: {})",
                topic, timestamp, now
            );
            return None;
        }
        if !is_recent(stat.end_ts, now, RECENT_DURATION) {
            debug!(
                "Calculated stat of topic '{}': is not recent (now: {}, stat timestamp: {})",
                topic, stat.end_ts, now
            );
            return None;
        }

        Some((latest_entry_id, stat.avg_record_size))
    }

    async fn flush_regions_in_topic(
        &self,
        topic: &str,
        latest_entry_id: u64,
        avg_record_size: usize,
    ) -> Result<()> {
        let region_ids = self
            .table_metadata_manager
            .topic_region_manager()
            .regions(topic)
            .await
            .context(error::TableMetadataManagerSnafu)?;
        if region_ids.is_empty() {
            debug!("No regions found for topic: {}", topic);
            return Ok(());
        }

        let (inactive_regions, active_regions): (Vec<_>, Vec<_>) = self
            .leader_region_registry
            .batch_get(region_ids.iter().cloned())
            .into_iter()
            .partition_map(|(region_id, region)| {
                if !region.manifest.is_inactive() {
                    itertools::Either::Left((region_id, region.manifest.prunable_entry_id()))
                } else {
                    itertools::Either::Right((region_id, region.manifest.prunable_entry_id()))
                }
            });

        // Selects regions to flush from the set of active regions.
        let mut regions_to_flush = select_regions_to_flush(
            topic,
            active_regions.into_iter(),
            avg_record_size as u64,
            latest_entry_id,
            self.flush_trigger_size,
        );

        let active_regions_num = regions_to_flush.len();
        // Selects regions to flush from the set of inactive regions.
        // For inactive regions, we use a lower flush trigger size (half of the normal size)
        // to encourage more aggressive flushing to update the region's topic latest entry id.
        let inactive_regions_to_flush = select_regions_to_flush(
            topic,
            inactive_regions.into_iter(),
            avg_record_size as u64,
            latest_entry_id,
            self.flush_trigger_size / 2,
        );
        let inactive_regions_num = inactive_regions_to_flush.len();
        regions_to_flush.extend(inactive_regions_to_flush);

        // Sends flush instructions to datanodes.
        if !regions_to_flush.is_empty() {
            self.send_flush_instructions(&regions_to_flush).await?;
            debug!(
                "Sent {} flush instructions to datanodes for topic: '{}' ({} inactive regions)",
                regions_to_flush.len(),
                topic,
                inactive_regions_num,
            );
        }

        metrics::METRIC_META_TRIGGERED_REGION_FLUSH_TOTAL
            .with_label_values(&[topic, "active"])
            .inc_by(active_regions_num as u64);
        metrics::METRIC_META_TRIGGERED_REGION_FLUSH_TOTAL
            .with_label_values(&[topic, "inactive"])
            .inc_by(inactive_regions_num as u64);

        Ok(())
    }

    async fn send_flush_instructions(&self, regions_to_flush: &[RegionId]) -> Result<()> {
        let leader_to_region_ids =
            group_regions_by_leader(&self.table_metadata_manager, regions_to_flush).await?;
        let flush_instructions = leader_to_region_ids
            .into_iter()
            .map(|(leader, region_ids)| {
                let flush_instruction = Instruction::FlushRegions(FlushRegions { region_ids });
                (leader, flush_instruction)
            });

        for (peer, flush_instruction) in flush_instructions {
            let msg = MailboxMessage::json_message(
                &format!("Flush regions: {}", flush_instruction),
                &format!("Metasrv@{}", self.server_addr),
                &format!("Datanode-{}@{}", peer.id, peer.addr),
                common_time::util::current_time_millis(),
                &flush_instruction,
            )
            .with_context(|_| error::SerializeToJsonSnafu {
                input: flush_instruction.to_string(),
            })?;
            if let Err(e) = self
                .mailbox
                .send_oneway(&Channel::Datanode(peer.id), msg)
                .await
            {
                error!(e; "Failed to send flush instruction to datanode {}", peer);
            }
        }

        Ok(())
    }
}

/// Select regions to flush based on the estimated replay size.
///
/// The regions are selected if the estimated replay size exceeds the flush trigger size.
/// The estimated replay size is calculated as:
/// `(latest_entry_id - prunable_entry_id) * avg_record_size`
fn select_regions_to_flush<I: Iterator<Item = (RegionId, u64)>>(
    topic: &str,
    regions: I,
    avg_record_size: u64,
    latest_entry_id: u64,
    flush_trigger_size: ReadableSize,
) -> Vec<RegionId> {
    let mut regions_to_flush = Vec::new();
    for (region_id, prunable_entry_id) in regions {
        if prunable_entry_id < latest_entry_id {
            let replay_size = (latest_entry_id - prunable_entry_id).saturating_mul(avg_record_size);
            if replay_size > flush_trigger_size.as_bytes() {
                debug!(
                    "Region {}: estimated replay size {} exceeds flush trigger size {}, prunable entry id: {}, topic latest entry id: {}, topic: '{}'",
                    region_id, ReadableSize(replay_size), flush_trigger_size, prunable_entry_id, latest_entry_id, topic
                );
                regions_to_flush.push(region_id);
            }
        }
    }

    regions_to_flush
}

/// Group regions by leader.
///
/// The regions are grouped by the leader of the region.
async fn group_regions_by_leader(
    table_metadata_manager: &TableMetadataManagerRef,
    regions_to_flush: &[RegionId],
) -> Result<HashMap<Peer, Vec<RegionId>>> {
    let table_ids = regions_to_flush
        .iter()
        .map(|region_id| region_id.table_id())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let table_ids_table_routes = table_metadata_manager
        .table_route_manager()
        .batch_get_physical_table_routes(&table_ids)
        .await
        .context(error::TableMetadataManagerSnafu)?;

    let mut peer_region_ids_map: HashMap<Peer, Vec<RegionId>> = HashMap::new();
    for region_id in regions_to_flush {
        let table_id = region_id.table_id();
        let table_route = table_ids_table_routes
            .get(&table_id)
            .context(error::TableRouteNotFoundSnafu { table_id })?;
        let Some(region_route) = table_route
            .region_routes
            .iter()
            .find(|r| r.region.id == *region_id)
        else {
            continue;
        };
        let Some(peer) = &region_route.leader_peer else {
            continue;
        };

        match peer_region_ids_map.get_mut(peer) {
            Some(region_ids) => {
                region_ids.push(*region_id);
            }
            None => {
                peer_region_ids_map.insert(peer.clone(), vec![*region_id]);
            }
        }
    }
    Ok(peer_region_ids_map)
}

/// Check if the timestamp is recent.
///
/// The timestamp is recent if the difference between the current time and the timestamp is less than the duration.
fn is_recent(timestamp: i64, now: i64, duration: Duration) -> bool {
    let duration = duration.as_millis() as i64;
    now.saturating_sub(timestamp) < duration
}

#[cfg(test)]
mod tests {
    use common_base::readable_size::ReadableSize;
    use store_api::storage::RegionId;

    use super::*;

    #[test]
    fn test_is_recent() {
        let now = current_time_millis();
        assert!(is_recent(now - 999, now, Duration::from_secs(1)));
        assert!(!is_recent(now - 1001, now, Duration::from_secs(1)));
    }

    fn region_id(table: u32, region: u32) -> RegionId {
        RegionId::new(table, region)
    }

    #[test]
    fn test_no_regions_to_flush_when_none_exceed_threshold() {
        let topic = "test_topic";
        let avg_record_size = 10;
        let latest_entry_id = 100;
        let flush_trigger_size = ReadableSize(1000); // 1000 bytes

        // All regions have prunable_entry_id close to latest_entry_id, so replay_size is small
        let regions = vec![
            (region_id(1, 1), 99), // replay_size = (100-99)*10 = 10
            (region_id(1, 2), 98), // replay_size = 20
            (region_id(1, 3), 95), // replay_size = 50
        ];

        let result = select_regions_to_flush(
            topic,
            regions.into_iter(),
            avg_record_size,
            latest_entry_id,
            flush_trigger_size,
        );
        assert!(result.is_empty());
    }

    #[test]
    fn test_regions_to_flush_when_some_exceed_threshold() {
        let topic = "test_topic";
        let avg_record_size = 10;
        let latest_entry_id = 100;
        let flush_trigger_size = ReadableSize(50); // 50 bytes

        // Only region 1,3 will exceed threshold: (100-90)*10 = 100 > 50
        let regions = vec![
            (region_id(1, 1), 99), // replay_size = 10
            (region_id(1, 2), 98), // replay_size = 20
            (region_id(1, 3), 90), // replay_size = 100
        ];

        let result = select_regions_to_flush(
            topic,
            regions.into_iter(),
            avg_record_size,
            latest_entry_id,
            flush_trigger_size,
        );
        assert_eq!(result, vec![region_id(1, 3)]);
    }

    #[test]
    fn test_regions_to_flush_with_zero_avg_record_size() {
        let topic = "test_topic";
        let avg_record_size = 0;
        let latest_entry_id = 100;
        let flush_trigger_size = ReadableSize(1);

        let regions = vec![(region_id(1, 1), 50), (region_id(1, 2), 10)];

        // replay_size will always be 0, so none should be flushed
        let result = select_regions_to_flush(
            topic,
            regions.into_iter(),
            avg_record_size,
            latest_entry_id,
            flush_trigger_size,
        );
        assert!(result.is_empty());
    }

    #[test]
    fn test_regions_to_flush_with_prunable_entry_id_equal_latest() {
        let topic = "test_topic";
        let avg_record_size = 10;
        let latest_entry_id = 100;
        let flush_trigger_size = ReadableSize(10);

        let regions = vec![
            (region_id(1, 1), 100), // prunable_entry_id == latest_entry_id, should not be flushed
            (region_id(1, 2), 99),  // replay_size = 10
        ];

        let result = select_regions_to_flush(
            topic,
            regions.into_iter(),
            avg_record_size,
            latest_entry_id,
            flush_trigger_size,
        );
        // Only region 1,2 should be flushed if replay_size > 10
        assert!(result.is_empty());
    }

    #[test]
    fn test_multiple_regions_to_flush() {
        let topic = "test_topic";
        let avg_record_size = 5;
        let latest_entry_id = 200;
        let flush_trigger_size = ReadableSize(20);

        let regions = vec![
            (region_id(1, 1), 190), // replay_size = 50
            (region_id(1, 2), 180), // replay_size = 100
            (region_id(1, 3), 199), // replay_size = 5
            (region_id(1, 4), 200), // replay_size = 0
        ];

        let result = select_regions_to_flush(
            topic,
            regions.into_iter(),
            avg_record_size,
            latest_entry_id,
            flush_trigger_size,
        );
        // Only regions 1,1 and 1,2 should be flushed
        assert_eq!(result, vec![region_id(1, 1), region_id(1, 2)]);
    }
}
