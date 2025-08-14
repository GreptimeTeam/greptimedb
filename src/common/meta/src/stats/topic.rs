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

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use common_telemetry::{debug, warn};
use datafusion_common::HashSet;

use crate::datanode::TopicStat;
use crate::distributed_time_constants::{
    TOPIC_STATS_REPORT_INTERVAL_SECS, TOPIC_STATS_RETENTION_SECS,
};
use crate::DatanodeId;

pub type TopicStatsRegistryRef = Arc<TopicStatsRegistry>;

/// Manages statistics for all topics across the cluster.
pub struct TopicStatsRegistry {
    inner: RwLock<TopicStatsStore>,
}

impl Default for TopicStatsRegistry {
    fn default() -> Self {
        Self::new(
            Duration::from_secs(TOPIC_STATS_RETENTION_SECS),
            Duration::from_secs(TOPIC_STATS_REPORT_INTERVAL_SECS),
        )
    }
}

impl TopicStatsRegistry {
    /// Creates a new topic stats registry.
    ///
    /// # Panics
    /// Panic if the window size is zero.
    fn new(retention: Duration, window_size: Duration) -> Self {
        let history_limit = (retention.as_secs() / window_size.as_secs()).max(10) as usize;
        Self {
            inner: RwLock::new(TopicStatsStore::new(history_limit, window_size)),
        }
    }

    /// Adds a topic stat for a given datanode at a specific timestamp.
    pub fn add_stat(&self, datanode_id: DatanodeId, stat: &TopicStat, millis_ts: i64) {
        let mut inner = self.inner.write().unwrap();
        inner.add_stat(datanode_id, stat, millis_ts);
    }

    /// Adds a list of topic stats for a given datanode at a specific timestamp.
    pub fn add_stats(&self, datanode_id: DatanodeId, stats: &[TopicStat], millis_ts: i64) {
        if stats.is_empty() {
            return;
        }

        let mut inner = self.inner.write().unwrap();
        for stat in stats {
            inner.add_stat(datanode_id, stat, millis_ts);
        }
    }

    /// Gets the calculated topic stat for a given topic.
    pub fn get_calculated_topic_stat(
        &self,
        topic: &str,
        period: Duration,
    ) -> Option<CalculatedTopicStat> {
        let inner = self.inner.read().unwrap();
        inner.get_calculated_topic_stat(topic, period)
    }

    /// Gets the latest entry id and timestamp for a given topic.
    pub fn get_latest_entry_id(&self, topic: &str) -> Option<(u64, i64)> {
        let inner = self.inner.read().unwrap();
        inner.get_latest_entry_id(topic)
    }
}

#[derive(Debug, PartialEq, Clone, Default)]
struct HistoryTopicStat {
    /// The latest entry id of the topic.
    pub latest_entry_id: u64,
    /// The total size in bytes of records appended to the topic.
    pub record_size: u64,
    /// The total number of records appended to the topic.
    pub record_num: u64,
    /// The start timestamp of the stat.
    start_ts: i64,
}

#[derive(Debug)]
struct PartialTopicStat {
    /// The latest entry id of the topic.
    pub latest_entry_id: u64,
    /// The total size in bytes of records appended to the topic.
    pub record_size: u64,
    /// The total number of records appended to the topic.
    pub record_num: u64,
    /// The timestamp of the partial topic stat.
    pub timestamp: i64,
}

struct ActiveBucket {
    buffer: HashMap<DatanodeId, HashMap<String, PartialTopicStat>>,
    start_ts: i64,
    window_size: Duration,
}

impl ActiveBucket {
    fn new(timestamp: i64, window_sec: Duration) -> Self {
        Self {
            buffer: HashMap::new(),
            start_ts: timestamp,
            window_size: window_sec,
        }
    }

    fn acceptable_ts(&self, millis_ts: i64) -> bool {
        let acceptable = millis_ts >= self.start_ts
            && millis_ts < self.start_ts + self.window_size.as_millis() as i64;
        if !acceptable {
            debug!(
                "acceptable range: ts >= {} && ts < {}, ts: {}",
                self.start_ts,
                self.start_ts + self.window_size.as_millis() as i64,
                millis_ts
            );
        }
        acceptable
    }

    /// Add a topic stat to the current topic stats.
    ///
    /// Returns true if the topic stat is added successfully (stale stat will be ignored directly),
    /// false if the topic stat is out of the window.
    fn add_stat(&mut self, datanode_id: DatanodeId, stat: &TopicStat, millis_ts: i64) -> bool {
        if !self.acceptable_ts(millis_ts) {
            return false;
        }

        let datanode_stats = self.buffer.entry(datanode_id).or_default();

        // Overwrite the topic stat if it already exists.
        if let Some(prev) = datanode_stats.get_mut(&stat.topic) {
            if millis_ts > prev.timestamp {
                *prev = PartialTopicStat {
                    latest_entry_id: stat.latest_entry_id,
                    record_size: stat.record_size,
                    record_num: stat.record_num,
                    timestamp: millis_ts,
                };
            } else {
                warn!(
                    "Ignore stale topic stat for topic: {}, timestamp: {}, last recorded timestamp: {}",
                    stat.topic, millis_ts, prev.timestamp
                );
            }
        } else {
            datanode_stats.insert(
                stat.topic.to_string(),
                PartialTopicStat {
                    latest_entry_id: stat.latest_entry_id,
                    record_size: stat.record_size,
                    record_num: stat.record_num,
                    timestamp: millis_ts,
                },
            );
        }
        true
    }

    fn merge(self) -> HashMap<String, HistoryTopicStat> {
        let all_topics = self
            .buffer
            .values()
            .flat_map(|stats| stats.keys())
            .collect::<HashSet<_>>();

        let mut output = HashMap::with_capacity(all_topics.len());
        for topic in all_topics {
            let stats = self
                .buffer
                .values()
                .flat_map(|stats| stats.get(topic))
                .collect::<Vec<_>>();
            debug!("stats: {:?} for topic: {}", stats, topic);
            let latest_entry_id = stats
                .iter()
                .map(|stat| stat.latest_entry_id)
                .max()
                .unwrap_or(0);
            let record_size = stats.iter().map(|stat| stat.record_size).sum::<u64>();
            let record_num = stats.iter().map(|stat| stat.record_num).sum::<u64>();

            output.insert(
                topic.to_string(),
                HistoryTopicStat {
                    latest_entry_id,
                    record_size,
                    record_num,
                    start_ts: self.start_ts,
                },
            );
        }

        output
    }

    /// Get the partial topic stat of a datanode.
    #[cfg(test)]
    fn get_stat(&self, datanode_id: DatanodeId, topic: &str) -> Option<&PartialTopicStat> {
        self.buffer
            .get(&datanode_id)
            .and_then(|stats| stats.get(topic))
    }
}

/// Manages topic statistics over time, including active and historical buckets.
struct TopicStatsStore {
    /// The currently active bucket collecting stats.
    active_bucket: Option<ActiveBucket>,
    /// Historical merged buckets, grouped by topic.
    history_by_topic: HashMap<String, VecDeque<HistoryTopicStat>>,
    /// Maximum number of historical windows to keep per topic.
    history_limit: usize,
    /// Duration of each stats window in seconds.
    window_size: Duration,
}

impl TopicStatsStore {
    /// Create a new topic stats.
    fn new(history_limit: usize, window_size: Duration) -> Self {
        Self {
            active_bucket: None,
            history_by_topic: HashMap::new(),
            history_limit,
            window_size,
        }
    }

    /// Aligns the timestamp to the nearest second.
    fn align_ts(millis_ts: i64) -> i64 {
        (millis_ts / 1000) * 1000
    }

    fn rotate_active_bucket(&mut self, start_ts: i64) {
        let aligned_ts = Self::align_ts(start_ts);
        if let Some(old_bucket) = self.active_bucket.take() {
            let merged = old_bucket.merge();
            for (topic, stat) in merged {
                debug!(
                    "Merge current topic: {}, stats into history: {:?}",
                    topic, stat
                );
                let history = self.history_by_topic.entry(topic).or_default();
                history.push_back(stat);
                if history.len() > self.history_limit {
                    history.pop_front();
                }
            }
        }

        self.active_bucket = Some(ActiveBucket::new(aligned_ts, self.window_size));
    }

    /// Adds a topic stat for a given datanode at a specific timestamp.
    fn add_stat(&mut self, datanode_id: DatanodeId, stat: &TopicStat, millis_ts: i64) {
        let aligned_ts = Self::align_ts(millis_ts);

        let need_rotate = match &self.active_bucket {
            Some(bucket) => !bucket.acceptable_ts(aligned_ts),
            None => true,
        };

        if need_rotate {
            debug!("Rotate active bucket at ts: {}", aligned_ts);
            self.rotate_active_bucket(aligned_ts);
        }

        // Safety: The current topic stats is initialized in the previous step.
        let active_bucket = self.active_bucket.as_mut().unwrap();
        let added = active_bucket.add_stat(datanode_id, stat, millis_ts);
        debug_assert!(added);
    }

    /// Gets the calculated topic stat for a given topic.
    fn get_calculated_topic_stat(
        &self,
        topic: &str,
        period: Duration,
    ) -> Option<CalculatedTopicStat> {
        let stats = self.history_by_topic.get(topic)?;
        calculate_topic_stat(stats, period)
    }

    /// Gets the latest entry id and timestamp for a given topic.
    fn get_latest_entry_id(&self, topic: &str) -> Option<(u64, i64)> {
        self.history_by_topic.get(topic).and_then(|stats| {
            stats
                .back()
                .map(|stat| (stat.latest_entry_id, stat.start_ts))
        })
    }
}

/// The calculated topic stat.
///
/// The average record size is the average record size of the topic over the window.
/// The start timestamp is the timestamp of the window start.
/// The end timestamp is the timestamp of the window end.
pub struct CalculatedTopicStat {
    pub avg_record_size: usize,
    pub start_ts: i64,
    pub end_ts: i64,
}

/// Calculates the average record size for a topic within a specified time window based on recent merged statistics.
///
/// Returns `Some(CalculatedTopicStat)` if the calculation is successful, or `None` if insufficient data is available.
fn calculate_topic_stat(
    stats: &VecDeque<HistoryTopicStat>,
    period: Duration,
) -> Option<CalculatedTopicStat> {
    if stats.len() < 2 {
        return None;
    }

    let last_stat = stats.back().unwrap();
    let first_stat = stats.front().unwrap();
    // Not enough stats data.
    if first_stat.start_ts + period.as_millis() as i64 > last_stat.start_ts {
        return None;
    }

    // Find the first stat whose timestamp is less than the last stat's timestamp - period.as_millis() as i64.
    // TODO(weny): Use binary search to find the target stat.
    let target_stat = stats
        .iter()
        .rev()
        .skip(1)
        .find(|stat| (stat.start_ts + period.as_millis() as i64) < last_stat.start_ts);

    let target_stat = target_stat?;

    // The target stat's record size and record num should be less than the last stat's record size and record num.
    if target_stat.record_size > last_stat.record_size
        || target_stat.record_num > last_stat.record_num
    {
        return None;
    }

    // Safety: the last stat's record size and record num must be greater than the target stat's record size and record num.
    let record_size = last_stat.record_size - target_stat.record_size;
    let record_num = last_stat.record_num - target_stat.record_num;
    let avg_record_size = record_size.checked_div(record_num).unwrap_or(0) as usize;

    let start_ts = target_stat.start_ts;
    let end_ts = last_stat.start_ts;
    Some(CalculatedTopicStat {
        avg_record_size,
        start_ts,
        end_ts,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use common_time::util::current_time_millis;

    use super::*;
    use crate::datanode::TopicStat;

    fn merged_stat(ts: i64, record_size: u64, record_num: u64) -> HistoryTopicStat {
        HistoryTopicStat {
            start_ts: ts,
            record_size,
            record_num,
            ..Default::default()
        }
    }

    #[test]
    fn test_empty_stats() {
        let stats: VecDeque<HistoryTopicStat> = VecDeque::new();
        assert!(calculate_topic_stat(&stats, Duration::from_secs(10)).is_none());
    }

    #[test]
    fn test_single_stat() {
        let mut stats = VecDeque::new();
        stats.push_back(merged_stat(1000, 100, 2));
        assert!(calculate_topic_stat(&stats, Duration::from_secs(10)).is_none());
    }

    #[test]
    fn test_no_target_stat_found() {
        let mut stats = VecDeque::new();
        stats.push_back(merged_stat(1000, 100, 2));
        stats.push_back(merged_stat(2000, 200, 4));
        // window_sec is large, so no stat will be found
        assert!(calculate_topic_stat(&stats, Duration::from_secs(100)).is_none());
    }

    #[test]
    fn test_target_stat_found() {
        let mut stats = VecDeque::new();
        stats.push_back(merged_stat(1000, 100, 2));
        stats.push_back(merged_stat(3000, 200, 4));
        stats.push_back(merged_stat(6000, 600, 6));
        let result = calculate_topic_stat(&stats, Duration::from_secs(2));
        assert!(result.is_some());
        let stat = result.unwrap();
        assert_eq!(stat.avg_record_size, 200); // (600 - 200) / (6 - 4)
        assert_eq!(stat.start_ts, 3000);
        assert_eq!(stat.end_ts, 6000);
    }

    #[test]
    fn test_target_stat_decreasing() {
        let mut stats = VecDeque::new();
        stats.push_back(merged_stat(1000, 100, 2));
        stats.push_back(merged_stat(3000, 200, 4));
        stats.push_back(merged_stat(6000, 100, 1)); // Reset or something wrong
        let result = calculate_topic_stat(&stats, Duration::from_secs(2));
        assert!(result.is_none());
    }

    #[test]
    fn test_multiple_stats_target_found() {
        let mut stats = VecDeque::new();
        stats.push_back(merged_stat(1000, 100, 2));
        stats.push_back(merged_stat(2000, 200, 4));
        stats.push_back(merged_stat(4000, 400, 8));
        stats.push_back(merged_stat(8000, 800, 16));
        let result = calculate_topic_stat(&stats, Duration::from_secs(3));
        assert!(result.is_some());
        let stat = result.unwrap();
        assert_eq!(stat.avg_record_size, 50); // (800 - 400) / (16 - 8)
        assert_eq!(stat.start_ts, 4000);
        assert_eq!(stat.end_ts, 8000);
    }

    #[test]
    fn test_active_bucket() {
        let ts = current_time_millis();
        let window_size = Duration::from_secs(3);
        let mut active_bucket = ActiveBucket::new(ts, window_size);

        assert!(active_bucket.add_stat(
            0,
            &TopicStat {
                topic: "test".to_string(),
                latest_entry_id: 1,
                record_size: 256,
                record_num: 1,
            },
            ts + 10,
        ));

        assert!(active_bucket.add_stat(
            1,
            &TopicStat {
                topic: "test".to_string(),
                latest_entry_id: 10,
                record_size: 5120,
                record_num: 10,
            },
            ts + 10,
        ));

        assert!(active_bucket.add_stat(
            0,
            &TopicStat {
                topic: "test1".to_string(),
                latest_entry_id: 2,
                record_size: 128,
                record_num: 2,
            },
            ts + 9,
        ));

        // Out of the window.
        assert!(!active_bucket.add_stat(
            0,
            &TopicStat {
                topic: "test".to_string(),
                latest_entry_id: 2,
                record_size: 2,
                record_num: 2,
            },
            ts + window_size.as_millis() as i64 + 1,
        ));

        // Out of the window.
        assert!(!active_bucket.add_stat(
            0,
            &TopicStat {
                topic: "test".to_string(),
                latest_entry_id: 2,
                record_size: 2,
                record_num: 2,
            },
            ts - 1
        ));

        // Overwrite the topic stat if the timestamp is larger.
        assert!(active_bucket.add_stat(
            0,
            &TopicStat {
                topic: "test".to_string(),
                latest_entry_id: 3,
                record_size: 1024,
                record_num: 3,
            },
            ts + 11,
        ));
        assert_eq!(
            active_bucket.get_stat(0, "test").unwrap().latest_entry_id,
            3
        );

        // Ignore stale topic stat.
        assert!(active_bucket.add_stat(
            0,
            &TopicStat {
                topic: "test".to_string(),
                latest_entry_id: 2,
                record_size: 512,
                record_num: 2,
            },
            ts + 9,
        ));

        assert_eq!(
            active_bucket.get_stat(0, "test").unwrap().latest_entry_id,
            3
        );

        let merged = active_bucket.merge();
        assert_eq!(merged.len(), 2);
        assert_eq!(merged.get("test").unwrap().latest_entry_id, 10);
        assert_eq!(merged.get("test").unwrap().record_size, 5120 + 1024);
        assert_eq!(merged.get("test").unwrap().record_num, 10 + 3);

        assert_eq!(merged.get("test1").unwrap().latest_entry_id, 2);
        assert_eq!(merged.get("test1").unwrap().record_size, 128);
        assert_eq!(merged.get("test1").unwrap().record_num, 2);
        assert_eq!(merged.get("test1").unwrap().start_ts, ts);
    }

    #[test]
    fn test_topic_stats() {
        let topic_name = "test";
        let window_size = Duration::from_secs(60);
        let mut topic_stats = TopicStatsStore::new(5, window_size);
        let ts = TopicStatsStore::align_ts(current_time_millis());
        debug!("add stat at ts: {}", ts);
        topic_stats.add_stat(
            0,
            &TopicStat {
                topic: topic_name.to_string(),
                latest_entry_id: 1,
                record_size: 1024,
                record_num: 1,
            },
            ts,
        );

        debug!("add stat at ts: {}", ts + window_size.as_millis() as i64);
        topic_stats.add_stat(
            1,
            &TopicStat {
                topic: topic_name.to_string(),
                latest_entry_id: 4,
                record_size: 4096,
                record_num: 4,
            },
            ts + window_size.as_millis() as i64 - 1,
        );

        topic_stats.add_stat(
            1,
            &TopicStat {
                topic: "another_topic".to_string(),
                latest_entry_id: 4,
                record_size: 4096,
                record_num: 4,
            },
            ts + window_size.as_millis() as i64 - 1,
        );

        debug!(
            "add stat at ts: {}",
            ts + window_size.as_millis() as i64 + 1
        );
        // Add a stat that is out of the window.
        topic_stats.add_stat(
            1,
            &TopicStat {
                topic: topic_name.to_string(),
                latest_entry_id: 5,
                record_size: 8192,
                record_num: 5,
            },
            ts + window_size.as_millis() as i64,
        );

        let history = topic_stats.history_by_topic.get(topic_name).unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(
            history[0],
            HistoryTopicStat {
                latest_entry_id: 4,
                record_size: 1024 + 4096,
                record_num: 1 + 4,
                start_ts: ts,
            }
        );
        assert!(topic_stats.active_bucket.is_some());
    }
}
