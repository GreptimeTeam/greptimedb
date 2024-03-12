use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::repr::{Diff, KeyValDiffRow, Row, Timestamp};

/// A shared state of key-value pair for various state
/// in dataflow execution
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Deserialize, Serialize)]
pub struct Arrangement {
    /// all the future updates that pending to be applied
    /// arranged in time -> (key -> (new_val, diff))
    spine: BTreeMap<Timestamp, BTreeMap<Row, (Row, Diff)>>,
    /// indicate all of the time of update for each key in spine
    key_to_time: BTreeMap<Row, BTreeSet<Timestamp>>,
    /// the current value of the arrangement,
    /// updated by spine when current time is no less than the time of the update
    /// TODO(discord9): consider if have val=(Row, Diff) support multicity of the same key
    current: BTreeMap<Row, (Row, Diff)>,
}

impl Arrangement {
    /// send back updates that can be applied (i.e. the time of the update is not greater than the current time)
    ///
    /// also send updates from spine which can be applied now back too.
    /// and only retain updates that are yet to come.
    /// Wouldn't update the current value of the arrangement.
    ///
    /// useful for Mfp Operator's temporal filter,
    pub fn filter_and_retain_future_updates(
        &mut self,
        now: Timestamp,
        mut updates: Vec<KeyValDiffRow>,
    ) -> Vec<KeyValDiffRow> {
        updates = updates
            .into_iter()
            .filter_map(|((key, val), time, diff)| {
                if time <= now {
                    Some(((key, val), time, diff))
                } else {
                    // future updates goes into spine for later application
                    self.spine.entry(time).or_default().insert(key, (val, diff));
                    None
                }
            })
            .collect_vec();

        // also append updates from spine which can be applied now back too
        // note that current time's updates is also send back
        let after = self.spine.split_off(&(now + 1));
        // TODO(discord9): consolidate updates with same key and time
        // BTreeMap;'s root is just a NodeRef which is a pointer so mem::take is not very expensive
        for (time, update) in std::mem::take(&mut self.spine).into_iter() {
            updates.extend(
                update
                    .into_iter()
                    .map(|(key, (val, diff))| ((key, val), time, diff)),
            );
        }
        self.spine = after;

        updates
    }

    /// apply updates <= now, and save future updates in spine
    pub fn apply_updates(&mut self, now: Timestamp, updates: Vec<KeyValDiffRow>) {
        for ((key, val), time, diff) in updates {
            if time <= now {
                // TODO(discord9): consider error handling including check old/new val eq etc.

                let new_val_diff = self
                    .current
                    .entry(key.clone())
                    .and_modify(|e| e.1 += diff)
                    .or_insert((val, diff));

                if new_val_diff.1 == 0 {
                    self.current.remove(&key);
                }
            } else {
                self.spine
                    .entry(time)
                    .or_default()
                    .insert(key.clone(), (val, diff));
                self.key_to_time.entry(key).or_default().insert(time);
            }
        }
    }

    // TODO(discord9): expire key, values by something

    /// useful for join to query existing keys
    pub fn get(&self, key: &Row) -> Option<&(Row, Diff)> {
        self.current.get(key)
    }
}

/// A handler to the inner Arrangement, can be cloned and shared
#[derive(Debug, Clone)]
pub struct Arranged {
    inner: Arc<Mutex<Arrangement>>,
}
