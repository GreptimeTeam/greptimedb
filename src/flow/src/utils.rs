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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use smallvec::{smallvec, SmallVec};
use tokio::sync::{Mutex, RwLock};

use crate::expr::error::InternalSnafu;
use crate::expr::{EvalError, ScalarExpr};
use crate::repr::{value_to_internal_ts, Diff, DiffRow, Duration, KeyValDiffRow, Row, Timestamp};

/// Determine when should a key expire according to it's event timestamp in key,
/// if a key is expired, any future updates to it should be ignored
/// Note that key is expired by it's event timestamp(contained in the key), not by the time it's inserted(system timestamp)
///
/// TODO(discord9): find a better way to handle key expiration, like write to disk or something instead of throw away
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Deserialize, Serialize)]
pub struct KeyExpiryManager {
    /// a map from event timestamp to key, used for expire keys
    event_ts_to_key: BTreeMap<Timestamp, BTreeSet<Row>>,
    /// duration after which a key is considered expired, and will be removed from state
    key_expiration_duration: Option<Duration>,
    /// using this to get timestamp from key row
    event_timestamp_from_row: Option<ScalarExpr>,
}

impl KeyExpiryManager {
    /// extract event timestamp from key row
    ///
    /// if no expire state is set, return None
    pub fn extract_event_ts(&self, row: &Row) -> Result<Option<Timestamp>, EvalError> {
        let ts = self
            .event_timestamp_from_row
            .as_ref()
            .map(|e| e.eval(&row.inner))
            .transpose()?
            .map(value_to_internal_ts)
            .transpose()?;
        Ok(ts)
    }

    /// return timestamp that should be expired by the time `now` by compute `now - expiration_duration`
    pub fn compute_expiration_timestamp(&self, now: Timestamp) -> Option<Timestamp> {
        self.key_expiration_duration.map(|d| now - d)
    }

    /// update the event timestamp to key mapping
    ///
    /// if given key is expired by now(that is lesser than `now - expiry_duration`), return the amount of time it's expired
    /// if it's not expired, return None
    pub fn update_event_ts(
        &mut self,
        now: Timestamp,
        row: &Row,
    ) -> Result<Option<Duration>, EvalError> {
        let ts = if let Some(event_ts) = self.extract_event_ts(row)? {
            let ret = self.compute_expiration_timestamp(now).and_then(|e| {
                if e > event_ts {
                    // return how much time it's expired
                    Some(e - event_ts)
                } else {
                    None
                }
            });
            if let Some(expire_by) = ret {
                return Ok(Some(expire_by));
            }
            event_ts
        } else {
            return Ok(None);
        };

        self.event_ts_to_key
            .entry(ts)
            .or_default()
            .insert(row.clone());
        Ok(None)
    }
}

/// A shared state of key-value pair for various state
/// in dataflow execution
///
/// i.e: Mfp operator with temporal filter need to store it's future output so that it can add now, and delete later.
/// To get all needed updates in a time span, use [`get_updates_in_range`]
///
/// And reduce operator need full state of it's output, so that it can query(and modify by calling [`apply_updates`])
/// existing state, also need a way to expire keys. To get a key's current value, use [`get`] with time being `now`
/// so it's like:
/// `mfp operator -> arrange(store futures only, no expire) -> reduce operator <-> arrange(full, with key expiring time) -> output`
///
/// Note the two way arrow between reduce operator and arrange, it's because reduce operator need to query existing state
/// and also need to update existing state
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Deserialize, Serialize)]
pub struct Arrangement {
    /// all the updates that pending to be applied
    /// arranged in time -> (key -> (new_val, diff))
    /// all updates where the update time is greater than the last key but less than or equal to the current key
    /// are updates are categorized under current key.
    ///
    /// that is: `last key < update time <= current key`
    /// or for time that's before the first key, just being categorized under the first key
    /// The first key is always `now` which include consolidated updates from past, representing the current state of arrangement
    ///
    /// Note that for a given time and key, there might be a bunch of updates and they should be applied in order
    /// And for consolidated batch(i.e. btach representing now), there should be only one update for each key with `diff==1`
    ///
    /// And since most time a key gots updated by first delete then insert, small vec with size of 2 make sense
    spine: BTreeMap<Timestamp, BTreeMap<Row, SmallVec<[DiffRow; 2]>>>,
    /// if set to false, will not update current value of the arrangement, useful for case like `map -> arrange -> reduce`
    full_arrangement: bool,
    /// flag to mark that this arrangement haven't been written to, so that it can be cloned and shared
    is_written: bool,
    /// manage the expire state of the arrangement
    expire_state: Option<KeyExpiryManager>,
}

impl Arrangement {
    pub fn new() -> Self {
        Self {
            spine: Default::default(),
            full_arrangement: false,
            is_written: false,
            expire_state: None,
        }
    }

    /// apply updates into spine, all updates should have timestamps that are larger than spine's first key
    ///
    /// return the maximum expire time(already expire by how much time) of all updates if any keys is already expired
    pub fn apply_updates(
        &mut self,
        now: Timestamp,
        updates: Vec<KeyValDiffRow>,
    ) -> Result<Option<Duration>, EvalError> {
        let mut max_late_by: Option<Duration> = None;
        if !self.is_written {
            self.is_written = true;
        }
        for ((key, val), ts, diff) in updates {
            // keep rows with expired event timestamp from being updated
            if let Some(s) = &mut self.expire_state {
                if let Some(late_by) = s.update_event_ts(now, &key)? {
                    max_late_by = Some(max_late_by.map_or(late_by, |v| v.max(late_by)));
                    continue;
                }
            }
            // the first batch with key that's greater or equal to ts
            let batch = if let Some((_, batch)) = self.spine.range_mut(ts..).next() {
                batch
            } else {
                // if no batch with `batch key >= ts`, then create a new batch with key being `ts`
                self.spine.entry(ts).or_default()
            };

            {
                let key_updates = batch.entry(key).or_insert(smallvec![]);
                key_updates.push((val, ts, diff));
            }
        }
        Ok(max_late_by)
    }

    /// advance time to `now` and consolidate all older(`now` included) updates to the first key
    ///
    /// return the maximum expire time(already expire by how much time) of all updates if any keys is already expired
    pub fn set_compaction(&mut self, now: Timestamp) -> Result<Option<Duration>, EvalError> {
        let mut max_late_by: Option<Duration> = None;

        let mut should_compact = self.spine.split_off(&(now + 1));
        std::mem::swap(&mut should_compact, &mut self.spine);

        // if a full arrangement is not needed, we can just discard everything before and including now
        if !self.full_arrangement {
            return Ok(None);
        }
        // else we update them into current key value pairs
        let mut compacted_batch: BTreeMap<Row, SmallVec<[DiffRow; 2]>> = Default::default();

        for (_, batch) in should_compact {
            for (key, updates) in batch {
                if let Some(s) = &mut self.expire_state {
                    if let Some(late_by) = s.update_event_ts(now, &key)? {
                        max_late_by = Some(max_late_by.map_or(late_by, |v| v.max(late_by)));
                        continue;
                    }
                }
                // if diff cancel out each other, then remove the key
                let mut old_row: Option<DiffRow> =
                    compacted_batch.get(&key).and_then(|v| v.first()).cloned();

                for new_row in updates {
                    old_row = compact_diff_row(old_row, &new_row);
                }
                if let Some(compacted_update) = old_row {
                    compacted_batch.insert(key, smallvec![compacted_update]);
                } else {
                    compacted_batch.remove(&key);
                }
            }
        }

        // insert the compacted batch into spine with key being `now`
        self.spine.insert(now, compacted_batch);
        Ok(max_late_by)
    }

    /// get the updates of the arrangement from the given range of time
    pub fn get_updates_in_range<R: std::ops::RangeBounds<Timestamp>>(
        &self,
        range: R,
    ) -> Vec<KeyValDiffRow> {
        let mut result = vec![];
        for (_ts, batch) in self.spine.range(range) {
            for (key, updates) in batch.clone() {
                for (val, ts, diff) in updates {
                    result.push(((key.clone(), val), ts, diff));
                }
            }
        }
        result
    }

    /// expire keys in now that are older than expire_time, intended for reducing memory usage and limit late data arrive
    pub fn trunc_expired(&mut self, now: Timestamp) {
        if let Some(s) = &mut self.expire_state {
            let expire_time = if let Some(t) = s.compute_expiration_timestamp(now) {
                t
            } else {
                // never expire
                return;
            };
            // find all keys smaller than or equal expire_time and silently remove them
            let mut after = s.event_ts_to_key.split_off(&(expire_time + 1));
            std::mem::swap(&mut s.event_ts_to_key, &mut after);
            let before = after;
            for key in before.into_iter().flat_map(|i| i.1.into_iter()) {
                for (_ts, batch) in self.spine.iter_mut() {
                    batch.remove(&key);
                }
            }
        }
    }

    /// get current state of things
    /// useful for query existing keys(i.e. reduce and join operator need to query existing state)
    pub fn get(&self, now: Timestamp, key: &Row) -> Option<(Row, Timestamp, Diff)> {
        if self
            .spine
            .first_key_value()
            .map(|(ts, _)| *ts >= now)
            .unwrap_or(false)
        {
            self.spine
                .first_key_value()
                .and_then(|(_ts, batch)| batch.get(key).and_then(|v| v.first()).cloned())
        } else {
            // check keys <= now to know current value
            let mut final_val = None;
            for (_ts, batch) in self.spine.range(..=now) {
                if let Some(new_rows) = batch.get(key).map(|v| v.iter()) {
                    for new_row in new_rows {
                        final_val = compact_diff_row(final_val, new_row);
                    }
                }
            }
            final_val
        }
    }
}

fn compact_diff_row(old_row: Option<DiffRow>, new_row: &DiffRow) -> Option<DiffRow> {
    let (val, ts, diff) = new_row;
    match (old_row, diff) {
        (Some((row, _old_ts, old_diff)), diff) if row == *val && old_diff + diff == 0 => {
            // the key is deleted now
            None
        }
        (Some((row, _old_ts, old_diff)), diff) if row == *val && old_diff + diff != 0 => {
            Some((row, *ts, old_diff + *diff))
        }
        // if old val not equal new val, simple consider it as being overwritten, for each key can only have one value
        // so it make sense to just replace the old value with new value
        _ => Some((val.clone(), *ts, *diff)),
    }
}

/// A handler to the inner Arrangement, can be cloned and shared, useful for query it's inner state
#[derive(Debug)]
pub struct ArrangeHandler {
    inner: Arc<RwLock<Arrangement>>,
}
impl ArrangeHandler {
    pub fn from(arr: Arrangement) -> Self {
        Self {
            inner: Arc::new(RwLock::new(arr)),
        }
    }
    pub fn write(&self) -> tokio::sync::RwLockWriteGuard<'_, Arrangement> {
        self.inner.blocking_write()
    }
    pub fn read(&self) -> tokio::sync::RwLockReadGuard<'_, Arrangement> {
        self.inner.blocking_read()
    }

    /// clone the handler, but only keep the future updates
    pub fn clone_future_only(&self) -> Option<Self> {
        if self.read().is_written {
            return None;
        }
        Some(Self {
            inner: self.inner.clone(),
        })
    }

    /// clone the handler, but keep all updates
    /// prevent illegal clone after the arrange have been written,
    /// because that will cause loss of data before clone
    pub fn clone_full_arrange(&self) -> Option<Self> {
        if self.read().is_written {
            return None;
        }
        let mut arr = self.write();
        arr.full_arrangement = true;
        drop(arr);
        Some(Self {
            inner: self.inner.clone(),
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_future_get() {
        // test if apply only future updates, whether get(future_time) can operate correctly
        let arr = Arrangement::new();
        let arr = ArrangeHandler::from(arr);

        {
            let mut arr = arr.write();
            let key = Row::new(vec![1.into()]);
            let updates: Vec<KeyValDiffRow> = vec![
                ((key.clone(), Row::new(vec![2.into()])), 1, 1),
                ((key.clone(), Row::new(vec![3.into()])), 2, 1),
                ((key.clone(), Row::new(vec![4.into()])), 3, 1),
            ];
            // all updates above are future updates
            arr.apply_updates(0, updates).unwrap();

            assert_eq!(arr.get(1, &key), Some((Row::new(vec![2.into()]), 1, 1)));

            assert_eq!(arr.get(2, &key), Some((Row::new(vec![3.into()]), 2, 1)));

            assert_eq!(arr.get(3, &key), Some((Row::new(vec![4.into()]), 3, 1)));
        }
    }

    #[test]
    fn only_save_future_updates() {
        // mfp operator's temporal filter need to record future updates so that it can delete on time
        // i.e. insert a record now, delete this record 5 minutes later
        // they will only need to keep future updates(if downstream don't need full arrangement that is)
        let arr = Arrangement::new();
        let arr = ArrangeHandler::from(arr);
        let arr1 = arr.clone_full_arrange();
        assert!(arr1.is_some());
        let arr2 = arr.clone_future_only();
        assert!(arr2.is_some());

        {
            let mut arr = arr.write();
            let updates: Vec<KeyValDiffRow> = vec![
                ((Row::new(vec![1.into()]), Row::new(vec![2.into()])), 1, 1),
                ((Row::new(vec![2.into()]), Row::new(vec![3.into()])), 2, 1),
                ((Row::new(vec![3.into()]), Row::new(vec![4.into()])), 3, 1),
            ];
            // all updates above are future updates
            arr.apply_updates(0, updates).unwrap();
            assert_eq!(
                arr.get_updates_in_range(1..=1),
                vec![((Row::new(vec![1.into()]), Row::new(vec![2.into()])), 1, 1)]
            );
            assert_eq!(arr.spine.len(), 3);
            arr.set_compaction(1).unwrap();
            assert_eq!(arr.spine.len(), 3);
        }

        let arr2 = arr.clone_full_arrange();
        assert!(arr2.is_none());
        {
            let mut arr = arr.write();
            assert_eq!(arr.spine.len(), 3);
            arr.set_compaction(2).unwrap();
            assert_eq!(arr.spine.len(), 2);
        }
    }

    #[test]
    fn test_reduce_expire_keys() {
        let mut arr = Arrangement::new();
        let expire_state = KeyExpiryManager {
            event_ts_to_key: Default::default(),
            key_expiration_duration: Some(10),
            event_timestamp_from_row: Some(ScalarExpr::Column(0)),
        };
        let expire_state = Some(expire_state);
        arr.expire_state = expire_state;
        arr.full_arrangement = true;

        let arr = ArrangeHandler::from(arr);
        let now = 0;
        let key = Row::new(vec![1i64.into()]);
        let updates: Vec<KeyValDiffRow> = vec![
            (
                (Row::new(vec![1i64.into()]), Row::new(vec![2.into()])),
                1,
                1,
            ),
            (
                (Row::new(vec![2i64.into()]), Row::new(vec![3.into()])),
                2,
                1,
            ),
            (
                (Row::new(vec![3i64.into()]), Row::new(vec![4.into()])),
                3,
                1,
            ),
        ];
        {
            let mut arr = arr.write();
            arr.apply_updates(now, updates.clone()).unwrap();
            // repeat the same updates means having multiple updates for the same key
            arr.apply_updates(now, updates).unwrap();
            assert_eq!(
                arr.get_updates_in_range(1..=1),
                vec![
                    ((key.clone(), Row::new(vec![2.into()])), 1, 1),
                    ((key.clone(), Row::new(vec![2.into()])), 1, 1)
                ]
            );
            assert_eq!(arr.spine.len(), 3);
            arr.set_compaction(1).unwrap();
            assert_eq!(arr.spine.len(), 3);
        }
        {
            let mut arr = arr.write();
            assert_eq!(arr.spine.len(), 3);
            assert_eq!(arr.get(10, &key), Some((Row::new(vec![2.into()]), 1, 2)));
            arr.trunc_expired(10);
            assert_eq!(arr.spine.len(), 3);
            arr.trunc_expired(11);
            assert_eq!(arr.get(11, &key), None);
            assert_eq!(arr.spine.len(), 3);
            assert_eq!(arr.expire_state.as_ref().unwrap().event_ts_to_key.len(), 2);
            arr.trunc_expired(12);
            assert_eq!(arr.spine.len(), 3);
            assert_eq!(arr.expire_state.as_ref().unwrap().event_ts_to_key.len(), 1);
        }
    }

    #[test]
    fn test_apply_expired_keys() {
        // apply updates with a expired key
        let mut arr = Arrangement::new();
        let expire_state = KeyExpiryManager {
            event_ts_to_key: Default::default(),
            key_expiration_duration: Some(10),
            event_timestamp_from_row: Some(ScalarExpr::Column(0)),
        };
        let expire_state = Some(expire_state);
        arr.expire_state = expire_state;

        let arr = ArrangeHandler::from(arr);

        let updates: Vec<KeyValDiffRow> = vec![
            (
                (Row::new(vec![1i64.into()]), Row::new(vec![2.into()])),
                1,
                1,
            ),
            (
                (Row::new(vec![2i64.into()]), Row::new(vec![3.into()])),
                2,
                1,
            ),
            (
                (Row::new(vec![3i64.into()]), Row::new(vec![4.into()])),
                3,
                1,
            ),
            (
                (Row::new(vec![3i64.into()]), Row::new(vec![4.into()])),
                3,
                1,
            ),
            (
                (Row::new(vec![1i64.into()]), Row::new(vec![42.into()])),
                10,
                1,
            ),
        ];
        {
            let mut arr = arr.write();
            arr.apply_updates(11, updates).unwrap();

            assert_eq!(
                arr.get(11, &Row::new(vec![1i64.into()])),
                Some((Row::new(vec![42.into()]), 10, 1))
            );
            arr.trunc_expired(12);
            assert_eq!(arr.get(12, &Row::new(vec![1i64.into()])), None);
        }
    }
}
