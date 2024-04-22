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

//! utilities for managing state of dataflow execution

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::sync::Arc;

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use smallvec::{smallvec, SmallVec};
use tokio::sync::{Mutex, RwLock};

use crate::expr::error::InternalSnafu;
use crate::expr::{EvalError, ScalarExpr};
use crate::repr::{value_to_internal_ts, Diff, DiffRow, Duration, KeyValDiffRow, Row, Timestamp};

/// A batch of updates, arranged by key
pub type Batch = BTreeMap<Row, SmallVec<[DiffRow; 2]>>;

/// A spine of batches, arranged by timestamp
pub type Spine = BTreeMap<Timestamp, Batch>;

/// Determine when should a key expire according to it's event timestamp in key,
/// if a key is expired, any future updates to it should be ignored
/// Note that key is expired by it's event timestamp(contained in the key), not by the time it's inserted(system timestamp)
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
    /// TODO: batch size balancing?
    spine: Spine,
    /// if set to false, will not update current value of the arrangement, useful for case like `map -> arrange -> reduce`
    ///
    /// in which `arrange` operator only need future updates, and don't need to keep all updates
    full_arrangement: bool,
    /// flag to mark that this arrangement haven't been written to, so that it can be cloned and shared
    is_written: bool,
    /// manage the expire state of the arrangement
    expire_state: Option<KeyExpiryManager>,
    /// the time that the last compaction happened, also know as current time
    last_compaction_time: Option<Timestamp>,
    name: Vec<String>,
}

impl Arrangement {
    /// create a new empty arrangement
    pub fn new() -> Self {
        Self {
            spine: Default::default(),
            full_arrangement: false,
            is_written: false,
            expire_state: None,
            last_compaction_time: None,
            name: vec![],
        }
    }

    pub fn new_with_name(name: Vec<String>) -> Self {
        Self {
            spine: Default::default(),
            full_arrangement: false,
            is_written: false,
            expire_state: None,
            last_compaction_time: None,
            name,
        }
    }

    /// apply updates into spine, with no respect of whether the updates are in futures, past, or now
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
                // a stable sort make updates sort in order of insertion
                // without changing the order of updates within same tick
                key_updates.sort_by_key(|r| r.1);
            }
        }
        Ok(max_late_by)
    }

    /// find out the time of next update in the future
    /// that is the next update with `timestamp > now`
    pub fn get_next_update_time(&self, now: &Timestamp) -> Option<Timestamp> {
        // iter over batches that only have updates of `timestamp>now` and find the first non empty batch, then get the minimum timestamp in that batch
        let next_batches = self.spine.range((Bound::Excluded(now), Bound::Unbounded));
        for (_ts, batch) in next_batches {
            let min_ts = batch
                .iter()
                .flat_map(|(_k, v)| v.iter().map(|(_, ts, _)| *ts))
                .min();
            if let Some(min_ts) = min_ts {
                return Some(min_ts);
            } else {
                continue;
            }
        }
        // all batches are empty, return now
        None
    }

    /// get the last compaction time
    pub fn last_compaction_time(&self) -> Option<Timestamp> {
        self.last_compaction_time
    }

    /// split spine off at `now`, and return the spine that's before `now`(including `now`)
    fn split_lte(&mut self, now: &Timestamp) -> Spine {
        let mut before = self.spine.split_off(&(now + 1));
        std::mem::swap(&mut before, &mut self.spine);

        // if before's last key == now, then all the keys we needed are found
        if before
            .last_key_value()
            .map(|(k, _v)| *k == *now)
            .unwrap_or(false)
        {
            return before;
        }

        // also need to move all keys from the first batch in spine with timestamp<=now to before
        // we know that all remaining keys to be split off are last key < key <= now, we will make them into a new batch
        if let Some(mut first_batch) = self.spine.first_entry() {
            let mut new_batch: Batch = Default::default();
            // remove all keys with val of empty vec
            first_batch.get_mut().retain(|key, updates| {
                // remove keys <= now from updates
                updates.retain(|(val, ts, diff)| {
                    if *ts <= *now {
                        new_batch.entry(key.clone()).or_insert(smallvec![]).push((
                            val.clone(),
                            *ts,
                            *diff,
                        ));
                    }
                    *ts > *now
                });
                !updates.is_empty()
            });

            before.entry(*now).or_default().extend(new_batch);
        }
        before
    }

    /// advance time to `now` and consolidate all older(`now` included) updates to the first key
    ///
    /// return the maximum expire time(already expire by how much time) of all updates if any keys is already expired
    pub fn compaction_to(&mut self, now: Timestamp) -> Result<Option<Duration>, EvalError> {
        let mut max_late_by: Option<Duration> = None;

        let should_compact = self.split_lte(&now);

        self.last_compaction_time = Some(now);
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
    pub fn get_updates_in_range<R: std::ops::RangeBounds<Timestamp> + Clone>(
        &self,
        range: R,
    ) -> Vec<KeyValDiffRow> {
        let mut result = vec![];
        // three part:
        // 1.the starting batch with first key >= range.start, which may contain updates that not in range
        // 2. the batches with key in range
        // 3. the last batch with first key > range.end, which may contain updates that are in range
        let mut is_first = true;
        for (_ts, batch) in self.spine.range(range.clone()) {
            if is_first {
                for (key, updates) in batch {
                    let iter = updates
                        .iter()
                        .filter(|(_val, ts, _diff)| range.contains(ts))
                        .map(|(val, ts, diff)| ((key.clone(), val.clone()), *ts, *diff));
                    result.extend(iter);
                }
                is_first = false;
            } else {
                for (key, updates) in batch.clone() {
                    result.extend(
                        updates
                            .iter()
                            .map(|(val, ts, diff)| ((key.clone(), val.clone()), *ts, *diff)),
                    );
                }
            }
        }

        // deal with boundary include start and end
        // and for the next batch with upper_bound >= range.end
        // we need to search for updates within range
        let neg_bound = match range.end_bound() {
            Bound::Included(b) => {
                // if boundary is aligned, the last batch in range actually cover the full range
                // then there will be no further keys we need in the next batch
                if self.spine.contains_key(b) {
                    return result;
                }
                Bound::Excluded(*b)
            }
            Bound::Excluded(b) => Bound::Included(*b),
            Bound::Unbounded => return result,
        };
        let search_range = (neg_bound, Bound::Unbounded);
        if let Some(last_batch) = self.spine.range(search_range).next() {
            for (key, updates) in last_batch.1 {
                let iter = updates
                    .iter()
                    .filter(|(_val, ts, _diff)| range.contains(ts))
                    .map(|(val, ts, diff)| ((key.clone(), val.clone()), *ts, *diff));
                result.extend(iter);
            }
        };
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
        if self.full_arrangement
            && self
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

            let with_extra_batch = {
                let unaligned = self.spine.range(..=now);
                if unaligned
                    .clone()
                    .last()
                    .map(|(ts, _)| *ts == now)
                    .unwrap_or(false)
                {
                    // this extra chain is there just to make type the same
                    unaligned.chain(None)
                } else {
                    // if the last key is not equal to now, then we need to include the next batch
                    // because we know last batch key < now < next batch key
                    // therefore next batch may contain updates that we want
                    unaligned.chain(
                        self.spine
                            .range((Bound::Excluded(now), Bound::Unbounded))
                            .next(),
                    )
                }
            };
            for (ts, batch) in with_extra_batch {
                if let Some(new_rows) = batch.get(key).map(|v| v.iter()) {
                    if *ts <= now {
                        for new_row in new_rows {
                            final_val = compact_diff_row(final_val, new_row);
                        }
                    } else {
                        for new_row in new_rows.filter(|new_row| new_row.1 <= now) {
                            final_val = compact_diff_row(final_val, new_row);
                        }
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

/// Simply a type alias for ReadGuard of Arrangement
pub type ArrangeReader<'a> = tokio::sync::RwLockReadGuard<'a, Arrangement>;
/// Simply a type alias for WriteGuard of Arrangement
pub type ArrangeWriter<'a> = tokio::sync::RwLockWriteGuard<'a, Arrangement>;

/// A handler to the inner Arrangement, can be cloned and shared, useful for query it's inner state
#[derive(Debug)]
pub struct ArrangeHandler {
    inner: Arc<RwLock<Arrangement>>,
}
impl ArrangeHandler {
    /// create a new handler from arrangement
    pub fn from(arr: Arrangement) -> Self {
        Self {
            inner: Arc::new(RwLock::new(arr)),
        }
    }

    /// write lock the arrangement
    pub fn write(&self) -> tokio::sync::RwLockWriteGuard<'_, Arrangement> {
        self.inner.blocking_write()
    }

    /// read lock the arrangement
    pub fn read(&self) -> tokio::sync::RwLockReadGuard<'_, Arrangement> {
        self.inner.blocking_read()
    }

    /// clone the handler, but only keep the future updates
    ///
    /// it's a cheap operation, since it's `Arc-ed` and only clone the `Arc`
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
    ///
    /// it's a cheap operation, since it's `Arc-ed` and only clone the `Arc`
    pub fn clone_full_arrange(&self) -> Option<Self> {
        {
            let zelf = self.read();
            if !zelf.full_arrangement && zelf.is_written {
                return None;
            }
        }

        self.write().full_arrangement = true;
        Some(Self {
            inner: self.inner.clone(),
        })
    }

    pub fn set_full_arrangement(&self, full: bool) {
        self.write().full_arrangement = full;
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
            arr.compaction_to(1).unwrap();
            assert_eq!(arr.spine.len(), 3);
        }

        let arr2 = arr.clone_full_arrange();
        assert!(arr2.is_some());
        {
            let mut arr = arr.write();
            assert_eq!(arr.spine.len(), 3);
            arr.compaction_to(2).unwrap();
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
            arr.compaction_to(1).unwrap();
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

    /// test if split_lte get ranges that are not aligned with batch boundaries
    /// this split_lte can correctly retrieve all updates in the range, including updates that are in the batches
    /// near the boundary of input range
    #[test]
    fn test_split_off() {
        let mut arr = Arrangement::new();
        // manually create batch ..=1 and 2..=3
        arr.spine.insert(1, Default::default());
        arr.spine.insert(3, Default::default());
        arr.apply_updates(
            2,
            vec![((Row::new(vec![1.into()]), Row::new(vec![2.into()])), 2, 1)],
        )
        .unwrap();
        // updates falls into the range of 2..=3
        let mut arr1 = arr.clone();
        {
            assert_eq!(arr.get_next_update_time(&1), Some(2));
            // split expect to take batch ..=1 and create a new batch 2..=2(which contain update)
            let split = &arr.split_lte(&2);
            assert_eq!(split.len(), 2);
            assert_eq!(split[&2].len(), 1);
            let _ = &arr.split_lte(&3);
            assert_eq!(arr.get_next_update_time(&1), None);
        }
        {
            // take all updates with timestamp <=1, will get no updates
            let split = &arr1.split_lte(&1);
            assert_eq!(split.len(), 1);
        }
    }

    /// test if get ranges is not aligned with boundary of batch,
    /// whether can get correct result
    #[test]
    fn test_get_by_range() {
        let mut arr = Arrangement::new();

        // will form {2: [2, 1], 4: [4,3], 6: [6,5]} three batch
        // TODO(discord9): manually set batch
        let updates: Vec<KeyValDiffRow> = vec![
            ((Row::new(vec![1i64.into()]), Row::empty()), 2, 1),
            ((Row::new(vec![1i64.into()]), Row::empty()), 1, 1),
            ((Row::new(vec![2i64.into()]), Row::empty()), 4, 1),
            ((Row::new(vec![3i64.into()]), Row::empty()), 3, 1),
            ((Row::new(vec![3i64.into()]), Row::empty()), 6, 1),
            ((Row::new(vec![1i64.into()]), Row::empty()), 5, 1),
        ];
        arr.apply_updates(0, updates).unwrap();
        assert_eq!(
            arr.get_updates_in_range(2..=5),
            vec![
                ((Row::new(vec![1i64.into()]), Row::empty()), 2, 1),
                ((Row::new(vec![2i64.into()]), Row::empty()), 4, 1),
                ((Row::new(vec![3i64.into()]), Row::empty()), 3, 1),
                ((Row::new(vec![1i64.into()]), Row::empty()), 5, 1),
            ]
        );
    }

    /// test if get with range unaligned with batch boundary
    ///  can get correct result
    #[test]
    fn test_get_unaligned() {
        let mut arr = Arrangement::new();

        // will form {2: [2, 1], 4: [4,3], 6: [6,5]} three batch
        // TODO(discord9): manually set batch
        let key = Row::new(vec![1i64.into()]);
        let updates: Vec<KeyValDiffRow> = vec![
            ((key.clone(), Row::new(vec![1i64.into()])), 2, 1),
            ((key.clone(), Row::new(vec![2i64.into()])), 1, 1),
            ((key.clone(), Row::new(vec![3i64.into()])), 4, 1),
            ((key.clone(), Row::new(vec![4i64.into()])), 3, 1),
            ((key.clone(), Row::new(vec![5i64.into()])), 6, 1),
            ((key.clone(), Row::new(vec![6i64.into()])), 5, 1),
        ];
        arr.apply_updates(0, updates).unwrap();
        // aligned with batch boundary
        assert_eq!(arr.get(2, &key), Some((Row::new(vec![1i64.into()]), 2, 1)));
        // unaligned with batch boundary
        assert_eq!(arr.get(3, &key), Some((Row::new(vec![4i64.into()]), 3, 1)));
    }

    /// test if out of order updates can be sorted correctly
    #[test]
    fn test_out_of_order_apply_updates() {
        let mut arr = Arrangement::new();

        let key = Row::new(vec![1i64.into()]);
        let updates: Vec<KeyValDiffRow> = vec![
            ((key.clone(), Row::new(vec![5i64.into()])), 6, 1),
            ((key.clone(), Row::new(vec![2i64.into()])), 2, -1),
            ((key.clone(), Row::new(vec![1i64.into()])), 2, 1),
            ((key.clone(), Row::new(vec![2i64.into()])), 1, 1),
            ((key.clone(), Row::new(vec![3i64.into()])), 4, 1),
            ((key.clone(), Row::new(vec![4i64.into()])), 3, 1),
            ((key.clone(), Row::new(vec![6i64.into()])), 5, 1),
        ];
        arr.apply_updates(0, updates.clone()).unwrap();
        let sorted = updates.iter().sorted_by_key(|r| r.1).cloned().collect_vec();
        assert_eq!(arr.get_updates_in_range(1..7), sorted);
    }
}
