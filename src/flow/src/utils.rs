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

use common_telemetry::trace;
use smallvec::{smallvec, SmallVec};
use tokio::sync::RwLock;

use crate::expr::{EvalError, ScalarExpr};
use crate::repr::{value_to_internal_ts, DiffRow, Duration, KeyValDiffRow, Row, Timestamp};

/// A batch of updates, arranged by key
pub type Batch = BTreeMap<Row, SmallVec<[DiffRow; 2]>>;

/// A spine of batches, arranged by timestamp
/// TODO(discord9): consider internally index by key, value, and timestamp for faster lookup
pub type Spine = BTreeMap<Timestamp, Batch>;

/// Determine when should a key expire according to it's event timestamp in key.
///
/// If a key is expired, any future updates to it should be ignored.
///
/// Note that key is expired by it's event timestamp (contained in the key), not by the time it's inserted (system timestamp).
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct KeyExpiryManager {
    /// A map from event timestamp to key, used for expire keys.
    event_ts_to_key: BTreeMap<Timestamp, BTreeSet<Row>>,

    /// Duration after which a key is considered expired, and will be removed from state
    key_expiration_duration: Option<Duration>,

    /// Expression to get timestamp from key row
    event_timestamp_from_row: Option<ScalarExpr>,
}

impl KeyExpiryManager {
    pub fn new(
        key_expiration_duration: Option<Duration>,
        event_timestamp_from_row: Option<ScalarExpr>,
    ) -> Self {
        Self {
            event_ts_to_key: Default::default(),
            key_expiration_duration,
            event_timestamp_from_row,
        }
    }

    /// Extract event timestamp from key row.
    ///
    /// If no expire state is set, return None.
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

    /// Return timestamp that should be expired by the time `now` by compute `now - expiration_duration`
    pub fn compute_expiration_timestamp(&self, now: Timestamp) -> Option<Timestamp> {
        self.key_expiration_duration.map(|d| now - d)
    }

    /// Update the event timestamp to key mapping.
    ///
    /// - If given key is expired by now (that is less than `now - expiry_duration`), return the amount of time it's expired.
    /// - If it's not expired, return None
    pub fn get_expire_duration_and_update_event_ts(
        &mut self,
        now: Timestamp,
        row: &Row,
    ) -> Result<Option<Duration>, EvalError> {
        let Some(event_ts) = self.extract_event_ts(row)? else {
            return Ok(None);
        };

        self.event_ts_to_key
            .entry(event_ts)
            .or_default()
            .insert(row.clone());

        if let Some(expire_time) = self.compute_expiration_timestamp(now) {
            if expire_time > event_ts {
                // return how much time it's expired
                return Ok(Some(expire_time - event_ts));
            }
        }

        Ok(None)
    }

    /// Get the expire duration of a key, if it's expired by now.
    ///
    /// Return None if the key is not expired
    pub fn get_expire_duration(
        &self,
        now: Timestamp,
        row: &Row,
    ) -> Result<Option<Duration>, EvalError> {
        let Some(event_ts) = self.extract_event_ts(row)? else {
            return Ok(None);
        };

        if let Some(expire_time) = self.compute_expiration_timestamp(now) {
            if expire_time > event_ts {
                // return how much time it's expired
                return Ok(Some(expire_time - event_ts));
            }
        }

        Ok(None)
    }

    /// Remove expired keys from the state, and return an iterator of removed keys with
    /// event_ts less than expire time (i.e. now - key_expiration_duration).
    pub fn remove_expired_keys(&mut self, now: Timestamp) -> Option<impl Iterator<Item = Row>> {
        let expire_time = self.compute_expiration_timestamp(now)?;

        let mut before = self.event_ts_to_key.split_off(&expire_time);
        std::mem::swap(&mut before, &mut self.event_ts_to_key);

        Some(before.into_iter().flat_map(|(_ts, keys)| keys.into_iter()))
    }
}

/// A shared state of key-value pair for various state in dataflow execution.
///
/// i.e: Mfp operator with temporal filter need to store it's future output so that it can add now, and delete later.
/// To get all needed updates in a time span, use [`get_updates_in_range`].
///
/// And reduce operator need full state of it's output, so that it can query (and modify by calling [`apply_updates`])
/// existing state, also need a way to expire keys. To get a key's current value, use [`get`] with time being `now`
/// so it's like:
/// `mfp operator -> arrange(store futures only, no expire) -> reduce operator <-> arrange(full, with key expiring time) -> output`
///
/// Note the two way arrow between reduce operator and arrange, it's because reduce operator need to query existing state
/// and also need to update existing state.
#[derive(Debug, Clone, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct Arrangement {
    /// A name or identifier for the arrangement which can be used for debugging or logging purposes.
    /// This field is not critical to the functionality but aids in monitoring and management of arrangements.
    name: Vec<String>,

    /// Manages a collection of pending updates in a `BTreeMap` where each key is a timestamp and each value is a `Batch` of updates.
    /// Updates are grouped into batched based on their timestamps.
    /// Each batch covers a range of time from the last key (exclusive) to the current key (inclusive).
    ///
    /// - Updates with a timestamp (`update_ts`) that falls between two keys are placed in the batch of the higher key.
    ///   For example, if the keys are `1, 5, 7, 9` and `update_ts` is `6`, the update goes into the batch with key `7`.
    /// - Updates with a timestamp before the first key are categorized under the first key.
    /// - Updates with a timestamp greater than the highest key result in a new batch being created with that timestamp as the key.
    ///
    /// The first key represents the current state and includes consolidated updates from the past. It is always set to `now`.
    /// Each key should have only one update per batch with a `diff=1` for the batch representing the current time (`now`).
    ///
    /// Since updates typically occur as a delete followed by an insert, a small vector of size 2 is used to store updates for efficiency.
    ///
    /// TODO(discord9): Consider balancing the batch size?
    spine: Spine,

    /// Indicates whether the arrangement maintains a complete history of updates.
    /// - `true`: Maintains all past and future updates, necessary for full state reconstruction at any point in time.
    /// - `false`: Only future updates are retained, optimizing for scenarios where past state is irrelevant and conserving resources.
    ///            Useful for case like `map -> arrange -> reduce`.
    full_arrangement: bool,

    /// Indicates whether the arrangement has been modified since its creation.
    /// - `true`: The arrangement has been written to, meaning it has received updates.
    ///           Cloning this arrangement is generally unsafe as it may lead to inconsistencies if the clone is modified independently.
    ///           However, cloning is safe when both the original and the clone require a full arrangement, as this ensures consistency.
    /// - `false`: The arrangement is in its initial state and has not been modified. It can be safely cloned and shared
    ///            without concerns of carrying over unintended state changes.
    is_written: bool,

    /// Manage the expire state of the arrangement.
    expire_state: Option<KeyExpiryManager>,

    /// The time that the last compaction happened, also known as the current time.
    last_compaction_time: Option<Timestamp>,
}

impl Arrangement {
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

    pub fn get_expire_state(&self) -> Option<&KeyExpiryManager> {
        self.expire_state.as_ref()
    }

    pub fn set_expire_state(&mut self, expire_state: KeyExpiryManager) {
        self.expire_state = Some(expire_state);
    }

    /// Apply updates into spine, with no respect of whether the updates are in futures, past, or now.
    ///
    /// Return the maximum expire time (already expire by how much time) of all updates if any keys is already expired.
    pub fn apply_updates(
        &mut self,
        now: Timestamp,
        updates: Vec<KeyValDiffRow>,
    ) -> Result<Option<Duration>, EvalError> {
        self.is_written = true;

        let mut max_expired_by: Option<Duration> = None;

        for ((key, val), update_ts, diff) in updates {
            // check if the key is expired
            if let Some(s) = &mut self.expire_state {
                if let Some(expired_by) = s.get_expire_duration_and_update_event_ts(now, &key)? {
                    max_expired_by = max_expired_by.max(Some(expired_by));
                    trace!(
                        "Expired key: {:?}, expired by: {:?} with time being now={}",
                        key,
                        expired_by,
                        now
                    );
                    continue;
                }
            }

            // If the `highest_ts` is less than `update_ts`, we need to create a new batch with key being `update_ts`.
            if self
                .spine
                .last_key_value()
                .map(|(highest_ts, _)| *highest_ts < update_ts)
                .unwrap_or(true)
            {
                self.spine.insert(update_ts, Default::default());
            }

            // Get the first batch with key that's greater or equal to `update_ts`.
            let (_, batch) = self
                .spine
                .range_mut(update_ts..)
                .next()
                .expect("Previous insert should have created the batch");

            let key_updates = batch.entry(key).or_default();
            key_updates.push((val, update_ts, diff));

            // a stable sort make updates sort in order of insertion
            // without changing the order of updates within same tick
            key_updates.sort_by_key(|(_val, ts, _diff)| *ts);
        }
        Ok(max_expired_by)
    }

    /// Find out the time of next update in the future that is the next update with `timestamp > now`.
    pub fn get_next_update_time(&self, now: &Timestamp) -> Option<Timestamp> {
        // iter over batches that only have updates of `timestamp>now` and find the first non empty batch, then get the minimum timestamp in that batch
        for (_ts, batch) in self.spine.range((Bound::Excluded(now), Bound::Unbounded)) {
            let min_ts = batch
                .iter()
                .flat_map(|(_k, v)| v.iter().map(|(_, ts, _)| *ts).min())
                .min();

            if min_ts.is_some() {
                return min_ts;
            }
        }

        None
    }

    /// Get the last compaction time.
    pub fn last_compaction_time(&self) -> Option<Timestamp> {
        self.last_compaction_time
    }

    /// Split spine off at `split_ts`, and return the spine that's before `split_ts` (including `split_ts`).
    fn split_spine_le(&mut self, split_ts: &Timestamp) -> Spine {
        self.split_batch_at(split_ts);
        let mut before = self.spine.split_off(&(split_ts + 1));
        std::mem::swap(&mut before, &mut self.spine);
        before
    }

    /// Split the batch at `split_ts` into two parts.
    fn split_batch_at(&mut self, split_ts: &Timestamp) {
        // FAST PATH:
        //
        // The `split_ts` hit the boundary of a batch, nothing to do.
        if self.spine.contains_key(split_ts) {
            return;
        }

        let Some((_, batch_to_split)) = self.spine.range_mut(split_ts..).next() else {
            return; // No batch to split, nothing to do.
        };

        // SLOW PATH:
        //
        // The `split_ts` is in the middle of a batch, we need to split the batch into two parts.
        let mut new_batch = Batch::default();

        batch_to_split.retain(|key, updates| {
            let mut new_updates = SmallVec::default();

            updates.retain(|(val, ts, diff)| {
                if *ts <= *split_ts {
                    // Move the updates that are less than or equal to `split_ts` to the new batch.
                    new_updates.push((val.clone(), *ts, *diff));
                }
                // Keep the updates that are greater than `split_ts` in the current batch.
                *ts > *split_ts
            });

            if !new_updates.is_empty() {
                new_batch.insert(key.clone(), new_updates);
            }

            // Keep the key in the current batch if it still has updates.
            !updates.is_empty()
        });

        if !new_batch.is_empty() {
            self.spine.insert(*split_ts, new_batch);
        }
    }

    /// Advance time to `now` and consolidate all older (`now` included) updates to the first key.
    ///
    /// Return the maximum expire time(already expire by how much time) of all updates if any keys is already expired.
    pub fn compact_to(&mut self, now: Timestamp) -> Result<Option<Duration>, EvalError> {
        let mut max_expired_by: Option<Duration> = None;

        let batches_to_compact = self.split_spine_le(&now);
        self.last_compaction_time = Some(now);

        // If a full arrangement is not needed, we can just discard everything before and including now,
        if !self.full_arrangement {
            return Ok(None);
        }

        // else we update them into current state.
        let mut compacting_batch = Batch::default();

        for (_, batch) in batches_to_compact {
            for (key, updates) in batch {
                // check if the key is expired
                if let Some(s) = &mut self.expire_state {
                    if let Some(expired_by) =
                        s.get_expire_duration_and_update_event_ts(now, &key)?
                    {
                        max_expired_by = max_expired_by.max(Some(expired_by));
                        continue;
                    }
                }

                let mut row = compacting_batch
                    .remove(&key)
                    // only one row in the updates during compaction
                    .and_then(|mut updates| updates.pop());

                for update in updates {
                    row = compact_diff_row(row, &update);
                }
                if let Some(compacted_update) = row {
                    compacting_batch.insert(key, smallvec![compacted_update]);
                }
            }
        }

        // insert the compacted batch into spine with key being `now`
        self.spine.insert(now, compacting_batch);
        Ok(max_expired_by)
    }

    /// Get the updates of the arrangement from the given range of time.
    pub fn get_updates_in_range<R: std::ops::RangeBounds<Timestamp> + Clone>(
        &self,
        range: R,
    ) -> Vec<KeyValDiffRow> {
        // Include the next batch in case the range is not aligned with the boundary of a batch.
        let batches = match range.end_bound() {
            Bound::Included(t) => self.spine.range(range.clone()).chain(
                self.spine
                    .range((Bound::Excluded(t), Bound::Unbounded))
                    .next(),
            ),
            Bound::Excluded(t) => self.spine.range(range.clone()).chain(
                self.spine
                    .range((Bound::Included(t), Bound::Unbounded))
                    .next(),
            ),
            _ => self.spine.range(range.clone()).chain(None),
        };

        let mut res = vec![];
        for (_, batch) in batches {
            for (key, updates) in batch {
                for (val, ts, diff) in updates {
                    if range.contains(ts) {
                        res.push(((key.clone(), val.clone()), *ts, *diff));
                    }
                }
            }
        }
        res
    }

    /// Expire keys in now that are older than expire_time, intended for reducing memory usage and limit late data arrive
    pub fn truncate_expired_keys(&mut self, now: Timestamp) {
        if let Some(s) = &mut self.expire_state {
            if let Some(expired_keys) = s.remove_expired_keys(now) {
                for key in expired_keys {
                    for (_, batch) in self.spine.iter_mut() {
                        batch.remove(&key);
                    }
                }
            }
        }
    }

    /// Get current state of things.
    ///
    /// Useful for query existing keys (i.e. reduce and join operator need to query existing state)
    pub fn get(&self, now: Timestamp, key: &Row) -> Option<DiffRow> {
        // FAST PATH:
        //
        // If `now <= last_compaction_time`, and it's full arrangement, we can directly return the value
        // from the current state (which should be the first batch in the spine if it exist).
        if let Some(last_compaction_time) = self.last_compaction_time()
            && now <= last_compaction_time
            && self.full_arrangement
        {
            // if the last compaction time's batch is not exist, it means the spine doesn't have it's first batch as current value
            return self
                .spine
                .get(&last_compaction_time)
                .and_then(|batch| batch.get(key))
                .and_then(|updates| updates.first().cloned());
        }

        // SLOW PATH:
        //
        // Accumulate updates from the oldest batch to the batch containing `now`.

        let batches = if self.spine.contains_key(&now) {
            // hit the boundary of a batch
            self.spine.range(..=now).chain(None)
        } else {
            // not hit the boundary of a batch, should include the next batch
            self.spine.range(..=now).chain(
                self.spine
                    .range((Bound::Excluded(now), Bound::Unbounded))
                    .next(),
            )
        };

        let mut final_val = None;
        for (ts, batch) in batches {
            if let Some(updates) = batch.get(key) {
                if *ts <= now {
                    for update in updates {
                        final_val = compact_diff_row(final_val, update);
                    }
                } else {
                    for update in updates.iter().filter(|(_, ts, _)| *ts <= now) {
                        final_val = compact_diff_row(final_val, update);
                    }
                }
            }
        }
        final_val
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
#[derive(Debug, Clone)]
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
    pub fn write(&self) -> ArrangeWriter<'_> {
        self.inner.blocking_write()
    }

    /// read lock the arrangement
    pub fn read(&self) -> ArrangeReader<'_> {
        self.inner.blocking_read()
    }

    /// Clone the handler, but only keep the future updates.
    ///
    /// It's a cheap operation, since it's `Arc-ed` and only clone the `Arc`.
    pub fn clone_future_only(&self) -> Option<Self> {
        if self.read().is_written {
            return None;
        }
        Some(Self {
            inner: self.inner.clone(),
        })
    }

    /// Clone the handler, but keep all updates.
    ///
    /// Prevent illegal clone after the arrange have been written,
    /// because that will cause loss of data before clone.
    ///
    /// It's a cheap operation, since it's `Arc-ed` and only clone the `Arc`.
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

    pub fn is_full_arrangement(&self) -> bool {
        self.read().full_arrangement
    }
}

#[cfg(test)]
mod test {
    use std::borrow::Borrow;

    use datatypes::value::Value;
    use itertools::Itertools;

    use super::*;

    fn lit(v: impl Into<Value>) -> Row {
        Row::new(vec![v.into()])
    }

    fn kv(key: impl Borrow<Row>, value: impl Borrow<Row>) -> (Row, Row) {
        (key.borrow().clone(), value.borrow().clone())
    }

    #[test]
    fn test_future_get() {
        // test if apply only future updates, whether get(future_time) can operate correctly
        let arr = ArrangeHandler::from(Arrangement::default());

        let mut arr = arr.write();

        let key = lit("a");
        let updates: Vec<KeyValDiffRow> = vec![
            (kv(&key, lit("b")), 1 /* ts */, 1 /* diff */),
            (kv(&key, lit("c")), 2 /* ts */, 1 /* diff */),
            (kv(&key, lit("d")), 3 /* ts */, 1 /* diff */),
        ];

        // all updates above are future updates
        arr.apply_updates(0, updates).unwrap();

        assert_eq!(arr.get(1, &key), Some((lit("b"), 1 /* ts */, 1 /* diff */)));
        assert_eq!(arr.get(2, &key), Some((lit("c"), 2 /* ts */, 1 /* diff */)));
        assert_eq!(arr.get(3, &key), Some((lit("d"), 3 /* ts */, 1 /* diff */)));
    }

    #[test]
    fn only_save_future_updates() {
        // mfp operator's temporal filter need to record future updates so that it can delete on time
        // i.e. insert a record now, delete this record 5 minutes later
        // they will only need to keep future updates(if downstream don't need full arrangement that is)
        let arr = ArrangeHandler::from(Arrangement::default());

        {
            let arr1 = arr.clone_full_arrange();
            assert!(arr1.is_some());
            let arr2 = arr.clone_future_only();
            assert!(arr2.is_some());
        }

        {
            let mut arr = arr.write();
            let updates: Vec<KeyValDiffRow> = vec![
                (kv(lit("a"), lit("x")), 1 /* ts */, 1 /* diff */),
                (kv(lit("b"), lit("y")), 2 /* ts */, 1 /* diff */),
                (kv(lit("c"), lit("z")), 3 /* ts */, 1 /* diff */),
            ];
            // all updates above are future updates
            arr.apply_updates(0, updates).unwrap();

            assert_eq!(
                arr.get_updates_in_range(1..=1),
                vec![(kv(lit("a"), lit("x")), 1 /* ts */, 1 /* diff */)]
            );
            assert_eq!(arr.spine.len(), 3);

            arr.compact_to(1).unwrap();
            assert_eq!(arr.spine.len(), 3);

            let key = &lit("a");
            assert_eq!(arr.get(3, key), Some((lit("x"), 1 /* ts */, 1 /* diff */)));
            let key = &lit("b");
            assert_eq!(arr.get(3, key), Some((lit("y"), 2 /* ts */, 1 /* diff */)));
            let key = &lit("c");
            assert_eq!(arr.get(3, key), Some((lit("z"), 3 /* ts */, 1 /* diff */)));
        }

        assert!(arr.clone_future_only().is_none());
        {
            let arr2 = arr.clone_full_arrange().unwrap();
            let mut arr = arr2.write();
            assert_eq!(arr.spine.len(), 3);

            arr.compact_to(2).unwrap();
            assert_eq!(arr.spine.len(), 2);
            let key = &lit("a");
            assert_eq!(arr.get(3, key), Some((lit("x"), 1 /* ts */, 1 /* diff */)));
            let key = &lit("b");
            assert_eq!(arr.get(3, key), Some((lit("y"), 2 /* ts */, 1 /* diff */)));
            let key = &lit("c");
            assert_eq!(arr.get(3, key), Some((lit("z"), 3 /* ts */, 1 /* diff */)));
        }
    }

    #[test]
    fn test_reduce_expire_keys() {
        let mut arr = Arrangement::default();
        let expire_state = KeyExpiryManager {
            event_ts_to_key: Default::default(),
            key_expiration_duration: Some(10),
            event_timestamp_from_row: Some(ScalarExpr::Column(0)),
        };
        arr.expire_state = Some(expire_state);
        arr.full_arrangement = true;

        let arr = ArrangeHandler::from(arr);

        let updates: Vec<KeyValDiffRow> = vec![
            (kv(lit(1i64), lit("x")), 1 /* ts */, 1 /* diff */),
            (kv(lit(2i64), lit("y")), 2 /* ts */, 1 /* diff */),
            (kv(lit(3i64), lit("z")), 3 /* ts */, 1 /* diff */),
        ];
        {
            let mut arr = arr.write();
            arr.apply_updates(0, updates.clone()).unwrap();
            // repeat the same updates means having multiple updates for the same key
            arr.apply_updates(0, updates).unwrap();

            assert_eq!(
                arr.get_updates_in_range(1..=1),
                vec![
                    (kv(lit(1i64), lit("x")), 1 /* ts */, 1 /* diff */),
                    (kv(lit(1i64), lit("x")), 1 /* ts */, 1 /* diff */)
                ]
            );
            assert_eq!(arr.spine.len(), 3);
            arr.compact_to(1).unwrap();
            assert_eq!(arr.spine.len(), 3);
        }

        {
            let mut arr = arr.write();
            assert_eq!(arr.spine.len(), 3);

            arr.truncate_expired_keys(11);
            assert_eq!(arr.spine.len(), 3);
            let key = &lit(1i64);
            assert_eq!(arr.get(11, key), Some((lit("x"), 1 /* ts */, 2 /* diff */)));
            let key = &lit(2i64);
            assert_eq!(arr.get(11, key), Some((lit("y"), 2 /* ts */, 2 /* diff */)));
            let key = &lit(3i64);
            assert_eq!(arr.get(11, key), Some((lit("z"), 3 /* ts */, 2 /* diff */)));

            arr.truncate_expired_keys(12);
            assert_eq!(arr.spine.len(), 3);
            let key = &lit(1i64);
            assert_eq!(arr.get(12, key), None);
            let key = &lit(2i64);
            assert_eq!(arr.get(12, key), Some((lit("y"), 2 /* ts */, 2 /* diff */)));
            let key = &lit(3i64);
            assert_eq!(arr.get(12, key), Some((lit("z"), 3 /* ts */, 2 /* diff */)));
            assert_eq!(arr.expire_state.as_ref().unwrap().event_ts_to_key.len(), 2);

            arr.truncate_expired_keys(13);
            assert_eq!(arr.spine.len(), 3);
            let key = &lit(1i64);
            assert_eq!(arr.get(13, key), None);
            let key = &lit(2i64);
            assert_eq!(arr.get(13, key), None);
            let key = &lit(3i64);
            assert_eq!(arr.get(13, key), Some((lit("z"), 3 /* ts */, 2 /* diff */)));
            assert_eq!(arr.expire_state.as_ref().unwrap().event_ts_to_key.len(), 1);
        }
    }

    #[test]
    fn test_apply_expired_keys() {
        // apply updates with a expired key
        let mut arr = Arrangement::default();
        let expire_state = KeyExpiryManager {
            event_ts_to_key: Default::default(),
            key_expiration_duration: Some(10),
            event_timestamp_from_row: Some(ScalarExpr::Column(0)),
        };
        arr.expire_state = Some(expire_state);

        let arr = ArrangeHandler::from(arr);

        let updates: Vec<KeyValDiffRow> = vec![
            (kv(lit(1i64), lit("x")), 1 /* ts */, 1 /* diff */),
            (kv(lit(2i64), lit("y")), 2 /* ts */, 1 /* diff */),
        ];
        {
            let mut arr = arr.write();
            let expired_by = arr.apply_updates(12, updates).unwrap();
            assert_eq!(expired_by, Some(1));

            let key = &lit(1i64);
            assert_eq!(arr.get(12, key), None);
            let key = &lit(2i64);
            assert_eq!(arr.get(12, key), Some((lit("y"), 2 /* ts */, 1 /* diff */)));
        }
    }

    /// test if split_spine_le get ranges that are not aligned with batch boundaries
    /// this split_spine_le can correctly retrieve all updates in the range, including updates that are in the batches
    /// near the boundary of input range
    #[test]
    fn test_split_off() {
        let mut arr = Arrangement::default();
        // manually create batch ..=1 and 2..=3
        arr.spine.insert(1, Batch::default());
        arr.spine.insert(3, Batch::default());

        let updates = vec![(kv(lit("a"), lit("x")), 2 /* ts */, 1 /* diff */)];
        // updates falls into the range of 2..=3
        arr.apply_updates(2, updates).unwrap();

        let mut arr1 = arr.clone();
        {
            assert_eq!(arr.get_next_update_time(&1), Some(2));
            // split expect to take batch ..=1 and create a new batch 2..=2 (which contains update)
            let split = &arr.split_spine_le(&2);
            assert_eq!(split.len(), 2);
            assert_eq!(split[&2].len(), 1);

            assert_eq!(arr.get_next_update_time(&1), None);
        }

        {
            // take all updates with timestamp <=1, will get no updates
            let split = &arr1.split_spine_le(&1);
            assert_eq!(split.len(), 1);
            assert_eq!(split[&1].len(), 0);
        }
    }

    /// test if get ranges is not aligned with boundary of batch,
    /// whether can get correct result
    #[test]
    fn test_get_by_range() {
        let mut arr = Arrangement::default();

        // will form {2: [2, 1], 4: [4,3], 6: [6,5]} three batch
        // TODO(discord9): manually set batch
        let updates: Vec<KeyValDiffRow> = vec![
            (kv(lit("a"), lit("")), 2 /* ts */, 1 /* diff */),
            (kv(lit("a"), lit("")), 1 /* ts */, 1 /* diff */),
            (kv(lit("b"), lit("")), 4 /* ts */, 1 /* diff */),
            (kv(lit("c"), lit("")), 3 /* ts */, 1 /* diff */),
            (kv(lit("c"), lit("")), 6 /* ts */, 1 /* diff */),
            (kv(lit("a"), lit("")), 5 /* ts */, 1 /* diff */),
        ];
        arr.apply_updates(0, updates).unwrap();
        assert_eq!(
            arr.get_updates_in_range(2..=5),
            vec![
                (kv(lit("a"), lit("")), 2 /* ts */, 1 /* diff */),
                (kv(lit("b"), lit("")), 4 /* ts */, 1 /* diff */),
                (kv(lit("c"), lit("")), 3 /* ts */, 1 /* diff */),
                (kv(lit("a"), lit("")), 5 /* ts */, 1 /* diff */),
            ]
        );
    }

    /// test if get with range unaligned with batch boundary
    /// can get correct result
    #[test]
    fn test_get_unaligned() {
        let mut arr = Arrangement::default();

        // will form {2: [2, 1], 4: [4,3], 6: [6,5]} three batch
        // TODO(discord9): manually set batch
        let key = &lit("a");
        let updates: Vec<KeyValDiffRow> = vec![
            (kv(key, lit(1)), 2 /* ts */, 1 /* diff */),
            (kv(key, lit(2)), 1 /* ts */, 1 /* diff */),
            (kv(key, lit(3)), 4 /* ts */, 1 /* diff */),
            (kv(key, lit(4)), 3 /* ts */, 1 /* diff */),
            (kv(key, lit(5)), 6 /* ts */, 1 /* diff */),
            (kv(key, lit(6)), 5 /* ts */, 1 /* diff */),
        ];
        arr.apply_updates(0, updates).unwrap();
        // aligned with batch boundary
        assert_eq!(arr.get(2, key), Some((lit(1), 2 /* ts */, 1 /* diff */)));
        // unaligned with batch boundary
        assert_eq!(arr.get(3, key), Some((lit(4), 3 /* ts */, 1 /* diff */)));
    }

    /// test if out of order updates can be sorted correctly
    #[test]
    fn test_out_of_order_apply_updates() {
        let mut arr = Arrangement::default();

        let key = &lit("a");
        let updates: Vec<KeyValDiffRow> = vec![
            (kv(key, lit(5)), 6 /* ts */, 1 /* diff */),
            (kv(key, lit(2)), 2 /* ts */, -1 /* diff */),
            (kv(key, lit(1)), 2 /* ts */, 1 /* diff */),
            (kv(key, lit(2)), 1 /* ts */, 1 /* diff */),
            (kv(key, lit(3)), 4 /* ts */, 1 /* diff */),
            (kv(key, lit(4)), 3 /* ts */, 1 /* diff */),
            (kv(key, lit(6)), 5 /* ts */, 1 /* diff */),
        ];
        arr.apply_updates(0, updates.clone()).unwrap();
        let sorted = updates
            .iter()
            .sorted_by_key(|(_, ts, _)| *ts)
            .cloned()
            .collect_vec();
        assert_eq!(arr.get_updates_in_range(1..7), sorted);
    }

    #[test]
    fn test_full_arrangement_get_from_first_entry() {
        let mut arr = Arrangement::default();
        // will form {3: [1, 2, 3]}
        let updates = vec![
            (kv(lit("a"), lit("x")), 3 /* ts */, 1 /* diff */),
            (kv(lit("b"), lit("y")), 1 /* ts */, 1 /* diff */),
            (kv(lit("b"), lit("y")), 2 /* ts */, -1 /* diff */),
        ];
        arr.apply_updates(0, updates).unwrap();
        assert_eq!(arr.get(2, &lit("b")), None /* deleted */);
        arr.full_arrangement = true;
        assert_eq!(arr.get(2, &lit("b")), None /* still deleted */);

        arr.compact_to(1).unwrap();

        assert_eq!(
            arr.get(1, &lit("b")),
            Some((lit("y"), 1, 1)) /* fast path */
        );
    }
}
