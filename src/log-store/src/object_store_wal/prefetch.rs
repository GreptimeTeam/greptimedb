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

//! Segment fetching shared by local reads and remote recovery.

use std::collections::VecDeque;
use std::sync::Arc;

use futures::stream::BoxStream;
use snafu::ResultExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::task::AbortOnDropHandle;

use crate::error::{InvalidWalObjectSnafu, Result, WalSegmentFetchSnafu};
use crate::object_store_wal::format::{FooterEntry, Record, decode_segment};
use crate::object_store_wal::io::WalObjectRead;

/// Number of segments a read fetches ahead of the one whose entries it
/// yields. On S3 a replay that fetched each segment after the previous one
/// spent about 30 ms per object; with eight fetches in flight a segment
/// arrives about every 4 ms. The segments fetched ahead also take their bytes
/// from [`READ_PREFETCH_BYTES`].
pub(crate) const READ_PREFETCH_DEPTH: usize = 8;
/// Bytes of decoded segments that all reads of one store, or of one remote
/// source prefix, together fetch ahead. A segment counts as its length, which
/// bounds its payloads, plus a [`Record`]
/// per entry. A read always fetches the segment it yields next, as a read that
/// fetched one segment at a time did, and a later segment only while it fits
/// in what is left, so regions replayed in parallel hold one segment each, up
/// to about `max_batch_bytes` or more for one oversized append, plus at most
/// these bytes, however many regions open at once. While a fetch copies the
/// payloads out of the downloaded segment it briefly holds the segment's
/// length once more.
pub(crate) const READ_PREFETCH_BYTES: usize = 64 * 1024 * 1024;
/// Fetches and decodes the segment `entry` describes in the object
/// `object_seq`. A segment that does not decode is fetched once more, since
/// the download rather than the object may be damaged; when it still does not
/// decode the error is returned as [`crate::error::Error::InvalidWalObject`]. A failed fetch
/// is returned as is.
async fn fetch_segment(
    io: &dyn WalObjectRead,
    object_seq: u64,
    entry: &FooterEntry,
) -> Result<Vec<Record>> {
    let fetch = || io.get_range(object_seq, entry.segment_offset, entry.segment_len);
    if let Ok(records) = decode_segment(&fetch().await?, entry) {
        return Ok(records);
    }
    decode_segment(&fetch().await?, entry).with_context(|_| InvalidWalObjectSnafu {
        path: io.object_path(object_seq),
    })
}

/// Takes from `budget` the bytes the segment `entry` describes holds once
/// decoded, if they are free: its length plus a [`Record`] per entry.
fn try_reserve_prefetch_bytes(
    budget: &Arc<Semaphore>,
    entry: &FooterEntry,
) -> Option<OwnedSemaphorePermit> {
    let records = u64::from(entry.entry_count) * std::mem::size_of::<Record>() as u64;
    let bytes = u32::try_from(entry.segment_len.checked_add(records)?).ok()?;
    budget.clone().try_acquire_many_owned(bytes).ok()
}

/// Runs [`fetch_segment`] on a task of its own, so the fetch proceeds while
/// the read is busy with earlier segments, and returns the outcome with
/// `permit`. The task holds the permit, so the reserved bytes are released
/// only with the buffers of the fetch. Dropping the returned future, as a
/// dropped read does, aborts the task; a task that panics fails the fetch with
/// [`crate::error::Error::WalSegmentFetch`].
fn spawn_fetch_segment(
    io: Arc<dyn WalObjectRead>,
    object_seq: u64,
    entry: FooterEntry,
    permit: Option<OwnedSemaphorePermit>,
) -> impl Future<Output = (Result<Vec<Record>>, Option<OwnedSemaphorePermit>)> + Send + 'static {
    let task_io = io.clone();
    let task = AbortOnDropHandle::new(common_runtime::spawn_global(async move {
        let fetched = fetch_segment(task_io.as_ref(), object_seq, &entry).await;
        (fetched, permit)
    }));
    async move {
        match task.await {
            Ok(fetched) => fetched,
            Err(error) => (
                Err(error).context(WalSegmentFetchSnafu {
                    path: io.object_path(object_seq),
                }),
                None,
            ),
        }
    }
}

/// Fetches segments in order, retaining prefetch permits until the consumer
/// advances. Errors are yielded for the caller to apply its own recovery policy.
pub(crate) fn segments(
    io: Arc<dyn WalObjectRead>,
    objects: Vec<(u64, FooterEntry)>,
    budget: Arc<Semaphore>,
) -> BoxStream<'static, (u64, FooterEntry, Result<Vec<Record>>)> {
    Box::pin(async_stream::stream! {
        let mut objects = objects.into_iter().peekable();
        let mut fetches = VecDeque::new();
        loop {
            while fetches.len() < READ_PREFETCH_DEPTH {
                let Some((_, entry)) = objects.peek() else { break; };
                let permit = if fetches.is_empty() { None } else {
                    match try_reserve_prefetch_bytes(&budget, entry) {
                        Some(permit) => Some(permit),
                        None => break,
                    }
                };
                let Some((seq, entry)) = objects.next() else { break; };
                let fetch = spawn_fetch_segment(io.clone(), seq, entry.clone(), permit);
                fetches.push_back((seq, entry, fetch));
            }
            let Some((seq, entry, fetch)) = fetches.pop_front() else { break; };
            let (fetched, _permit) = fetch.await;
            yield (seq, entry, fetched);
        }
    })
}
