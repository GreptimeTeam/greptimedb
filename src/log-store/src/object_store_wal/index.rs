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

//! Shared WAL object indexing and chain traversal.

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::ops::Range;

use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use snafu::{IntoError, OptionExt, ensure};

use crate::error::{CorruptedWalObjectSnafu, Error, InvalidWalObjectSnafu, Result};
use crate::object_store_wal::format::{
    FixedTrailer, FooterEntry, HEADER_LEN, Header, MIN_OBJECT_LEN, RELEASE_REGION_ID, TRAILER_LEN,
    decode_footer, decode_header, decode_trailer, footer_range, verify_segment_ranges,
};
use crate::object_store_wal::io::{ListedObject, WalObjectRead};

/// Bytes recovery reads from the end of an object in one request. The window
/// holds the trailer and the footer of an object with up to 1364 regions of
/// 48 bytes each, so a second request for the footer is rare, and usually the
/// segment of release records, which is the last segment.
pub(crate) const RECOVERY_TAIL_WINDOW: usize = 64 * 1024;
/// Largest segment of release records that recovery keeps from the bytes it
/// read for a footer. It keeps them before it knows which objects are on the
/// chain, so the bound caps what objects off the chain hold until recovery
/// ends; a larger segment is read again if its object is on the chain. A
/// release seals its record at once, so a segment usually holds the records of
/// one or a few regions, 16 bytes each.
pub(crate) const MAX_KEPT_RELEASE_BYTES: u64 = 4 * 1024;

/// The objects recovery fetched, ordered by sequence, and the listed objects
/// that were gone when their header or footer was fetched, with the error the
/// fetch met. A collection of an earlier instance may complete after the
/// listing.
pub(crate) struct FetchedObjects {
    pub(crate) objects: Vec<FetchedObject>,
    pub(crate) vanished: Vec<(u64, Error)>,
}

/// An object as recovery fetched it: its key, header and footer, and its
/// segment of release records when the reads of the footer returned it.
pub(crate) struct FetchedObject {
    pub(crate) object: ListedObject,
    pub(crate) header: Header,
    pub(crate) footer: Vec<FooterEntry>,
    pub(crate) releases: Option<Bytes>,
}

/// Verified header and region index of an immutable object.
#[derive(Debug)]
pub(crate) struct FetchedFooter {
    pub(crate) header: Header,
    pub(crate) footer: Vec<FooterEntry>,
}

/// Returns, in sequence order, the objects on the chain that ends at the tip:
/// the complete object with the largest epoch, then the largest sequence.
///
/// An object is complete when every link on its chain holds, judged with the
/// reclaim boundary of that object: the chain starts at an object without a
/// predecessor or reaches a predecessor below the boundary, whether or not it
/// is present, and every other link names a present object that carries the
/// epoch the link records. The chain of the tip ends there, so objects below
/// its boundary are not replayed. Only one instance writes
/// under an epoch, so the epoch tells an object of the linking instance from
/// another object under the same sequence. A create that
/// was reported as failed may still leave its object, but every object
/// written after that failure links past it, and every instance writes under
/// an epoch above every object present when it opened, so neither such an
/// object nor a late object of an earlier instance ends the chosen chain.
pub(crate) fn select_chain(headers: &BTreeMap<u64, &Header>) -> Vec<u64> {
    let mut candidates = headers.iter().collect::<Vec<_>>();
    candidates.sort_unstable_by_key(|(object_seq, header)| Reverse((header.epoch, **object_seq)));
    candidates
        .into_iter()
        .find_map(|(object_seq, _)| chain_ending_at(headers, *object_seq))
        .unwrap_or_default()
}

/// Returns, in sequence order, the chain that ends at `tip` if `tip` is
/// complete, see [`select_chain`].
pub(crate) fn chain_ending_at(headers: &BTreeMap<u64, &Header>, tip: u64) -> Option<Vec<u64>> {
    chain_from(headers, tip, headers.get(&tip)?.reclaim_boundary)
}

/// Walks predecessors down to the inclusive lower object boundary.
/// A missing object below the boundary does not belong to this read.
pub(crate) fn chain_from(
    headers: &BTreeMap<u64, &Header>,
    tip: u64,
    boundary: u64,
) -> Option<Vec<u64>> {
    let mut chain = vec![tip];
    let mut header = *headers.get(&tip)?;
    while let Some(link) = header.prev {
        if link.object_seq >= header.object_seq {
            return None;
        }
        if link.object_seq < boundary {
            break;
        }
        header = headers
            .get(&link.object_seq)
            .filter(|prev| prev.epoch == link.epoch)?;
        chain.push(link.object_seq);
    }
    chain.reverse();
    Some(chain)
}

/// Fetches and verifies the headers and footers of `objects` as they are
/// listed, up to `concurrency` objects at a time, and returns them ordered by
/// object sequence whatever the order the fetches complete in. An object the
/// object store no longer has is returned as vanished; a failed listing or
/// any other failure abandons the remaining fetches.
pub(crate) async fn fetch_footers(
    io: &dyn WalObjectRead,
    objects: impl Stream<Item = Result<ListedObject>>,
    concurrency: usize,
) -> Result<FetchedObjects> {
    let outcomes = objects
        .map_ok(|object| async move {
            let object_seq = object.object_seq;
            match fetch_footer_with_releases(io, object).await {
                Ok(fetched) => Ok(Ok(fetched)),
                Err(error) if is_not_found(&error) => Ok(Err((object_seq, error))),
                Err(error) => Err(error),
            }
        })
        .try_buffer_unordered(concurrency)
        .try_collect::<Vec<_>>()
        .await?;
    let mut objects = Vec::with_capacity(outcomes.len());
    let mut vanished = Vec::new();
    for outcome in outcomes {
        match outcome {
            Ok(object) => objects.push(object),
            Err(gone) => vanished.push(gone),
        }
    }
    objects.sort_unstable_by_key(|fetched| fetched.object.object_seq);
    Ok(FetchedObjects { objects, vanished })
}

pub(crate) fn is_not_found(error: &Error) -> bool {
    matches!(error, Error::WalObjectStore { error, .. } if error.kind() == object_store::ErrorKind::NotFound)
}

/// Reads and verifies the header and footer without copying release records.
pub(crate) async fn fetch_footer(
    io: &dyn WalObjectRead,
    object: &ListedObject,
) -> Result<FetchedFooter> {
    Ok(fetch_footer_window(io, object).await?.index)
}

/// The verified index and the download window, retained only during fetching.
struct FooterWindow {
    index: FetchedFooter,
    tail: Bytes,
    tail_start: usize,
}

/// Retains a small release segment for startup recovery without another GET.
async fn fetch_footer_with_releases(
    io: &dyn WalObjectRead,
    object: ListedObject,
) -> Result<FetchedObject> {
    let FooterWindow {
        index,
        tail,
        tail_start,
    } = fetch_footer_window(io, &object).await?;
    // Copy only the release bytes so recovery does not retain the whole window.
    let releases = index
        .footer
        .iter()
        .find(|entry| entry.region_id == RELEASE_REGION_ID)
        .filter(|entry| {
            entry.segment_offset >= tail_start as u64 && entry.segment_len <= MAX_KEPT_RELEASE_BYTES
        })
        .map(|entry| {
            let start = entry.segment_offset as usize - tail_start;
            Bytes::copy_from_slice(&tail[start..start + entry.segment_len as usize])
        });
    Ok(FetchedObject {
        object,
        header: index.header,
        footer: index.footer,
        releases,
    })
}

/// Verifies object sequence, header, trailer, footer checksum and segment ranges.
/// Small objects are read whole; large ones use concurrent header and tail reads,
/// followed by a separate footer read only when it begins before the tail window.
async fn fetch_footer_window(
    io: &dyn WalObjectRead,
    object: &ListedObject,
) -> Result<FooterWindow> {
    let ListedObject {
        object_seq, size, ..
    } = *object;
    let invalid = |source: Error| {
        InvalidWalObjectSnafu {
            path: object.path.clone(),
        }
        .into_error(source)
    };
    let object_len = usize::try_from(size)
        .ok()
        .filter(|len| *len >= MIN_OBJECT_LEN)
        .with_context(|| CorruptedWalObjectSnafu {
            reason: format!(
                "truncated object, expected at least {MIN_OBJECT_LEN} bytes, actual {size}"
            ),
        })
        .map_err(invalid)?;

    let window = object_len.min(RECOVERY_TAIL_WINDOW);
    let tail_start = object_len - window;
    let (head, tail) = if tail_start == 0 {
        let bytes = io.get(object_seq).await?;
        (bytes.clone(), bytes)
    } else {
        futures::try_join!(
            io.get_range(object_seq, 0, HEADER_LEN as u64),
            io.get_range(object_seq, tail_start as u64, window as u64)
        )?
    };
    if head.len() < HEADER_LEN || tail.len() != window {
        return Err(invalid(
            CorruptedWalObjectSnafu {
                reason: format!(
                    "object holds fewer bytes than the listed {size}, head {} bytes, tail {} bytes",
                    head.len(),
                    tail.len()
                ),
            }
            .build(),
        ));
    }

    let (header, trailer, footer_range) =
        locate_footer(object_seq, object_len, &head, &tail).map_err(invalid)?;
    let footer = if footer_range.start >= tail_start {
        tail.slice(footer_range.start - tail_start..footer_range.end - tail_start)
    } else {
        io.get_range(
            object_seq,
            footer_range.start as u64,
            footer_range.len() as u64,
        )
        .await?
    };
    let footer = decode_footer(&footer, trailer).map_err(invalid)?;
    verify_segment_ranges(&footer, footer_range.start).map_err(invalid)?;
    Ok(FooterWindow {
        index: FetchedFooter { header, footer },
        tail,
        tail_start,
    })
}

/// Verifies the header and trailer of the object `object_seq` of `object_len`
/// bytes from its first bytes `head` and its last bytes `tail`, and returns
/// the header and the trailer with the range the footer occupies in the object.
fn locate_footer(
    object_seq: u64,
    object_len: usize,
    head: &[u8],
    tail: &[u8],
) -> Result<(Header, FixedTrailer, Range<usize>)> {
    let header = decode_header(head)?;
    ensure!(
        header.object_seq == object_seq,
        CorruptedWalObjectSnafu {
            reason: format!(
                "header sequence {} does not match key sequence {object_seq}",
                header.object_seq
            ),
        }
    );
    let trailer = decode_trailer(&tail[tail.len() - TRAILER_LEN..])?;
    let footer_range = footer_range(trailer, object_len)?;
    Ok((header, trailer, footer_range))
}
