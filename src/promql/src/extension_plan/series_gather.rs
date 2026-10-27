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

//! Reads several raw series streams at once without splitting a series.
//!
//! A scan keeps each series in one of its partitions, and the native runtime needs each input's
//! series in one piece. Reading the partitions one after another keeps that, but it also leaves
//! every other partition, and the scan feeding it, waiting on the one being read. This reads all of
//! them and interleaves them only where a series ends.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use common_error::ext::BoxedError;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datatypes::error::InvalidSeriesSnafu;
use datatypes::series::SeriesOrdering;
use datatypes::series::frame::LabelDecodeSpec;
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::samples::SeriesSchema;
use futures::{Stream, StreamExt};
use store_api::region_engine::{RawLabelSource, RawSeriesStream, SendableRawSeriesStream};
use tokio::sync::mpsc;

use crate::native::execute::raw_bytes;

/// Interleaves raw series streams at series boundaries, per source.
///
/// Every stream belongs to a source, and the chunks handed over for one source never interleave
/// two series: while one stream of a source has a series open, a chunk another stream of that
/// source reads is held back until that series closes. Sources do not constrain each other.
///
/// A stream holding a closed series stops being polled until that series is handed over, so what
/// is held back is at most one series per stream. It is charged to `reservation`.
pub(crate) struct SeriesGather {
    partitions: Vec<Partition>,
    /// Per source, the stream whose open series is being handed over.
    open: Vec<Option<usize>>,
    /// Chunks cleared for hand-over, with the bytes each is still charged.
    ready: VecDeque<(usize, RawSeriesChunk, usize)>,
    /// The stream polled first on the next sweep, so every stream is reached even while one of
    /// them stays ready: the scans behind a stream only start once it is polled.
    next: usize,
    reservation: MemoryReservation,
}

struct Partition {
    source: usize,
    /// `None` once the stream ended.
    stream: Option<SendableRawSeriesStream>,
    /// Chunks of this stream's current series, read while another stream of the source was open.
    held: Vec<(RawSeriesChunk, usize)>,
    /// Whether `held` ends with the chunk closing its series.
    held_closed: bool,
}

impl SeriesGather {
    pub(crate) fn new(
        streams: Vec<(usize, SendableRawSeriesStream)>,
        reservation: MemoryReservation,
    ) -> Self {
        let sources = streams
            .iter()
            .map(|(source, _)| source + 1)
            .max()
            .unwrap_or(0);
        Self {
            partitions: streams
                .into_iter()
                .map(|(source, stream)| Partition {
                    source,
                    stream: Some(stream),
                    held: Vec::new(),
                    held_closed: false,
                })
                .collect(),
            open: vec![None; sources],
            ready: VecDeque::new(),
            next: 0,
            reservation,
        }
    }

    fn route(&mut self, index: usize, chunk: RawSeriesChunk) -> Result<(), BoxedError> {
        let source = self.partitions[index].source;
        match self.open[source] {
            Some(open) if open != index => {
                let bytes = raw_bytes(&chunk);
                self.reservation
                    .try_grow(bytes)
                    .map_err(|error| BoxedError::new(common_query::error::Error::from(error)))?;
                let partition = &mut self.partitions[index];
                partition.held_closed = chunk.is_last;
                partition.held.push((chunk, bytes));
            }
            _ => {
                self.open[source] = (!chunk.is_last).then_some(index);
                self.ready.push_back((source, chunk, 0));
                if self.open[source].is_none() {
                    self.release(source);
                }
            }
        }
        Ok(())
    }

    /// Hands over the held series of `source` until one of them is still open.
    fn release(&mut self, source: usize) {
        while self.open[source].is_none() {
            let Some(index) = self
                .partitions
                .iter()
                .position(|partition| partition.source == source && !partition.held.is_empty())
            else {
                return;
            };
            let partition = &mut self.partitions[index];
            self.ready.extend(
                partition
                    .held
                    .drain(..)
                    .map(|(chunk, bytes)| (source, chunk, bytes)),
            );
            if !std::mem::take(&mut partition.held_closed) {
                self.open[source] = Some(index);
            }
        }
    }

    fn fail(&mut self) {
        self.partitions.clear();
        self.ready.clear();
        self.reservation.free();
    }
}

impl Stream for SeriesGather {
    type Item = Result<(usize, RawSeriesChunk), BoxedError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some((source, chunk, bytes)) = this.ready.pop_front() {
                this.reservation.shrink(bytes);
                return Poll::Ready(Some(Ok((source, chunk))));
            }
            let count = this.partitions.len();
            let mut live = false;
            let mut routed = false;
            for offset in 0..count {
                let index = (this.next + offset) % count;
                let partition = &mut this.partitions[index];
                if partition.held_closed {
                    live = true;
                    continue;
                }
                let Some(stream) = partition.stream.as_mut() else {
                    continue;
                };
                match stream.as_mut().poll_next(cx) {
                    Poll::Pending => live = true,
                    Poll::Ready(None) => {
                        partition.stream = None;
                        let source = partition.source;
                        // The decoder below refuses a stream that ends with its series open, so
                        // this is a producer that bypassed it; left alone it would hold its
                        // source's other streams back forever.
                        if this.open[source] == Some(index) || !partition.held.is_empty() {
                            this.fail();
                            return Poll::Ready(Some(Err(BoxedError::new(
                                InvalidSeriesSnafu {
                                    reason: "raw series stream ended with its series open",
                                }
                                .build(),
                            ))));
                        }
                    }
                    Poll::Ready(Some(Err(error))) => {
                        this.fail();
                        return Poll::Ready(Some(Err(error)));
                    }
                    Poll::Ready(Some(Ok(chunk))) => {
                        if let Err(error) = this.route(index, chunk) {
                            this.fail();
                            return Poll::Ready(Some(Err(error)));
                        }
                        this.next = (index + 1) % count;
                        routed = true;
                        break;
                    }
                }
            }
            if routed {
                continue;
            }
            return if live {
                Poll::Pending
            } else {
                Poll::Ready(None)
            };
        }
    }
}

/// Moves each stream onto its own task when there is more than one.
///
/// Decoding a partition (the scan's batch conversion, the frame codec and the label decode) is most
/// of what reading it costs, and the gather runs on one task: without this, that work for every
/// partition of every input runs there one chunk at a time.
///
/// The chunks a task has read and not handed over yet are charged to `pool`.
pub(crate) fn spawn_partitions(
    streams: Vec<(usize, SendableRawSeriesStream)>,
    pool: &Arc<dyn MemoryPool>,
) -> Vec<(usize, SendableRawSeriesStream)> {
    if streams.len() < 2 {
        return streams;
    }
    streams
        .into_iter()
        .map(|(source, stream)| {
            let reservation = MemoryConsumer::new("SpawnedRawSeriesPartition").register(pool);
            (source, spawn_partition(stream, reservation))
        })
        .collect()
}

/// Chunks a spawned partition queues ahead of the gather. With the one its task holds while the
/// queue is full, a partition reads at most this many plus one chunks ahead, of any series: this
/// bound is per partition and separate from the one series per stream the gather holds back.
const SPAWNED_READ_AHEAD: usize = 2;

type SpawnedItem = Result<(RawSeriesChunk, MemoryReservation), BoxedError>;

fn spawn_partition(
    mut stream: SendableRawSeriesStream,
    reservation: MemoryReservation,
) -> SendableRawSeriesStream {
    let schema = Arc::clone(stream.schema());
    let label_source = stream.label_source().clone();
    let label_decode_spec = stream.label_decode_spec().cloned();
    let ordering = stream.ordering();
    let (sender, receiver) = mpsc::channel::<SpawnedItem>(SPAWNED_READ_AHEAD);
    let task = common_runtime::spawn_query(async move {
        while let Some(item) = stream.next().await {
            // Each chunk carries its own charge, so a chunk dropped anywhere on the way, in the
            // queue or in a task that is aborted, frees what it was charged.
            let item = item.and_then(|chunk| {
                let charge = reservation.new_empty();
                charge
                    .try_grow(raw_bytes(&chunk))
                    .map_err(|error| BoxedError::new(common_query::error::Error::from(error)))?;
                Ok((chunk, charge))
            });
            let failed = item.is_err();
            if sender.send(item).await.is_err() || failed {
                return;
            }
        }
    });
    Box::pin(SpawnedPartition {
        schema,
        label_source,
        label_decode_spec,
        ordering,
        receiver,
        task,
        finished: false,
    })
}

struct SpawnedPartition {
    schema: Arc<SeriesSchema>,
    label_source: RawLabelSource,
    label_decode_spec: Option<LabelDecodeSpec>,
    ordering: SeriesOrdering,
    receiver: mpsc::Receiver<SpawnedItem>,
    task: common_runtime::JoinHandle<()>,
    /// Whether `task` was seen to finish, after which its handle must not be polled again.
    finished: bool,
}

impl Drop for SpawnedPartition {
    fn drop(&mut self) {
        // A task waiting on its scan would otherwise keep the scan, and whatever feeds it, alive
        // after the query is gone.
        self.task.abort();
    }
}

impl Stream for SpawnedPartition {
    type Item = Result<RawSeriesChunk, BoxedError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.receiver.poll_recv(cx) {
            Poll::Ready(None) if !this.finished => {}
            // The gather charges what it holds on to itself.
            polled => return polled.map(|item| item.map(|item| item.map(|(chunk, _)| chunk))),
        }
        // The channel also closes when the task panics, and that end must not read as the end of
        // the partition: what it had not sent yet would be missing from the answer.
        let joined = std::task::ready!(Pin::new(&mut this.task).poll(cx));
        this.finished = true;
        Poll::Ready(joined.err().map(|error| {
            Err(BoxedError::new(common_query::error::Error::from(
                datafusion::error::DataFusionError::ExecutionJoin(Box::new(error)),
            )))
        }))
    }
}

impl RawSeriesStream for SpawnedPartition {
    fn schema(&self) -> &Arc<SeriesSchema> {
        &self.schema
    }

    fn label_source(&self) -> &RawLabelSource {
        &self.label_source
    }

    fn label_decode_spec(&self) -> Option<&LabelDecodeSpec> {
        self.label_decode_spec.as_ref()
    }

    fn ordering(&self) -> SeriesOrdering {
        self.ordering
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use datafusion::arrow::array::Float64Array;
    use datafusion::arrow::buffer::{Buffer, ScalarBuffer};
    use datafusion::execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryPool, UnboundedMemoryPool,
    };
    use datatypes::series::raw::RawTimestamps;
    use datatypes::series::samples::{FieldSpec, Samples, SeriesFields};
    use futures::FutureExt;

    use super::*;

    fn schema() -> Arc<SeriesSchema> {
        Arc::new(SeriesSchema {
            fields: Arc::from([FieldSpec {
                name: Arc::from("value"),
            }]),
            eval_grid: None,
        })
    }

    /// One chunk of the series keyed `key`, holding one sample at `time`.
    fn chunk(key: &str, time: i64, last: bool) -> RawSeriesChunk {
        RawSeriesChunk::try_new(
            &schema(),
            Buffer::from_slice_ref(key.as_bytes()),
            None,
            RawTimestamps::try_new(ScalarBuffer::from(vec![time])).unwrap(),
            SeriesFields::one(Samples::from_float(Float64Array::from(vec![1.0]))),
            last,
        )
        .unwrap()
    }

    enum Step {
        Chunk(RawSeriesChunk),
        /// Answers `Pending` once, waking at once.
        Pending,
        Error,
    }

    /// A stream that hands over `steps` in order and reports when it is dropped.
    struct Scripted {
        schema: Arc<SeriesSchema>,
        steps: VecDeque<Step>,
        dropped: Arc<AtomicBool>,
    }

    impl Stream for Scripted {
        type Item = Result<RawSeriesChunk, BoxedError>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            match self.get_mut().steps.pop_front() {
                None => Poll::Ready(None),
                Some(Step::Chunk(chunk)) => Poll::Ready(Some(Ok(chunk))),
                Some(Step::Pending) => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Some(Step::Error) => Poll::Ready(Some(Err(BoxedError::new(
                    InvalidSeriesSnafu { reason: "scripted" }.build(),
                )))),
            }
        }
    }

    impl RawSeriesStream for Scripted {
        fn schema(&self) -> &Arc<SeriesSchema> {
            &self.schema
        }

        fn label_source(&self) -> &RawLabelSource {
            &RawLabelSource::Explicit
        }

        fn ordering(&self) -> SeriesOrdering {
            SeriesOrdering::Contiguous
        }
    }

    fn scripted(steps: Vec<Step>) -> (SendableRawSeriesStream, Arc<AtomicBool>) {
        let dropped = Arc::new(AtomicBool::new(false));
        let stream = Box::pin(Scripted {
            schema: schema(),
            steps: steps.into(),
            dropped: Arc::clone(&dropped),
        });
        (stream, dropped)
    }

    impl Drop for Scripted {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    /// A partition stream fed by a channel, as a scan's distributor feeds its partitions.
    struct Channel {
        schema: Arc<SeriesSchema>,
        receiver: mpsc::Receiver<RawSeriesChunk>,
    }

    impl Stream for Channel {
        type Item = Result<RawSeriesChunk, BoxedError>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.get_mut()
                .receiver
                .poll_recv(cx)
                .map(|chunk| chunk.map(Ok))
        }
    }

    impl RawSeriesStream for Channel {
        fn schema(&self) -> &Arc<SeriesSchema> {
            &self.schema
        }

        fn label_source(&self) -> &RawLabelSource {
            &RawLabelSource::Explicit
        }

        fn ordering(&self) -> SeriesOrdering {
            SeriesOrdering::Contiguous
        }
    }

    fn channel(receiver: mpsc::Receiver<RawSeriesChunk>) -> SendableRawSeriesStream {
        Box::pin(Channel {
            schema: schema(),
            receiver,
        })
    }

    fn unbounded_pool() -> Arc<dyn MemoryPool> {
        Arc::new(UnboundedMemoryPool::default())
    }

    fn unbounded() -> MemoryReservation {
        MemoryConsumer::new("test").register(&unbounded_pool())
    }

    fn keys(chunks: &[(usize, RawSeriesChunk)]) -> Vec<(usize, String, bool)> {
        chunks
            .iter()
            .map(|(source, chunk)| {
                (
                    *source,
                    String::from_utf8(chunk.primary_key.to_vec()).unwrap(),
                    chunk.is_last,
                )
            })
            .collect()
    }

    /// A producer that blocks on whichever partition it feeds next stalls a reader that drains one
    /// partition before polling the next: the producer waits on the unread partition, and the read
    /// one never ends.
    #[tokio::test]
    async fn partitions_fed_by_one_producer_are_read_concurrently() {
        const SERIES: usize = 64;
        let (senders, receivers): (Vec<_>, Vec<_>) = (0..2).map(|_| mpsc::channel(1)).unzip();
        let producer = tokio::spawn(async move {
            for series in 0..SERIES {
                senders[series % 2]
                    .send(chunk(&format!("s{series}"), 0, true))
                    .await
                    .unwrap();
            }
        });
        let streams = receivers
            .into_iter()
            .map(|receiver| (0, channel(receiver)))
            .collect();
        let gathered = tokio::time::timeout(
            Duration::from_secs(5),
            SeriesGather::new(streams, unbounded()).collect::<Vec<_>>(),
        )
        .await
        .expect("the gather stalled on a partition it did not poll");
        producer.await.unwrap();
        assert_eq!(SERIES, gathered.into_iter().map(Result::unwrap).count());
    }

    /// A series another partition of the same source reads while one is open waits for that one to
    /// close; a series of another source does not.
    #[tokio::test]
    async fn series_interleave_only_at_boundaries_of_one_source() {
        let run = |second_source| async move {
            let (open, _) = scripted(vec![
                Step::Chunk(chunk("a", 1, false)),
                Step::Pending,
                Step::Pending,
                Step::Chunk(chunk("a", 2, true)),
            ]);
            let (other, _) = scripted(vec![
                Step::Chunk(chunk("b", 1, true)),
                Step::Chunk(chunk("c", 1, false)),
                Step::Chunk(chunk("c", 2, true)),
            ]);
            let gathered = SeriesGather::new(vec![(0, open), (second_source, other)], unbounded())
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            keys(&gathered)
        };
        assert_eq!(
            vec![
                (0, "a".to_string(), false),
                (0, "a".to_string(), true),
                (0, "b".to_string(), true),
                (0, "c".to_string(), false),
                (0, "c".to_string(), true),
            ],
            run(0).await
        );
        assert_eq!(
            vec![
                (0, "a".to_string(), false),
                (1, "b".to_string(), true),
                (1, "c".to_string(), false),
                (1, "c".to_string(), true),
                (0, "a".to_string(), true),
            ],
            run(1).await
        );
    }

    /// The poll that finds the last stream ended reports the end: nothing is left to wake it again.
    #[test]
    fn the_end_is_reported_by_the_poll_that_finds_it() {
        let (first, _) = scripted(vec![Step::Chunk(chunk("a", 1, true))]);
        let (second, _) = scripted(Vec::new());
        let mut gather = SeriesGather::new(vec![(0, first), (0, second)], unbounded());
        assert!(matches!(gather.next().now_or_never(), Some(Some(Ok(_)))));
        assert!(matches!(gather.next().now_or_never(), Some(None)));
    }

    /// What is held back is charged to the reservation until it is handed over, and a partition
    /// holding a closed series is not read further.
    #[test]
    fn held_series_are_charged_until_handed_over() {
        let pool = Arc::new(GreedyMemoryPool::new(1 << 20)) as Arc<dyn MemoryPool>;
        let (open, _) = scripted(vec![
            Step::Chunk(chunk("a", 1, false)),
            Step::Pending,
            Step::Pending,
            Step::Chunk(chunk("a", 2, true)),
        ]);
        let (other, _) = scripted(vec![
            Step::Chunk(chunk("b", 1, false)),
            Step::Chunk(chunk("b", 2, true)),
            Step::Chunk(chunk("c", 1, true)),
        ]);
        let held = 2 * raw_bytes(&chunk("b", 1, false));
        let mut gather = SeriesGather::new(
            vec![(0, open), (0, other)],
            MemoryConsumer::new("test").register(&pool),
        );
        let first = gather.next().now_or_never().unwrap().unwrap().unwrap();
        assert_eq!(b"a", first.1.primary_key.as_slice());
        assert!(gather.next().now_or_never().is_none());
        assert_eq!(held, pool.reserved());
        let rest = futures::executor::block_on(gather.collect::<Vec<_>>())
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            vec![
                (0, "a".to_string(), true),
                (0, "b".to_string(), false),
                (0, "b".to_string(), true),
                (0, "c".to_string(), true),
            ],
            keys(&rest)
        );
        assert_eq!(0, pool.reserved());
    }

    /// An error from one partition ends the gather and releases every other partition and what was
    /// held for it, even if the consumer keeps the stream.
    #[test]
    fn an_error_releases_every_partition() {
        let pool = Arc::new(GreedyMemoryPool::new(1 << 20)) as Arc<dyn MemoryPool>;
        let (open, open_dropped) = scripted(vec![
            Step::Chunk(chunk("a", 1, false)),
            Step::Pending,
            Step::Error,
        ]);
        let (other, other_dropped) = scripted(vec![Step::Chunk(chunk("b", 1, true))]);
        let mut gather = SeriesGather::new(
            vec![(0, open), (0, other)],
            MemoryConsumer::new("test").register(&pool),
        );
        let outcome = futures::executor::block_on(async {
            let mut items = Vec::new();
            while let Some(item) = gather.next().await {
                let failed = item.is_err();
                items.push(item);
                if failed {
                    break;
                }
            }
            items
        });
        assert_eq!(2, outcome.len());
        assert!(outcome[1].is_err());
        assert!(open_dropped.load(Ordering::SeqCst));
        assert!(other_dropped.load(Ordering::SeqCst));
        assert_eq!(0, pool.reserved());
        assert!(futures::executor::block_on(gather.next()).is_none());
    }

    /// Dropping a spawned partition stops its task, which releases the stream it was reading even
    /// while that stream has nothing to hand over.
    #[tokio::test]
    async fn dropping_a_spawned_partition_releases_its_stream() {
        let (sender, receiver) = mpsc::channel(1);
        let (other, _) = scripted(Vec::new());
        drop(spawn_partitions(
            vec![(0, channel(receiver)), (0, other)],
            &unbounded_pool(),
        ));
        tokio::time::timeout(Duration::from_secs(5), sender.closed())
            .await
            .expect("the spawned task kept reading after its partition was dropped");
    }

    /// The chunks a spawned partition has read ahead are charged until they are handed over, and a
    /// pool that cannot hold the next one ends the partition in an error.
    #[tokio::test]
    async fn read_ahead_chunks_are_charged_until_handed_over() {
        async fn reserved_reaches(pool: &Arc<dyn MemoryPool>, bytes: usize) {
            tokio::time::timeout(Duration::from_secs(5), async {
                while pool.reserved() != bytes {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("reserved {} bytes, expected {bytes}", pool.reserved()));
        }
        let key = "k".repeat(1024);
        let chunks = || {
            scripted(
                (0..6)
                    .map(|time| Step::Chunk(chunk(&key, time, time == 5)))
                    .collect(),
            )
            .0
        };
        let bytes = raw_bytes(&chunk(&key, 0, false));
        let pool = Arc::new(GreedyMemoryPool::new(1 << 20)) as Arc<dyn MemoryPool>;

        let (empty, _) = scripted(Vec::new());
        let mut streams = spawn_partitions(vec![(0, chunks()), (0, empty)], &pool);
        // Two queued and the one the task holds while the queue is full.
        reserved_reaches(&pool, (SPAWNED_READ_AHEAD + 1) * bytes).await;
        let mut partition = streams.remove(0).1;
        for _ in 0..3 {
            partition.next().await.unwrap().unwrap();
        }
        // What was handed over is no longer charged: only the last three chunks are.
        reserved_reaches(&pool, 3 * bytes).await;
        let mut read = 3;
        while let Some(chunk) = partition.next().await {
            chunk.unwrap();
            read += 1;
        }
        assert_eq!(6, read);
        assert_eq!(0, pool.reserved());

        let small = Arc::new(GreedyMemoryPool::new(bytes - 1)) as Arc<dyn MemoryPool>;
        let (empty, _) = scripted(Vec::new());
        let mut streams = spawn_partitions(vec![(0, chunks()), (0, empty)], &small);
        let mut partition = streams.remove(0).1;
        assert!(matches!(partition.next().await, Some(Err(_))));
        assert!(partition.next().await.is_none());
    }

    /// A partition whose task panics ends in an error, not in an end that would drop the chunks it
    /// had not sent.
    #[tokio::test]
    async fn a_panicking_partition_reports_an_error() {
        struct Panics(Arc<SeriesSchema>);
        impl Stream for Panics {
            type Item = Result<RawSeriesChunk, BoxedError>;
            fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
                panic!("scripted panic");
            }
        }
        impl RawSeriesStream for Panics {
            fn schema(&self) -> &Arc<SeriesSchema> {
                &self.0
            }
            fn label_source(&self) -> &RawLabelSource {
                &RawLabelSource::Explicit
            }
            fn ordering(&self) -> SeriesOrdering {
                SeriesOrdering::Contiguous
            }
        }
        let (other, _) = scripted(Vec::new());
        let mut streams = spawn_partitions(
            vec![(0, other), (0, Box::pin(Panics(schema())))],
            &unbounded_pool(),
        );
        let mut panicking = streams.pop().unwrap().1;
        assert!(matches!(panicking.next().await, Some(Err(_))));
        assert!(panicking.next().await.is_none());
    }
}
