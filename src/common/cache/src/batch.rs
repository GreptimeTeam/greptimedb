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

//! Request-local batching of cache initializers.

use std::future::Future;
use std::pin::pin;
use std::task::Poll;

use futures::channel::{mpsc, oneshot};
use futures::future::{self, Either};
use futures::stream::{FuturesUnordered, StreamExt};

/// Submits backend requests from the initializers elected by the cache.
/// Only the active initializer calls [`Self::request`]. A waiter enqueues
/// nothing until it takes over a cancelled initializer.
pub struct BatchSender<K, V> {
    requests: mpsc::UnboundedSender<(K, oneshot::Sender<V>)>,
}

impl<K, V> Clone for BatchSender<K, V> {
    fn clone(&self) -> Self {
        Self {
            requests: self.requests.clone(),
        }
    }
}

impl<K, V> BatchSender<K, V> {
    /// Enqueues one request and waits for its reply. Returns `None` if the batch closes.
    pub async fn request(&self, key: K) -> Option<V> {
        let (reply, receive) = oneshot::channel();
        self.requests.unbounded_send((key, reply)).ok()?;
        receive.await.ok()
    }
}

/// Drives one caller's per-key loads and batches requests from their active initializers.
/// Each dispatch receives ready keys and their corresponding reply senders in the same order.
/// Completed loads are delivered to `on_loaded` in completion order. An error or cancellation
/// drops the remaining loads and any in-progress dispatch.
pub async fn load<K, V, T, E, I, D>(
    make_loads: impl FnOnce(BatchSender<K, V>) -> I,
    mut dispatch: impl FnMut(Vec<K>, Vec<oneshot::Sender<V>>) -> D,
    mut on_loaded: impl FnMut(T),
) -> Result<(), E>
where
    I: ExactSizeIterator,
    I::Item: Future<Output = Result<T, E>>,
    D: Future<Output = ()>,
{
    let (requests, receiver) = mpsc::unbounded();
    let loads = make_loads(BatchSender { requests });
    let batch_size = loads.len();
    if batch_size == 0 {
        return Ok(());
    }

    let reads = async move {
        let mut pending = FuturesUnordered::new();
        // Poll every key before dispatch so stream fairness does not split ready misses.
        for load in loads {
            let mut load = Box::pin(load);
            match futures::poll!(&mut load) {
                Poll::Ready(value) => on_loaded(value?),
                Poll::Pending => pending.push(load),
            }
        }
        while let Some(value) = pending.next().await {
            on_loaded(value?);
        }
        Ok(())
    };
    let batches = async move {
        let mut batches = receiver.ready_chunks(batch_size);
        while let Some(batch) = batches.next().await {
            let (keys, replies) = batch.into_iter().unzip();
            dispatch(keys, replies).await;
        }
    };
    match future::select(pin!(reads), pin!(batches)).await {
        Either::Left((result, _)) => result,
        Either::Right(((), reads)) => reads.await,
    }
}

#[cfg(test)]
mod tests {
    use futures::future;

    use super::*;

    #[tokio::test]
    async fn test_empty_loads_skip_dispatch() {
        let result = load(
            |_: BatchSender<(), ()>| std::iter::empty::<future::Ready<Result<(), ()>>>(),
            |_, _| async { panic!("unexpected dispatch") },
            |_| panic!("unexpected load"),
        )
        .await;
        assert_eq!(result, Ok(()));
    }

    #[tokio::test]
    async fn test_delivers_replies_in_completion_order() {
        let (send_batch, mut batches) = mpsc::unbounded();
        let (send_value, mut values) = mpsc::unbounded();
        let mut read = Box::pin(load(
            |requests: BatchSender<u32, u32>| {
                [1, 2].into_iter().map(move |key| {
                    let requests = requests.clone();
                    async move {
                        requests
                            .request(key)
                            .await
                            .map(|value| (key, value))
                            .ok_or(())
                    }
                })
            },
            move |keys, replies| {
                send_batch.unbounded_send((keys, replies)).unwrap();
                future::ready(())
            },
            move |value| send_value.unbounded_send(value).unwrap(),
        ));
        assert!(futures::poll!(&mut read).is_pending());
        let (keys, replies) = batches.try_recv().unwrap();
        assert_eq!(keys, [1, 2]);
        let [first, second]: [_; 2] = replies.try_into().unwrap();
        second.send(20).unwrap();
        assert!(futures::poll!(&mut read).is_pending());
        assert_eq!(values.try_recv().unwrap(), (2, 20));
        first.send(10).unwrap();
        read.await.unwrap();
        assert_eq!(values.try_recv().unwrap(), (1, 10));
    }

    #[tokio::test]
    async fn test_dropped_reply_cancels_pending_dispatch() {
        let (release, gate) = oneshot::channel();
        let mut gate = Some(gate);
        let read = load(
            |requests: BatchSender<(), ()>| {
                std::iter::once(async move { requests.request(()).await.ok_or("reply dropped") })
            },
            |_, replies| {
                let gate = gate.take().unwrap();
                async move {
                    drop(replies);
                    gate.await.unwrap();
                }
            },
            |_| panic!("unexpected successful load"),
        );
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), read)
            .await
            .unwrap();
        assert_eq!(result, Err("reply dropped"));
        assert!(release.send(()).is_err());
    }

    #[tokio::test]
    async fn test_waits_for_load_after_queue_closes() {
        let (release, gate) = oneshot::channel();
        let mut value = None;
        let mut read = Box::pin(load(
            |_: BatchSender<(), ()>| std::iter::once(gate),
            |_, _| async { panic!("unexpected dispatch") },
            |v| value = Some(v),
        ));
        assert!(futures::poll!(&mut read).is_pending());
        release.send(42).unwrap();
        read.await.unwrap();
        assert_eq!(value, Some(42));
    }
}
