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
