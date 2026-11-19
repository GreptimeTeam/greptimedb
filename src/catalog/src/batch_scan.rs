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

use std::future::Future;

use futures::{Stream, StreamExt};

/// Bounds both the number of keys per metadata read and concurrent reads per scan.
pub(crate) struct BatchScanner {
    batch_size: usize,
    max_in_flight: usize,
}

impl BatchScanner {
    pub(crate) const METADATA: Self = Self {
        batch_size: 128,
        max_in_flight: 8,
    };

    pub(crate) fn scan<T, U, E, F, Fut>(
        &self,
        input: impl Stream<Item = Result<T, E>>,
        mut load: F,
    ) -> impl Stream<Item = Result<U, E>>
    where
        F: FnMut(Vec<T>) -> Fut,
        Fut: Future<Output = Result<U, E>>,
    {
        input
            .ready_chunks(self.batch_size)
            .map(move |items| {
                let batch = items
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()
                    .map(&mut load);
                async move { batch?.await }
            })
            .buffered(self.max_in_flight)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use futures::channel::oneshot;
    use futures::{StreamExt, TryStreamExt, future, stream};

    use super::*;

    #[tokio::test]
    async fn test_in_flight_limit_order_and_cancellation() {
        let scanner = BatchScanner {
            batch_size: 1,
            max_in_flight: 2,
        };
        let (mut sends, mut replies): (VecDeque<_>, VecDeque<_>) =
            (0..4).map(|_| oneshot::channel::<()>()).unzip();
        let started = std::cell::Cell::new(0);
        let input = stream::iter((0..4).map(Ok::<_, ()>));
        let mut output = Box::pin(scanner.scan(input, |batch| {
            started.set(started.get() + 1);
            let reply = replies.pop_front().unwrap();
            async move {
                reply.await.unwrap();
                Ok(batch)
            }
        }));
        assert!(futures::poll!(output.next()).is_pending());
        assert_eq!(started.get(), 2);
        sends.remove(1).unwrap().send(()).unwrap();
        assert!(futures::poll!(output.next()).is_pending());
        sends.pop_front().unwrap().send(()).unwrap();
        assert_eq!(output.try_next().await.unwrap(), Some(vec![0]));
        assert_eq!(output.try_next().await.unwrap(), Some(vec![1]));
        drop(output);
        assert!(sends.pop_front().unwrap().send(()).is_err());
        assert_eq!(started.get(), 3);
    }

    #[tokio::test]
    async fn test_partial_batch_does_not_wait_for_full_input() {
        let input = stream::iter([Ok::<_, ()>(1)]).chain(stream::pending());
        let mut output =
            Box::pin(BatchScanner::METADATA.scan(input, |batch| future::ready(Ok(batch))));
        assert_eq!(output.try_next().await.unwrap(), Some(vec![1]));
    }
}
