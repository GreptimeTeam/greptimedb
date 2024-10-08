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

use std::ops::Add;
use std::pin::Pin;
use std::time::{Duration, Instant};

use async_stream::stream;
use futures::Stream;
use tokio::select;
use tokio::time::Sleep;

use crate::error;

pub fn bounded<T>(size: usize) -> (Sender<T>, Receiver<T>) {
    let (tx_low, rx_low) = async_channel::bounded(size);
    let (tx_high, rx_high) = async_channel::bounded(size);
    (Sender { tx_low, tx_high }, Receiver { rx_low, rx_high })
}

pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    let (tx_low, rx_low) = async_channel::unbounded();
    let (tx_high, rx_high) = async_channel::unbounded();
    (Sender { tx_low, tx_high }, Receiver { rx_low, rx_high })
}

#[derive(Clone)]
pub struct Sender<T> {
    tx_low: async_channel::Sender<(Instant, T)>,
    tx_high: async_channel::Sender<T>,
}

impl<T> Sender<T> {
    pub fn try_send_high(&self, item: T) -> error::Result<()> {
        self.tx_high.try_send(item).map_err(|e| {
            error::SendToChannelSnafu {
                err_msg: format!("{:?}", e),
            }
            .build()
        })
    }

    pub fn try_send_low(&self, item: T) -> error::Result<()> {
        self.tx_high.try_send(item).map_err(|e| {
            error::SendToChannelSnafu {
                err_msg: format!("{:?}", e),
            }
            .build()
        })
    }

    pub async fn send_high(&self, item: T) -> error::Result<()> {
        self.tx_high.send(item).await.map_err(|e| {
            error::SendToChannelSnafu {
                err_msg: format!("{:?}", e),
            }
            .build()
        })
    }

    pub async fn send_low(&self, item: T, deadline: Option<Duration>) -> error::Result<()> {
        self.tx_low
            .send((
                Instant::now().add(deadline.unwrap_or(Duration::from_secs(0))),
                item,
            ))
            .await
            .map_err(|e| {
                error::SendToChannelSnafu {
                    err_msg: format!("{:?}", e),
                }
                .build()
            })
    }
}

pub struct Receiver<T> {
    rx_low: async_channel::Receiver<(Instant, T)>,
    rx_high: async_channel::Receiver<T>,
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Self {
            rx_high: self.rx_high.clone(),
            rx_low: self.rx_low.clone(),
        }
    }
}

impl<T> Receiver<T>
where
    T: Send + 'static,
{
    pub fn into_stream(self) -> Pin<Box<dyn Stream<Item = T> + Send>> {
        let s = stream!({
            let mut timer: Option<Pin<Box<Sleep>>> = None;
            let mut pending: Option<T> = None;
            loop {
                select! {
                    biased;
                    Some(_) = maybe_timeout(&mut timer) => {
                        if let Some(timed) = std::mem::take(&mut pending){
                            yield timed
                        }
                    }

                    Ok(high) = self.rx_high.recv() => {
                        // also check if low channel has pending.
                        if pending.is_none() && let Ok((deadline, low_item)) = self.rx_low.try_recv() {
                            let now = Instant::now();
                            if deadline > now {
                                let tta = deadline - now;
                                timer = Some(Box::pin(tokio::time::sleep(tta)));
                            }
                            pending = Some(low_item);
                        }
                        yield high;
                    }

                    Ok((_, low)) = self.rx_low.recv() => {
                        if let Some(timed) = std::mem::take(&mut pending){
                            timer = None;
                            yield timed;
                        }
                        yield low
                    }

                    else => {
                        if self.rx_high.is_closed() && self.rx_low.is_closed() {
                            return;
                        }
                    }
                }
            }
        });
        Box::pin(s)
    }
}

async fn maybe_timeout(f: &mut Option<Pin<Box<Sleep>>>) -> Option<()> {
    match f {
        None => None,
        Some(sleeper) => {
            sleeper.await;
            *f = None;
            Some(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use futures::StreamExt;
    use tokio::time::timeout;

    use super::*;

    #[tokio::test]
    async fn test_high() {
        let (tx, rx) = unbounded();
        tx.send_high(1).await.unwrap();
        tx.send_high(2).await.unwrap();
        tx.send_high(3).await.unwrap();

        drop(tx);
        let s = rx.into_stream();
        assert_eq!(vec![1, 2, 3], s.collect::<Vec<_>>().await);
    }

    #[tokio::test]
    async fn test_low() {
        let (tx, rx) = unbounded();
        tx.send_low(1, None).await.unwrap();
        tx.send_low(2, None).await.unwrap();
        tx.send_low(3, None).await.unwrap();

        drop(tx);
        let s = rx.into_stream();
        assert_eq!(vec![1, 2, 3], s.collect::<Vec<_>>().await);
    }

    #[tokio::test]
    async fn test_high_and_low() {
        let (tx, rx) = unbounded();
        tx.send_low(1, None).await.unwrap();
        tx.send_low(2, None).await.unwrap();
        tx.send_low(3, None).await.unwrap();

        tx.send_high(4).await.unwrap();
        tx.send_high(5).await.unwrap();
        tx.send_high(6).await.unwrap();
        drop(tx);
        let s = rx.into_stream();
        assert_eq!(
            vec![4, 5, 6, 1, 2, 3],
            timeout(Duration::from_secs(5), s.collect::<Vec<_>>())
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_high_and_low_with_deadline() {
        let (tx, rx) = unbounded();

        for i in 0..10 {
            tx.send_high(i).await.unwrap();
        }

        tx.send_low(11, Some(Duration::from_millis(10)))
            .await
            .unwrap();

        let mut s = rx.into_stream();
        let mut res = vec![];
        drop(tx);
        while let Some(v) = s.next().await {
            res.push(v);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_empty() {
        let (tx, rx) = unbounded();
        tx.send_low(1, None).await.unwrap();
        drop(tx);
        assert_eq!(vec![1], rx.into_stream().collect::<Vec<_>>().await);
    }

    #[derive(Clone)]
    struct Item {
        high: bool,
        submitted: Instant,
    }

    impl Item {
        fn new(high_priority: bool) -> Self {
            Item {
                high: high_priority,
                submitted: Instant::now(),
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_high_workload() {
        let (tx, rx) = unbounded();

        const HIGH_COUNT: u32 = 100000;
        const LOW_COUNT: u32 = 100;
        const LOW_DEADLINE_US: u64 = 5000;

        let sender = tx.clone();
        tokio::spawn(async move {
            for _ in 0..HIGH_COUNT {
                sender.send_high(Item::new(true)).await.unwrap();
                tokio::task::yield_now().await;
            }
        });

        let sender = tx.clone();
        tokio::spawn(async move {
            for _ in 0..LOW_COUNT {
                sender
                    .send_low(
                        Item::new(false),
                        Some(Duration::from_micros(LOW_DEADLINE_US)),
                    )
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });

        let mut rx = rx.into_stream();
        let mut low_count = 0;
        let mut high_count = 0;
        drop(tx);
        let mut low_delay = Duration::from_secs(0);
        while let Some(v) = rx.next().await {
            if !v.high {
                low_count += 1;
                low_delay += Instant::now() - v.submitted;
            } else {
                high_count += 1;
            }
            if low_count >= LOW_COUNT && high_count >= HIGH_COUNT {
                break;
            }
        }

        assert_eq!(HIGH_COUNT, high_count);
        assert_eq!(LOW_COUNT, low_count);
        let avg_delay_micros = low_delay.as_micros() / LOW_COUNT as u128;
        assert!(avg_delay_micros < LOW_DEADLINE_US as u128);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_random_send() {
        let low_send = Arc::new(AtomicUsize::new(0));
        let high_send = Arc::new(AtomicUsize::new(0));

        let (tx, rx) = unbounded();

        let low_send_clone = low_send.clone();
        let high_send_clone = high_send.clone();
        tokio::spawn(async move {
            for _ in 0..10000 {
                let p: bool = rand::random();
                if p {
                    tx.send_high(Item::new(true)).await.unwrap();
                    high_send_clone.fetch_add(1, Ordering::Relaxed);
                } else {
                    tx.send_low(Item::new(false), None).await.unwrap();
                    low_send_clone.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        let mut high_recv = 0;
        let mut low_recv = 0;
        let mut s = rx.into_stream();
        while let Some(v) = s.next().await {
            if v.high {
                high_recv += 1;
            } else {
                low_recv += 1;
            }
        }
        assert_eq!(low_send.load(Ordering::Relaxed), low_recv);
        assert_eq!(high_send.load(Ordering::Relaxed), high_recv);
    }
}
