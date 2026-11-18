// Copyright 2023-2026 GrepTime Inc.
//
// This file is part of the GreptimeDB Enterprise Edition and is licensed under
// the GreptimeDB Enterprise License. You may not use this file except in
// compliance with that license. A copy of the license is available at the root
// of this repository in the file LICENSE-ENTERPRISE.
//
// Unless required by applicable law or agreed to in writing, this software is
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.

//! Bounded writer messages. Individual Arrow batches retain their dictionaries and order.
use datatypes::arrow::record_batch::RecordBatch;
use smallvec::SmallVec;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

pub(super) type Packet = SmallVec<[RecordBatch; 1]>;

pub(super) struct Sink {
    sender: mpsc::Sender<Packet>,
    pending: Packet,
    bytes: usize,
    rows: usize,
    target_rows: usize,
    max_bytes: usize,
}

impl Sink {
    pub(super) fn new(sender: mpsc::Sender<Packet>, target_rows: usize, slot_bytes: usize) -> Self {
        Self {
            sender,
            pending: Packet::new(),
            bytes: 0,
            rows: 0,
            target_rows,
            max_bytes: slot_bytes.min(4 << 20),
        }
    }

    async fn send(&mut self, packet: Packet, wait: &mut Duration) -> bool {
        match self.sender.try_send(packet) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(packet)) => {
                let start = Instant::now();
                let sent = self.sender.send(packet).await.is_ok();
                *wait += start.elapsed();
                sent
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    }

    pub(super) async fn flush(&mut self, wait: &mut Duration) -> bool {
        if self.pending.is_empty() {
            return true;
        }
        self.rows = 0;
        self.bytes = 0;
        let packet = std::mem::take(&mut self.pending);
        self.send(packet, wait).await
    }

    pub(super) async fn push(&mut self, fragment: RecordBatch, wait: &mut Duration) -> bool {
        if self.target_rows <= 1 {
            return self.send(smallvec::smallvec![fragment], wait).await;
        }
        let bytes = fragment.get_array_memory_size();
        if bytes > self.max_bytes.saturating_sub(self.bytes) && !self.flush(wait).await {
            return false;
        }
        self.bytes += bytes;
        self.rows += fragment.num_rows();
        self.pending.push(fragment);
        if self.rows >= self.target_rows || self.bytes >= self.max_bytes || self.pending.len() == 16
        {
            return self.flush(wait).await;
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datatypes::arrow::array::{ArrayRef, UInt64Array};
    use std::sync::Arc;

    fn fragment(value: u64) -> RecordBatch {
        RecordBatch::try_from_iter([("n", Arc::new(UInt64Array::from(vec![value])) as ArrayRef)])
            .unwrap()
    }

    #[tokio::test]
    async fn retains_order_and_delivers_last_partial_packet() {
        let (tx, mut rx) = mpsc::channel(3);
        let mut sink = Sink::new(tx, 3, 65536);
        let mut wait = Duration::ZERO;
        for i in 0..7 {
            assert!(sink.push(fragment(i), &mut wait).await);
        }
        assert!(sink.flush(&mut wait).await);
        drop(sink);
        let mut values = vec![];
        let mut sizes = vec![];
        while let Some(packet) = rx.recv().await {
            sizes.push(packet.len());
            for f in packet {
                values.push(
                    f.column(0)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap()
                        .value(0),
                );
            }
        }
        assert_eq!(sizes, [3, 3, 1]);
        assert_eq!(values, (0..7).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn caps_bytes_and_reports_closed_writer_on_tail() {
        let (tx, mut rx) = mpsc::channel(1);
        let bytes = fragment(0).get_array_memory_size();
        let mut sink = Sink::new(tx, 8192, bytes + 1);
        let mut wait = Duration::ZERO;
        assert!(sink.push(fragment(1), &mut wait).await);
        assert!(sink.push(fragment(2), &mut wait).await);
        assert_eq!(rx.recv().await.unwrap().len(), 1);
        drop(rx);
        assert!(!sink.flush(&mut wait).await);
    }
}
