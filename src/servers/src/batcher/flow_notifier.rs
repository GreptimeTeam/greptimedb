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

use std::num::NonZeroUsize;

use common_batcher::notifier::{Notifier, run_notifier};
use common_meta::cache::TableFlownodeSetCacheRef;
use common_meta::node_manager::NodeManagerRef;
use common_runtime::spawn_global;
use common_telemetry::{error, warn};
use prometheus::IntCounterVec;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::error::TrySendError;

use crate::batcher::flow_sender::{FlowNotification, FlowSender};

/// Best-effort queue admission and diagnostics, independent of payload extraction.
#[derive(Clone)]
pub(in crate::batcher) struct FlowNotifier {
    notifier: Notifier<FlowNotification>,
    dropped: IntCounterVec,
}

impl FlowNotifier {
    /// Each construction creates an independent queue with caller-owned metrics.
    pub fn try_new(
        capacity: NonZeroUsize,
        dropped: IntCounterVec,
    ) -> Option<(Self, Receiver<FlowNotification>)> {
        let (notifier, receiver) = Notifier::try_new(capacity.get())?;
        Some((Self { notifier, dropped }, receiver))
    }

    /// Never waits for queue capacity or delivery and never changes the write result.
    pub fn try_notify(&self, notification: FlowNotification) -> bool {
        match self.notifier.try_notify(notification) {
            Ok(()) => true,
            Err(TrySendError::Full(notification)) => {
                self.dropped.with_label_values(&["full"]).inc();
                warn!(
                    "Dropping flow notification because queue is full, table_id: {}, queue_capacity: {}",
                    notification.table_id,
                    self.notifier.max_capacity()
                );
                false
            }
            Err(TrySendError::Closed(notification)) => {
                self.dropped.with_label_values(&["closed"]).inc();
                error!(
                    "Dropping flow notification because queue is closed, table_id: {}, queue_capacity: {}",
                    notification.table_id,
                    self.notifier.max_capacity()
                );
                false
            }
        }
    }
}

/// Composes queue consumption and delivery without giving the notifier task ownership.
pub(in crate::batcher) fn start_flow_notification_worker(
    receiver: Receiver<FlowNotification>,
    cache: TableFlownodeSetCacheRef,
    node_manager: NodeManagerRef,
) {
    let sender = FlowSender::new(cache, node_manager);
    let concurrency = NonZeroUsize::new(8).unwrap();
    spawn_global(run_notifier(receiver, concurrency, move |notification| {
        let sender = sender.clone();
        async move { sender.send(notification).await }
    }));
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use prometheus::{IntCounterVec, Opts};

    use crate::batcher::flow_notifier::FlowNotifier;
    use crate::batcher::flow_sender::FlowNotification;

    fn dropped_counter() -> IntCounterVec {
        // Unregistered collectors remain independent even with identical names.
        IntCounterVec::new(
            Opts::new("test_flow_dropped", "Dropped notifications"),
            &["reason"],
        )
        .unwrap()
    }

    fn notification(table_id: u32) -> FlowNotification {
        FlowNotification {
            table_id,
            timestamps: vec![-1, 42, 1_700_000_000_000_000_001, 42],
        }
    }

    fn dropped_counts(counter: &IntCounterVec) -> (u64, u64) {
        (
            counter.with_label_values(&["full"]).get(),
            counter.with_label_values(&["closed"]).get(),
        )
    }

    #[test]
    fn test_queues_and_metrics_are_independent() {
        let first_dropped = dropped_counter();
        let second_dropped = dropped_counter();
        let (first, mut first_rx) =
            FlowNotifier::try_new(NonZeroUsize::new(1).unwrap(), first_dropped.clone()).unwrap();
        let (second, mut second_rx) =
            FlowNotifier::try_new(NonZeroUsize::new(2).unwrap(), second_dropped.clone()).unwrap();

        assert!(first.try_notify(notification(1)));
        assert!(!first.try_notify(notification(2)));
        assert_eq!(dropped_counts(&first_dropped), (1, 0));
        assert_eq!(dropped_counts(&second_dropped), (0, 0));
        assert!(second.try_notify(notification(3)));
        assert!(second.try_notify(notification(4)));
        assert!(!second.try_notify(notification(5)));
        assert_eq!(dropped_counts(&second_dropped), (1, 0));

        let payload = first_rx.try_recv().unwrap();
        assert_eq!(payload.table_id, 1);
        assert_eq!(payload.timestamps, notification(1).timestamps);
        assert!(first_rx.try_recv().is_err());
        for table_id in [3, 4] {
            let payload = second_rx.try_recv().unwrap();
            assert_eq!(payload.table_id, table_id);
            assert_eq!(payload.timestamps, notification(table_id).timestamps);
        }
        assert!(second_rx.try_recv().is_err());

        drop(first_rx);
        assert!(!first.try_notify(notification(6)));
        assert_eq!(dropped_counts(&first_dropped), (1, 1));
        assert_eq!(dropped_counts(&second_dropped), (1, 0));
        assert!(second.try_notify(notification(7)));
        assert_eq!(second_rx.try_recv().unwrap().table_id, 7);
        drop(second_rx);
        assert!(!second.try_notify(notification(8)));
        assert_eq!(dropped_counts(&second_dropped), (1, 1));
    }

    #[test]
    fn test_clones_share_queue_and_drop_metrics() {
        let dropped = dropped_counter();
        let (notifier, mut receiver) =
            FlowNotifier::try_new(NonZeroUsize::new(1).unwrap(), dropped.clone()).unwrap();
        let clone = notifier.clone();
        assert!(notifier.try_notify(notification(1)));
        assert!(!clone.try_notify(notification(2)));
        assert_eq!(dropped_counts(&dropped), (1, 0));
        assert_eq!(receiver.try_recv().unwrap().table_id, 1);
        drop(notifier);
        assert!(clone.try_notify(notification(3)));
        let payload = receiver.try_recv().unwrap();
        assert_eq!(payload.table_id, 3);
        assert_eq!(payload.timestamps, notification(3).timestamps);
        drop(receiver);
        assert!(!clone.try_notify(notification(4)));
        assert_eq!(dropped_counts(&dropped), (1, 1));
    }
}
