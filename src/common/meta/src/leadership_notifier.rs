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

use std::sync::Arc;

use async_trait::async_trait;
use common_telemetry::error;

use crate::error::Result;

pub type LeadershipChangeNotifierCustomizerRef = Arc<dyn LeadershipChangeNotifierCustomizer>;

/// A trait for customizing the leadership change notifier.
pub trait LeadershipChangeNotifierCustomizer: Send + Sync {
    fn customize(&self, notifier: &mut LeadershipChangeNotifier);
}

/// A trait for handling leadership change events in a distributed system.
#[async_trait]
pub trait LeadershipChangeListener: Send + Sync {
    /// Returns the listener name.
    fn name(&self) -> &str;

    /// Called after a node has become the leader.
    async fn on_become_leader(&self) -> Result<()>;

    /// Called after a node has become the follower.
    async fn on_become_follower(&self) -> Result<()>;
}

/// A notifier for leadership change events.
#[derive(Default)]
pub struct LeadershipChangeNotifier {
    listeners: Vec<Arc<dyn LeadershipChangeListener>>,
}

impl LeadershipChangeNotifier {
    /// Adds a listener to the notifier.
    pub fn add_listener(&mut self, listener: Arc<dyn LeadershipChangeListener>) {
        self.listeners.push(listener);
    }

    /// Notify all listeners that the node has become a leader.
    pub async fn notify_become_leader(&self) {
        for listener in &self.listeners {
            if let Err(err) = listener.on_become_leader().await {
                error!(
                    err;
                    "Failed to notify become leader event, listener: {}",
                    listener.name()
                );
            }
        }
    }

    /// Notify all listeners that the node has become a follower.
    pub async fn notify_become_follower(&self) {
        for listener in &self.listeners {
            if let Err(err) = listener.on_become_follower().await {
                error!(
                    err;
                    "Failed to notify become follower event, listener: {}",
                    listener.name()
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use super::*;

    struct MockListener {
        name: String,
        on_become_leader_fn: Option<Box<dyn Fn() -> Result<()> + Send + Sync>>,
        on_become_follower_fn: Option<Box<dyn Fn() -> Result<()> + Send + Sync>>,
    }

    #[async_trait::async_trait]
    impl LeadershipChangeListener for MockListener {
        fn name(&self) -> &str {
            &self.name
        }

        async fn on_become_leader(&self) -> Result<()> {
            if let Some(f) = &self.on_become_leader_fn {
                return f();
            }
            Ok(())
        }

        async fn on_become_follower(&self) -> Result<()> {
            if let Some(f) = &self.on_become_follower_fn {
                return f();
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_leadership_change_notifier() {
        let mut notifier = LeadershipChangeNotifier::default();
        let listener1 = Arc::new(MockListener {
            name: "listener1".to_string(),
            on_become_leader_fn: None,
            on_become_follower_fn: None,
        });
        let called_become_leader = Arc::new(AtomicBool::new(false));
        let called_become_follower = Arc::new(AtomicBool::new(false));
        let called_become_leader_moved = called_become_leader.clone();
        let called_become_follower_moved = called_become_follower.clone();
        let listener2 = Arc::new(MockListener {
            name: "listener2".to_string(),
            on_become_leader_fn: Some(Box::new(move || {
                called_become_leader_moved.store(true, Ordering::Relaxed);
                Ok(())
            })),
            on_become_follower_fn: Some(Box::new(move || {
                called_become_follower_moved.store(true, Ordering::Relaxed);
                Ok(())
            })),
        });

        notifier.add_listener(listener1);
        notifier.add_listener(listener2);

        let listener1 = notifier.listeners.first().unwrap();
        let listener2 = notifier.listeners.get(1).unwrap();

        assert_eq!(listener1.name(), "listener1");
        assert_eq!(listener2.name(), "listener2");

        notifier.notify_become_leader().await;
        assert!(!called_become_follower.load(Ordering::Relaxed));
        assert!(called_become_leader.load(Ordering::Relaxed));

        notifier.notify_become_follower().await;
        assert!(called_become_follower.load(Ordering::Relaxed));
        assert!(called_become_leader.load(Ordering::Relaxed));
    }
}
