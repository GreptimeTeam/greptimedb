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

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use common_telemetry::{error, info};

use crate::error::Result;

pub type LeadershipChangeNotifierCustomizerRef = Arc<dyn LeadershipChangeNotifierCustomizer>;

/// A trait for customizing the leadership change notifier.
pub trait LeadershipChangeNotifierCustomizer: Send + Sync {
    fn customize(&self, notifier: &mut LeadershipChangeNotifier);

    fn add_listener(&self, listener: Arc<dyn LeadershipChangeListener>);
}

/// A trait for handling leadership change events in a distributed system.
#[async_trait]
pub trait LeadershipChangeListener: Send + Sync {
    /// Returns the listener name.
    fn name(&self) -> &str;

    /// Called when the node transitions to the leader role.
    async fn on_leader_start(&self) -> Result<()>;

    /// Called when the node transitions to the follower role.
    async fn on_leader_stop(&self) -> Result<()>;
}

/// A notifier for leadership change events.
#[derive(Default)]
pub struct LeadershipChangeNotifier {
    listeners: Vec<Arc<dyn LeadershipChangeListener>>,
}

#[derive(Default)]
pub struct DefaultLeadershipChangeNotifierCustomizer {
    listeners: Mutex<Vec<Arc<dyn LeadershipChangeListener>>>,
}

impl DefaultLeadershipChangeNotifierCustomizer {
    pub fn new() -> Self {
        Self {
            listeners: Mutex::new(Vec::new()),
        }
    }
}

impl LeadershipChangeNotifierCustomizer for DefaultLeadershipChangeNotifierCustomizer {
    fn customize(&self, notifier: &mut LeadershipChangeNotifier) {
        info!("Customizing leadership change notifier");
        let listeners = self.listeners.lock().unwrap().clone();
        notifier.listeners.extend(listeners);
    }

    fn add_listener(&self, listener: Arc<dyn LeadershipChangeListener>) {
        self.listeners.lock().unwrap().push(listener);
    }
}

impl LeadershipChangeNotifier {
    /// Adds a listener to the notifier.
    pub fn add_listener(&mut self, listener: Arc<dyn LeadershipChangeListener>) {
        self.listeners.push(listener);
    }

    /// Notify all listeners that the node has become a leader.
    pub async fn notify_on_leader_start(&self) {
        for listener in &self.listeners {
            if let Err(err) = listener.on_leader_start().await {
                error!(
                    err;
                    "Failed to notify listener: {}, event 'on_leader_start'",
                    listener.name()
                );
            }
        }
    }

    /// Notify all listeners that the node has become a follower.
    pub async fn notify_on_leader_stop(&self) {
        for listener in &self.listeners {
            if let Err(err) = listener.on_leader_stop().await {
                error!(
                    err;
                    "Failed to notify listener: {}, event: 'on_follower_start'",
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
        on_leader_start_fn: Option<Box<dyn Fn() -> Result<()> + Send + Sync>>,
        on_follower_start_fn: Option<Box<dyn Fn() -> Result<()> + Send + Sync>>,
    }

    #[async_trait::async_trait]
    impl LeadershipChangeListener for MockListener {
        fn name(&self) -> &str {
            &self.name
        }

        async fn on_leader_start(&self) -> Result<()> {
            if let Some(f) = &self.on_leader_start_fn {
                return f();
            }
            Ok(())
        }

        async fn on_leader_stop(&self) -> Result<()> {
            if let Some(f) = &self.on_follower_start_fn {
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
            on_leader_start_fn: None,
            on_follower_start_fn: None,
        });
        let called_on_leader_start = Arc::new(AtomicBool::new(false));
        let called_on_follower_start = Arc::new(AtomicBool::new(false));
        let called_on_leader_start_moved = called_on_leader_start.clone();
        let called_on_follower_start_moved = called_on_follower_start.clone();
        let listener2 = Arc::new(MockListener {
            name: "listener2".to_string(),
            on_leader_start_fn: Some(Box::new(move || {
                called_on_leader_start_moved.store(true, Ordering::Relaxed);
                Ok(())
            })),
            on_follower_start_fn: Some(Box::new(move || {
                called_on_follower_start_moved.store(true, Ordering::Relaxed);
                Ok(())
            })),
        });

        notifier.add_listener(listener1);
        notifier.add_listener(listener2);

        let listener1 = notifier.listeners.first().unwrap();
        let listener2 = notifier.listeners.get(1).unwrap();

        assert_eq!(listener1.name(), "listener1");
        assert_eq!(listener2.name(), "listener2");

        notifier.notify_on_leader_start().await;
        assert!(!called_on_follower_start.load(Ordering::Relaxed));
        assert!(called_on_leader_start.load(Ordering::Relaxed));

        notifier.notify_on_leader_stop().await;
        assert!(called_on_follower_start.load(Ordering::Relaxed));
        assert!(called_on_leader_start.load(Ordering::Relaxed));
    }
}
