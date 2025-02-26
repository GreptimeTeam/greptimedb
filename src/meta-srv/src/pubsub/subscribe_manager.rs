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

use common_telemetry::info;
use dashmap::DashMap;
use tokio::sync::mpsc::Sender;

use crate::error::Result;
use crate::pubsub::{Message, Subscriber, SubscriberRef, Topic, Transport};

pub trait SubscriptionQuery<T>: Send + Sync {
    fn subscribers_by_topic(&self, topic: &Topic) -> Vec<SubscriberRef<T>>;
}

pub trait SubscriptionManager<T>: SubscriptionQuery<T> {
    fn subscribe(&self, req: SubscribeRequest<T>) -> Result<()>;

    fn unsubscribe(&self, req: UnsubscribeRequest) -> Result<()>;

    fn unsubscribe_all(&self) -> Result<()>;
}

pub type SubscriptionManagerRef = Arc<dyn SubscriptionManager<Sender<Message>>>;

pub struct SubscribeRequest<T> {
    pub topics: Vec<Topic>,
    pub subscriber: Subscriber<T>,
}

#[derive(Debug, Clone)]
pub struct UnsubscribeRequest {
    pub subscriber_id: u32,
}

pub struct DefaultSubscribeManager<T> {
    topic_to_subscribers: DashMap<Topic, Vec<Arc<Subscriber<T>>>>,
}

impl<T> Default for DefaultSubscribeManager<T> {
    fn default() -> Self {
        Self {
            topic_to_subscribers: DashMap::new(),
        }
    }
}

impl<T> SubscriptionQuery<T> for DefaultSubscribeManager<T>
where
    T: Transport,
{
    fn subscribers_by_topic(&self, topic: &Topic) -> Vec<SubscriberRef<T>> {
        self.topic_to_subscribers
            .get(topic)
            .map(|list_ref| list_ref.clone())
            .unwrap_or_default()
    }
}

impl<T> SubscriptionManager<T> for DefaultSubscribeManager<T>
where
    T: Transport,
{
    fn subscribe(&self, req: SubscribeRequest<T>) -> Result<()> {
        let SubscribeRequest { topics, subscriber } = req;

        info!(
            "Add a subscriber, subscriber_id: {}, subscriber_name: {}, topics: {:?}",
            subscriber.id(),
            subscriber.name(),
            topics
        );

        let subscriber = Arc::new(subscriber);

        for topic in topics {
            let mut entry = self.topic_to_subscribers.entry(topic).or_default();
            entry.push(subscriber.clone());
        }

        Ok(())
    }

    fn unsubscribe(&self, req: UnsubscribeRequest) -> Result<()> {
        let UnsubscribeRequest { subscriber_id } = req;

        info!("Remove a subscriber, subscriber_id: {}", subscriber_id);

        for mut subscribers in self.topic_to_subscribers.iter_mut() {
            subscribers.retain(|subscriber| subscriber.id() != subscriber_id)
        }

        Ok(())
    }

    fn unsubscribe_all(&self) -> Result<()> {
        self.topic_to_subscribers.clear();

        Ok(())
    }
}
