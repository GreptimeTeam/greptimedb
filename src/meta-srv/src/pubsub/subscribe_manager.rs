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

pub trait SubscribeQuery<T>: Send + Sync {
    fn subscribers_by_topic(&self, topic: &Topic) -> Vec<SubscriberRef<T>>;
}

pub trait SubscribeManager<T>: SubscribeQuery<T> {
    fn subscribe(&self, req: AddSubRequest<T>) -> Result<()>;

    fn un_subscribe(&self, req: UnSubRequest) -> Result<()>;

    fn un_subscribe_all(&self) -> Result<()>;
}

pub type SubscribeManagerRef = Arc<dyn SubscribeManager<Sender<Message>>>;

pub struct AddSubRequest<T> {
    pub topic_list: Vec<Topic>,
    pub subscriber: Subscriber<T>,
}

#[derive(Debug, Clone)]
pub struct UnSubRequest {
    pub subscriber_id: u32,
}
pub struct DefaultSubscribeManager<T> {
    topic2sub: DashMap<Topic, Vec<Arc<Subscriber<T>>>>,
}

impl<T> Default for DefaultSubscribeManager<T> {
    fn default() -> Self {
        Self {
            topic2sub: DashMap::new(),
        }
    }
}

impl<T> SubscribeQuery<T> for DefaultSubscribeManager<T>
where
    T: Transport,
{
    fn subscribers_by_topic(&self, topic: &Topic) -> Vec<SubscriberRef<T>> {
        self.topic2sub
            .get(topic)
            .map(|list_ref| list_ref.clone())
            .unwrap_or_default()
    }
}

impl<T> SubscribeManager<T> for DefaultSubscribeManager<T>
where
    T: Transport,
{
    fn subscribe(&self, req: AddSubRequest<T>) -> Result<()> {
        let AddSubRequest {
            topic_list,
            subscriber,
        } = req;

        info!(
            "Add a subscription, subscriber_id: {}, subscriber_name: {}, topic list: {:?}",
            subscriber.id(),
            subscriber.name(),
            topic_list
        );

        let subscriber = Arc::new(subscriber);

        for topic in topic_list {
            let mut entry = self.topic2sub.entry(topic).or_default();
            entry.push(subscriber.clone());
        }

        Ok(())
    }

    fn un_subscribe(&self, req: UnSubRequest) -> Result<()> {
        let UnSubRequest { subscriber_id } = req;

        info!("Add a un_subscription, subscriber_id: {}", subscriber_id);

        for mut sub_list in self.topic2sub.iter_mut() {
            sub_list.retain(|subscriber| subscriber.id() != subscriber_id)
        }

        Ok(())
    }

    fn un_subscribe_all(&self) -> Result<()> {
        self.topic2sub.clear();

        Ok(())
    }
}
