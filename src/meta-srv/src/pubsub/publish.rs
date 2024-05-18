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

use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use common_telemetry::error;

use crate::pubsub::{Message, SubscriptionManager, Transport, UnsubscribeRequest};

/// This trait provides a `publish` method that can be used by other modules
/// of meta to publish [Message].
#[async_trait::async_trait]
pub trait Publisher: Send + Sync {
    async fn publish(&self, message: Message);
}

pub type PublisherRef = Arc<dyn Publisher>;

/// The default implementation of [Publisher]
pub struct DefaultPublisher<M, T> {
    subscription_manager: Arc<M>,
    _transport: PhantomData<T>,
}

impl<M, T> DefaultPublisher<M, T> {
    pub fn new(subscription_manager: Arc<M>) -> Self {
        Self {
            subscription_manager,
            _transport: PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<M, T> Publisher for DefaultPublisher<M, T>
where
    M: SubscriptionManager<T>,
    T: Transport + Debug,
{
    async fn publish(&self, message: Message) {
        let subscribers = self
            .subscription_manager
            .subscribers_by_topic(&message.topic());

        for subscriber in subscribers {
            if subscriber.transport_msg(message.clone()).await.is_err() {
                // If an error occurs, we consider the subscriber offline,
                // so un_subscribe here.
                let req = UnsubscribeRequest {
                    subscriber_id: subscriber.id(),
                };

                if let Err(e) = self.subscription_manager.unsubscribe(req.clone()) {
                    error!(e; "failed to unsubscribe, req: {:?}", req);
                }
            }
        }
    }
}
