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

use crate::pubsub::{Message, SubscribeManager, Transport, UnSubRequest};

/// This trait provides a `send_msg` method that can be used by other modules
/// of meta to publish [Message].
#[async_trait::async_trait]
pub trait Publish: Send + Sync {
    async fn send_msg(&self, message: Message);
}

pub type PublishRef = Arc<dyn Publish>;

/// The default implementation of [Publish]
pub struct DefaultPublish<M, T> {
    subscribe_manager: Arc<M>,
    _transport: PhantomData<T>,
}

impl<M, T> DefaultPublish<M, T> {
    pub fn new(subscribe_manager: Arc<M>) -> Self {
        Self {
            subscribe_manager,
            _transport: PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<M, T> Publish for DefaultPublish<M, T>
where
    M: SubscribeManager<T>,
    T: Transport + Debug,
{
    async fn send_msg(&self, message: Message) {
        let sub_list = self
            .subscribe_manager
            .subscribers_by_topic(&message.topic());

        for sub in sub_list {
            if sub.transport_msg(message.clone()).await.is_err() {
                // If an error occurs, we consider the subscriber offline,
                // so un_subscribe here.
                let req = UnSubRequest {
                    subscriber_id: sub.id(),
                };

                if let Err(e) = self.subscribe_manager.un_subscribe(req.clone()) {
                    error!(e; "failed to un_subscribe, req: {:?}", req);
                }
            }
        }
    }
}
