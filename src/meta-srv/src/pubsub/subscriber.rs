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

use snafu::ResultExt;
use tokio::sync::mpsc::Sender;

use crate::error::{self, Result};
use crate::pubsub::Message;

#[derive(Debug)]
pub struct Subscriber<T> {
    /// Subscriber's id, globally unique, assigned by leader meta.
    id: u32,
    /// Subscriber's name, passed in by subscriber.
    name: String,
    /// Transport channel from meta to subscriber.
    transporter: T,
}

pub type SubscriberRef<T> = Arc<Subscriber<T>>;

impl<T> Subscriber<T> {
    pub fn new(id: u32, name: impl Into<String>, transporter: T) -> Self {
        let name = name.into();

        Self {
            id,
            name,
            transporter,
        }
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<T> Subscriber<T>
where
    T: Transport,
{
    pub async fn transport_msg(&self, message: Message) -> Result<()> {
        self.transporter.transport_msg(message).await
    }
}

/// This trait defines how messages are delivered from meta to the subscriber.
#[async_trait::async_trait]
pub trait Transport: Send + Sync {
    async fn transport_msg(&self, msg: Message) -> Result<()>;
}

#[async_trait::async_trait]
impl Transport for Sender<Message> {
    async fn transport_msg(&self, msg: Message) -> Result<()> {
        self.send(msg).await.context(error::PublishMessageSnafu)
    }
}
