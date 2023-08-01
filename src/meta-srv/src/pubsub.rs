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

use api::v1::meta::HeartbeatRequest;

mod publish;
mod subscribe_manager;
mod subscriber;
#[cfg(test)]
mod tests;

pub use publish::{DefaultPublish, Publish, PublishRef};
pub use subscribe_manager::{
    AddSubRequest, DefaultSubscribeManager, SubscribeManager, SubscribeManagerRef, SubscribeQuery,
    UnSubRequest,
};
pub use subscriber::{Subscriber, SubscriberRef, Transport};

/// Subscribed topic type, determined by the ability of meta.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum Topic {
    Heartbeat,
}

#[derive(Clone, Debug)]
pub enum Message {
    Heartbeat(Box<HeartbeatRequest>),
}

impl Message {
    pub fn topic(&self) -> Topic {
        match self {
            Message::Heartbeat(_) => Topic::Heartbeat,
        }
    }
}
