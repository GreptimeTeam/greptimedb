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

use api::v1::meta::HeartbeatRequest;
use tokio::sync::mpsc::{Receiver, Sender};

use super::DefaultSubscribeManager;
use crate::pubsub::{
    AddSubRequest, DefaultPublish, Message, Publish, SubscribeManager, SubscribeQuery, Subscriber,
    Topic, UnSubRequest,
};

#[tokio::test]
async fn test_pubsub() {
    let manager = Arc::new(DefaultSubscribeManager::default());

    let (subscriber1, mut rx1) = mock_subscriber(1, "tidigong");
    let req = AddSubRequest {
        topic_list: vec![Topic::Heartbeat],
        subscriber: subscriber1,
    };
    manager.subscribe(req).unwrap();

    let (subscriber2, mut rx2) = mock_subscriber(2, "gcrm");
    let req = AddSubRequest {
        topic_list: vec![Topic::Heartbeat],
        subscriber: subscriber2,
    };
    manager.subscribe(req).unwrap();

    let manager_clone = manager.clone();
    let message_number: usize = 5;
    tokio::spawn(async move {
        let publisher: DefaultPublish<DefaultSubscribeManager<Sender<Message>>, Sender<Message>> =
            DefaultPublish::new(manager_clone);
        for _ in 0..message_number {
            publisher.send_msg(mock_message()).await;
        }
    });

    for _ in 0..message_number {
        let msg = rx1.recv().await.unwrap();
        check_message(msg);
        let msg = rx2.recv().await.unwrap();
        check_message(msg);
    }

    manager
        .un_subscribe(UnSubRequest { subscriber_id: 1 })
        .unwrap();
    let may_msg = rx1.recv().await;
    assert!(may_msg.is_none());

    manager.un_subscribe_all().unwrap();
    let may_msg = rx2.recv().await;
    assert!(may_msg.is_none());
}

#[tokio::test]
async fn test_subscriber_disconnect() {
    let manager = Arc::new(DefaultSubscribeManager::default());

    let (subscriber1, rx1) = mock_subscriber(1, "tidigong");
    let req = AddSubRequest {
        topic_list: vec![Topic::Heartbeat],
        subscriber: subscriber1,
    };
    manager.subscribe(req).unwrap();

    let (subscriber2, rx2) = mock_subscriber(2, "gcrm");
    let req = AddSubRequest {
        topic_list: vec![Topic::Heartbeat],
        subscriber: subscriber2,
    };
    manager.subscribe(req).unwrap();

    let manager_clone = manager.clone();
    let message_number: usize = 5;
    let join = tokio::spawn(async move {
        let publisher: DefaultPublish<DefaultSubscribeManager<Sender<Message>>, Sender<Message>> =
            DefaultPublish::new(manager_clone);
        for _ in 0..message_number {
            publisher.send_msg(mock_message()).await;
        }
    });

    // Simulate subscriber disconnection.
    std::mem::drop(rx1);
    std::mem::drop(rx2);

    join.await.unwrap();

    let subscriber_list = manager.subscribers_by_topic(&Topic::Heartbeat);
    assert!(subscriber_list.is_empty());
}

#[test]
fn test_message() {
    let msg = Message::Heartbeat(Box::default());
    assert_eq!(Topic::Heartbeat, msg.topic());
}

#[test]
fn test_sub_manager() {
    let manager = DefaultSubscribeManager::default();

    let subscriber = mock_subscriber(1, "tidigong").0;
    let req = AddSubRequest {
        topic_list: vec![Topic::Heartbeat],
        subscriber,
    };
    manager.subscribe(req).unwrap();
    let ret = manager.subscribers_by_topic(&Topic::Heartbeat);
    assert_eq!(1, ret.len());

    let subscriber = mock_subscriber(2, "gcrm").0;
    let req = AddSubRequest {
        topic_list: vec![Topic::Heartbeat],
        subscriber,
    };
    manager.subscribe(req).unwrap();
    let ret = manager.subscribers_by_topic(&Topic::Heartbeat);
    assert_eq!(2, ret.len());

    let req = UnSubRequest { subscriber_id: 1 };
    manager.un_subscribe(req).unwrap();
    let ret = manager.subscribers_by_topic(&Topic::Heartbeat);
    assert_eq!(1, ret.len());

    let req = UnSubRequest { subscriber_id: 2 };
    manager.un_subscribe(req).unwrap();
    let ret = manager.subscribers_by_topic(&Topic::Heartbeat);
    assert_eq!(0, ret.len());
}

#[tokio::test]
async fn test_subscriber() {
    let (subscriber, mut rx) = mock_subscriber(1, "tudigong");
    assert_eq!(1, subscriber.id());
    assert_eq!("tudigong", subscriber.name());

    subscriber.transport_msg(mock_message()).await.unwrap();

    let may_msg = rx.recv().await;
    assert!(may_msg.is_some());
    match may_msg.unwrap() {
        Message::Heartbeat(hb) => {
            assert_eq!(123, hb.duration_since_epoch);
        }
    }
}

fn mock_subscriber(id: u32, name: &str) -> (Subscriber<Sender<Message>>, Receiver<Message>) {
    let (tx, rx) = tokio::sync::mpsc::channel(1024);
    let sub = Subscriber::new(id, name, tx);
    (sub, rx)
}

fn mock_message() -> Message {
    Message::Heartbeat(Box::new(HeartbeatRequest {
        duration_since_epoch: 123,
        ..Default::default()
    }))
}

fn check_message(message: Message) {
    match message {
        Message::Heartbeat(hb) => {
            assert_eq!(123, hb.duration_since_epoch);
        }
    }
}
