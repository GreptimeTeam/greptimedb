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

use std::collections::HashMap;

use testcontainers::core::WaitFor;

/// Through which port the Zookeeper node listens for external traffics, i.e. traffics from the Kafka node.
pub const ZOOKEEPER_PORT: u16 = 2181;
/// Through which port the Kafka node listens for internal traffics, i.e. traffics between Kafka nodes in the same Kafka cluster.
pub const KAFAK_LISTENER_PORT: u16 = 19092;
/// Through which port the Kafka node listens for external traffics, e.g. traffics from Kafka clients.
pub const KAFKA_ADVERTISED_LISTENER_PORT: u16 = 9092;

/// Configurations for a Kafka runtime.
/// Since the runtime corresponds to a cluster with a single Kafka node and a single Zookeeper node, the ports are all singletons.
pub struct Config {
    /// The name of the Kafka image hosted in the docker hub.
    pub image_name: String,
    /// The tag of the kafka image hosted in the docker hub.
    /// Warning: please use a tag with long-term support. Do not use `latest` or any other tags that
    /// the underlying image may suddenly change.
    pub image_tag: String,
    /// Through which port clients could connect with the runtime.
    pub exposed_port: u16,
    /// The runtime is regarded ready to be used if all ready conditions are met.
    /// Warning: be sure to update the conditions when necessary if the image is altered.
    pub ready_conditions: Vec<WaitFor>,
    /// The environment variables required to run the runtime.
    /// Warning: be sure to update the environment variables when necessary if the image is altered.
    pub env_vars: HashMap<String, String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            image_name: "confluentinc/cp-kafka".to_string(),
            image_tag: "7.4.3".to_string(),
            exposed_port: KAFKA_ADVERTISED_LISTENER_PORT,
            // The runtime is safe to be used as long as this message is printed on stdout.
            ready_conditions: vec![WaitFor::message_on_stdout(
                "started (kafka.server.KafkaServer)",
            )],
            env_vars: build_env_vars(
                ZOOKEEPER_PORT,
                KAFAK_LISTENER_PORT,
                KAFKA_ADVERTISED_LISTENER_PORT,
            ),
        }
    }
}

fn build_env_vars(
    zookeeper_port: u16,
    kafka_listener_port: u16,
    kafka_advertised_listener_port: u16,
) -> HashMap<String, String> {
    [
        (
            "KAFKA_ZOOKEEPER_CONNECT".to_string(),
            format!("localhost:{zookeeper_port}"),
        ),
        (
            "KAFKA_LISTENERS".to_string(),
            format!("PLAINTEXT://0.0.0.0:{kafka_advertised_listener_port},PLAINTEXT://0.0.0.0:{kafka_listener_port}"),
        ),
        (
            "KAFKA_ADVERTISED_LISTENERS".to_string(),
            format!("PLAINTEXT://localhost:{kafka_advertised_listener_port},PLAINTEXT://localhost:{kafka_listener_port}",),
        ),
        (
            "KAFKA_INTER_BROKER_LISTENER_NAME".to_string(),
            "BROKER".to_string(),
        ),
        ("KAFKA_BROKER_ID".to_string(), "1".to_string()),
        (
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR".to_string(),
            "1".to_string(),
        ),
    ]
    .into()
}
