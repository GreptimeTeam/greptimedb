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

use std::time::Duration;

use common_base::readable_size::ReadableSize;
use common_grpc::channel_manager::{self, ChannelConfig};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DatanodeClientOptions {
    pub client: ClientOptions,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ClientOptions {
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub connect_timeout: Duration,
    pub tcp_nodelay: bool,
    /// Maximum size of a message received from a datanode.
    pub max_recv_message_size: ReadableSize,
    /// Maximum size of a message sent to a datanode.
    pub max_send_message_size: ReadableSize,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(channel_manager::DEFAULT_GRPC_REQUEST_TIMEOUT_SECS),
            connect_timeout: Duration::from_secs(
                channel_manager::DEFAULT_GRPC_CONNECT_TIMEOUT_SECS,
            ),
            tcp_nodelay: true,
            max_recv_message_size: channel_manager::DEFAULT_MAX_GRPC_RECV_MESSAGE_SIZE,
            max_send_message_size: channel_manager::DEFAULT_MAX_GRPC_SEND_MESSAGE_SIZE,
        }
    }
}

impl ClientOptions {
    /// Creates a gRPC [`ChannelConfig`] from these datanode client options.
    pub fn channel_config(&self) -> ChannelConfig {
        ChannelConfig {
            timeout: Some(self.timeout),
            connect_timeout: Some(self.connect_timeout),
            tcp_nodelay: self.tcp_nodelay,
            max_recv_message_size: self.max_recv_message_size,
            max_send_message_size: self.max_send_message_size,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use common_base::readable_size::ReadableSize;
    use serde_json::json;

    use super::ClientOptions;

    #[test]
    fn test_client_options_backward_compatibility() {
        let options: ClientOptions = serde_json::from_value(json!({
            "timeout": "10s",
            "connect_timeout": "5s",
            "tcp_nodelay": false
        }))
        .unwrap();

        assert_eq!(ReadableSize::mb(512), options.max_recv_message_size);
        assert_eq!(ReadableSize::mb(512), options.max_send_message_size);
    }

    #[test]
    fn test_client_options_channel_config() {
        let options: ClientOptions = serde_json::from_value(json!({
            "timeout": "20s",
            "connect_timeout": "8s",
            "tcp_nodelay": false,
            "max_recv_message_size": "1GB",
            "max_send_message_size": "2GB"
        }))
        .unwrap();

        let channel_config = options.channel_config();
        assert_eq!(Some(Duration::from_secs(20)), channel_config.timeout);
        assert_eq!(Some(Duration::from_secs(8)), channel_config.connect_timeout);
        assert!(!channel_config.tcp_nodelay);
        assert_eq!(ReadableSize::gb(1), channel_config.max_recv_message_size);
        assert_eq!(ReadableSize::gb(2), channel_config.max_send_message_size);
    }
}
