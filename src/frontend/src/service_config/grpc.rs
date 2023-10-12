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

use common_base::readable_size::ReadableSize;
use common_grpc::channel_manager::{
    DEFAULT_MAX_GRPC_RECV_MESSAGE_SIZE, DEFAULT_MAX_GRPC_SEND_MESSAGE_SIZE,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GrpcOptions {
    pub addr: String,
    pub runtime_size: usize,
    // Max gRPC receiving(decoding) message size
    pub max_recv_message_size: ReadableSize,
    // Max gRPC sending(encoding) message size
    pub max_send_message_size: ReadableSize,
}

impl Default for GrpcOptions {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:4001".to_string(),
            runtime_size: 8,
            max_recv_message_size: DEFAULT_MAX_GRPC_RECV_MESSAGE_SIZE,
            max_send_message_size: DEFAULT_MAX_GRPC_SEND_MESSAGE_SIZE,
        }
    }
}
