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

/// Gets broker endpoints from environment variables with the given key.
/// Returns the default ["localhost:9092"] if no environment variables set for broker endpoints.
#[macro_export]
macro_rules! get_broker_endpoints {
    () => {{
        let broker_endpoints = std::env::var("GT_KAFKA_ENDPOINTS")
            .unwrap_or("localhost:9092".to_string())
            .split(',')
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        assert!(!broker_endpoints.is_empty());
        broker_endpoints
    }};
}
