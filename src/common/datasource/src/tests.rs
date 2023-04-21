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

use crate::test_util::{self};

#[tokio::test]
async fn test_stream_to_json() {
    // A small threshold
    // Triggers the flush each writes
    test_util::setup_stream_to_json_test(
        &test_util::get_data_dir("tests/json/basic.json")
            .display()
            .to_string(),
        |size| size / 2,
    )
    .await;

    // A large threshold
    // Only triggers the flush at last
    test_util::setup_stream_to_json_test(
        &test_util::get_data_dir("tests/json/basic.json")
            .display()
            .to_string(),
        |size| size * 2,
    )
    .await;
}
