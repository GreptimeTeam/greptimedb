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

fn main() {
    tonic_build::configure()
        .compile(
            &[
                "greptime/v1/database.proto",
                "greptime/v1/meta/common.proto",
                "greptime/v1/meta/heartbeat.proto",
                "greptime/v1/meta/route.proto",
                "greptime/v1/meta/store.proto",
                "prometheus/remote/remote.proto",
            ],
            &["."],
        )
        .expect("compile proto");
}
