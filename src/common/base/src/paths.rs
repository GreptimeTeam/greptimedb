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

//! Path constants for table engines, cluster states and WAL
/// All paths relative to data_home(file storage) or root path(S3, OSS etc).

/// WAL dir for local file storage
pub const WAL_DIR: &str = "wal/";

/// Data dir for table engines
pub const DATA_DIR: &str = "data/";

/// Cluster state dir
pub const CLUSTER_DIR: &str = "cluster/";
