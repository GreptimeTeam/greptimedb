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

#[macro_use]
mod grpc;
#[macro_use]
mod http;
#[macro_use]
mod sql;
#[macro_use]
mod region_migration;
// #[macro_use]
// mod region_failover;

grpc_tests!(File, S3, S3WithCache, Oss, Azblob, Gcs);
http_tests!(File, S3, S3WithCache, Oss, Azblob, Gcs);
// region_failover_tests!(File, S3, S3WithCache, Oss, Azblob);
sql_tests!(File);

region_migration_tests!(File);

// TODO(niebayes): add integration tests for remote wal.
