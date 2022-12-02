// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(clippy::all)]
tonic::include_proto!("greptime.storage.wal.v1");

use crate::write_batch::{Mutation, WriteBatch};

pub fn gen_mutation_types(write_batch: &WriteBatch) -> Vec<i32> {
    write_batch
        .iter()
        .map(|m| match m {
            Mutation::Put(_) => MutationType::Put.into(),
        })
        .collect::<Vec<_>>()
}

impl WalHeader {
    pub fn with_last_manifest_version(last_manifest_version: u64) -> Self {
        Self {
            last_manifest_version,
            ..Default::default()
        }
    }
}
