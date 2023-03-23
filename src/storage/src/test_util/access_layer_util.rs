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

use crate::read::BoxedBatchReader;
use crate::sst::{AccessLayer, FileHandle, FileId, ReadOptions, Source, SstInfo, WriteOptions};

#[derive(Debug)]
pub struct MockAccessLayer;

#[async_trait::async_trait]
impl AccessLayer for MockAccessLayer {
    fn sst_file_path(&self, file_name: &str) -> String {
        file_name.to_string()
    }

    async fn write_sst(
        &self,
        _file_id: FileId,
        _source: Source,
        _opts: &WriteOptions,
    ) -> crate::error::Result<SstInfo> {
        unimplemented!()
    }

    async fn read_sst(
        &self,
        _file_handle: FileHandle,
        _opts: &ReadOptions,
    ) -> crate::error::Result<BoxedBatchReader> {
        unimplemented!()
    }

    async fn delete_sst(&self, _file_id: FileId) -> crate::error::Result<()> {
        Ok(())
    }
}
