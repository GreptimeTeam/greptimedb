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

use std::io::BufReader;

use arrow_schema::Schema;
use async_trait::async_trait;
use datafusion::avro_to_arrow::{self, read_avro_schema_from_reader};
use object_store::{ObjectStore, Reader};
use snafu::ResultExt;
use tokio_util::io::SyncIoBridge;

use crate::error::{self, Result};
use crate::file_format::FileFormat;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AvroFormat {}

// #[derive(Debug, Clone, Builder)]
pub struct AvroConfig {
// TODO
}

#[async_trait]
impl FileFormat for AvroFormat {

    async fn infer_schema(&self, store: &ObjectStore, path: &str) -> Result<Schema> {
        let reader: Reader = store
            .reader(path)
            .await
            .context(error::ReadObjectSnafu { path })?;

        common_runtime::spawn_blocking_read(move || {
            let mut buf_reader = BufReader::new(SyncIoBridge::new(reader));

            let schema = read_avro_schema_from_reader(&mut buf_reader)
                .context(error::AvroToSchemaSnafu)?;
            Ok(schema)
        })
        .await
        .context(error::JoinHandleSnafu)?
    }
}

#[derive(Debug, Clone)]
pub struct AvroOpener {
    //TODO
    config: Arc<AvroConfig>,
    object_store: Arc<ObjectStore>,    
}