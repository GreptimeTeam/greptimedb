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

use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_function::handlers::MetadataSnapshotHandler;
use common_meta::snapshot::MetadataSnapshotManager;
use common_query::error as query_error;
use common_query::error::Result as QueryResult;
use snafu::ResultExt;

/// The operator of the metadata snapshot.
pub struct MetadataSnapshotOperator {
    operator: MetadataSnapshotManager,
}

impl MetadataSnapshotOperator {
    pub fn new(operator: MetadataSnapshotManager) -> Self {
        Self { operator }
    }
}

#[async_trait]
impl MetadataSnapshotHandler for MetadataSnapshotOperator {
    async fn dump(&self, path: &str, filename: &str) -> QueryResult<String> {
        self.operator
            .dump(path, filename)
            .await
            .map_err(BoxedError::new)
            .map(|(file, _)| file)
            .context(query_error::MetadataSnapshotSnafu)
    }

    async fn restore(&self, path: &str, filename: &str) -> QueryResult<u64> {
        let filepath = format!("{}{}", path, filename);
        self.operator
            .restore(&filepath)
            .await
            .map_err(BoxedError::new)
            .context(query_error::MetadataSnapshotSnafu)
    }
}
