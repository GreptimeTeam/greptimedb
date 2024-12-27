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

use common_datasource::file_format::Format;
use common_query::Output;
use common_telemetry::{debug, tracing};
use snafu::ResultExt;
use table::requests::CopyQueryToRequest;

use crate::error;
use crate::error::Result;
use crate::statement::StatementExecutor;

impl StatementExecutor {
    #[tracing::instrument(skip_all)]
    pub(crate) async fn copy_query_to(
        &self,
        req: CopyQueryToRequest,
        query_output: Output,
    ) -> Result<usize> {
        let CopyQueryToRequest {
            with,
            connection,
            location,
        } = &req;
        let format = Format::try_from(with).context(error::ParseFileFormatSnafu)?;

        self.copy_to_file(&format, query_output, location, connection, |path| {
            debug!("Copy query to path: {path}")
        })
        .await
    }
}
