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

use std::sync::Arc;

use api::v1::RowInsertRequests;
use client::error::{ExternalSnafu, Result as ClientResult};
use client::inserter::{Context, InsertOptions, Inserter};
use common_error::ext::BoxedError;
use snafu::ResultExt;

use crate::utils::database::{DatabaseContext, DatabaseOperatorRef};

pub type InsertForwarderRef = Arc<InsertForwarder>;

/// [`InsertForwarder`] is the inserter for the metasrv.
/// It forwards insert requests to available frontend instances.
pub struct InsertForwarder {
    database_operator: DatabaseOperatorRef,
    options: Option<InsertOptions>,
}

impl InsertForwarder {
    /// Creates a new InsertForwarder with the given peer lookup service.
    pub fn new(database_operator: DatabaseOperatorRef, options: Option<InsertOptions>) -> Self {
        Self {
            database_operator,
            options,
        }
    }
}

#[async_trait::async_trait]
impl Inserter for InsertForwarder {
    async fn insert_rows(
        &self,
        context: &Context<'_>,
        requests: RowInsertRequests,
    ) -> ClientResult<()> {
        let ctx = DatabaseContext::new(context.catalog, context.schema);
        let hints = self.options.as_ref().map_or(vec![], |o| o.to_hints());

        self.database_operator
            .insert(
                &ctx,
                requests,
                &hints
                    .iter()
                    .map(|(k, v)| (*k, v.as_str()))
                    .collect::<Vec<_>>(),
            )
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;

        Ok(())
    }

    fn set_options(&mut self, options: &InsertOptions) {
        self.options = Some(*options);
    }
}
