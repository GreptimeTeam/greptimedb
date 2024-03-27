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

use std::any::Any;

use common_procedure::Status;
use serde::{Deserialize, Serialize};

use crate::ddl::drop_database::{DropDatabaseContext, State};
use crate::ddl::DdlContext;
use crate::error::Result;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DropDatabaseEnd;

#[async_trait::async_trait]
#[typetag::serde]
impl State for DropDatabaseEnd {
    async fn next(
        &mut self,
        _: &DdlContext,
        _: &mut DropDatabaseContext,
    ) -> Result<(Box<dyn State>, Status)> {
        Ok((Box::new(DropDatabaseEnd), Status::done()))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
