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

use crate::adapter::FlowWorkerManagerRef;
use crate::{Error, FlownodeBuilder};

impl FlownodeBuilder {
    /// Create a series of tasks to refill flow, will be transfer to flownode if
    ///
    /// tasks havn't completed, and will show up in `flows` table
    async fn start_refill_flows(&self, manager: &FlowWorkerManagerRef) -> Result<(), Error> {
        todo!()
    }
}
