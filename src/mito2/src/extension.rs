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
use common_time::range::TimestampRange;
use common_time::Timestamp;
use store_api::storage::{RegionId, ScanRequest};

use crate::error::Result;
use crate::read::BoxedBatchStream;

pub type InclusiveTimeRange = (Timestamp, Timestamp);

pub trait ExtensionRange: Send + Sync {
    fn num_rows(&self) -> u64;

    fn time_range(&self) -> InclusiveTimeRange;

    fn num_row_groups(&self) -> u64;

    fn reader(&self) -> BoxedExtensionRangeReader;
}

pub(crate) type BoxedExtensionRange = Box<dyn ExtensionRange>;

pub trait ExtensionRangeReader: Send {
    fn read(self: Box<Self>) -> BoxedBatchStream;
}

pub type BoxedExtensionRangeReader = Box<dyn ExtensionRangeReader>;

#[async_trait]
pub(crate) trait ExtensionRangeProvider: Send + Sync {
    async fn find_extension_ranges(
        &self,
        region_id: RegionId,
        timestamp_range: TimestampRange,
        request: &ScanRequest,
    ) -> Result<Vec<BoxedExtensionRange>> {
        let _ = region_id;
        let _ = timestamp_range;
        let _ = request;
        Ok(vec![])
    }
}

pub(crate) type BoxedExtensionRangeProvider = Box<dyn ExtensionRangeProvider>;
