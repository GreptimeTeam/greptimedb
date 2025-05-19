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

use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_time::range::TimestampRange;
use common_time::Timestamp;
use store_api::storage::ScanRequest;

use crate::error::Result;
use crate::read::range::RowGroupIndex;
use crate::read::scan_region::StreamContext;
use crate::read::scan_util::PartitionMetrics;
use crate::read::BoxedBatchStream;
use crate::region::MitoRegionRef;

pub type InclusiveTimeRange = (Timestamp, Timestamp);

pub trait ExtensionRange: Send + Sync {
    fn num_rows(&self) -> u64;

    fn time_range(&self) -> InclusiveTimeRange;

    fn num_row_groups(&self) -> u64;

    fn reader(&self, context: &StreamContext) -> BoxedExtensionRangeReader;
}

pub type BoxedExtensionRange = Box<dyn ExtensionRange>;

#[async_trait]
pub trait ExtensionRangeReader: Send {
    async fn read(
        self: Box<Self>,
        stream_ctx: Arc<StreamContext>,
        part_metrics: PartitionMetrics,
        index: RowGroupIndex,
    ) -> Result<BoxedBatchStream, BoxedError>;
}

pub type BoxedExtensionRangeReader = Box<dyn ExtensionRangeReader>;

#[async_trait]
pub trait ExtensionRangeProvider: Send + Sync {
    async fn find_extension_ranges(
        &self,
        timestamp_range: TimestampRange,
        request: &ScanRequest,
    ) -> Result<Vec<BoxedExtensionRange>> {
        let _ = timestamp_range;
        let _ = request;
        Ok(vec![])
    }
}

pub type BoxedExtensionRangeProvider = Box<dyn ExtensionRangeProvider>;

pub trait ExtensionRangeProviderFactory: Send + Sync {
    fn create_extension_range_provider(&self, region: MitoRegionRef)
        -> BoxedExtensionRangeProvider;
}

pub type BoxedExtensionRangeProviderFactory = Box<dyn ExtensionRangeProviderFactory>;
