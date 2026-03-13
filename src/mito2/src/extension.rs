use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_time::Timestamp;
use common_time::range::TimestampRange;
use store_api::storage::{ScanRequest, SequenceNumber};

use crate::error::Result;
use crate::read::range::RowGroupIndex;
use crate::read::scan_region::StreamContext;
use crate::read::scan_util::PartitionMetrics;
use crate::read::{BoxedBatchStream, BoxedRecordBatchStream};
use crate::region::MitoRegionRef;

pub type InclusiveTimeRange = (Timestamp, Timestamp);

/// [`ExtensionRange`] is used to represent a scannable "range" for mito engine, just like the
/// memtable range and sst file range, but resides on the outside.
/// It can be scanned side by side as other ranges to produce the final result, so it's very useful
/// to extend the source of data in GreptimeDB.
pub trait ExtensionRange: Debug + Send + Sync {
    /// The number of rows in this range.
    fn num_rows(&self) -> u64;

    /// The timestamp of the start and end (both inclusive) of the data within this range.
    fn time_range(&self) -> InclusiveTimeRange;

    /// The row groups number in this range.
    fn num_row_groups(&self) -> u64;

    /// Create the reader for reading this range.
    fn reader(&self, context: &StreamContext) -> BoxedExtensionRangeReader;

    /// Create the flat reader for reading this range in flat format.
    fn flat_reader(&self, context: &StreamContext) -> BoxedExtensionFlatRangeReader;
}

pub type BoxedExtensionRange = Box<dyn ExtensionRange>;

/// The reader to read an extension range.
#[async_trait]
pub trait ExtensionRangeReader: Send {
    /// Read the extension range by creating a stream that produces [`Batch`].
    async fn read(
        self: Box<Self>,
        context: Arc<StreamContext>,
        metrics: PartitionMetrics,
        index: RowGroupIndex,
    ) -> Result<BoxedBatchStream, BoxedError>;
}

pub type BoxedExtensionRangeReader = Box<dyn ExtensionRangeReader>;

/// The reader to read an extension range in flat format (producing [`RecordBatch`]).
#[async_trait]
pub trait ExtensionFlatRangeReader: Send {
    /// Read the extension range by creating a stream that produces [`RecordBatch`].
    async fn read(
        self: Box<Self>,
        context: Arc<StreamContext>,
        metrics: PartitionMetrics,
        index: RowGroupIndex,
    ) -> Result<BoxedRecordBatchStream, BoxedError>;
}

pub type BoxedExtensionFlatRangeReader = Box<dyn ExtensionFlatRangeReader>;

/// The provider to feed the extension ranges into the mito scanner.
#[async_trait]
pub trait ExtensionRangeProvider: Send + Sync {
    /// Find the extension ranges by the timestamp filter and the [`ScanRequest`].
    async fn find_extension_ranges(
        &self,
        flushed_sequence: SequenceNumber,
        timestamp_range: TimestampRange,
        request: &ScanRequest,
    ) -> Result<Vec<BoxedExtensionRange>> {
        let _ = flushed_sequence;
        let _ = timestamp_range;
        let _ = request;
        Ok(vec![])
    }
}

pub type BoxedExtensionRangeProvider = Box<dyn ExtensionRangeProvider>;

/// The factory to create an [`ExtensionRangeProvider`], injecting some utilities.
pub trait ExtensionRangeProviderFactory: Send + Sync {
    fn create_extension_range_provider(&self, region: MitoRegionRef)
    -> BoxedExtensionRangeProvider;
}

pub type BoxedExtensionRangeProviderFactory = Box<dyn ExtensionRangeProviderFactory>;
