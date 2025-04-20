use common_macro::admin_fn;
use common_query::error::{MissingMetadataSnapshotHandlerSnafu, Result};
use common_query::prelude::{Signature, Volatility};
use datatypes::prelude::*;
use session::context::QueryContextRef;

use crate::handlers::MetadataSnapshotHandlerRef;

const METADATA_DIR: &str = "/snaphost/";
const METADATA_FILE_NAME: &str = "dump_metadata";
const METADATA_FILE_EXTENSION: &str = "metadata.fb";

#[admin_fn(
    name = DumpMetadataFunction,
    display_name = dump_metadata,
    sig_fn = dump_signature,
    ret = string
)]
pub(crate) async fn dump_metadata(
    metadata_snapshot_handler: &MetadataSnapshotHandlerRef,
    _query_ctx: &QueryContextRef,
    _params: &[ValueRef<'_>],
) -> Result<Value> {
    let filename = metadata_snapshot_handler
        .dump(METADATA_DIR, METADATA_FILE_NAME)
        .await?;
    Ok(Value::from(filename))
}

fn dump_signature() -> Signature {
    Signature::uniform(0, vec![], Volatility::Immutable)
}

#[admin_fn(
    name = RestoreMetadataFunction,
    display_name = restore_metadata,
    sig_fn = restore_signature,
    ret = uint64,
)]
pub(crate) async fn restore_metadata(
    metadata_snapshot_handler: &MetadataSnapshotHandlerRef,
    _query_ctx: &QueryContextRef,
    _params: &[ValueRef<'_>],
) -> Result<Value> {
    let num_keyvalues = metadata_snapshot_handler
        .restore(
            METADATA_DIR,
            &format!("{METADATA_FILE_NAME}.{METADATA_FILE_EXTENSION}"),
        )
        .await?;
    Ok(Value::from(num_keyvalues))
}

fn restore_signature() -> Signature {
    Signature::uniform(0, vec![], Volatility::Immutable)
}
