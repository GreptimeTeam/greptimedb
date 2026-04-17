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
use clap::Parser;
use common_error::ext::BoxedError;
use common_meta::key::catalog_name::{CatalogNameKey, CatalogNameValue};
use common_meta::key::datanode_table::DatanodeTableKey;
use common_meta::key::flow::flow_state::FlowStateValue;
use common_meta::key::flow::{
    flow_info_key_prefix, flow_name_key_prefix, flow_route_key_prefix, flow_state_full_key,
    flownode_flow_key_prefix, table_flow_key_prefix,
};
use common_meta::key::node_address::{NodeAddressKey, NodeAddressValue};
use common_meta::key::schema_name::{SchemaNameKey, SchemaNameValue};
use common_meta::key::table_info::TableInfoKey;
use common_meta::key::table_name::TableNameKey;
use common_meta::key::table_repart::{TableRepartKey, TableRepartValue};
use common_meta::key::table_route::TableRouteKey;
use common_meta::key::topic_name::{TopicNameKey, TopicNameValue};
use common_meta::key::topic_region::{TopicRegionKey, TopicRegionValue};
use common_meta::key::view_info::{ViewInfoKey, ViewInfoValue};
use common_meta::key::{
    CATALOG_NAME_KEY_PREFIX, DATANODE_TABLE_KEY_PREFIX, KAFKA_TOPIC_KEY_PREFIX, MetadataKey,
    MetadataValue, NODE_ADDRESS_PREFIX, SCHEMA_NAME_KEY_PREFIX, TABLE_INFO_KEY_PREFIX,
    TABLE_NAME_KEY_PREFIX, TABLE_REPART_PREFIX, TABLE_ROUTE_PREFIX, TOPIC_REGION_PREFIX,
    VIEW_INFO_KEY_PREFIX,
};
use common_meta::kv_backend::KvBackendRef;
use common_meta::rpc::store::PutRequest;

use crate::Tool;
use crate::common::StoreConfig;
use crate::error::InvalidArgumentsSnafu;
use crate::metadata::control::put::read_value;

/// Put a key-value pair into the metadata store.
#[derive(Debug, Default, Parser)]
pub struct PutKeyCommand {
    /// The key to put into the metadata store.
    key: String,

    /// Read the value to put into the metadata store from standard input.
    #[clap(long, required = true)]
    value_stdin: bool,

    /// Skip metadata validation before writing.
    #[clap(long)]
    no_validate: bool,

    #[clap(flatten)]
    store: StoreConfig,
}

impl PutKeyCommand {
    pub async fn build(&self) -> Result<Box<dyn Tool>, BoxedError> {
        let kv_backend = self.store.build().await?;
        self.build_tool(tokio::io::stdin(), kv_backend).await
    }

    async fn build_tool<R>(
        &self,
        reader: R,
        kv_backend: KvBackendRef,
    ) -> Result<Box<dyn Tool>, BoxedError>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        Ok(Box::new(PutKeyTool {
            kv_backend,
            key: self.key.clone(),
            value: read_value(reader).await?,
            no_validate: self.no_validate,
        }))
    }
}

struct PutKeyTool {
    kv_backend: KvBackendRef,
    key: String,
    value: Vec<u8>,
    no_validate: bool,
}

#[async_trait]
impl Tool for PutKeyTool {
    async fn do_work(&self) -> Result<(), BoxedError> {
        if !self.no_validate {
            validate_metadata_value(&self.key, &self.value)?;
        }

        let request = PutRequest::new()
            .with_key(self.key.as_bytes())
            .with_value(self.value.clone());
        self.kv_backend
            .put(request)
            .await
            .map_err(BoxedError::new)?;

        println!("Key({}) updated", self.key);
        Ok(())
    }
}

fn validate_metadata_value(key: &str, value: &[u8]) -> Result<(), BoxedError> {
    if key == flow_state_full_key() {
        validate_value(key, value, FlowStateValue::try_from_raw_value)?;
        return Ok(());
    }

    if matches_key_prefix(key, TABLE_ROUTE_PREFIX) {
        validate_key(TableRouteKey::from_bytes(key.as_bytes()), key)?;
    }

    if matches_key_prefix(key, TABLE_INFO_KEY_PREFIX) {
        validate_key(TableInfoKey::from_bytes(key.as_bytes()), key)?;
    }

    if matches_key_prefix(key, TABLE_NAME_KEY_PREFIX) {
        validate_key(TableNameKey::from_bytes(key.as_bytes()), key)?;
    }

    if matches_key_prefix(key, DATANODE_TABLE_KEY_PREFIX) {
        validate_key(DatanodeTableKey::from_bytes(key.as_bytes()), key)?;
    }

    if let Some(reason) = unsupported_direct_put_reason(key) {
        return Err(BoxedError::new(
            InvalidArgumentsSnafu {
                msg: format!("{reason}, use --no-validate to bypass"),
            }
            .build(),
        ));
    }

    if matches_key_prefix(key, VIEW_INFO_KEY_PREFIX) {
        validate_key(ViewInfoKey::from_bytes(key.as_bytes()), key)?;
        validate_value(key, value, ViewInfoValue::try_from_raw_value)?;
        return Ok(());
    }

    if matches_key_prefix(key, CATALOG_NAME_KEY_PREFIX) {
        validate_key(CatalogNameKey::from_bytes(key.as_bytes()), key)?;
        CatalogNameValue::try_from_raw_value(value).map_err(BoxedError::new)?;
        return Ok(());
    }

    if matches_key_prefix(key, SCHEMA_NAME_KEY_PREFIX) {
        validate_key(SchemaNameKey::from_bytes(key.as_bytes()), key)?;
        validate_value(key, value, SchemaNameValue::try_from_raw_value)?;
        return Ok(());
    }

    if matches_key_prefix(key, TABLE_REPART_PREFIX) {
        validate_key(TableRepartKey::from_bytes(key.as_bytes()), key)?;
        validate_value(key, value, TableRepartValue::try_from_raw_value)?;
        return Ok(());
    }

    if matches_key_prefix(key, NODE_ADDRESS_PREFIX) {
        validate_key(NodeAddressKey::from_bytes(key.as_bytes()), key)?;
        validate_value(key, value, NodeAddressValue::try_from_raw_value)?;
        return Ok(());
    }

    if matches_key_prefix(key, KAFKA_TOPIC_KEY_PREFIX) {
        validate_key(TopicNameKey::from_bytes(key.as_bytes()), key)?;
        validate_value(key, value, TopicNameValue::try_from_raw_value)?;
        return Ok(());
    }

    if matches_key_prefix(key, TOPIC_REGION_PREFIX) {
        validate_key(TopicRegionKey::from_bytes(key.as_bytes()), key)?;
        validate_value(key, value, TopicRegionValue::try_from_raw_value)?;
        return Ok(());
    }

    Err(BoxedError::new(
        InvalidArgumentsSnafu {
            msg: format!(
                "Unsupported metadata key for validation: {key}, use --no-validate to bypass"
            ),
        }
        .build(),
    ))
}

/// Returns the rejection reason for keys that should not be updated by `put key`.
///
/// These keys may be decodable, but they are not safe to update via raw KV writes.
/// `__table_route/*` is the canonical example.
fn unsupported_direct_put_reason(key: &str) -> Option<String> {
    let flow_info_prefix = flow_info_key_prefix();
    let flow_name_prefix = flow_name_key_prefix();
    let flow_route_prefix = flow_route_key_prefix();
    let table_flow_prefix = table_flow_key_prefix();
    let flownode_flow_prefix = flownode_flow_key_prefix();

    let (prefix, target) = [
        (TABLE_ROUTE_PREFIX, "table route metadata"),
        (TABLE_INFO_KEY_PREFIX, "table info metadata"),
        (TABLE_NAME_KEY_PREFIX, "table name metadata"),
        (DATANODE_TABLE_KEY_PREFIX, "datanode table metadata"),
        (&flow_info_prefix, "flow info metadata"),
        (&flow_name_prefix, "flow name metadata"),
        (&flow_route_prefix, "flow route metadata"),
        (&table_flow_prefix, "flow source table metadata"),
        (&flownode_flow_prefix, "flownode flow metadata"),
    ]
    .into_iter()
    .find(|(prefix, _)| matches_key_prefix(key, prefix))?;

    Some(format!(
        "Direct put is not supported for {target} ({prefix}*); use a dedicated metadata update interface instead"
    ))
}

fn matches_key_prefix(key: &str, prefix: &str) -> bool {
    key == prefix
        || key
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('/'))
}

fn validate_value<T, F>(key: &str, value: &[u8], parser: F) -> Result<(), BoxedError>
where
    F: FnOnce(&[u8]) -> common_meta::error::Result<T>,
{
    parser(value).map_err(|e| {
        BoxedError::new(
            InvalidArgumentsSnafu {
                msg: format!("Invalid metadata value for key: {key}: {e}"),
            }
            .build(),
        )
    })?;
    Ok(())
}

fn validate_key<T>(result: common_meta::error::Result<T>, key: &str) -> Result<(), BoxedError> {
    result.map_err(|e| {
        BoxedError::new(
            InvalidArgumentsSnafu {
                msg: format!("Invalid metadata key: {key}: {e}"),
            }
            .build(),
        )
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use clap::Parser;
    use common_error::ext::{BoxedError, ErrorExt};
    use common_meta::key::flow::flow_state::FlowStateValue;
    use common_meta::key::flow::flow_state_full_key;
    use common_meta::key::schema_name::SchemaNameValue;
    use common_meta::key::topic_name::TopicNameValue;
    use common_meta::key::{KAFKA_TOPIC_KEY_PREFIX, MetadataValue, SCHEMA_NAME_KEY_PREFIX};
    use common_meta::kv_backend::KvBackendRef;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use tokio::io::BufReader;

    use super::{
        PutKeyCommand, PutKeyTool, TABLE_ROUTE_PREFIX, matches_key_prefix,
        unsupported_direct_put_reason, validate_metadata_value,
    };
    use crate::Tool;

    impl PutKeyCommand {
        async fn build_for_test<R>(
            &self,
            reader: R,
            kv_backend: KvBackendRef,
        ) -> Result<Box<dyn Tool>, BoxedError>
        where
            R: tokio::io::AsyncRead + Unpin,
        {
            self.build_tool(reader, kv_backend).await
        }
    }

    #[test]
    fn test_validate_supported_key_success() {
        let value = SchemaNameValue::default().try_as_raw_value().unwrap();

        validate_metadata_value(&format!("{SCHEMA_NAME_KEY_PREFIX}/greptime/public"), &value)
            .unwrap();
    }

    #[test]
    fn test_validate_supported_key_invalid_value() {
        let err = validate_metadata_value(
            &format!("{KAFKA_TOPIC_KEY_PREFIX}/test-topic"),
            b"not-a-valid-json-value",
        )
        .unwrap_err();

        assert!(err.output_msg().contains("Invalid metadata value for key"));
    }

    #[test]
    fn test_validate_complex_key_fails() {
        let value = serde_json::to_vec(&BTreeMap::<u32, u32>::new()).unwrap();
        let err =
            validate_metadata_value(&format!("{TABLE_ROUTE_PREFIX}/1024"), &value).unwrap_err();

        assert!(
            err.output_msg()
                .contains("Direct put is not supported for table route metadata")
        );
    }

    #[test]
    fn test_validate_unknown_key_fails() {
        let err = validate_metadata_value("__unknown/foo", b"{}").unwrap_err();

        assert!(
            err.output_msg()
                .contains("Unsupported metadata key for validation")
        );
    }

    #[test]
    fn test_validate_invalid_supported_key_fails() {
        let value = SchemaNameValue::default().try_as_raw_value().unwrap();
        let err = validate_metadata_value("__schema_name/greptime", &value).unwrap_err();

        assert!(
            err.output_msg()
                .contains("Invalid metadata key: __schema_name/greptime")
        );
    }

    #[test]
    fn test_validate_invalid_complex_key_fails_before_unsupported() {
        let err = validate_metadata_value("__table_route/not-a-number", b"{}").unwrap_err();

        assert!(
            err.output_msg()
                .contains("Invalid metadata key: __table_route/not-a-number")
        );
    }

    #[test]
    fn test_unsupported_direct_put_reason_covers_complex_keys() {
        let cases = [
            "__table_route/1024",
            "__table_info/1024",
            "__table_name/greptime/public/demo",
            "__dn_table/1/1024",
            "__flow/route/1/1",
        ];

        for key in cases {
            assert!(unsupported_direct_put_reason(key).is_some(), "key: {key}");
        }
    }

    #[test]
    fn test_matches_key_prefix() {
        assert!(matches_key_prefix("__table_route", "__table_route"));
        assert!(matches_key_prefix("__table_route/1024", "__table_route"));
        assert!(!matches_key_prefix(
            "__table_route_extra/1024",
            "__table_route"
        ));
        assert!(!matches_key_prefix("__table_routex", "__table_route"));
        assert!(!matches_key_prefix(
            "__topic_name/kafka_backup/foo",
            "__topic_name/kafka"
        ));
    }

    #[test]
    fn test_validate_exact_flow_state_key() {
        let value = FlowStateValue::new(BTreeMap::new(), BTreeMap::new())
            .try_as_raw_value()
            .unwrap();

        validate_metadata_value(&flow_state_full_key(), &value).unwrap();
    }

    #[tokio::test]
    async fn test_put_key_tool_writes_supported_key() {
        let kv_backend = Arc::new(MemoryKvBackend::new()) as KvBackendRef;
        let value = TopicNameValue::new(42).try_as_raw_value().unwrap();
        let key = format!("{KAFKA_TOPIC_KEY_PREFIX}/test-topic");
        let tool = PutKeyTool {
            kv_backend: kv_backend.clone(),
            key: key.clone(),
            value: value.clone(),
            no_validate: false,
        };

        tool.do_work().await.unwrap();

        let stored = kv_backend.get(key.as_bytes()).await.unwrap().unwrap();
        assert_eq!(stored.value, value);
    }

    #[tokio::test]
    async fn test_put_key_tool_bypasses_validation() {
        let kv_backend = Arc::new(MemoryKvBackend::new()) as KvBackendRef;
        let key = format!("{TABLE_ROUTE_PREFIX}/1024");
        let value = b"not-json".to_vec();
        let tool = PutKeyTool {
            kv_backend: kv_backend.clone(),
            key: key.clone(),
            value: value.clone(),
            no_validate: true,
        };

        tool.do_work().await.unwrap();

        let stored = kv_backend.get(key.as_bytes()).await.unwrap().unwrap();
        assert_eq!(stored.value, value);
    }

    #[test]
    fn test_put_key_command_requires_value_stdin() {
        let err = PutKeyCommand::try_parse_from([
            "key",
            "__topic_name/kafka/test-cli-topic",
            "--backend",
            "memory-store",
            "--store-addrs",
            "memory://",
        ])
        .unwrap_err();

        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[tokio::test]
    async fn test_put_key_command_builds_tool_with_stdin() {
        let value = TopicNameValue::new(7).try_as_raw_value().unwrap();
        let command = PutKeyCommand::parse_from([
            "key",
            "__topic_name/kafka/test-cli-topic",
            "--value-stdin",
            "--backend",
            "memory-store",
            "--store-addrs",
            "memory://",
        ]);

        let tool = command
            .build_for_test(
                BufReader::new(value.as_slice()),
                Arc::new(MemoryKvBackend::new()) as KvBackendRef,
            )
            .await
            .unwrap();
        tool.do_work().await.unwrap();
    }

    #[tokio::test]
    async fn test_put_key_command_validate_failure() {
        let tool = PutKeyTool {
            kv_backend: Arc::new(MemoryKvBackend::new()) as KvBackendRef,
            key: "__table_route/1024".to_string(),
            value: b"{}".to_vec(),
            no_validate: false,
        };
        let err = tool.do_work().await.unwrap_err();

        assert!(
            err.output_msg()
                .contains("Direct put is not supported for table route metadata")
        );
    }
}
