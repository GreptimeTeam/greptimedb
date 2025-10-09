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

use chrono::Utc;
use common_catalog::format_full_table_name;
use common_error::ext::BoxedError;
use common_meta::cache_invalidator::Context;
use common_meta::error::Error as MetaError;
use common_meta::instruction::CacheIdent;
use common_meta::key::flow::flow_info::{FlowInfoKey, FlowInfoValue};
use common_meta::key::flow::flow_route::FlowRouteValue;
use common_meta::key::table_info::{TableInfoKey, TableInfoValue};
use common_meta::key::{
    DeserializedValueWithBytes, FlowId, FlowPartitionId, MetadataKey, MetadataValue,
};
use common_meta::rpc::store::PutRequest;
use common_query::Output;
use datatypes::schema::COMMENT_KEY as COLUMN_COMMENT_KEY;
use session::context::QueryContextRef;
use session::table_name::table_idents_to_full_name;
use snafu::{GenerateImplicitData, Location, OptionExt, ResultExt};
use sql::ast::{Ident, ObjectName, ObjectNamePartExt};
use sql::statements::comment::{Comment, CommentObject};
use table::metadata::{RawTableInfo, TableId};
use table::requests::COMMENT_KEY as TABLE_COMMENT_KEY;
use table::table_name::TableName;

use crate::error::{
    CatalogSnafu, ColumnNotFoundSnafu, ExternalSnafu, FlowNotFoundSnafu, InvalidSqlSnafu,
    InvalidateTableCacheSnafu, Result, TableMetadataManagerSnafu, TableNotFoundSnafu,
};
use crate::statement::StatementExecutor;

struct ResolvedTable {
    catalog: String,
    schema: String,
    table: String,
    table_id: TableId,
    table_name: TableName,
    table_info: DeserializedValueWithBytes<TableInfoValue>,
}

impl StatementExecutor {
    pub async fn comment(&self, stmt: Comment, query_ctx: QueryContextRef) -> Result<Output> {
        match stmt.object {
            CommentObject::Table(table) => {
                self.comment_on_table(table, stmt.comment, query_ctx).await
            }
            CommentObject::Column { table, column } => {
                self.comment_on_column(table, column, stmt.comment, query_ctx)
                    .await
            }
            CommentObject::Flow(flow) => self.comment_on_flow(flow, stmt.comment, query_ctx).await,
        }
    }

    async fn comment_on_table(
        &self,
        table: ObjectName,
        comment: Option<String>,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let ResolvedTable {
            table_id,
            table_name,
            table_info,
            ..
        } = self.resolve_table_for_comment(&table, &query_ctx).await?;

        let mut new_table_info = table_info.table_info.clone();
        new_table_info.desc = comment;
        sync_table_comment_option(
            &mut new_table_info.meta.options,
            new_table_info.desc.as_deref(),
        );

        self.update_table_info_with_fallback(&table_info, new_table_info)
            .await?;

        self.invalidate_table_cache_by_name(table_id, table_name)
            .await?;

        Ok(Output::new_with_affected_rows(0))
    }

    async fn comment_on_column(
        &self,
        table: ObjectName,
        column: Ident,
        comment: Option<String>,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let ResolvedTable {
            catalog,
            schema,
            table,
            table_id,
            table_name,
            table_info,
        } = self.resolve_table_for_comment(&table, &query_ctx).await?;

        let full_table_name = format_full_table_name(&catalog, &schema, &table);
        let mut new_table_info = table_info.table_info.clone();

        let column_name = column.value.clone();
        let column_schema = new_table_info
            .meta
            .schema
            .column_schemas
            .iter_mut()
            .find(|col| col.name == column_name)
            .with_context(|| ColumnNotFoundSnafu {
                msg: format!("{} column `{}`", full_table_name, column_name),
            })?;

        update_column_comment_metadata(column_schema, comment);

        self.update_table_info_with_fallback(&table_info, new_table_info)
            .await?;

        self.invalidate_table_cache_by_name(table_id, table_name)
            .await?;

        Ok(Output::new_with_affected_rows(0))
    }

    async fn comment_on_flow(
        &self,
        flow_name: ObjectName,
        comment: Option<String>,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let (catalog_name, flow_name_str) = match &flow_name.0[..] {
            [flow] => (
                query_ctx.current_catalog().to_string(),
                flow.to_string_unquoted(),
            ),
            [catalog, flow] => (catalog.to_string_unquoted(), flow.to_string_unquoted()),
            _ => {
                return InvalidSqlSnafu {
                    err_msg: format!(
                        "expect flow name to be <catalog>.<flow_name> or <flow_name>, actual: {flow_name}"
                    ),
                }
                .fail();
            }
        };

        let flow_name_val = self
            .flow_metadata_manager
            .flow_name_manager()
            .get(&catalog_name, &flow_name_str)
            .await
            .context(TableMetadataManagerSnafu)?
            .context(FlowNotFoundSnafu {
                flow_name: &flow_name_str,
            })?;

        let flow_id = flow_name_val.flow_id();
        let flow_info = self
            .flow_metadata_manager
            .flow_info_manager()
            .get_raw(flow_id)
            .await
            .context(TableMetadataManagerSnafu)?
            .context(FlowNotFoundSnafu {
                flow_name: &flow_name_str,
            })?;

        let mut new_flow_info = flow_info.get_inner_ref().clone();
        new_flow_info.comment = comment.unwrap_or_default();
        new_flow_info.updated_time = Utc::now();

        let flow_routes = self
            .flow_metadata_manager
            .flow_route_manager()
            .routes(flow_id)
            .await
            .context(TableMetadataManagerSnafu)?
            .into_iter()
            .map(|(key, value)| (key.partition_id(), value))
            .collect::<Vec<_>>();

        self.update_flow_metadata_with_fallback(flow_id, &flow_info, new_flow_info, flow_routes)
            .await?;

        Ok(Output::new_with_affected_rows(0))
    }

    async fn resolve_table_for_comment(
        &self,
        table: &ObjectName,
        query_ctx: &QueryContextRef,
    ) -> Result<ResolvedTable> {
        let (catalog_name, schema_name, table_name) = table_idents_to_full_name(table, query_ctx)
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;

        let full_table_name = format_full_table_name(&catalog_name, &schema_name, &table_name);

        let table_ref = self
            .catalog_manager
            .table(&catalog_name, &schema_name, &table_name, Some(query_ctx))
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: full_table_name.clone(),
            })?;

        let table_id = table_ref.table_info().table_id();
        let table_info = self
            .table_metadata_manager
            .table_info_manager()
            .get(table_id)
            .await
            .context(TableMetadataManagerSnafu)?
            .context(TableNotFoundSnafu {
                table_name: full_table_name,
            })?;

        Ok(ResolvedTable {
            catalog: catalog_name.clone(),
            schema: schema_name.clone(),
            table: table_name.clone(),
            table_id,
            table_name: TableName::new(catalog_name, schema_name, table_name),
            table_info,
        })
    }

    async fn invalidate_table_cache_by_name(
        &self,
        table_id: TableId,
        table_name: TableName,
    ) -> Result<()> {
        let cache_ident = [
            CacheIdent::TableId(table_id),
            CacheIdent::TableName(table_name),
        ];
        self.cache_invalidator
            .invalidate(&Context::default(), &cache_ident)
            .await
            .context(InvalidateTableCacheSnafu)?;
        Ok(())
    }

    async fn update_table_info_with_fallback(
        &self,
        current_table_info: &DeserializedValueWithBytes<TableInfoValue>,
        new_table_info: RawTableInfo,
    ) -> Result<()> {
        let try_update = self
            .table_metadata_manager
            .update_table_info(current_table_info, None, new_table_info.clone())
            .await
            .context(TableMetadataManagerSnafu);

        match try_update {
            Ok(()) => Ok(()),
            Err(err) if Self::is_txn_unsupported(&err) => {
                self.update_table_info_without_txn(current_table_info, new_table_info)
                    .await
            }
            Err(err) => Err(err),
        }
    }

    async fn update_flow_metadata_with_fallback(
        &self,
        flow_id: FlowId,
        current_flow_info: &DeserializedValueWithBytes<FlowInfoValue>,
        new_flow_info: FlowInfoValue,
        flow_routes: Vec<(FlowPartitionId, FlowRouteValue)>,
    ) -> Result<()> {
        let try_update = self
            .flow_metadata_manager
            .update_flow_metadata(flow_id, current_flow_info, &new_flow_info, flow_routes)
            .await
            .context(TableMetadataManagerSnafu);

        match try_update {
            Ok(()) => Ok(()),
            Err(err) if Self::is_txn_unsupported(&err) => {
                self.update_flow_metadata_without_txn(flow_id, new_flow_info)
                    .await
            }
            Err(err) => Err(err),
        }
    }

    async fn update_table_info_without_txn(
        &self,
        current_table_info: &DeserializedValueWithBytes<TableInfoValue>,
        new_table_info: RawTableInfo,
    ) -> Result<()> {
        let kv_backend = Arc::clone(self.table_metadata_manager.kv_backend());
        let table_id = current_table_info.table_info.ident.table_id;
        let new_table_info_value = current_table_info.update(new_table_info);
        let raw_value = new_table_info_value
            .try_as_raw_value()
            .context(TableMetadataManagerSnafu)?;

        kv_backend
            .put(
                PutRequest::new()
                    .with_key(TableInfoKey::new(table_id).to_bytes())
                    .with_value(raw_value),
            )
            .await
            .context(TableMetadataManagerSnafu)?;

        Ok(())
    }

    async fn update_flow_metadata_without_txn(
        &self,
        flow_id: FlowId,
        new_flow_info: FlowInfoValue,
    ) -> Result<()> {
        let kv_backend = Arc::clone(self.table_metadata_manager.kv_backend());
        let raw_value = serde_json::to_vec(&new_flow_info)
            .map_err(|err| common_meta::error::Error::SerdeJson {
                error: err,
                location: Location::generate(),
            })
            .context(TableMetadataManagerSnafu)?;

        kv_backend
            .put(
                PutRequest::new()
                    .with_key(FlowInfoKey::new(flow_id).to_bytes())
                    .with_value(raw_value),
            )
            .await
            .context(TableMetadataManagerSnafu)?;

        Ok(())
    }

    fn is_txn_unsupported(error: &crate::error::Error) -> bool {
        matches!(
            error,
            crate::error::Error::TableMetadataManager { source, .. }
                if matches!(source, MetaError::Unsupported { operation, .. }
                    if operation == "txn" || operation.ends_with("::txn"))
        )
    }
}

fn update_column_comment_metadata(
    column_schema: &mut datatypes::schema::ColumnSchema,
    comment: Option<String>,
) {
    match comment {
        Some(value) => {
            column_schema
                .mut_metadata()
                .insert(COLUMN_COMMENT_KEY.to_string(), value);
        }
        None => {
            column_schema.mut_metadata().remove(COLUMN_COMMENT_KEY);
        }
    }
}

fn sync_table_comment_option(options: &mut table::requests::TableOptions, comment: Option<&str>) {
    match comment {
        Some(value) => {
            options
                .extra_options
                .insert(TABLE_COMMENT_KEY.to_string(), value.to_string());
        }
        None => {
            options.extra_options.remove(TABLE_COMMENT_KEY);
        }
    }
}

#[cfg(test)]
mod tests {
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{COMMENT_KEY as COLUMN_COMMENT_KEY, ColumnSchema};
    use table::requests::TableOptions;

    use super::{sync_table_comment_option, update_column_comment_metadata};

    #[test]
    fn test_update_column_comment_metadata_uses_schema_key() {
        let mut column_schema = ColumnSchema::new("col", ConcreteDataType::string_datatype(), true);

        update_column_comment_metadata(&mut column_schema, Some("note".to_string()));

        assert_eq!(
            column_schema
                .metadata()
                .get(COLUMN_COMMENT_KEY)
                .map(|value| value.as_str()),
            Some("note")
        );
        // Make sure it's not the `COMMENT_KEY` from `table::request`
        assert!(column_schema.metadata().get("comment").is_none());

        update_column_comment_metadata(&mut column_schema, None);

        assert!(column_schema.metadata().get(COLUMN_COMMENT_KEY).is_none());
    }

    #[test]
    fn test_sync_table_comment_option() {
        let mut options = TableOptions::default();

        sync_table_comment_option(&mut options, Some("table comment"));
        assert_eq!(
            options.extra_options.get(super::TABLE_COMMENT_KEY),
            Some(&"table comment".to_string())
        );

        sync_table_comment_option(&mut options, None);
        assert!(!options.extra_options.contains_key(super::TABLE_COMMENT_KEY));
    }
}
