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

use async_recursion::async_recursion;
use async_trait::async_trait;
use bytes::{Buf, Bytes, BytesMut};
use catalog::table_source::DfTableSourceProvider;
use catalog::CatalogManagerRef;
use common_catalog::format_full_table_name;
use common_telemetry::debug;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::common::{DFField, DFSchema};
use datafusion::datasource::DefaultTableSource;
use datafusion::physical_plan::project_schema;
use datafusion::sql::TableReference;
use datafusion_expr::{Filter, LogicalPlan, TableScan};
use prost::Message;
use session::context::QueryContext;
use snafu::{ensure, OptionExt, ResultExt};
use substrait_proto::proto::expression::mask_expression::{StructItem, StructSelect};
use substrait_proto::proto::expression::MaskExpression;
use substrait_proto::proto::extensions::simple_extension_declaration::MappingType;
use substrait_proto::proto::plan_rel::RelType as PlanRelType;
use substrait_proto::proto::read_rel::{NamedTable, ReadType};
use substrait_proto::proto::rel::RelType;
use substrait_proto::proto::{FilterRel, Plan, PlanRel, ReadRel, Rel};
use table::table::adapter::DfTableProviderAdapter;

use crate::context::ConvertorContext;
use crate::df_expr::{expression_from_df_expr, to_df_expr};
use crate::error::{
    self, DFInternalSnafu, DecodeRelSnafu, EmptyPlanSnafu, EncodeRelSnafu, Error,
    InvalidParametersSnafu, MissingFieldSnafu, ResolveTableSnafu, SchemaNotMatchSnafu,
    UnknownPlanSnafu, UnsupportedExprSnafu, UnsupportedPlanSnafu,
};
use crate::schema::{from_schema, to_schema};
use crate::SubstraitPlan;

pub struct DFLogicalSubstraitConvertor;

#[async_trait]
impl SubstraitPlan for DFLogicalSubstraitConvertor {
    type Error = Error;

    type Plan = LogicalPlan;

    async fn decode<B: Buf + Send>(
        &self,
        message: B,
        catalog_manager: CatalogManagerRef,
    ) -> Result<Self::Plan, Self::Error> {
        let plan = Plan::decode(message).context(DecodeRelSnafu)?;
        self.convert_plan(plan, catalog_manager).await
    }

    fn encode(&self, plan: Self::Plan) -> Result<Bytes, Self::Error> {
        let plan = self.convert_df_plan(plan)?;

        let mut buf = BytesMut::new();
        plan.encode(&mut buf).context(EncodeRelSnafu)?;

        Ok(buf.freeze())
    }
}

impl DFLogicalSubstraitConvertor {
    async fn convert_plan(
        &self,
        mut plan: Plan,
        catalog_manager: CatalogManagerRef,
    ) -> Result<LogicalPlan, Error> {
        // prepare convertor context
        let mut ctx = ConvertorContext::default();
        for simple_ext in plan.extensions {
            if let Some(MappingType::ExtensionFunction(function_extension)) =
                simple_ext.mapping_type
            {
                ctx.register_scalar_with_anchor(
                    function_extension.name,
                    function_extension.function_anchor,
                );
            } else {
                debug!("Encounter unsupported substrait extension {:?}", simple_ext);
            }
        }

        // extract rel
        let rel = if let Some(PlanRel { rel_type }) = plan.relations.pop()
        && let Some(PlanRelType::Rel(rel)) = rel_type {
            rel
        } else {
            UnsupportedPlanSnafu {
                name: "Emply or non-Rel relation",
            }
            .fail()?
        };

        // TODO(LFC): Create table provider from outside, respect "disallow_cross_schema_query" option in query engine state.
        let mut table_provider =
            DfTableSourceProvider::new(catalog_manager, false, &QueryContext::new());
        self.rel_to_logical_plan(&mut ctx, Box::new(rel), &mut table_provider)
            .await
    }

    #[async_recursion]
    async fn rel_to_logical_plan(
        &self,
        ctx: &mut ConvertorContext,
        rel: Box<Rel>,
        table_provider: &mut DfTableSourceProvider,
    ) -> Result<LogicalPlan, Error> {
        let rel_type = rel.rel_type.context(EmptyPlanSnafu)?;

        // build logical plan
        let logical_plan = match rel_type {
            RelType::Read(read_rel) => self.convert_read_rel(ctx, read_rel, table_provider).await?,
            RelType::Filter(filter) => {
                let FilterRel {
                    common: _,
                    input,
                    condition,
                    advanced_extension: _,
                } = *filter;

                let input = input.context(MissingFieldSnafu {
                    field: "input",
                    plan: "Filter",
                })?;
                let input = Arc::new(self.rel_to_logical_plan(ctx, input, table_provider).await?);

                let condition = condition.context(MissingFieldSnafu {
                    field: "condition",
                    plan: "Filter",
                })?;

                let schema = ctx.df_schema().context(InvalidParametersSnafu {
                    reason: "the underlying TableScan plan should have included a table schema",
                })?;
                let schema = schema
                    .clone()
                    .try_into()
                    .context(error::ConvertDfSchemaSnafu)?;
                let predicate = to_df_expr(ctx, *condition, &schema)?;

                LogicalPlan::Filter(Filter::try_new(predicate, input).context(DFInternalSnafu)?)
            }
            RelType::Fetch(_fetch_rel) => UnsupportedPlanSnafu {
                name: "Fetch Relation",
            }
            .fail()?,
            RelType::Aggregate(_aggr_rel) => UnsupportedPlanSnafu {
                name: "Fetch Relation",
            }
            .fail()?,
            RelType::Sort(_sort_rel) => UnsupportedPlanSnafu {
                name: "Sort Relation",
            }
            .fail()?,
            RelType::Join(_join_rel) => UnsupportedPlanSnafu {
                name: "Join Relation",
            }
            .fail()?,
            RelType::Project(_project_rel) => UnsupportedPlanSnafu {
                name: "Project Relation",
            }
            .fail()?,
            RelType::Set(_set_rel) => UnsupportedPlanSnafu {
                name: "Set Relation",
            }
            .fail()?,
            RelType::ExtensionSingle(_ext_single_rel) => UnsupportedPlanSnafu {
                name: "Extension Single Relation",
            }
            .fail()?,
            RelType::ExtensionMulti(_ext_multi_rel) => UnsupportedPlanSnafu {
                name: "Extension Multi Relation",
            }
            .fail()?,
            RelType::ExtensionLeaf(_ext_leaf_rel) => UnsupportedPlanSnafu {
                name: "Extension Leaf Relation",
            }
            .fail()?,
            RelType::Cross(_cross_rel) => UnsupportedPlanSnafu {
                name: "Cross Relation",
            }
            .fail()?,
        };

        Ok(logical_plan)
    }

    async fn convert_read_rel(
        &self,
        ctx: &mut ConvertorContext,
        read_rel: Box<ReadRel>,
        table_provider: &mut DfTableSourceProvider,
    ) -> Result<LogicalPlan, Error> {
        // Extract the catalog, schema and table name from NamedTable. Assume the first three are those names.
        let read_type = read_rel.read_type.context(MissingFieldSnafu {
            field: "read_type",
            plan: "Read",
        })?;
        let (table_name, schema_name, catalog_name) = match read_type {
            ReadType::NamedTable(mut named_table) => {
                ensure!(
                    named_table.names.len() == 3,
                    InvalidParametersSnafu {
                        reason:
                            "NamedTable should contains three names for catalog, schema and table",
                    }
                );
                (
                    named_table.names.pop().unwrap(),
                    named_table.names.pop().unwrap(),
                    named_table.names.pop().unwrap(),
                )
            }
            ReadType::VirtualTable(_) | ReadType::LocalFiles(_) | ReadType::ExtensionTable(_) => {
                UnsupportedExprSnafu {
                    name: "Non-NamedTable Read",
                }
                .fail()?
            }
        };

        // Get projection indices
        let projection = read_rel
            .projection
            .map(|mask_expr| self.convert_mask_expression(mask_expr));

        let table_ref = TableReference::full(
            catalog_name.clone(),
            schema_name.clone(),
            table_name.clone(),
        );
        let adapter = table_provider
            .resolve_table(table_ref.clone())
            .await
            .with_context(|_| ResolveTableSnafu {
                table_name: format_full_table_name(&catalog_name, &schema_name, &table_name),
            })?;

        // Get schema directly from the table, and compare it with the schema retrieved from substrait proto.
        let stored_schema = adapter.schema();
        let retrieved_schema = to_schema(read_rel.base_schema.unwrap_or_default())?;
        let retrieved_arrow_schema = retrieved_schema.arrow_schema();
        ensure!(
            same_schema_without_metadata(&stored_schema, retrieved_arrow_schema),
            SchemaNotMatchSnafu {
                substrait_schema: retrieved_arrow_schema.clone(),
                storage_schema: stored_schema
            }
        );

        // Convert filter
        let filters = if let Some(filter) = read_rel.filter {
            vec![to_df_expr(ctx, *filter, &retrieved_schema)?]
        } else {
            vec![]
        };

        // Calculate the projected schema
        let projected_schema = Arc::new(
            project_schema(&stored_schema, projection.as_ref())
                .and_then(|x| {
                    DFSchema::new_with_metadata(
                        x.fields()
                            .iter()
                            .map(|f| DFField::from_qualified(table_ref.clone(), f.clone()))
                            .collect(),
                        x.metadata().clone(),
                    )
                })
                .context(DFInternalSnafu)?,
        );

        ctx.set_df_schema(projected_schema.clone());

        // TODO(ruihang): Support limit(fetch)
        Ok(LogicalPlan::TableScan(TableScan {
            table_name: table_ref,
            source: adapter,
            projection,
            projected_schema,
            filters,
            fetch: None,
        }))
    }

    fn convert_mask_expression(&self, mask_expression: MaskExpression) -> Vec<usize> {
        mask_expression
            .select
            .unwrap_or_default()
            .struct_items
            .into_iter()
            .map(|select| select.field as _)
            .collect()
    }
}

impl DFLogicalSubstraitConvertor {
    fn logical_plan_to_rel(
        &self,
        ctx: &mut ConvertorContext,
        plan: Arc<LogicalPlan>,
    ) -> Result<Rel, Error> {
        Ok(match &*plan {
            LogicalPlan::Projection(_) => UnsupportedPlanSnafu {
                name: "DataFusion Logical Projection",
            }
            .fail()?,
            LogicalPlan::Filter(filter) => {
                let input = Some(Box::new(
                    self.logical_plan_to_rel(ctx, filter.input.clone())?,
                ));

                let schema = plan
                    .schema()
                    .clone()
                    .try_into()
                    .context(error::ConvertDfSchemaSnafu)?;
                let condition = Some(Box::new(expression_from_df_expr(
                    ctx,
                    &filter.predicate,
                    &schema,
                )?));

                let rel = FilterRel {
                    common: None,
                    input,
                    condition,
                    advanced_extension: None,
                };
                Rel {
                    rel_type: Some(RelType::Filter(Box::new(rel))),
                }
            }
            LogicalPlan::Window(_) => UnsupportedPlanSnafu {
                name: "DataFusion Logical Window",
            }
            .fail()?,
            LogicalPlan::Aggregate(_) => UnsupportedPlanSnafu {
                name: "DataFusion Logical Aggregate",
            }
            .fail()?,
            LogicalPlan::Sort(_) => UnsupportedPlanSnafu {
                name: "DataFusion Logical Sort",
            }
            .fail()?,
            LogicalPlan::Join(_) => UnsupportedPlanSnafu {
                name: "DataFusion Logical Join",
            }
            .fail()?,
            LogicalPlan::CrossJoin(_) => UnsupportedPlanSnafu {
                name: "DataFusion Logical CrossJoin",
            }
            .fail()?,
            LogicalPlan::Repartition(_) => UnsupportedPlanSnafu {
                name: "DataFusion Logical Repartition",
            }
            .fail()?,
            LogicalPlan::Union(_) => UnsupportedPlanSnafu {
                name: "DataFusion Logical Union",
            }
            .fail()?,
            LogicalPlan::TableScan(table_scan) => {
                let read_rel = self.convert_table_scan_plan(ctx, table_scan)?;
                Rel {
                    rel_type: Some(RelType::Read(Box::new(read_rel))),
                }
            }
            LogicalPlan::EmptyRelation(_) => UnsupportedPlanSnafu {
                name: "DataFusion Logical EmptyRelation",
            }
            .fail()?,
            LogicalPlan::Limit(_) => UnsupportedPlanSnafu {
                name: "DataFusion Logical Limit",
            }
            .fail()?,

            LogicalPlan::Subquery(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::CreateView(_)
            | LogicalPlan::CreateCatalogSchema(_)
            | LogicalPlan::CreateCatalog(_)
            | LogicalPlan::DropView(_)
            | LogicalPlan::Distinct(_)
            | LogicalPlan::SetVariable(_)
            | LogicalPlan::CreateExternalTable(_)
            | LogicalPlan::CreateMemoryTable(_)
            | LogicalPlan::DropTable(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::Explain(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Extension(_)
            | LogicalPlan::Prepare(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::DescribeTable(_)
            | LogicalPlan::Unnest(_) => InvalidParametersSnafu {
                reason: format!(
                    "Trying to convert DDL/DML plan to substrait proto, plan: {plan:?}",
                ),
            }
            .fail()?,
        })
    }

    fn convert_df_plan(&self, plan: LogicalPlan) -> Result<Plan, Error> {
        let mut ctx = ConvertorContext::default();

        let rel = self.logical_plan_to_rel(&mut ctx, Arc::new(plan))?;

        // convert extension
        let extensions = ctx.generate_function_extension();

        // assemble PlanRel
        let plan_rel = PlanRel {
            rel_type: Some(PlanRelType::Rel(rel)),
        };

        Ok(Plan {
            extension_uris: vec![],
            extensions,
            relations: vec![plan_rel],
            advanced_extensions: None,
            expected_type_urls: vec![],
            ..Default::default()
        })
    }

    pub fn convert_table_scan_plan(
        &self,
        ctx: &mut ConvertorContext,
        table_scan: &TableScan,
    ) -> Result<ReadRel, Error> {
        let provider = table_scan
            .source
            .as_any()
            .downcast_ref::<DefaultTableSource>()
            .context(UnknownPlanSnafu)?
            .table_provider
            .as_any()
            .downcast_ref::<DfTableProviderAdapter>()
            .context(UnknownPlanSnafu)?;
        let table_info = provider.table().table_info();

        // assemble NamedTable and ReadType
        let catalog_name = table_info.catalog_name.clone();
        let schema_name = table_info.schema_name.clone();
        let table_name = table_info.name.clone();
        let named_table = NamedTable {
            names: vec![catalog_name, schema_name, table_name],
            advanced_extension: None,
        };
        let read_type = ReadType::NamedTable(named_table);

        // assemble projection
        let projection = table_scan
            .projection
            .as_ref()
            .map(|x| self.convert_schema_projection(x));

        // assemble base (unprojected) schema using Table's schema.
        let base_schema = from_schema(&provider.table().schema())?;

        // make conjunction over a list of filters and convert the result to substrait
        let filter = if let Some(conjunction) = table_scan
            .filters
            .iter()
            .cloned()
            .reduce(|accum, expr| accum.and(expr))
        {
            Some(Box::new(expression_from_df_expr(
                ctx,
                &conjunction,
                &provider.table().schema(),
            )?))
        } else {
            None
        };

        let read_rel = ReadRel {
            common: None,
            base_schema: Some(base_schema),
            filter,
            projection,
            advanced_extension: None,
            read_type: Some(read_type),
            ..Default::default()
        };

        Ok(read_rel)
    }

    /// Convert a index-based schema projection to substrait's [MaskExpression].
    fn convert_schema_projection(&self, projections: &[usize]) -> MaskExpression {
        let struct_items = projections
            .iter()
            .map(|index| StructItem {
                field: *index as i32,
                child: None,
            })
            .collect();
        MaskExpression {
            select: Some(StructSelect { struct_items }),
            // TODO(ruihang): this field is unspecified
            maintain_singular_struct: true,
        }
    }
}

fn same_schema_without_metadata(lhs: &ArrowSchemaRef, rhs: &ArrowSchemaRef) -> bool {
    lhs.fields.len() == rhs.fields.len()
        && lhs.fields.iter().zip(rhs.fields.iter()).all(|(x, y)| {
            x.name() == y.name()
                && x.data_type() == y.data_type()
                && x.is_nullable() == y.is_nullable()
        })
}

#[cfg(test)]
mod test {
    use catalog::local::{LocalCatalogManager, MemoryCatalogProvider, MemorySchemaProvider};
    use catalog::{CatalogList, CatalogProvider, RegisterTableRequest};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, MITO_ENGINE};
    use datafusion::common::{DFSchema, ToDFSchema};
    use datafusion_expr::TableSource;
    use datatypes::schema::RawSchema;
    use table::engine::manager::MemoryTableEngineManager;
    use table::requests::CreateTableRequest;
    use table::test_util::{EmptyTable, MockTableEngine};

    use super::*;
    use crate::schema::test::supported_types;

    const DEFAULT_TABLE_NAME: &str = "SubstraitTable";

    async fn build_mock_catalog_manager() -> CatalogManagerRef {
        let mock_table_engine = Arc::new(MockTableEngine::new());
        let engine_manager = Arc::new(MemoryTableEngineManager::new(
            MITO_ENGINE,
            mock_table_engine.clone(),
        ));
        let catalog_manager = Arc::new(LocalCatalogManager::try_new(engine_manager).await.unwrap());
        let schema_provider = Arc::new(MemorySchemaProvider::new());
        let catalog_provider = Arc::new(MemoryCatalogProvider::new());
        catalog_provider
            .register_schema(DEFAULT_SCHEMA_NAME.to_string(), schema_provider)
            .unwrap();
        catalog_manager
            .register_catalog(DEFAULT_CATALOG_NAME.to_string(), catalog_provider)
            .unwrap();

        catalog_manager.init().await.unwrap();
        catalog_manager
    }

    fn build_create_table_request<N: ToString>(table_name: N) -> CreateTableRequest {
        CreateTableRequest {
            id: 1,
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: table_name.to_string(),
            desc: None,
            schema: RawSchema::new(supported_types()),
            region_numbers: vec![0],
            primary_key_indices: vec![],
            create_if_not_exists: true,
            table_options: Default::default(),
        }
    }

    async fn logical_plan_round_trip(plan: LogicalPlan, catalog: CatalogManagerRef) {
        let convertor = DFLogicalSubstraitConvertor;

        let proto = convertor.encode(plan.clone()).unwrap();
        let tripped_plan = convertor.decode(proto, catalog).await.unwrap();

        assert_eq!(format!("{plan:?}"), format!("{tripped_plan:?}"));
    }

    #[tokio::test]
    async fn test_table_scan() {
        let catalog_manager = build_mock_catalog_manager().await;
        let table_ref = Arc::new(EmptyTable::new(build_create_table_request(
            DEFAULT_TABLE_NAME,
        )));
        catalog_manager
            .register_table(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: DEFAULT_TABLE_NAME.to_string(),
                table_id: 1,
                table: table_ref.clone(),
            })
            .await
            .unwrap();
        let adapter = Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(table_ref),
        )));

        let projection = vec![1, 3, 5];
        let df_schema = adapter.schema().to_dfschema().unwrap();
        let projected_fields = projection
            .iter()
            .map(|index| df_schema.field(*index).clone())
            .collect();
        let projected_schema =
            Arc::new(DFSchema::new_with_metadata(projected_fields, Default::default()).unwrap());

        let table_name = TableReference::full(
            DEFAULT_CATALOG_NAME,
            DEFAULT_SCHEMA_NAME,
            DEFAULT_TABLE_NAME,
        );
        let table_scan_plan = LogicalPlan::TableScan(TableScan {
            table_name,
            source: adapter,
            projection: Some(projection),
            projected_schema,
            filters: vec![],
            fetch: None,
        });

        logical_plan_round_trip(table_scan_plan, catalog_manager).await;
    }
}
