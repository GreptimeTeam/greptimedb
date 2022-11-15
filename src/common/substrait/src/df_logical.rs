// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use bytes::{Buf, Bytes, BytesMut};
use catalog::CatalogManagerRef;
use common_error::prelude::BoxedError;
use datafusion::datasource::TableProvider;
use datafusion::logical_plan::{LogicalPlan, TableScan, ToDFSchema};
use datafusion::physical_plan::project_schema;
use prost::Message;
use snafu::{ensure, OptionExt, ResultExt};
use substrait_proto::protobuf::expression::mask_expression::{StructItem, StructSelect};
use substrait_proto::protobuf::expression::MaskExpression;
use substrait_proto::protobuf::plan_rel::RelType as PlanRelType;
use substrait_proto::protobuf::read_rel::{NamedTable, ReadType};
use substrait_proto::protobuf::rel::RelType;
use substrait_proto::protobuf::{PlanRel, ReadRel, Rel};
use table::table::adapter::DfTableProviderAdapter;

use crate::error::{
    DFInternalSnafu, DecodeRelSnafu, EmptyPlanSnafu, EncodeRelSnafu, Error, InternalSnafu,
    InvalidParametersSnafu, MissingFieldSnafu, SchemaNotMatchSnafu, TableNotFoundSnafu,
    UnknownPlanSnafu, UnsupportedExprSnafu, UnsupportedPlanSnafu,
};
use crate::schema::{from_schema, to_schema};
use crate::SubstraitPlan;

pub struct DFLogicalSubstraitConvertor {
    catalog_manager: CatalogManagerRef,
}

impl SubstraitPlan for DFLogicalSubstraitConvertor {
    type Error = Error;

    type Plan = LogicalPlan;

    fn decode<B: Buf + Send>(&self, message: B) -> Result<Self::Plan, Self::Error> {
        let plan_rel = PlanRel::decode(message).context(DecodeRelSnafu)?;
        let rel = match plan_rel.rel_type.context(EmptyPlanSnafu)? {
            PlanRelType::Rel(rel) => rel,
            PlanRelType::Root(_) => UnsupportedPlanSnafu {
                name: "Root Relation",
            }
            .fail()?,
        };
        self.convert_rel(rel)
    }

    fn encode(&self, plan: Self::Plan) -> Result<Bytes, Self::Error> {
        let rel = self.convert_plan(plan)?;
        let plan_rel = PlanRel {
            rel_type: Some(PlanRelType::Rel(rel)),
        };

        let mut buf = BytesMut::new();
        plan_rel.encode(&mut buf).context(EncodeRelSnafu)?;

        Ok(buf.freeze())
    }
}

impl DFLogicalSubstraitConvertor {
    pub fn new(catalog_manager: CatalogManagerRef) -> Self {
        Self { catalog_manager }
    }
}

impl DFLogicalSubstraitConvertor {
    pub fn convert_rel(&self, rel: Rel) -> Result<LogicalPlan, Error> {
        let rel_type = rel.rel_type.context(EmptyPlanSnafu)?;
        let logical_plan = match rel_type {
            RelType::Read(read_rel) => self.convert_read_rel(read_rel),
            RelType::Filter(_filter_rel) => UnsupportedPlanSnafu {
                name: "Filter Relation",
            }
            .fail()?,
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
        }?;

        Ok(logical_plan)
    }

    fn convert_read_rel(&self, read_rel: Box<ReadRel>) -> Result<LogicalPlan, Error> {
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

        // Get table handle from catalog manager
        let table_ref = self
            .catalog_manager
            .table(&catalog_name, &schema_name, &table_name)
            .map_err(BoxedError::new)
            .context(InternalSnafu)?
            .context(TableNotFoundSnafu {
                name: format!("{}.{}.{}", catalog_name, schema_name, table_name),
            })?;
        let adapter = Arc::new(DfTableProviderAdapter::new(table_ref));

        // Get schema directly from the table, and compare it with the schema retrived from substrait proto.
        let stored_schema = adapter.schema();
        let retrived_schema = to_schema(read_rel.base_schema.unwrap_or_default())?;
        let retrived_arrow_schema = retrived_schema.arrow_schema();
        ensure!(
            stored_schema.fields == retrived_arrow_schema.fields,
            SchemaNotMatchSnafu {
                substrait_schema: retrived_arrow_schema.clone(),
                storage_schema: stored_schema
            }
        );

        // Calculate the projected schema
        let projected_schema = project_schema(&stored_schema, projection.as_ref())
            .context(DFInternalSnafu)?
            .to_dfschema_ref()
            .context(DFInternalSnafu)?;

        // TODO(ruihang): Support filters and limit
        Ok(LogicalPlan::TableScan(TableScan {
            table_name,
            source: adapter,
            projection,
            projected_schema,
            filters: vec![],
            limit: None,
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
    pub fn convert_plan(&self, plan: LogicalPlan) -> Result<Rel, Error> {
        match plan {
            LogicalPlan::Projection(_) => UnsupportedPlanSnafu {
                name: "DataFusion Logical Projection",
            }
            .fail()?,
            LogicalPlan::Filter(_) => UnsupportedPlanSnafu {
                name: "DataFusion Logical Filter",
            }
            .fail()?,
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
                let read_rel = self.convert_table_scan_plan(table_scan)?;
                Ok(Rel {
                    rel_type: Some(RelType::Read(Box::new(read_rel))),
                })
            }
            LogicalPlan::EmptyRelation(_) => UnsupportedPlanSnafu {
                name: "DataFusion Logical EmptyRelation",
            }
            .fail()?,
            LogicalPlan::Limit(_) => UnsupportedPlanSnafu {
                name: "DataFusion Logical Limit",
            }
            .fail()?,
            LogicalPlan::CreateExternalTable(_)
            | LogicalPlan::CreateMemoryTable(_)
            | LogicalPlan::DropTable(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::Explain(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Extension(_) => InvalidParametersSnafu {
                reason: format!(
                    "Trying to convert DDL/DML plan to substrait proto, plan: {:?}",
                    plan
                ),
            }
            .fail()?,
        }
    }

    pub fn convert_table_scan_plan(&self, table_scan: TableScan) -> Result<ReadRel, Error> {
        let provider = table_scan
            .source
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
            .map(|proj| self.convert_schema_projection(&proj));

        // assemble base (unprojected) schema using Table's schema.
        let base_schema = from_schema(&provider.table().schema())?;

        let read_rel = ReadRel {
            common: None,
            base_schema: Some(base_schema),
            filter: None,
            projection,
            advanced_extension: None,
            read_type: Some(read_type),
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

#[cfg(test)]
mod test {
    use catalog::local::{LocalCatalogManager, MemoryCatalogProvider, MemorySchemaProvider};
    use catalog::{CatalogList, CatalogProvider, RegisterTableRequest};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use datafusion::logical_plan::DFSchema;
    use datatypes::schema::Schema;
    use table::requests::CreateTableRequest;
    use table::test_util::{EmptyTable, MockTableEngine};

    use super::*;
    use crate::schema::test::supported_types;

    const DEFAULT_TABLE_NAME: &str = "SubstraitTable";

    async fn build_mock_catalog_manager() -> CatalogManagerRef {
        let mock_table_engine = Arc::new(MockTableEngine::new());
        let catalog_manager = Arc::new(
            LocalCatalogManager::try_new(mock_table_engine)
                .await
                .unwrap(),
        );
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
            schema: Arc::new(Schema::new(supported_types())),
            region_numbers: vec![0],
            primary_key_indices: vec![],
            create_if_not_exists: true,
            table_options: Default::default(),
        }
    }

    async fn logical_plan_round_trip(plan: LogicalPlan, catalog: CatalogManagerRef) {
        let convertor = DFLogicalSubstraitConvertor::new(catalog);

        let proto = convertor.encode(plan.clone()).unwrap();
        let tripped_plan = convertor.decode(proto).unwrap();

        assert_eq!(format!("{:?}", plan), format!("{:?}", tripped_plan));
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
        let adapter = Arc::new(DfTableProviderAdapter::new(table_ref));
        let projection = vec![1, 3, 5];
        let df_schema = adapter.schema().to_dfschema().unwrap();
        let projected_fields = projection
            .iter()
            .map(|index| df_schema.field(*index).clone())
            .collect();
        let projected_schema =
            Arc::new(DFSchema::new_with_metadata(projected_fields, Default::default()).unwrap());

        let table_scan_plan = LogicalPlan::TableScan(TableScan {
            table_name: DEFAULT_TABLE_NAME.to_string(),
            source: adapter,
            projection: Some(projection),
            projected_schema,
            filters: vec![],
            limit: None,
        });

        logical_plan_round_trip(table_scan_plan, catalog_manager).await;
    }
}
