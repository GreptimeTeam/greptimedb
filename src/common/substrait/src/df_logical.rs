use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Buf, Bytes, BytesMut};
use catalog::CatalogManagerRef;
use datafusion::datasource::TableProvider;
use datafusion::logical_plan::{LogicalPlan, TableScan, ToDFSchema};
use prost::Message;
use snafu::ensure;
use snafu::{OptionExt, ResultExt};
use substrait_proto::protobuf::plan_rel::RelType as PlanRelType;
use substrait_proto::protobuf::read_rel::{NamedTable, ReadType};
use substrait_proto::protobuf::rel::RelType;
use substrait_proto::protobuf::PlanRel;
use substrait_proto::protobuf::ReadRel;
use substrait_proto::protobuf::Rel;
use table::table::adapter::DfTableProviderAdapter;

use crate::error::Error;
use crate::error::{
    DFInternalSnafu, DecodeRelSnafu, EmptyPlanSnafu, EncodeRelSnafu, InternalSnafu,
    InvalidParametersSnafu, MissingFieldSnafu, TableNotFoundSnafu, UnknownPlanSnafu,
    UnsupportedExprSnafu, UnsupportedPlanSnafu,
};
use crate::SubstraitPlan;

pub struct DFLogicalSubstraitConvertor {
    catalog_manager: CatalogManagerRef,
}

#[async_trait]
impl SubstraitPlan for DFLogicalSubstraitConvertor {
    type Error = Error;

    type Plan = LogicalPlan;

    async fn convert_buf<B: Buf + Send>(&self, message: B) -> Result<Self::Plan, Self::Error> {
        let plan_rel = PlanRel::decode(message).context(DecodeRelSnafu)?;
        let rel = match plan_rel.rel_type.context(EmptyPlanSnafu)? {
            PlanRelType::Rel(rel) => rel,
            PlanRelType::Root(_) => UnsupportedPlanSnafu {
                name: "Root Relation",
            }
            .fail()?,
        };
        self.convert_rel(Box::new(rel))
    }

    fn convert_plan(&self, plan: Self::Plan) -> Result<Bytes, Self::Error> {
        let rel = self.convert_plan(plan)?;

        let mut buf = BytesMut::new();
        rel.encode(&mut buf).context(EncodeRelSnafu)?;

        Ok(buf.freeze())
    }
}

impl DFLogicalSubstraitConvertor {
    pub fn new(catalog_manager: CatalogManagerRef) -> Self {
        Self { catalog_manager }
    }
}

impl DFLogicalSubstraitConvertor {
    pub fn convert_rel(&self, rel: Box<Rel>) -> Result<LogicalPlan, Error> {
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

        // Get table handle from catalog manager
        let table_ref = self
            .catalog_manager
            .table(Some(&catalog_name), Some(&schema_name), &table_name)
            .map_err(|e| Box::new(e) as _)
            .context(InternalSnafu)?
            .context(TableNotFoundSnafu {
                name: format!("{}-{}-{}", catalog_name, schema_name, table_name),
            })?;
        let adapter = Arc::new(DfTableProviderAdapter::new(table_ref));
        // Get schema direct from the table.
        // TODO(ruihang): Maybe need to verify the schema with the one in Substrait?
        let schema = adapter
            .schema()
            .to_dfschema_ref()
            .context(DFInternalSnafu)?;

        // TODO(ruihang): Support projection, filters and limit
        Ok(LogicalPlan::TableScan(TableScan {
            table_name,
            source: adapter,
            projection: None,
            projected_schema: schema,
            filters: vec![],
            limit: None,
        }))
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
                name: "DataFusion Logical Projection",
            }
            .fail()?,
            LogicalPlan::Window(_) => UnsupportedPlanSnafu {
                name: "DataFusion Logical Projection",
            }
            .fail()?,
            LogicalPlan::Aggregate(_) => UnsupportedPlanSnafu {
                name: "DataFusion Logical Projection",
            }
            .fail()?,
            LogicalPlan::Sort(_) => UnsupportedPlanSnafu {
                name: "DataFusion Logical Projection",
            }
            .fail()?,
            LogicalPlan::Join(_) => UnsupportedPlanSnafu {
                name: "DataFusion Logical Projection",
            }
            .fail()?,
            LogicalPlan::CrossJoin(_) => UnsupportedPlanSnafu {
                name: "DataFusion Logical Projection",
            }
            .fail()?,
            LogicalPlan::Repartition(_) => UnsupportedPlanSnafu {
                name: "DataFusion Logical Projection",
            }
            .fail()?,
            LogicalPlan::Union(_) => UnsupportedPlanSnafu {
                name: "DataFusion Logical Projection",
            }
            .fail()?,
            LogicalPlan::TableScan(table_scan) => {
                let read_rel = self.convert_table_scan_plan(table_scan)?;
                Ok(Rel {
                    rel_type: Some(RelType::Read(Box::new(read_rel))),
                })
            }
            LogicalPlan::EmptyRelation(_) => UnsupportedPlanSnafu {
                name: "DataFusion Logical Projection",
            }
            .fail()?,
            LogicalPlan::Limit(_) => UnsupportedPlanSnafu {
                name: "DataFusion Logical Projection",
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

        let catalog_name = table_info.catalog_name.clone();
        let schema_name = table_info.schema_name.clone();
        let table_name = table_info.name.clone();

        let named_table = NamedTable {
            names: vec![catalog_name, schema_name, table_name],
            advanced_extension: None,
        };
        let read_type = ReadType::NamedTable(named_table);

        let read_rel = ReadRel {
            common: None,
            base_schema: None,
            filter: None,
            projection: None,
            advanced_extension: None,
            read_type: Some(read_type),
        };

        Ok(read_rel)
    }
}

#[cfg(test)]
mod test {
    use catalog::{
        memory::{MemoryCatalogProvider, MemorySchemaProvider},
        CatalogList, CatalogProvider, LocalCatalogManager, RegisterTableRequest,
        DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME,
    };
    use datatypes::schema::Schema;
    use table::{engine::MockTableEngine, requests::CreateTableRequest, test_util::EmptyTable};

    use super::*;

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
        catalog_provider.register_schema(DEFAULT_SCHEMA_NAME.to_string(), schema_provider);
        catalog_manager.register_catalog(DEFAULT_CATALOG_NAME.to_string(), catalog_provider);

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
            schema: Arc::new(Schema::new(vec![])),
            primary_key_indices: vec![],
            create_if_not_exists: true,
            table_options: Default::default(),
        }
    }

    async fn logical_plan_round_trip(plan: LogicalPlan, catalog: CatalogManagerRef) {
        let convertor = DFLogicalSubstraitConvertor::new(catalog);

        let rel = convertor.convert_plan(plan.clone()).unwrap();
        let tripped_plan = convertor.convert_rel(Box::new(rel)).unwrap();

        assert_eq!(format!("{:?}", plan), format!("{:?}", tripped_plan));
    }

    #[tokio::test]
    async fn test_bare_table_scan() {
        let catalog_manager = build_mock_catalog_manager().await;
        let table_ref = Arc::new(EmptyTable::new(build_create_table_request(
            DEFAULT_TABLE_NAME,
        )));
        catalog_manager
            .register_table(RegisterTableRequest {
                catalog: Some(DEFAULT_CATALOG_NAME.to_string()),
                schema: Some(DEFAULT_SCHEMA_NAME.to_string()),
                table_name: DEFAULT_TABLE_NAME.to_string(),
                table_id: 1,
                table: table_ref.clone(),
            })
            .await
            .unwrap();
        let adapter = Arc::new(DfTableProviderAdapter::new(table_ref));
        let schema = adapter.schema().to_dfschema_ref().unwrap();

        let table_scan_plan = LogicalPlan::TableScan(TableScan {
            table_name: DEFAULT_TABLE_NAME.to_string(),
            source: adapter,
            projection: None,
            projected_schema: schema,
            filters: vec![],
            limit: None,
        });

        logical_plan_round_trip(table_scan_plan, catalog_manager).await;
    }
}
