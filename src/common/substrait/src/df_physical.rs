use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Buf, Bytes, BytesMut};
use catalog::CatalogManagerRef;
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::filter::FilterExec;
use futures::future::BoxFuture;
use futures::FutureExt;
use prost::Message;
use snafu::ensure;
use snafu::{OptionExt, ResultExt};
use substrait::protobuf::expression::RexType;
use substrait::protobuf::plan_rel::RelType as PlanRelType;
use substrait::protobuf::read_rel::ReadType;
use substrait::protobuf::rel::RelType;
use substrait::protobuf::Rel;
use substrait::protobuf::{Expression, ReadRel};
use substrait::protobuf::{FilterRel, PlanRel};
use table::table::adapter::DfTableProviderAdapter;

use crate::error::Error;
use crate::error::{
    DFInternalSnafu, DecodeRelSnafu, EmptyExprSnafu, EmptyPlanSnafu, EncodeRelSnafu, InternalSnafu,
    InvalidParametersSnafu, MissingFieldSnafu, TableNotFoundSnafu, UnsupportedExprSnafu,
    UnsupportedPlanSnafu,
};
use crate::{ExecutionPlanRef, PhysicalExprRef, SubstraitPlan};

pub struct DFExecutionSubstraitConvertor {
    catalog_manager: CatalogManagerRef,
}

#[async_trait]
impl SubstraitPlan for DFExecutionSubstraitConvertor {
    type Error = Error;

    type Plan = ExecutionPlanRef;

    async fn from_buf<B: Buf + Send>(&self, message: B) -> Result<Self::Plan, Self::Error> {
        let plan_rel = PlanRel::decode(message).context(DecodeRelSnafu)?;
        let rel = match plan_rel.rel_type.context(EmptyPlanSnafu)? {
            PlanRelType::Rel(rel) => rel,
            PlanRelType::Root(_) => UnsupportedPlanSnafu {
                name: "Root Relation",
            }
            .fail()?,
        };
        self.convert_rel(Box::new(rel)).await
    }

    fn from_plan(&self, plan: Self::Plan) -> Result<Bytes, Self::Error> {
        let rel = self.convert_plan(plan)?;

        let mut buf = BytesMut::new();
        rel.encode(&mut buf).context(EncodeRelSnafu)?;

        Ok(buf.freeze())
    }
}

impl DFExecutionSubstraitConvertor {
    pub fn new(catalog_manager: CatalogManagerRef) -> Self {
        Self { catalog_manager }
    }
}

impl DFExecutionSubstraitConvertor {
    fn convert_rel<'a, 'b>(
        &'b self,
        rel: Box<Rel>,
    ) -> BoxFuture<'a, Result<ExecutionPlanRef, Error>>
    where
        'b: 'a,
    {
        async move {
            let rel_type = rel.rel_type.context(EmptyPlanSnafu)?;
            let exec_plan = match rel_type {
                RelType::Read(read_rel) => self.convert_read_rel(read_rel).await,
                RelType::Filter(filter_rel) => self.convert_filter_rel(filter_rel).await,
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
            Ok(exec_plan)
        }
        .boxed()
    }

    async fn convert_read_rel(&self, read_rel: Box<ReadRel>) -> Result<ExecutionPlanRef, Error> {
        // Extract the catalog, schema and table name from NamedTable. Assume the first three are those names.
        let read_type = read_rel.read_type.context(MissingFieldSnafu {
            field: "read_type",
            plan: "Read",
        })?;
        let (catalog_name, schema_name, table_name) = match read_type {
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

        // Build table scan execution plan via DfTableProviderAdapter
        let adapter = DfTableProviderAdapter::new(table_ref);
        // TODO(ruihang): Support projection, filters and limit
        adapter
            .scan(&None, &[], None)
            .await
            .context(DFInternalSnafu)
    }

    async fn convert_filter_rel(
        &self,
        filter_rel: Box<FilterRel>,
    ) -> Result<ExecutionPlanRef, Error> {
        let input_rel = filter_rel.input.context(EmptyPlanSnafu)?;
        let input = self.convert_rel(input_rel).await?;

        let expr = filter_rel.condition.context(EmptyExprSnafu)?;
        let predicate = Self::convert_expression(expr)?;

        let filter = FilterExec::try_new(predicate, input).context(DFInternalSnafu)?;
        Ok(Arc::new(filter))
    }

    fn convert_expression(expr: Box<Expression>) -> Result<PhysicalExprRef, Error> {
        let rex = expr.rex_type.context(EmptyExprSnafu)?;

        match rex {
            RexType::Literal(_) => todo!(),
            RexType::Selection(_) => todo!(),
            RexType::ScalarFunction(_) => todo!(),
            RexType::WindowFunction(_) => todo!(),
            RexType::IfThen(_) => todo!(),
            RexType::SwitchExpression(_) => todo!(),
            RexType::SingularOrList(_) => todo!(),
            RexType::MultiOrList(_) => todo!(),
            RexType::Cast(_) => todo!(),
            RexType::Subquery(_) => todo!(),
            RexType::Enum(_) => todo!(),
        }
    }
}

impl DFExecutionSubstraitConvertor {
    pub fn convert_plan(&self, _plan: ExecutionPlanRef) -> Result<Box<Rel>, Error> {
        todo!()
    }
}
