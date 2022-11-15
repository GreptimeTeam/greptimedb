use std::result::Result;
use std::sync::Arc;

use api::v1::codec;
use datafusion::physical_plan::expressions::Column as DfColumn;
use datafusion::physical_plan::PhysicalExpr as DfPhysicalExpr;
use snafu::OptionExt;

use crate::error::{EmptyPhysicalExprSnafu, Error, UnsupportedDfExprSnafu};

// grpc -> datafusion (physical expr)
pub(crate) fn parse_grpc_physical_expr(
    proto: &codec::PhysicalExprNode,
) -> Result<Arc<dyn DfPhysicalExpr>, Error> {
    let expr_type = proto.expr_type.as_ref().context(EmptyPhysicalExprSnafu {
        name: format!("{:?}", proto),
    })?;

    // TODO(fys): impl other physical expr
    let pexpr: Arc<dyn DfPhysicalExpr> = match expr_type {
        codec::physical_expr_node::ExprType::Column(c) => {
            let pcol = DfColumn::new(&c.name, c.index as usize);
            Arc::new(pcol)
        }
    };
    Ok(pexpr)
}

// datafusion -> grpc (physical expr)
pub(crate) fn parse_df_physical_expr(
    df_expr: Arc<dyn DfPhysicalExpr>,
) -> Result<codec::PhysicalExprNode, Error> {
    let expr = df_expr.as_any();

    // TODO(fys): impl other physical expr
    if let Some(expr) = expr.downcast_ref::<DfColumn>() {
        Ok(codec::PhysicalExprNode {
            expr_type: Some(codec::physical_expr_node::ExprType::Column(
                codec::PhysicalColumn {
                    name: expr.name().to_string(),
                    index: expr.index() as u64,
                },
            )),
        })
    } else {
        UnsupportedDfExprSnafu {
            name: df_expr.to_string(),
        }
        .fail()?
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::codec::physical_expr_node::ExprType::Column;
    use api::v1::codec::{PhysicalColumn, PhysicalExprNode};
    use datafusion::physical_plan::expressions::Column as DfColumn;
    use datafusion::physical_plan::PhysicalExpr;

    use crate::physical::expr::{parse_df_physical_expr, parse_grpc_physical_expr};

    #[test]
    fn test_column_convert() {
        // mock df_column_expr
        let df_column = DfColumn::new("name", 11);
        let df_column_clone = df_column.clone();
        let df_expr = Arc::new(df_column) as Arc<dyn PhysicalExpr>;

        // mock grpc_column_expr
        let grpc_expr = PhysicalExprNode {
            expr_type: Some(Column(PhysicalColumn {
                name: "name".to_owned(),
                index: 11,
            })),
        };

        let result = parse_df_physical_expr(df_expr).unwrap();
        assert_eq!(grpc_expr, result);

        let result = parse_grpc_physical_expr(&grpc_expr).unwrap();
        let df_column = result.as_any().downcast_ref::<DfColumn>().unwrap();
        assert_eq!(df_column_clone, df_column.to_owned());
    }
}
