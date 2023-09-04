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

use std::collections::HashMap;

use api::helper::ColumnDataTypeWrapper;
use api::v1::alter_expr::Kind;
use api::v1::ddl_request::Expr as DdlExpr;
use api::v1::greptime_request::Request;
use api::v1::region::{
    region_request, InsertRequest as RegionInsertRequest, InsertRequests as RegionInsertRequests,
};
use api::v1::value::ValueData;
use api::v1::{
    AlterExpr, Column, ColumnDataType, ColumnSchema, DdlRequest, InsertRequest, InsertRequests,
    Row, RowInsertRequest, RowInsertRequests, Rows, SemanticType, Value,
};
use catalog::CatalogManagerRef;
use common_base::BitVec;
use common_catalog::consts::default_engine;
use common_grpc_expr::util::{extract_new_columns, ColumnExpr};
use common_query::Output;
use common_telemetry::info;
use datatypes::schema::Schema;
use datatypes::vectors::VectorRef;
use servers::query_handler::grpc::GrpcQueryHandlerRef;
use session::context::QueryContextRef;
use snafu::prelude::*;
use store_api::storage::RegionId;
use table::engine::TableReference;
use table::metadata::TableInfoRef;
use table::requests::InsertRequest as TableInsertRequest;
use table::TableRef;

use crate::error::{
    CatalogSnafu, ColumnDataTypeSnafu, ColumnNotFoundSnafu, EmptyDataSnafu, Error,
    FindNewColumnsOnInsertionSnafu, InvalidInsertRequestSnafu, MissingTimeIndexColumnSnafu, Result,
    TableNotFoundSnafu,
};
use crate::expr_factory::CreateExprFactory;
use crate::instance::region_handler::RegionRequestHandlerRef;

pub(crate) struct Inserter<'a> {
    catalog_manager: &'a CatalogManagerRef,
    create_expr_factory: &'a CreateExprFactory,
    grpc_query_handler: &'a GrpcQueryHandlerRef<Error>,
    region_request_handler: &'a RegionRequestHandlerRef,
}

impl<'a> Inserter<'a> {
    pub fn new(
        catalog_manager: &'a CatalogManagerRef,
        create_expr_factory: &'a CreateExprFactory,
        grpc_query_handler: &'a GrpcQueryHandlerRef<Error>,
        region_request_handler: &'a RegionRequestHandlerRef,
    ) -> Self {
        Self {
            catalog_manager,
            create_expr_factory,
            grpc_query_handler,
            region_request_handler,
        }
    }

    pub async fn handle_column_inserts(
        &self,
        requests: InsertRequests,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        let row_inserts = requests_column_to_row(requests)?;
        self.handle_row_inserts(row_inserts, ctx).await
    }

    pub async fn handle_row_inserts(
        &self,
        requests: RowInsertRequests,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        requests.inserts.iter().try_for_each(|req| {
            let non_empty = req.rows.as_ref().map(|r| !r.rows.is_empty());
            let non_empty = non_empty.unwrap_or_default();
            non_empty.then_some(()).with_context(|| EmptyDataSnafu {
                msg: format!("insert to table: {:?}", &req.table_name),
            })
        })?;

        self.create_or_alter_tables_on_demand(&requests, &ctx)
            .await?;
        let region_request = self.convert_req_row_to_region(requests, &ctx).await?;
        let response = self
            .region_request_handler
            .handle(region_request, ctx)
            .await?;
        Ok(Output::AffectedRows(response.affected_rows as _))
    }

    pub fn convert_req_table_to_region(
        table_info: &TableInfoRef,
        insert: TableInsertRequest,
    ) -> Result<RegionInsertRequests> {
        let region_id = RegionId::new(table_info.table_id(), insert.region_number).into();
        let row_count = row_count(&insert.columns_values)?;
        let schema = column_schema(table_info, &insert.columns_values)?;
        let rows = api::helper::vectors_to_rows(insert.columns_values.values(), row_count);
        Ok(RegionInsertRequests {
            requests: vec![RegionInsertRequest {
                region_id,
                rows: Some(Rows { schema, rows }),
            }],
        })
    }
}

impl<'a> Inserter<'a> {
    // check if tables already exist:
    // - if table does not exist, create table by inferred CreateExpr
    // - if table exist, check if schema matches. If any new column found, alter table by inferred `AlterExpr`
    async fn create_or_alter_tables_on_demand(
        &self,
        requests: &RowInsertRequests,
        ctx: &QueryContextRef,
    ) -> Result<()> {
        // TODO(jeremy): create and alter in batch?
        for req in &requests.inserts {
            match self.get_table(req, ctx).await? {
                Some(table) => {
                    validate_request_with_table(req, &table)?;
                    self.alter_table_on_demand(req, table, ctx).await?
                }
                None => self.create_table(req, ctx).await?,
            }
        }

        Ok(())
    }

    async fn get_table(
        &self,
        req: &RowInsertRequest,
        ctx: &QueryContextRef,
    ) -> Result<Option<TableRef>> {
        self.catalog_manager
            .table(ctx.current_catalog(), ctx.current_schema(), &req.table_name)
            .await
            .context(CatalogSnafu)
    }

    async fn alter_table_on_demand(
        &self,
        req: &RowInsertRequest,
        table: TableRef,
        ctx: &QueryContextRef,
    ) -> Result<()> {
        let catalog_name = ctx.current_catalog();
        let schema_name = ctx.current_schema();

        let request_schema = req.rows.as_ref().unwrap().schema.as_slice();
        let column_exprs = ColumnExpr::from_column_schemas(request_schema);
        let add_columns = extract_new_columns(&table.schema(), column_exprs)
            .context(FindNewColumnsOnInsertionSnafu)?;
        let Some(add_columns) = add_columns else {
            return Ok(());
        };

        let table_name = table.table_info().name.clone();
        info!(
            "Adding new columns: {:?} to table: {}.{}.{}",
            add_columns, catalog_name, schema_name, table_name
        );

        let alter_table_expr = AlterExpr {
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
            kind: Some(Kind::AddColumns(add_columns)),
            ..Default::default()
        };

        let req = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::Alter(alter_table_expr)),
        });
        self.grpc_query_handler.do_query(req, ctx.clone()).await?;

        info!(
            "Successfully added new columns to table: {}.{}.{}",
            catalog_name, schema_name, table_name
        );

        Ok(())
    }

    async fn create_table(&self, req: &RowInsertRequest, ctx: &QueryContextRef) -> Result<()> {
        let table_ref =
            TableReference::full(ctx.current_catalog(), ctx.current_schema(), &req.table_name);
        let request_schema = req.rows.as_ref().unwrap().schema.as_slice();

        info!(
            "Table {}.{}.{} does not exist, try create table",
            table_ref.catalog, table_ref.schema, table_ref.table,
        );

        let create_table_expr = self
            .create_expr_factory
            .create_table_expr_by_column_schemas(&table_ref, request_schema, default_engine())?;

        let req = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::CreateTable(create_table_expr)),
        });
        self.grpc_query_handler.do_query(req, ctx.clone()).await?;

        info!(
            "Successfully created table on insertion: {}.{}.{}",
            table_ref.catalog, table_ref.schema, table_ref.table,
        );

        Ok(())
    }

    async fn convert_req_row_to_region(
        &self,
        requests: RowInsertRequests,
        ctx: &QueryContextRef,
    ) -> Result<region_request::Body> {
        let mut region_request = Vec::with_capacity(requests.inserts.len());
        for request in requests.inserts {
            let table = self.get_table(&request, ctx).await?;
            let table = table.with_context(|| TableNotFoundSnafu {
                table_name: request.table_name.clone(),
            })?;

            let region_id = RegionId::new(table.table_info().table_id(), request.region_number);
            let insert_request = RegionInsertRequest {
                region_id: region_id.into(),
                rows: request.rows,
            };
            region_request.push(insert_request);
        }

        Ok(region_request::Body::Inserts(RegionInsertRequests {
            requests: region_request,
        }))
    }
}

fn requests_column_to_row(requests: InsertRequests) -> Result<RowInsertRequests> {
    requests
        .inserts
        .into_iter()
        .map(request_column_to_row)
        .collect::<Result<Vec<_>>>()
        .map(|inserts| RowInsertRequests { inserts })
}

fn request_column_to_row(request: InsertRequest) -> Result<RowInsertRequest> {
    let row_count = request.row_count as usize;
    let column_count = request.columns.len();
    let mut schema = Vec::with_capacity(column_count);
    let mut rows = vec![
        Row {
            values: Vec::with_capacity(column_count)
        };
        row_count
    ];
    for column in request.columns {
        let column_schema = ColumnSchema {
            column_name: column.column_name.clone(),
            datatype: column.datatype,
            semantic_type: column.semantic_type,
        };
        schema.push(column_schema);

        push_column_to_rows(column, &mut rows)?;
    }

    Ok(RowInsertRequest {
        table_name: request.table_name,
        rows: Some(Rows { schema, rows }),
        region_number: request.region_number,
    })
}

fn push_column_to_rows(column: Column, rows: &mut [Row]) -> Result<()> {
    let null_mask = BitVec::from_vec(column.null_mask);
    let column_type = ColumnDataTypeWrapper::try_new(column.datatype)
        .context(ColumnDataTypeSnafu)?
        .datatype();
    let column_values = column.values.unwrap_or_default();

    macro_rules! push_column_values_match_types {
        ($( ($arm:tt, $value_data_variant:tt, $field_name:tt), )*) => { match column_type { $(

        ColumnDataType::$arm => {
            let row_count = rows.len();
            let actual_row_count = null_mask.count_ones() + column_values.$field_name.len();
            ensure!(
                actual_row_count == row_count,
                InvalidInsertRequestSnafu {
                    reason: format!(
                        "Expecting {} rows of data for column '{}', but got {}.",
                        row_count, column.column_name, actual_row_count
                    ),
                }
            );

            let mut null_mask_iter = null_mask.into_iter();
            let mut values_iter = column_values.$field_name.into_iter();

            for row in rows {
                let value_is_null = null_mask_iter.next();
                if value_is_null == Some(true) {
                    row.values.push(Value { value_data: None });
                } else {
                    // previous check ensures that there is a value for each row
                    let value = values_iter.next().unwrap();
                    row.values.push(Value {
                        value_data: Some(ValueData::$value_data_variant(value)),
                    });
                }
            }
        }

        )* }}
    }

    push_column_values_match_types!(
        (Boolean, BoolValue, bool_values),
        (Int8, I8Value, i8_values),
        (Int16, I16Value, i16_values),
        (Int32, I32Value, i32_values),
        (Int64, I64Value, i64_values),
        (Uint8, U8Value, u8_values),
        (Uint16, U16Value, u16_values),
        (Uint32, U32Value, u32_values),
        (Uint64, U64Value, u64_values),
        (Float32, F32Value, f32_values),
        (Float64, F64Value, f64_values),
        (Binary, BinaryValue, binary_values),
        (String, StringValue, string_values),
        (Date, DateValue, date_values),
        (Datetime, DatetimeValue, datetime_values),
        (TimestampSecond, TsSecondValue, ts_second_values),
        (
            TimestampMillisecond,
            TsMillisecondValue,
            ts_millisecond_values
        ),
        (
            TimestampMicrosecond,
            TsMicrosecondValue,
            ts_microsecond_values
        ),
        (TimestampNanosecond, TsNanosecondValue, ts_nanosecond_values),
        (TimeSecond, TimeSecondValue, time_second_values),
        (
            TimeMillisecond,
            TimeMillisecondValue,
            time_millisecond_values
        ),
        (
            TimeMicrosecond,
            TimeMicrosecondValue,
            time_microsecond_values
        ),
        (TimeNanosecond, TimeNanosecondValue, time_nanosecond_values),
        (
            IntervalYearMonth,
            IntervalYearMonthValues,
            interval_year_month_values
        ),
        (
            IntervalDayTime,
            IntervalDayTimeValues,
            interval_day_time_values
        ),
        (
            IntervalMonthDayNano,
            IntervalMonthDayNanoValues,
            interval_month_day_nano_values
        ),
    );

    Ok(())
}

fn validate_request_with_table(req: &RowInsertRequest, table: &TableRef) -> Result<()> {
    let request_schema = req.rows.as_ref().unwrap().schema.as_slice();
    let table_schema = table.schema();

    validate_required_columns(request_schema, &table_schema)?;

    Ok(())
}

fn validate_required_columns(request_schema: &[ColumnSchema], table_schema: &Schema) -> Result<()> {
    for column_schema in table_schema.column_schemas() {
        if column_schema.is_nullable() || column_schema.default_constraint().is_some() {
            continue;
        }
        if !request_schema
            .iter()
            .any(|c| c.column_name == column_schema.name)
        {
            return InvalidInsertRequestSnafu {
                reason: format!(
                    "Expecting insert data to be presented on a not null or no default value column '{}'.",
                    &column_schema.name
                )
            }.fail();
        }
    }
    Ok(())
}

fn row_count(columns: &HashMap<String, VectorRef>) -> Result<usize> {
    let mut columns_iter = columns.values();

    let len = columns_iter
        .next()
        .map(|column| column.len())
        .unwrap_or_default();
    ensure!(
        columns_iter.all(|column| column.len() == len),
        InvalidInsertRequestSnafu {
            reason: "The row count of columns is not the same."
        }
    );

    Ok(len)
}

fn column_schema(
    table_info: &TableInfoRef,
    columns: &HashMap<String, VectorRef>,
) -> Result<Vec<ColumnSchema>> {
    let table_meta = &table_info.meta;
    let mut schema = vec![];

    for (column_name, vector) in columns {
        let time_index_column = &table_meta
            .schema
            .timestamp_column()
            .with_context(|| table::error::MissingTimeIndexColumnSnafu {
                table_name: table_info.name.to_string(),
            })
            .context(MissingTimeIndexColumnSnafu)?
            .name;
        let semantic_type = if column_name == time_index_column {
            SemanticType::Timestamp
        } else {
            let column_index = table_meta
                .schema
                .column_index_by_name(column_name)
                .context(ColumnNotFoundSnafu {
                    msg: format!("unable to find column {column_name} in table schema"),
                })?;
            if table_meta.primary_key_indices.contains(&column_index) {
                SemanticType::Tag
            } else {
                SemanticType::Field
            }
        };

        let datatype: ColumnDataTypeWrapper =
            vector.data_type().try_into().context(ColumnDataTypeSnafu)?;

        schema.push(ColumnSchema {
            column_name: column_name.clone(),
            datatype: datatype.datatype().into(),
            semantic_type: semantic_type.into(),
        });
    }

    Ok(schema)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::column::Values;
    use api::v1::SemanticType;
    use common_base::bit_vec::prelude::*;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use datatypes::prelude::{ConcreteDataType, Value as DtValue};
    use datatypes::scalars::ScalarVectorBuilder;
    use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema as DtColumnSchema};
    use datatypes::vectors::{Int16VectorBuilder, MutableVector, StringVectorBuilder};
    use table::metadata::{TableInfoBuilder, TableMetaBuilder};

    use super::*;

    #[test]
    fn test_validate_required_columns() {
        let schema = Schema::new(vec![
            DtColumnSchema::new("a", ConcreteDataType::int32_datatype(), true)
                .with_default_constraint(None)
                .unwrap(),
            DtColumnSchema::new("b", ConcreteDataType::int32_datatype(), true)
                .with_default_constraint(Some(ColumnDefaultConstraint::Value(DtValue::Int32(100))))
                .unwrap(),
        ]);
        let request_schema = &[ColumnSchema {
            column_name: "c".to_string(),
            ..Default::default()
        }];
        // If nullable is true, it doesn't matter whether the insert request has the column.
        validate_required_columns(request_schema, &schema).unwrap();

        let schema = Schema::new(vec![
            DtColumnSchema::new("a", ConcreteDataType::int32_datatype(), false)
                .with_default_constraint(None)
                .unwrap(),
            DtColumnSchema::new("b", ConcreteDataType::int32_datatype(), false)
                .with_default_constraint(Some(ColumnDefaultConstraint::Value(DtValue::Int32(-100))))
                .unwrap(),
        ]);
        let request_schema = &[ColumnSchema {
            column_name: "a".to_string(),
            ..Default::default()
        }];
        // If nullable is false, but the column is defined with default value,
        // it also doesn't matter whether the insert request has the column.
        validate_required_columns(request_schema, &schema).unwrap();

        let request_schema = &[ColumnSchema {
            column_name: "b".to_string(),
            ..Default::default()
        }];
        // Neither of the above cases.
        assert!(validate_required_columns(request_schema, &schema).is_err());
    }

    #[test]
    fn test_request_column_to_row() {
        let insert_request = InsertRequest {
            table_name: String::from("test_table"),
            row_count: 3,
            region_number: 1,
            columns: vec![
                Column {
                    column_name: String::from("col1"),
                    datatype: ColumnDataType::Int32.into(),
                    semantic_type: SemanticType::Field.into(),
                    null_mask: bitvec![u8, Lsb0; 1, 0, 1].into_vec(),
                    values: Some(Values {
                        i32_values: vec![42],
                        ..Default::default()
                    }),
                },
                Column {
                    column_name: String::from("col2"),
                    datatype: ColumnDataType::String.into(),
                    semantic_type: SemanticType::Tag.into(),
                    null_mask: vec![],
                    values: Some(Values {
                        string_values: vec![
                            String::from("value1"),
                            String::from("value2"),
                            String::from("value3"),
                        ],
                        ..Default::default()
                    }),
                },
            ],
        };

        let result = request_column_to_row(insert_request);
        let row_insert_request = result.unwrap();
        assert_eq!(row_insert_request.table_name, "test_table");
        assert_eq!(row_insert_request.region_number, 1);
        let rows = row_insert_request.rows.unwrap();
        assert_eq!(
            rows.schema,
            vec![
                ColumnSchema {
                    column_name: String::from("col1"),
                    datatype: ColumnDataType::Int32.into(),
                    semantic_type: SemanticType::Field.into(),
                },
                ColumnSchema {
                    column_name: String::from("col2"),
                    datatype: ColumnDataType::String.into(),
                    semantic_type: SemanticType::Tag.into(),
                },
            ]
        );
        assert_eq!(
            rows.rows,
            vec![
                Row {
                    values: vec![
                        Value { value_data: None },
                        Value {
                            value_data: Some(ValueData::StringValue(String::from("value1"))),
                        },
                    ],
                },
                Row {
                    values: vec![
                        Value {
                            value_data: Some(ValueData::I32Value(42)),
                        },
                        Value {
                            value_data: Some(ValueData::StringValue(String::from("value2"))),
                        },
                    ],
                },
                Row {
                    values: vec![
                        Value { value_data: None },
                        Value {
                            value_data: Some(ValueData::StringValue(String::from("value3"))),
                        },
                    ],
                },
            ]
        );

        let invalid_request_with_wrong_type = InsertRequest {
            table_name: String::from("test_table"),
            row_count: 3,
            region_number: 1,
            columns: vec![Column {
                column_name: String::from("col1"),
                datatype: ColumnDataType::Int32.into(),
                semantic_type: SemanticType::Field.into(),
                null_mask: bitvec![u8, Lsb0; 1, 0, 1].into_vec(),
                values: Some(Values {
                    i8_values: vec![42],
                    ..Default::default()
                }),
            }],
        };
        assert!(request_column_to_row(invalid_request_with_wrong_type).is_err());

        let invalid_request_with_wrong_row_count = InsertRequest {
            table_name: String::from("test_table"),
            row_count: 3,
            region_number: 1,
            columns: vec![Column {
                column_name: String::from("col1"),
                datatype: ColumnDataType::Int32.into(),
                semantic_type: SemanticType::Field.into(),
                null_mask: bitvec![u8, Lsb0; 0, 0, 1].into_vec(),
                values: Some(Values {
                    i32_values: vec![42],
                    ..Default::default()
                }),
            }],
        };
        assert!(request_column_to_row(invalid_request_with_wrong_row_count).is_err());

        let invalid_request_with_wrong_row_count = InsertRequest {
            table_name: String::from("test_table"),
            row_count: 3,
            region_number: 1,
            columns: vec![Column {
                column_name: String::from("col1"),
                datatype: ColumnDataType::Int32.into(),
                semantic_type: SemanticType::Field.into(),
                null_mask: vec![],
                values: Some(Values {
                    i32_values: vec![42],
                    ..Default::default()
                }),
            }],
        };
        assert!(request_column_to_row(invalid_request_with_wrong_row_count).is_err());
    }

    #[test]
    fn test_insert_request_table_to_region() {
        let schema = Schema::new(vec![
            DtColumnSchema::new("ts", ConcreteDataType::int64_datatype(), false)
                .with_time_index(true),
            DtColumnSchema::new("id", ConcreteDataType::int16_datatype(), false),
            DtColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
        ]);

        let table_meta = TableMetaBuilder::default()
            .schema(Arc::new(schema))
            .primary_key_indices(vec![2])
            .next_column_id(3)
            .build()
            .unwrap();

        let table_info = Arc::new(
            TableInfoBuilder::default()
                .name("demo")
                .meta(table_meta)
                .table_id(1)
                .build()
                .unwrap(),
        );

        let insert_request = mock_insert_request();
        let mut request =
            Inserter::convert_req_table_to_region(&table_info, insert_request).unwrap();

        assert_eq!(request.requests.len(), 1);
        verify_region_insert_request(request.requests.pop().unwrap());
    }

    fn mock_insert_request() -> TableInsertRequest {
        let mut builder = StringVectorBuilder::with_capacity(3);
        builder.push(Some("host1"));
        builder.push(None);
        builder.push(Some("host3"));
        let host = builder.to_vector();

        let mut builder = Int16VectorBuilder::with_capacity(3);
        builder.push(Some(1_i16));
        builder.push(Some(2_i16));
        builder.push(Some(3_i16));
        let id = builder.to_vector();

        let columns_values = HashMap::from([("host".to_string(), host), ("id".to_string(), id)]);

        TableInsertRequest {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: "demo".to_string(),
            columns_values,
            region_number: 0,
        }
    }

    fn verify_region_insert_request(request: RegionInsertRequest) {
        assert_eq!(request.region_id, RegionId::new(1, 0).as_u64());

        let rows = request.rows.unwrap();
        for (i, column) in rows.schema.iter().enumerate() {
            let name = &column.column_name;
            if name == "id" {
                assert_eq!(ColumnDataType::Int16 as i32, column.datatype);
                assert_eq!(SemanticType::Field as i32, column.semantic_type);
                let values = rows
                    .rows
                    .iter()
                    .map(|row| row.values[i].value_data.clone())
                    .collect::<Vec<_>>();
                assert_eq!(
                    vec![
                        Some(ValueData::I16Value(1)),
                        Some(ValueData::I16Value(2)),
                        Some(ValueData::I16Value(3))
                    ],
                    values
                );
            }
            if name == "host" {
                assert_eq!(ColumnDataType::String as i32, column.datatype);
                assert_eq!(SemanticType::Tag as i32, column.semantic_type);
                let values = rows
                    .rows
                    .iter()
                    .map(|row| row.values[i].value_data.clone())
                    .collect::<Vec<_>>();
                assert_eq!(
                    vec![
                        Some(ValueData::StringValue("host1".to_string())),
                        None,
                        Some(ValueData::StringValue("host3".to_string()))
                    ],
                    values
                );
            }
        }
    }
}
