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

use std::str::FromStr;

use api::v1::auth_header::AuthScheme;
use api::v1::ddl_request::Expr as DdlExpr;
use api::v1::greptime_request::Request;
use api::v1::query_request::Query;
use api::v1::{
    AlterExpr, AuthHeader, CreateTableExpr, DdlRequest, DropTableExpr, GreptimeRequest,
    InsertRequest, QueryRequest, RequestHeader,
};
use arrow_flight::{FlightData, Ticket};
use common_error::prelude::*;
use common_grpc::flight::{flight_messages_to_recordbatches, FlightDecoder, FlightMessage};
use common_query::Output;
use futures_util::{TryFutureExt, TryStreamExt};
use prost::Message;
use snafu::{ensure, ResultExt};

use crate::error::{ConvertFlightDataSnafu, IllegalFlightMessagesSnafu};
use crate::{error, Client, Result};

#[derive(Clone, Debug)]
pub struct Database {
    // The "catalog" and "schema" to be used in processing the requests at the server side.
    // They are the "hint" or "context", just like how the "database" in "USE" statement is treated in MySQL.
    // They will be carried in the request header.
    catalog: String,
    schema: String,

    client: Client,
    ctx: FlightContext,
}

impl Database {
    pub fn new(catalog: impl Into<String>, schema: impl Into<String>, client: Client) -> Self {
        Self {
            catalog: catalog.into(),
            schema: schema.into(),
            client,
            ctx: FlightContext::default(),
        }
    }

    pub fn catalog(&self) -> &String {
        &self.catalog
    }

    pub fn set_catalog(&mut self, catalog: impl Into<String>) {
        self.catalog = catalog.into();
    }

    pub fn schema(&self) -> &String {
        &self.schema
    }

    pub fn set_schema(&mut self, schema: impl Into<String>) {
        self.schema = schema.into();
    }

    pub fn set_auth(&mut self, auth: AuthScheme) {
        self.ctx.auth_header = Some(AuthHeader {
            auth_scheme: Some(auth),
        });
    }

    pub async fn insert(&self, request: InsertRequest) -> Result<Output> {
        self.do_get(Request::Insert(request)).await
    }

    pub async fn sql(&self, sql: &str) -> Result<Output> {
        self.do_get(Request::Query(QueryRequest {
            query: Some(Query::Sql(sql.to_string())),
        }))
        .await
    }

    pub async fn logical_plan(&self, logical_plan: Vec<u8>) -> Result<Output> {
        self.do_get(Request::Query(QueryRequest {
            query: Some(Query::LogicalPlan(logical_plan)),
        }))
        .await
    }

    pub async fn create(&self, expr: CreateTableExpr) -> Result<Output> {
        self.do_get(Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::CreateTable(expr)),
        }))
        .await
    }

    pub async fn alter(&self, expr: AlterExpr) -> Result<Output> {
        self.do_get(Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::Alter(expr)),
        }))
        .await
    }

    pub async fn drop_table(&self, expr: DropTableExpr) -> Result<Output> {
        self.do_get(Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::DropTable(expr)),
        }))
        .await
    }

    async fn do_get(&self, request: Request) -> Result<Output> {
        let request = GreptimeRequest {
            header: Some(RequestHeader {
                catalog: self.catalog.clone(),
                schema: self.schema.clone(),
                authorization: self.ctx.auth_header.clone(),
            }),
            request: Some(request),
        };
        let request = Ticket {
            ticket: request.encode_to_vec().into(),
        };

        let mut client = self.client.make_client()?;

        // TODO(LFC): Streaming get flight data.
        let flight_data: Vec<FlightData> = client
            .mut_inner()
            .do_get(request)
            .and_then(|response| response.into_inner().try_collect())
            .await
            .map_err(|e| {
                let code = get_metadata_value(&e, INNER_ERROR_CODE)
                    .and_then(|s| StatusCode::from_str(&s).ok())
                    .unwrap_or(StatusCode::Unknown);
                let msg = get_metadata_value(&e, INNER_ERROR_MSG).unwrap_or(e.to_string());
                error::ExternalSnafu { code, msg }
                    .fail::<()>()
                    .map_err(BoxedError::new)
                    .context(error::FlightGetSnafu {
                        tonic_code: e.code(),
                        addr: client.addr(),
                    })
                    .unwrap_err()
            })?;

        let decoder = &mut FlightDecoder::default();
        let flight_messages = flight_data
            .into_iter()
            .map(|x| decoder.try_decode(x).context(ConvertFlightDataSnafu))
            .collect::<Result<Vec<_>>>()?;

        let output = if let Some(FlightMessage::AffectedRows(rows)) = flight_messages.get(0) {
            ensure!(
                flight_messages.len() == 1,
                IllegalFlightMessagesSnafu {
                    reason: "Expect 'AffectedRows' Flight messages to be one and only!"
                }
            );
            Output::AffectedRows(*rows)
        } else {
            let recordbatches = flight_messages_to_recordbatches(flight_messages)
                .context(ConvertFlightDataSnafu)?;
            Output::RecordBatches(recordbatches)
        };
        Ok(output)
    }
}

fn get_metadata_value(e: &tonic::Status, key: &str) -> Option<String> {
    e.metadata()
        .get(key)
        .and_then(|v| String::from_utf8(v.as_bytes().to_vec()).ok())
}

#[derive(Default, Debug, Clone)]
pub struct FlightContext {
    auth_header: Option<AuthHeader>,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::helper::ColumnDataTypeWrapper;
    use api::v1::auth_header::AuthScheme;
    use api::v1::{AuthHeader, Basic, Column};
    use common_grpc::select::{null_mask, values};
    use common_grpc_expr::column_to_vector;
    use datatypes::prelude::{Vector, VectorRef};
    use datatypes::vectors::{
        BinaryVector, BooleanVector, DateTimeVector, DateVector, Float32Vector, Float64Vector,
        Int16Vector, Int32Vector, Int64Vector, Int8Vector, StringVector, UInt16Vector,
        UInt32Vector, UInt64Vector, UInt8Vector,
    };

    use crate::database::FlightContext;

    #[test]
    fn test_column_to_vector() {
        let mut column = create_test_column(Arc::new(BooleanVector::from(vec![true])));
        column.datatype = -100;
        let result = column_to_vector(&column, 1);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Column datatype error, source: Unknown proto column datatype: -100"
        );

        macro_rules! test_with_vector {
            ($vector: expr) => {
                let vector = Arc::new($vector);
                let column = create_test_column(vector.clone());
                let result = column_to_vector(&column, vector.len() as u32).unwrap();
                assert_eq!(result, vector as VectorRef);
            };
        }

        test_with_vector!(BooleanVector::from(vec![Some(true), None, Some(false)]));
        test_with_vector!(Int8Vector::from(vec![Some(i8::MIN), None, Some(i8::MAX)]));
        test_with_vector!(Int16Vector::from(vec![
            Some(i16::MIN),
            None,
            Some(i16::MAX)
        ]));
        test_with_vector!(Int32Vector::from(vec![
            Some(i32::MIN),
            None,
            Some(i32::MAX)
        ]));
        test_with_vector!(Int64Vector::from(vec![
            Some(i64::MIN),
            None,
            Some(i64::MAX)
        ]));
        test_with_vector!(UInt8Vector::from(vec![Some(u8::MIN), None, Some(u8::MAX)]));
        test_with_vector!(UInt16Vector::from(vec![
            Some(u16::MIN),
            None,
            Some(u16::MAX)
        ]));
        test_with_vector!(UInt32Vector::from(vec![
            Some(u32::MIN),
            None,
            Some(u32::MAX)
        ]));
        test_with_vector!(UInt64Vector::from(vec![
            Some(u64::MIN),
            None,
            Some(u64::MAX)
        ]));
        test_with_vector!(Float32Vector::from(vec![
            Some(f32::MIN),
            None,
            Some(f32::MAX)
        ]));
        test_with_vector!(Float64Vector::from(vec![
            Some(f64::MIN),
            None,
            Some(f64::MAX)
        ]));
        test_with_vector!(BinaryVector::from(vec![
            Some(b"".to_vec()),
            None,
            Some(b"hello".to_vec())
        ]));
        test_with_vector!(StringVector::from(vec![Some(""), None, Some("foo"),]));
        test_with_vector!(DateVector::from(vec![Some(1), None, Some(3)]));
        test_with_vector!(DateTimeVector::from(vec![Some(4), None, Some(6)]));
    }

    fn create_test_column(vector: VectorRef) -> Column {
        let wrapper: ColumnDataTypeWrapper = vector.data_type().try_into().unwrap();
        Column {
            column_name: "test".to_string(),
            semantic_type: 1,
            values: Some(values(&[vector.clone()]).unwrap()),
            null_mask: null_mask(&[vector.clone()], vector.len()),
            datatype: wrapper.datatype() as i32,
        }
    }

    #[test]
    fn test_flight_ctx() {
        let mut ctx = FlightContext::default();
        assert!(ctx.auth_header.is_none());

        let basic = AuthScheme::Basic(Basic {
            username: "u".to_string(),
            password: "p".to_string(),
        });

        ctx.auth_header = Some(AuthHeader {
            auth_scheme: Some(basic),
        });

        assert!(matches!(
            ctx.auth_header,
            Some(AuthHeader {
                auth_scheme: Some(AuthScheme::Basic(_)),
            })
        ))
    }
}
