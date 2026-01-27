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

use std::time::Duration;

use common_error::ext::BoxedError;
use common_macro::admin_fn;
use common_meta::rpc::procedure::{GcRegionsRequest, GcTableRequest};
use common_query::error::{
    InvalidFuncArgsSnafu, MissingProcedureServiceHandlerSnafu, Result, TableMutationSnafu,
    UnsupportedInputDataTypeSnafu,
};
use datafusion_expr::{Signature, TypeSignature, Volatility};
use datatypes::arrow::datatypes::DataType as ArrowDataType;
use datatypes::prelude::*;
use session::context::QueryContextRef;
use snafu::{ResultExt, ensure};

use crate::handlers::ProcedureServiceHandlerRef;
use crate::helper::cast_u64;

const DEFAULT_GC_TIMEOUT: Duration = Duration::from_secs(60);

#[admin_fn(
    name = GcRegionsFunction,
    display_name = gc_regions,
    sig_fn = gc_regions_signature,
    ret = uint64
)]
pub(crate) async fn gc_regions(
    procedure_service_handler: &ProcedureServiceHandlerRef,
    _ctx: &QueryContextRef,
    params: &[ValueRef<'_>],
) -> Result<Value> {
    ensure!(
        !params.is_empty(),
        InvalidFuncArgsSnafu {
            err_msg: "The length of the args is not correct, expect at least 1 region id, have 0"
                .to_string(),
        }
    );

    let mut region_ids = Vec::with_capacity(params.len());
    for param in params {
        let Some(region_id) = cast_u64(param)? else {
            return UnsupportedInputDataTypeSnafu {
                function: "gc_regions",
                datatypes: params.iter().map(|v| v.data_type()).collect::<Vec<_>>(),
            }
            .fail();
        };
        region_ids.push(region_id);
    }

    let resp = procedure_service_handler
        .gc_regions(GcRegionsRequest {
            region_ids,
            full_file_listing: false,
            timeout: DEFAULT_GC_TIMEOUT,
        })
        .await?;

    Ok(Value::from(resp.processed_regions))
}

#[admin_fn(
    name = GcTableFunction,
    display_name = gc_table,
    sig_fn = gc_table_signature,
    ret = uint64
)]
pub(crate) async fn gc_table(
    procedure_service_handler: &ProcedureServiceHandlerRef,
    query_ctx: &QueryContextRef,
    params: &[ValueRef<'_>],
) -> Result<Value> {
    ensure!(
        params.len() == 1,
        InvalidFuncArgsSnafu {
            err_msg: format!(
                "The length of the args is not correct, expect 1, have: {}",
                params.len()
            ),
        }
    );

    let ValueRef::String(table_name) = params[0] else {
        return UnsupportedInputDataTypeSnafu {
            function: "gc_table",
            datatypes: params.iter().map(|v| v.data_type()).collect::<Vec<_>>(),
        }
        .fail();
    };

    let (catalog_name, schema_name, table_name) =
        session::table_name::table_name_to_full_name(table_name, query_ctx)
            .map_err(BoxedError::new)
            .context(TableMutationSnafu)?;

    let resp = procedure_service_handler
        .gc_table(GcTableRequest {
            catalog_name,
            schema_name,
            table_name,
            full_file_listing: false,
            timeout: DEFAULT_GC_TIMEOUT,
        })
        .await?;

    Ok(Value::from(resp.processed_regions))
}

fn gc_regions_signature() -> Signature {
    Signature::variadic(
        ConcreteDataType::numerics()
            .into_iter()
            .map(|dt| dt.as_arrow_type())
            .collect(),
        Volatility::Immutable,
    )
}

fn gc_table_signature() -> Signature {
    Signature::one_of(
        vec![TypeSignature::Uniform(1, vec![ArrowDataType::Utf8])],
        Volatility::Immutable,
    )
}
