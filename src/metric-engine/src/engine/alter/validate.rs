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

use datatypes::schema::COMPRESSION_KEY;
use snafu::ensure;
use store_api::region_request::{AlterKind, RegionAlterRequest};
use store_api::storage::RegionId;

use crate::error::{Result, UnexpectedRequestSnafu, UnsupportedAlterKindSnafu};

/// Validate the alter region requests.
pub fn validate_alter_region_requests(requests: &[(RegionId, RegionAlterRequest)]) -> Result<()> {
    for (_, request) in requests {
        ensure!(
            matches!(request.kind, AlterKind::AddColumns { .. }),
            UnsupportedAlterKindSnafu {
                kind: request.kind.as_ref()
            }
        );
        if let AlterKind::AddColumns { columns } = &request.kind {
            for column in columns {
                ensure!(
                    !column.column_metadata.column_schema.metadata().contains_key(COMPRESSION_KEY),
                    UnexpectedRequestSnafu {
                        reason: "COMPRESSION is not supported on logical tables; configure it on the physical table".to_string(),
                    }
                );
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use api::v1::SemanticType;
    use common_error::ext::ErrorExt;
    use common_error::status_code::StatusCode;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, CompressionOptions};
    use store_api::metadata::ColumnMetadata;
    use store_api::region_request::AddColumn;

    use super::*;

    #[test]
    fn test_reject_logical_column_compression() {
        let column_schema = ColumnSchema::new("host", ConcreteDataType::string_datatype(), true)
            .with_compression_options(CompressionOptions {
                level: Some(3),
                ..Default::default()
            })
            .unwrap();
        let request = RegionAlterRequest {
            kind: AlterKind::AddColumns {
                columns: vec![AddColumn {
                    column_metadata: ColumnMetadata {
                        column_schema,
                        semantic_type: SemanticType::Tag,
                        column_id: 3,
                    },
                    location: None,
                }],
            },
        };
        let err = validate_alter_region_requests(&[(RegionId::new(1, 1), request)]).unwrap_err();
        assert_eq!(StatusCode::InvalidArguments, err.status_code());
        assert!(
            err.to_string()
                .contains("configure it on the physical table")
        );
    }
}
