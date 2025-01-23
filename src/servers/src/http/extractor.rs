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

use core::str;

use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::http::StatusCode;
use http::HeaderMap;
use pipeline::SelectInfo;

use crate::http::header::constants::{
    GREPTIME_LOG_EXTRACT_KEYS_HEADER_NAME, GREPTIME_LOG_PIPELINE_NAME_HEADER_NAME,
    GREPTIME_LOG_PIPELINE_VERSION_HEADER_NAME, GREPTIME_LOG_TABLE_NAME_HEADER_NAME,
    GREPTIME_TRACE_TABLE_NAME_HEADER_NAME,
};

/// Axum extractor for optional target log table name from HTTP header
/// using [`GREPTIME_LOG_TABLE_NAME_HEADER_NAME`] as key.
pub struct LogTableName(pub Option<String>);

impl<S> FromRequestParts<S> for LogTableName
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, String);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let headers = &parts.headers;
        string_value_from_header(headers, GREPTIME_LOG_TABLE_NAME_HEADER_NAME).map(LogTableName)
    }
}

/// Axum extractor for optional target trace table name from HTTP header
/// using [`GREPTIME_TRACE_TABLE_NAME_HEADER_NAME`] as key.
pub struct TraceTableName(pub Option<String>);

impl<S> FromRequestParts<S> for TraceTableName
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, String);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let headers = &parts.headers;
        string_value_from_header(headers, GREPTIME_TRACE_TABLE_NAME_HEADER_NAME).map(TraceTableName)
    }
}

/// Axum extractor for select keys from HTTP header,
/// to extract and uplift key-values from OTLP attributes.
/// See [`SelectInfo`] for more details.
pub struct SelectInfoWrapper(pub SelectInfo);

impl<S> FromRequestParts<S> for SelectInfoWrapper
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, String);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let select =
            string_value_from_header(&parts.headers, GREPTIME_LOG_EXTRACT_KEYS_HEADER_NAME)?;

        match select {
            Some(name) => {
                if name.is_empty() {
                    Ok(SelectInfoWrapper(Default::default()))
                } else {
                    Ok(SelectInfoWrapper(SelectInfo::from(name)))
                }
            }
            None => Ok(SelectInfoWrapper(Default::default())),
        }
    }
}

/// Axum extractor for optional Pipeline name and version
/// from HTTP headers.
pub struct PipelineInfo {
    pub pipeline_name: Option<String>,
    pub pipeline_version: Option<String>,
}

impl<S> FromRequestParts<S> for PipelineInfo
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, String);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let headers = &parts.headers;
        let pipeline_name =
            string_value_from_header(headers, GREPTIME_LOG_PIPELINE_NAME_HEADER_NAME)?;
        let pipeline_version =
            string_value_from_header(headers, GREPTIME_LOG_PIPELINE_VERSION_HEADER_NAME)?;
        match (pipeline_name, pipeline_version) {
            (Some(name), Some(version)) => Ok(PipelineInfo {
                pipeline_name: Some(name),
                pipeline_version: Some(version),
            }),
            (None, _) => Ok(PipelineInfo {
                pipeline_name: None,
                pipeline_version: None,
            }),
            (Some(name), None) => Ok(PipelineInfo {
                pipeline_name: Some(name),
                pipeline_version: None,
            }),
        }
    }
}

#[inline]
fn string_value_from_header(
    headers: &HeaderMap,
    header_key: &str,
) -> Result<Option<String>, (StatusCode, String)> {
    headers
        .get(header_key)
        .map(|value| {
            String::from_utf8(value.as_bytes().to_vec()).map_err(|_| {
                (
                    StatusCode::BAD_REQUEST,
                    format!("`{}` header is not valid UTF-8 string type.", header_key),
                )
            })
        })
        .transpose()
}
