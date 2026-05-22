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

use common_telemetry::tracing::warn;
use datafusion_expr::Expr;
use datatypes::schema::Schema;

use crate::batching_mode::state::FilterExprInfo;
use crate::batching_mode::utils::IncrementalAggregateAnalysis;
use crate::{Error, FlowId};

pub(super) fn build_sink_dirty_time_window_filter_expr(
    flow_id: FlowId,
    analysis: &IncrementalAggregateAnalysis,
    sink_schema: &Schema,
    dirty_filter: Option<&FilterExprInfo>,
) -> Result<Option<Expr>, Error> {
    let Some(dirty_filter) = dirty_filter else {
        return Ok(None);
    };

    let Some(sink_filter_col) =
        infer_sink_time_window_filter_col(flow_id, analysis, sink_schema, dirty_filter)
    else {
        return Ok(None);
    };

    dirty_filter.predicate_for_col(&sink_filter_col)
}

fn infer_sink_time_window_filter_col(
    flow_id: FlowId,
    analysis: &IncrementalAggregateAnalysis,
    sink_schema: &Schema,
    dirty_filter: &FilterExprInfo,
) -> Option<String> {
    if analysis.group_key_names.is_empty() {
        return None;
    }

    let is_timestamp_group_key = |name: &str| {
        analysis.group_key_names.iter().any(|key| key == name)
            && sink_schema
                .column_schema_by_name(name)
                .is_some_and(|col| col.data_type.is_timestamp())
    };

    if is_timestamp_group_key(&dirty_filter.col_name) {
        return Some(dirty_filter.col_name.clone());
    }

    let candidates = analysis
        .group_key_names
        .iter()
        .filter(|name| is_timestamp_group_key(name))
        .cloned()
        .collect::<Vec<_>>();

    match candidates.as_slice() {
        [name] => Some(name.clone()),
        [] => {
            warn!(
                "Flow {} cannot infer sink dirty-window filter column: no timestamp group key in {:?}",
                flow_id, analysis.group_key_names
            );
            None
        }
        _ => {
            warn!(
                "Flow {} cannot infer sink dirty-window filter column: ambiguous timestamp group keys {:?}",
                flow_id, candidates
            );
            None
        }
    }
}

#[cfg(test)]
mod test {
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::adapter::AUTO_CREATED_UPDATE_AT_TS_COL;
    use crate::batching_mode::state::FilterExprInfo;
    use crate::batching_mode::utils::IncrementalAggregateAnalysis;

    fn test_analysis_with_group_keys(group_key_names: Vec<&str>) -> IncrementalAggregateAnalysis {
        IncrementalAggregateAnalysis {
            group_key_names: group_key_names
                .into_iter()
                .map(|name| name.to_string())
                .collect(),
            merge_columns: vec![],
            literal_columns: vec![],
            output_field_names: vec![],
            unsupported_exprs: vec![],
        }
    }

    fn test_dirty_filter(col_name: &str) -> FilterExprInfo {
        FilterExprInfo {
            expr: datafusion_expr::col(col_name),
            col_name: col_name.to_string(),
            time_ranges: vec![],
            window_size: chrono::Duration::seconds(1),
        }
    }

    fn test_sink_schema(columns: Vec<(&str, ConcreteDataType)>) -> Schema {
        Schema::new(
            columns
                .into_iter()
                .map(|(name, data_type)| ColumnSchema::new(name, data_type, true))
                .collect(),
        )
    }

    #[test]
    fn test_infer_sink_time_window_filter_col_uses_matching_source_group_key() {
        let analysis = test_analysis_with_group_keys(vec!["ts", "host"]);
        let sink_schema = test_sink_schema(vec![
            ("ts", ConcreteDataType::timestamp_millisecond_datatype()),
            ("host", ConcreteDataType::string_datatype()),
        ]);
        let dirty_filter = test_dirty_filter("ts");

        assert_eq!(
            Some("ts".to_string()),
            infer_sink_time_window_filter_col(1, &analysis, &sink_schema, &dirty_filter)
        );
    }

    #[test]
    fn test_infer_sink_time_window_filter_col_uses_unique_timestamp_group_key() {
        let analysis = test_analysis_with_group_keys(vec!["host", "time_window"]);
        let sink_schema = test_sink_schema(vec![
            ("host", ConcreteDataType::string_datatype()),
            (
                "time_window",
                ConcreteDataType::timestamp_millisecond_datatype(),
            ),
            (
                AUTO_CREATED_UPDATE_AT_TS_COL,
                ConcreteDataType::timestamp_millisecond_datatype(),
            ),
        ]);
        let dirty_filter = test_dirty_filter("ts");

        assert_eq!(
            Some("time_window".to_string()),
            infer_sink_time_window_filter_col(1, &analysis, &sink_schema, &dirty_filter)
        );
    }

    #[test]
    fn test_infer_sink_time_window_filter_col_skips_global_aggregate() {
        let analysis = test_analysis_with_group_keys(vec![]);
        let sink_schema = test_sink_schema(vec![
            ("number", ConcreteDataType::uint32_datatype()),
            (
                "time_window",
                ConcreteDataType::timestamp_millisecond_datatype(),
            ),
        ]);
        let dirty_filter = test_dirty_filter("ts");

        assert_eq!(
            None,
            infer_sink_time_window_filter_col(1, &analysis, &sink_schema, &dirty_filter)
        );
    }

    #[test]
    fn test_infer_sink_time_window_filter_col_skips_without_timestamp_group_key() {
        let analysis = test_analysis_with_group_keys(vec!["host", "device"]);
        let sink_schema = test_sink_schema(vec![
            ("host", ConcreteDataType::string_datatype()),
            ("device", ConcreteDataType::string_datatype()),
            (
                AUTO_CREATED_UPDATE_AT_TS_COL,
                ConcreteDataType::timestamp_millisecond_datatype(),
            ),
        ]);
        let dirty_filter = test_dirty_filter("ts");

        assert_eq!(
            None,
            infer_sink_time_window_filter_col(1, &analysis, &sink_schema, &dirty_filter)
        );
    }

    #[test]
    fn test_infer_sink_time_window_filter_col_skips_ambiguous_timestamp_group_keys() {
        let analysis = test_analysis_with_group_keys(vec!["ts", "time_window"]);
        let sink_schema = test_sink_schema(vec![
            ("ts", ConcreteDataType::timestamp_millisecond_datatype()),
            (
                "time_window",
                ConcreteDataType::timestamp_millisecond_datatype(),
            ),
        ]);
        let dirty_filter = test_dirty_filter("source_ts");

        assert_eq!(
            None,
            infer_sink_time_window_filter_col(1, &analysis, &sink_schema, &dirty_filter)
        );
    }
}
