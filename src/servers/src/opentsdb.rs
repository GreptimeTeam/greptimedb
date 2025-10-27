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

pub mod codec;

use api::v1::RowInsertRequests;
use common_grpc::precision::Precision;
use common_query::prelude::{greptime_timestamp, greptime_value};

use self::codec::DataPoint;
use crate::error::Result;
use crate::row_writer::{self, MultiTableData};

pub fn data_point_to_grpc_row_insert_requests(
    data_points: Vec<DataPoint>,
) -> Result<(RowInsertRequests, usize)> {
    let mut multi_table_data = MultiTableData::new();

    for mut data_point in data_points {
        let tags: Vec<(String, String)> = std::mem::take(data_point.tags_mut());
        let table_name = data_point.metric();
        let value = data_point.value();
        let timestamp = data_point.ts_millis();
        // length of tags + 2 extra columns for greptime_timestamp and the value
        let num_columns = tags.len() + 2;

        let table_data = multi_table_data.get_or_default_table_data(table_name, num_columns, 1);
        let mut one_row = table_data.alloc_one_row();

        // tags
        row_writer::write_tags(table_data, tags.into_iter(), &mut one_row)?;

        // value
        row_writer::write_f64(table_data, greptime_value(), value, &mut one_row)?;
        // timestamp
        row_writer::write_ts_to_millis(
            table_data,
            greptime_timestamp(),
            Some(timestamp),
            Precision::Millisecond,
            &mut one_row,
        )?;

        table_data.add_row(one_row);
    }

    Ok(multi_table_data.into_row_insert_requests())
}
