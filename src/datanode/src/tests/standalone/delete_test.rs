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

#[tokio::test(flavor = "multi_thread")]
async fn test_delete() {
    let instance = MockInstance::new("test_delete").await;

    let output = execute_sql(
        &instance,
        r#"create table test_table(
                            host string,
                            ts timestamp,
                            cpu double default 0,
                            memory double,
                            TIME INDEX (ts),
                            PRIMARY KEY(host)
                        ) engine=mito with(regions=1);"#,
    )
        .await;
    assert!(matches!(output, Output::AffectedRows(0)));

    let output = execute_sql(
        &instance,
        r#"insert into test_table(host, cpu, memory, ts) values
                           ('host1', 66.6, 1024, 1655276557000),
                           ('host2', 77.7,  2048, 1655276558000),
                           ('host3', 88.8,  3072, 1655276559000)
                           "#,
    )
        .await;
    assert!(matches!(output, Output::AffectedRows(3)));

    let output = execute_sql(
        &instance,
        "delete from test_table where host = host1 and ts = 1655276557000 ",
    )
        .await;
    assert!(matches!(output, Output::AffectedRows(1)));

    let output = execute_sql(&instance, "select * from test_table").await;
    let expect = "\
+-------+---------------------+------+--------+
| host  | ts                  | cpu  | memory |
+-------+---------------------+------+--------+
| host2 | 2022-06-15T07:02:38 | 77.7 | 2048   |
| host3 | 2022-06-15T07:02:39 | 88.8 | 3072   |
+-------+---------------------+------+--------+\
"
        .to_string();
    check_output_stream(output, expect).await;
}
