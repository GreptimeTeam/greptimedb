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

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicU32;
    use std::sync::Arc;

    use api::v1::region::QueryRequest;
    use common_base::Plugins;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_meta::key::table_name::TableNameKey;
    use common_meta::rpc::router::region_distribution;
    use common_query::Output;
    use common_recordbatch::RecordBatches;
    use common_telemetry::debug;
    use frontend::error::{self, Error, Result};
    use frontend::instance::Instance;
    use query::parser::QueryLanguageParser;
    use query::plan::LogicalPlan;
    use servers::interceptor::{SqlQueryInterceptor, SqlQueryInterceptorRef};
    use servers::query_handler::sql::SqlQueryHandler;
    use session::context::{QueryContext, QueryContextRef};
    use sql::statements::statement::Statement;
    use store_api::storage::RegionId;
    use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};

    use crate::standalone::GreptimeDbStandaloneBuilder;
    use crate::tests;
    use crate::tests::MockDistributedInstance;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_standalone_exec_sql() {
        let standalone = GreptimeDbStandaloneBuilder::new("test_standalone_exec_sql")
            .build()
            .await;
        let instance = standalone.instance.as_ref();

        let sql = r#"
            CREATE TABLE demo(
                host STRING,
                ts TIMESTAMP,
                cpu DOUBLE NULL,
                memory DOUBLE NULL,
                disk_util DOUBLE DEFAULT 9.9,
                TIME INDEX (ts),
                PRIMARY KEY(host)
            ) engine=mito"#;
        create_table(instance, sql).await;

        insert_and_query(instance).await;

        drop_table(instance).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_distributed_exec_sql() {
        common_telemetry::init_default_ut_logging();

        let distributed = tests::create_distributed_instance("test_distributed_exec_sql").await;
        let frontend = distributed.frontend();
        let instance = frontend.as_ref();

        let sql = r#"
            CREATE TABLE demo(
                host STRING,
                ts TIMESTAMP,
                cpu DOUBLE NULL,
                memory DOUBLE NULL,
                disk_util DOUBLE DEFAULT 9.9,
                TIME INDEX (ts),
                PRIMARY KEY(host)
            )
            PARTITION BY RANGE COLUMNS (host) (
                PARTITION r0 VALUES LESS THAN ('550-A'),
                PARTITION r1 VALUES LESS THAN ('550-W'),
                PARTITION r2 VALUES LESS THAN ('MOSS'),
                PARTITION r3 VALUES LESS THAN (MAXVALUE),
            )
            engine=mito"#;
        create_table(instance, sql).await;

        insert_and_query(instance).await;

        verify_data_distribution(
            &distributed,
            HashMap::from([
                (
                    0u32,
                    "\
+---------------------+------+
| ts                  | host |
+---------------------+------+
| 2013-12-31T16:00:00 | 490  |
+---------------------+------+",
                ),
                (
                    1u32,
                    "\
+---------------------+-------+
| ts                  | host  |
+---------------------+-------+
| 2022-12-31T16:00:00 | 550-A |
+---------------------+-------+",
                ),
                (
                    2u32,
                    "\
+---------------------+-------+
| ts                  | host  |
+---------------------+-------+
| 2023-12-31T16:00:00 | 550-W |
+---------------------+-------+",
                ),
                (
                    3u32,
                    "\
+---------------------+------+
| ts                  | host |
+---------------------+------+
| 2043-12-31T16:00:00 | MOSS |
+---------------------+------+",
                ),
            ]),
        )
        .await;

        drop_table(instance).await;

        verify_table_is_dropped(&distributed).await;
    }

    async fn query(instance: &Instance, sql: &str) -> Output {
        SqlQueryHandler::do_query(instance, sql, QueryContext::arc())
            .await
            .remove(0)
            .unwrap()
    }

    async fn create_table(instance: &Instance, sql: &str) {
        let output = query(instance, sql).await;
        let Output::AffectedRows(x) = output else {
            unreachable!()
        };
        assert_eq!(x, 0);
    }

    async fn insert_and_query(instance: &Instance) {
        let sql = r#"INSERT INTO demo(host, cpu, memory, ts) VALUES
                                ('490', 0.1, 1, 1388505600000),
                                ('550-A', 1, 100, 1672502400000),
                                ('550-W', 10000, 1000000, 1704038400000),
                                ('MOSS', 100000000, 10000000000, 2335190400000)
                                "#;
        let output = query(instance, sql).await;
        let Output::AffectedRows(x) = output else {
            unreachable!()
        };
        assert_eq!(x, 4);

        let sql = "SELECT * FROM demo WHERE ts > cast(1000000000 as timestamp) ORDER BY host"; // use nanoseconds as where condition
        let output = query(instance, sql).await;
        let Output::Stream(s) = output else {
            unreachable!()
        };
        let batches = common_recordbatch::util::collect_batches(s).await.unwrap();
        let pretty_print = batches.pretty_print().unwrap();
        let expected = "\
+-------+---------------------+-------------+-----------+-----------+
| host  | ts                  | cpu         | memory    | disk_util |
+-------+---------------------+-------------+-----------+-----------+
| 490   | 2013-12-31T16:00:00 | 0.1         | 1.0       | 9.9       |
| 550-A | 2022-12-31T16:00:00 | 1.0         | 100.0     | 9.9       |
| 550-W | 2023-12-31T16:00:00 | 10000.0     | 1000000.0 | 9.9       |
| MOSS  | 2043-12-31T16:00:00 | 100000000.0 | 1.0e10    | 9.9       |
+-------+---------------------+-------------+-----------+-----------+";
        assert_eq!(pretty_print, expected);
    }

    async fn verify_data_distribution(
        instance: &MockDistributedInstance,
        expected_distribution: HashMap<u32, &str>,
    ) {
        let manager = instance.table_metadata_manager();
        let table_id = manager
            .table_name_manager()
            .get(TableNameKey::new(
                DEFAULT_CATALOG_NAME,
                DEFAULT_SCHEMA_NAME,
                "demo",
            ))
            .await
            .unwrap()
            .unwrap()
            .table_id();
        debug!("Reading table {table_id}");

        let table_route_value = manager
            .table_route_manager()
            .get(table_id)
            .await
            .unwrap()
            .unwrap()
            .into_inner();

        let region_to_dn_map = region_distribution(
            table_route_value
                .region_routes()
                .expect("region routes should be physical"),
        )
        .iter()
        .map(|(k, v)| (v[0], *k))
        .collect::<HashMap<u32, u64>>();
        assert!(region_to_dn_map.len() <= instance.datanodes().len());

        let stmt = QueryLanguageParser::parse_sql("SELECT ts, host FROM demo ORDER BY ts").unwrap();
        let LogicalPlan::DfPlan(plan) = instance
            .frontend()
            .statement_executor()
            .plan(stmt, QueryContext::arc())
            .await
            .unwrap();
        let plan = DFLogicalSubstraitConvertor.encode(&plan).unwrap();

        for (region, dn) in region_to_dn_map.iter() {
            let region_server = instance.datanodes().get(dn).unwrap().region_server();

            let region_id = RegionId::new(table_id, *region);

            let stream = region_server
                .handle_read(QueryRequest {
                    region_id: region_id.as_u64(),
                    plan: plan.to_vec(),
                    ..Default::default()
                })
                .await
                .unwrap();

            let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
            let actual = recordbatches.pretty_print().unwrap();

            let expected = expected_distribution.get(region).unwrap();
            assert_eq!(&actual, expected);
        }
    }

    async fn drop_table(instance: &Instance) {
        let sql = "DROP TABLE demo";
        let output = query(instance, sql).await;
        let Output::AffectedRows(x) = output else {
            unreachable!()
        };
        assert_eq!(x, 0);
    }

    async fn verify_table_is_dropped(instance: &MockDistributedInstance) {
        assert!(instance
            .frontend()
            .catalog_manager()
            .table("greptime", "public", "demo")
            .await
            .unwrap()
            .is_none())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_sql_interceptor_plugin() {
        #[derive(Default)]
        struct AssertionHook {
            pub(crate) c: AtomicU32,
        }

        impl SqlQueryInterceptor for AssertionHook {
            type Error = Error;

            fn pre_parsing<'a>(
                &self,
                query: &'a str,
                _query_ctx: QueryContextRef,
            ) -> Result<Cow<'a, str>> {
                let _ = self.c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                assert!(query.starts_with("CREATE TABLE demo"));
                Ok(Cow::Borrowed(query))
            }

            fn post_parsing(
                &self,
                statements: Vec<Statement>,
                _query_ctx: QueryContextRef,
            ) -> Result<Vec<Statement>> {
                let _ = self.c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                assert!(matches!(statements[0], Statement::CreateTable(_)));
                Ok(statements)
            }

            fn pre_execute(
                &self,
                _statement: &Statement,
                _plan: Option<&query::plan::LogicalPlan>,
                _query_ctx: QueryContextRef,
            ) -> Result<()> {
                let _ = self.c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(())
            }

            fn post_execute(
                &self,
                mut output: Output,
                _query_ctx: QueryContextRef,
            ) -> Result<Output> {
                let _ = self.c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                match &mut output {
                    Output::AffectedRows(rows) => {
                        assert_eq!(*rows, 0);
                        // update output result
                        *rows = 10;
                    }
                    _ => unreachable!(),
                }
                Ok(output)
            }
        }

        let plugins = Plugins::new();
        let counter_hook = Arc::new(AssertionHook::default());
        plugins.insert::<SqlQueryInterceptorRef<Error>>(counter_hook.clone());

        let standalone = GreptimeDbStandaloneBuilder::new("test_sql_interceptor_plugin")
            .with_plugin(plugins)
            .build()
            .await;
        let instance = standalone.instance;

        let sql = r#"CREATE TABLE demo(
                            host STRING,
                            ts TIMESTAMP,
                            cpu DOUBLE NULL,
                            memory DOUBLE NULL,
                            disk_util DOUBLE DEFAULT 9.9,
                            TIME INDEX (ts),
                            PRIMARY KEY(host)
                        ) engine=mito with(regions=1);"#;
        let output = SqlQueryHandler::do_query(&*instance, sql, QueryContext::arc())
            .await
            .remove(0)
            .unwrap();

        // assert that the hook is called 3 times
        assert_eq!(4, counter_hook.c.load(std::sync::atomic::Ordering::Relaxed));
        match output {
            Output::AffectedRows(rows) => assert_eq!(rows, 10),
            _ => unreachable!(),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_disable_db_operation_plugin() {
        #[derive(Default)]
        struct DisableDBOpHook;

        impl SqlQueryInterceptor for DisableDBOpHook {
            type Error = Error;

            fn post_parsing(
                &self,
                statements: Vec<Statement>,
                _query_ctx: QueryContextRef,
            ) -> Result<Vec<Statement>> {
                for s in &statements {
                    match s {
                        Statement::CreateDatabase(_) | Statement::ShowDatabases(_) => {
                            return Err(Error::NotSupported {
                                feat: "Database operations".to_owned(),
                            })
                        }
                        _ => {}
                    }
                }

                Ok(statements)
            }
        }

        let query_ctx = QueryContext::arc();

        let plugins = Plugins::new();
        let hook = Arc::new(DisableDBOpHook);
        plugins.insert::<SqlQueryInterceptorRef<Error>>(hook.clone());

        let standalone = GreptimeDbStandaloneBuilder::new("test_disable_db_operation_plugin")
            .with_plugin(plugins)
            .build()
            .await;
        let instance = standalone.instance;

        let sql = r#"CREATE TABLE demo(
                            host STRING,
                            ts TIMESTAMP,
                            cpu DOUBLE NULL,
                            memory DOUBLE NULL,
                            disk_util DOUBLE DEFAULT 9.9,
                            TIME INDEX (ts),
                            PRIMARY KEY(host)
                        ) engine=mito with(regions=1);"#;
        let output = SqlQueryHandler::do_query(&*instance, sql, query_ctx.clone())
            .await
            .remove(0)
            .unwrap();

        match output {
            Output::AffectedRows(rows) => assert_eq!(rows, 0),
            _ => unreachable!(),
        }

        let sql = r#"CREATE DATABASE tomcat"#;
        if let Err(e) = SqlQueryHandler::do_query(&*instance, sql, query_ctx.clone())
            .await
            .remove(0)
        {
            assert!(matches!(e, error::Error::NotSupported { .. }));
        } else {
            unreachable!();
        }

        let sql = r#"SELECT 1; SHOW DATABASES"#;
        if let Err(e) = SqlQueryHandler::do_query(&*instance, sql, query_ctx.clone())
            .await
            .remove(0)
        {
            assert!(matches!(e, error::Error::NotSupported { .. }));
        } else {
            unreachable!();
        }
    }
}
