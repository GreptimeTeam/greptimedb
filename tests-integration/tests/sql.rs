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
use sqlx::mysql::MySqlPoolOptions;
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;
use tests_integration::test_util::{setup_mysql_server, setup_pg_server, StorageType};

#[macro_export]
macro_rules! sql_test {
    ($service:ident, $($(#[$meta:meta])* $test:ident),*,) => {
        paste::item! {
            mod [<integration_sql_ $service:lower _test>] {
                $(
                    #[tokio::test(flavor = "multi_thread")]
                    $(
                        #[$meta]
                    )*
                    async fn [< $test >]() {
                        let store_type = tests_integration::test_util::StorageType::$service;
                        if store_type.test_on() {
                            let _ = $crate::sql::$test(store_type).await;
                        }

                    }
                )*
            }
        }
    };
}

#[macro_export]
macro_rules! sql_tests {
    ($($service:ident),*) => {
        $(
            sql_test!(
                $service,

                test_mysql_crud,
                test_postgres_crud,
            );
        )*
    };
}

pub async fn test_mysql_crud(store_type: StorageType) {
    let (addr, mut guard, fe_mysql_server) = setup_mysql_server(store_type, "sql_crud").await;

    let pool = MySqlPoolOptions::new()
        .max_connections(2)
        .connect(&format!("mysql://{addr}/public"))
        .await
        .unwrap();

    sqlx::query("create table demo(i bigint, ts timestamp time index)")
        .execute(&pool)
        .await
        .unwrap();
    for i in 0..10 {
        sqlx::query("insert into demo values(?, ?)")
            .bind(i)
            .bind(i)
            .execute(&pool)
            .await
            .unwrap();
    }

    let rows = sqlx::query("select i from demo")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 10);

    for (i, row) in rows.iter().enumerate() {
        let ret: i64 = row.get(0);
        assert_eq!(ret, i as i64);
    }

    sqlx::query("delete from demo")
        .execute(&pool)
        .await
        .unwrap();
    let rows = sqlx::query("select i from demo")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 0);

    let _ = fe_mysql_server.shutdown().await;
    guard.remove_all().await;
}

pub async fn test_postgres_crud(store_type: StorageType) {
    let (addr, mut guard, fe_pg_server) = setup_pg_server(store_type, "sql_crud").await;

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&format!("postgres://{addr}/public"))
        .await
        .unwrap();

    sqlx::query("create table demo(i bigint, ts timestamp time index)")
        .execute(&pool)
        .await
        .unwrap();
    for i in 0..10 {
        sqlx::query("insert into demo values($1, $2)")
            .bind(i)
            .bind(i)
            .execute(&pool)
            .await
            .unwrap();
    }

    let rows = sqlx::query("select i from demo")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 10);

    for (i, row) in rows.iter().enumerate() {
        let ret: i64 = row.get(0);
        assert_eq!(ret, i as i64);
    }

    sqlx::query("delete from demo")
        .execute(&pool)
        .await
        .unwrap();
    let rows = sqlx::query("select i from demo")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 0);

    let _ = fe_pg_server.shutdown().await;
    guard.remove_all().await;
}
