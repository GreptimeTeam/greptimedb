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

use auth::user_provider_from_option;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use sqlx::mysql::{MySqlDatabaseError, MySqlPoolOptions};
use sqlx::postgres::{PgDatabaseError, PgPoolOptions};
use sqlx::Row;
use tests_integration::test_util::{
    setup_mysql_server, setup_mysql_server_with_user_provider, setup_pg_server,
    setup_pg_server_with_user_provider, StorageType,
};
use tokio_postgres::NoTls;

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

                test_mysql_auth,
                // ignore: https://github.com/GreptimeTeam/greptimedb/issues/2445
                // test_mysql_crud,
                test_postgres_auth,
                // ignore: https://github.com/GreptimeTeam/greptimedb/issues/2445
                // test_postgres_crud,
                // ignore: https://github.com/GreptimeTeam/greptimedb/issues/2445
                // test_postgres_parameter_inference,
            );
        )*
    };
}

pub async fn test_mysql_auth(store_type: StorageType) {
    let user_provider = user_provider_from_option(
        &"static_user_provider:cmd:greptime_user=greptime_pwd".to_string(),
    )
    .unwrap();

    let (addr, mut guard, fe_mysql_server) =
        setup_mysql_server_with_user_provider(store_type, "sql_crud", Some(user_provider)).await;

    // 1. no auth
    let conn_re = MySqlPoolOptions::new()
        .max_connections(2)
        .connect(&format!("mysql://{addr}/public"))
        .await;

    assert!(conn_re.is_err());
    assert_eq!(
        conn_re
            .err()
            .unwrap()
            .into_database_error()
            .unwrap()
            .downcast::<MySqlDatabaseError>()
            .code(),
        Some("28000")
    );

    // 2. wrong pwd
    let conn_re = MySqlPoolOptions::new()
        .max_connections(2)
        .connect(&format!("mysql://greptime_user:wrong_pwd@{addr}/public"))
        .await;

    assert!(conn_re.is_err());
    assert_eq!(
        conn_re
            .err()
            .unwrap()
            .into_database_error()
            .unwrap()
            .downcast::<MySqlDatabaseError>()
            .code(),
        Some("28000")
    );

    // 3. right pwd
    let conn_re = MySqlPoolOptions::new()
        .max_connections(2)
        .connect(&format!("mysql://greptime_user:greptime_pwd@{addr}/public"))
        .await;

    assert!(conn_re.is_ok());

    let _ = fe_mysql_server.shutdown().await;
    guard.remove_all().await;
}

#[allow(dead_code)]
pub async fn test_mysql_crud(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();

    let (addr, mut guard, fe_mysql_server) = setup_mysql_server(store_type, "sql_crud").await;

    let pool = MySqlPoolOptions::new()
        .max_connections(2)
        .connect(&format!("mysql://{addr}/public"))
        .await
        .unwrap();

    sqlx::query(
        "create table demo(i bigint, ts timestamp time index, d date, dt datetime, b blob)",
    )
    .execute(&pool)
    .await
    .unwrap();
    for i in 0..10 {
        let dt: DateTime<Utc> = DateTime::from_naive_utc_and_offset(
            NaiveDateTime::from_timestamp_opt(60, i).unwrap(),
            Utc,
        );
        let d = NaiveDate::from_yo_opt(2015, 100).unwrap();
        let hello = format!("hello{i}");
        let bytes = hello.as_bytes();
        sqlx::query("insert into demo values(?, ?, ?, ?, ?)")
            .bind(i)
            .bind(i)
            .bind(d)
            .bind(dt)
            .bind(bytes)
            .execute(&pool)
            .await
            .unwrap();
    }

    let rows = sqlx::query("select i, d, dt, b from demo")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 10);

    for (i, row) in rows.iter().enumerate() {
        let ret: i64 = row.get(0);
        let d: NaiveDate = row.get(1);
        let dt: DateTime<Utc> = row.get(2);
        let bytes: Vec<u8> = row.get(3);
        assert_eq!(ret, i as i64);
        let expected_d = NaiveDate::from_yo_opt(2015, 100).unwrap();
        assert_eq!(expected_d, d);
        let expected_dt: DateTime<Utc> = DateTime::from_naive_utc_and_offset(
            NaiveDateTime::from_timestamp_opt(60, i as u32).unwrap(),
            Utc,
        );
        assert_eq!(
            format!("{}", expected_dt.format("%Y-%m-%d %H:%M:%S")),
            format!("{}", dt.format("%Y-%m-%d %H:%M:%S"))
        );
        assert_eq!(format!("hello{i}"), String::from_utf8_lossy(&bytes));
    }

    let rows = sqlx::query("select i from demo where i=?")
        .bind(6)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    for row in rows {
        let ret: i64 = row.get(0);
        assert_eq!(ret, 6);
    }

    let _ = sqlx::query("delete from demo")
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

pub async fn test_postgres_auth(store_type: StorageType) {
    let user_provider = user_provider_from_option(
        &"static_user_provider:cmd:greptime_user=greptime_pwd".to_string(),
    )
    .unwrap();

    let (addr, mut guard, fe_pg_server) =
        setup_pg_server_with_user_provider(store_type, "sql_crud", Some(user_provider)).await;

    // 1. no auth
    let conn_re = PgPoolOptions::new()
        .max_connections(2)
        .connect(&format!("postgres://{addr}/public"))
        .await;

    assert!(conn_re.is_err());
    assert_eq!(
        conn_re
            .err()
            .unwrap()
            .into_database_error()
            .unwrap()
            .downcast::<PgDatabaseError>()
            .code(),
        "28P01"
    );

    // 2. wrong pwd
    let conn_re = PgPoolOptions::new()
        .max_connections(2)
        .connect(&format!("postgres://greptime_user:wrong_pwd@{addr}/public"))
        .await;

    assert!(conn_re.is_err());
    assert_eq!(
        conn_re
            .err()
            .unwrap()
            .into_database_error()
            .unwrap()
            .downcast::<PgDatabaseError>()
            .code(),
        "28P01"
    );

    // 2. right pwd
    let conn_re = PgPoolOptions::new()
        .max_connections(2)
        .connect(&format!(
            "postgres://greptime_user:greptime_pwd@{addr}/public"
        ))
        .await;

    assert!(conn_re.is_ok());

    let _ = fe_pg_server.shutdown().await;
    guard.remove_all().await;
}

#[allow(dead_code)]
pub async fn test_postgres_crud(store_type: StorageType) {
    let (addr, mut guard, fe_pg_server) = setup_pg_server(store_type, "sql_crud").await;

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&format!("postgres://{addr}/public"))
        .await
        .unwrap();

    sqlx::query("create table demo(i bigint, ts timestamp time index, d date, dt datetime)")
        .execute(&pool)
        .await
        .unwrap();

    for i in 0..10 {
        let d = NaiveDate::from_yo_opt(2015, 100).unwrap();
        let dt = d.and_hms_opt(0, 0, 0).unwrap().timestamp_millis();

        sqlx::query("insert into demo values($1, $2, $3, $4)")
            .bind(i)
            .bind(i)
            .bind(d)
            .bind(dt)
            .execute(&pool)
            .await
            .unwrap();
    }

    let rows = sqlx::query("select i,d,dt from demo")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 10);

    for (i, row) in rows.iter().enumerate() {
        let ret: i64 = row.get(0);
        let d: NaiveDate = row.get(1);
        let dt: NaiveDateTime = row.get(2);

        assert_eq!(ret, i as i64);

        let expected_d = NaiveDate::from_yo_opt(2015, 100).unwrap();
        assert_eq!(expected_d, d);

        let expected_dt = NaiveDate::from_yo_opt(2015, 100)
            .and_then(|d| d.and_hms_opt(0, 0, 0))
            .unwrap();
        assert_eq!(expected_dt, dt);
    }

    let rows = sqlx::query("select i from demo where i=$1")
        .bind(6)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    for row in rows {
        let ret: i64 = row.get(0);
        assert_eq!(ret, 6);
    }

    let _ = sqlx::query("delete from demo")
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

#[allow(dead_code)]
pub async fn test_postgres_parameter_inference(store_type: StorageType) {
    let (addr, mut guard, fe_pg_server) = setup_pg_server(store_type, "sql_inference").await;

    let (client, connection) = tokio_postgres::connect(&format!("postgres://{addr}/public"), NoTls)
        .await
        .unwrap();

    tokio::spawn(async move {
        connection.await.unwrap();
    });

    // Create demo table
    let _ = client
        .simple_query("create table demo(i bigint, ts timestamp time index, d date, dt datetime)")
        .await
        .unwrap();

    let d = NaiveDate::from_yo_opt(2015, 100).unwrap();
    let dt = d.and_hms_opt(0, 0, 0).unwrap();
    let _ = client
        .execute(
            "INSERT INTO demo VALUES($1, $2, $3, $4)",
            &[&0i64, &dt, &d, &dt],
        )
        .await
        .unwrap();

    let rows = client
        .query("SELECT * FROM demo WHERE i = $1", &[&0i64])
        .await
        .unwrap();

    assert_eq!(1, rows.len());

    let _ = fe_pg_server.shutdown().await;
    guard.remove_all().await;
}
