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

use std::collections::HashMap;

use auth::user_provider_from_option;
use chrono::{DateTime, NaiveDate, NaiveDateTime, SecondsFormat, Utc};
use sqlx::mysql::{MySqlConnection, MySqlDatabaseError, MySqlPoolOptions};
use sqlx::postgres::{PgDatabaseError, PgPoolOptions};
use sqlx::{Connection, Executor, Row};
use tests_integration::test_util::{
    setup_mysql_server, setup_mysql_server_with_user_provider, setup_pg_server,
    setup_pg_server_with_user_provider, StorageType,
};
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

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
                        common_telemetry::init_default_ut_logging();

                        let store_type = tests_integration::test_util::StorageType::$service;
                        if store_type.test_on() {
                            common_telemetry::info!("test {} starts, store_type: {:?}", stringify!($test), store_type);

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
                test_mysql_crud,
                test_mysql_timezone,
                test_mysql_async_timestamp,
                test_postgres_auth,
                test_postgres_crud,
                test_postgres_timezone,
                test_postgres_bytea,
                test_postgres_datestyle,
                test_postgres_parameter_inference,
                test_mysql_prepare_stmt_insert_timestamp,
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

pub async fn test_mysql_crud(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();

    let (addr, mut guard, fe_mysql_server) = setup_mysql_server(store_type, "sql_crud").await;

    let pool = MySqlPoolOptions::new()
        .max_connections(2)
        .connect(&format!("mysql://{addr}/public"))
        .await
        .unwrap();

    sqlx::query(
        "create table demo(i bigint, ts timestamp time index default current_timestamp, d date default null, dt datetime default null, b blob default null)",
    )
    .execute(&pool)
    .await
    .unwrap();
    for i in 0..10 {
        let dt: DateTime<Utc> = DateTime::from_naive_utc_and_offset(
            chrono::DateTime::from_timestamp(60, i).unwrap().naive_utc(),
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
        let ret: i64 = row.get("i");
        let d: NaiveDate = row.get("d");
        let dt: DateTime<Utc> = row.get("dt");
        let bytes: Vec<u8> = row.get("b");
        assert_eq!(ret, i as i64);
        let expected_d = NaiveDate::from_yo_opt(2015, 100).unwrap();
        assert_eq!(expected_d, d);
        let expected_dt: DateTime<Utc> = DateTime::from_naive_utc_and_offset(
            chrono::DateTime::from_timestamp(60, i as u32)
                .unwrap()
                .naive_utc(),
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
        let ret: i64 = row.get("i");
        assert_eq!(ret, 6);
    }

    // parameter type mismatch
    let query_re = sqlx::query("select i from demo where i = ?")
        .bind("test")
        .fetch_all(&pool)
        .await;
    assert!(query_re.is_err());
    let err = query_re.unwrap_err();
    common_telemetry::info!("Error is {}", err);
    assert_eq!(
        err.into_database_error()
            .unwrap()
            .downcast::<MySqlDatabaseError>()
            .number(),
        1210,
    );

    let _ = sqlx::query("delete from demo")
        .execute(&pool)
        .await
        .unwrap();
    let rows = sqlx::query("select i from demo")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 0);

    // test prepare with default columns
    sqlx::query("insert into demo(i) values(?)")
        .bind(99)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("insert into demo(i) values(?)")
        .bind(-99)
        .execute(&pool)
        .await
        .unwrap();
    let rows = sqlx::query("select * from demo")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);

    for row in rows {
        let i: i64 = row.get("i");
        let ts: DateTime<Utc> = row.get("ts");
        let now = common_time::util::current_time_millis();
        assert!(now - ts.timestamp_millis() < 1000);
        assert_eq!(i.abs(), 99);
    }

    let _ = fe_mysql_server.shutdown().await;
    guard.remove_all().await;
}

pub async fn test_mysql_timezone(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();

    let (addr, mut guard, fe_mysql_server) = setup_mysql_server(store_type, "mysql_timezone").await;
    let mut conn = MySqlConnection::connect(&format!("mysql://{addr}/public"))
        .await
        .unwrap();

    let _ = conn
        .execute("SET time_zone = 'Asia/Shanghai'")
        .await
        .unwrap();
    let timezone = conn.fetch_all("SELECT @@time_zone").await.unwrap();
    assert_eq!(timezone[0].get::<String, usize>(0), "Asia/Shanghai");
    let timezone = conn.fetch_all("SELECT @@system_time_zone").await.unwrap();
    assert_eq!(timezone[0].get::<String, usize>(0), "UTC");
    let _ = conn.execute("SET time_zone = 'UTC'").await.unwrap();
    let timezone = conn.fetch_all("SELECT @@time_zone").await.unwrap();
    assert_eq!(timezone[0].get::<String, usize>(0), "UTC");
    let timezone = conn.fetch_all("SELECT @@system_time_zone").await.unwrap();
    assert_eq!(timezone[0].get::<String, usize>(0), "UTC");

    // test data
    let _ = conn
        .execute("create table demo(i bigint, ts timestamp time index)")
        .await
        .unwrap();
    let _ = conn
        .execute("insert into demo values(1, 1667446797450)")
        .await
        .unwrap();
    let rows = conn.fetch_all("select ts from demo").await.unwrap();
    assert_eq!(
        rows[0]
            .get::<chrono::DateTime<Utc>, usize>(0)
            .to_rfc3339_opts(SecondsFormat::Millis, true),
        "2022-11-03T03:39:57.450Z"
    );

    let _ = conn.execute("SET time_zone = '+08:00'").await.unwrap();
    let rows2 = conn.fetch_all("select ts from demo").await.unwrap();
    // we use Utc here for format only
    assert_eq!(
        rows2[0]
            .get::<chrono::DateTime<Utc>, usize>(0)
            .to_rfc3339_opts(SecondsFormat::Millis, true),
        "2022-11-03T11:39:57.450Z"
    );

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
        let dt = d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_millis();

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
        let ret: i64 = row.get("i");
        let d: NaiveDate = row.get("d");
        let dt: NaiveDateTime = row.get("dt");

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
        let ret: i64 = row.get("i");
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
pub async fn test_postgres_bytea(store_type: StorageType) {
    let (addr, mut guard, fe_pg_server) = setup_pg_server(store_type, "sql_bytea_output").await;

    let (client, connection) = tokio_postgres::connect(&format!("postgres://{addr}/public"), NoTls)
        .await
        .unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        connection.await.unwrap();
        tx.send(()).unwrap();
    });
    let _ = client
        .simple_query("CREATE TABLE test(b BLOB, ts TIMESTAMP TIME INDEX)")
        .await
        .unwrap();
    let _ = client
        .simple_query("INSERT INTO test VALUES(X'6162636b6c6d2aa954', 0)")
        .await
        .unwrap();
    let get_row = |mess: Vec<SimpleQueryMessage>| -> String {
        match &mess[1] {
            SimpleQueryMessage::Row(row) => row.get(0).unwrap().to_string(),
            _ => unreachable!(),
        }
    };

    let r = client.simple_query("SELECT b FROM test").await.unwrap();
    let b = get_row(r);
    assert_eq!(b, "\\x6162636b6c6d2aa954");

    let _ = client.simple_query("SET bytea_output='hex'").await.unwrap();
    let r = client.simple_query("SELECT b FROM test").await.unwrap();
    let b = get_row(r);
    assert_eq!(b, "\\x6162636b6c6d2aa954");

    let _ = client
        .simple_query("SET bytea_output='escape'")
        .await
        .unwrap();
    let r = client.simple_query("SELECT b FROM test").await.unwrap();
    let b = get_row(r);
    assert_eq!(b, "abcklm*\\251T");

    let _e = client
        .simple_query("SET bytea_output='invalid'")
        .await
        .unwrap_err();

    // binary format shall not be affected by bytea_output
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&format!("postgres://{addr}/public"))
        .await
        .unwrap();

    let row = sqlx::query("select b from test")
        .fetch_one(&pool)
        .await
        .unwrap();
    let val: Vec<u8> = row.get("b");
    assert_eq!(val, [97, 98, 99, 107, 108, 109, 42, 169, 84]);

    drop(client);
    rx.await.unwrap();

    let _ = fe_pg_server.shutdown().await;
    guard.remove_all().await;
}

pub async fn test_postgres_datestyle(store_type: StorageType) {
    let (addr, mut guard, fe_pg_server) = setup_pg_server(store_type, "various datestyle").await;

    let (client, connection) = tokio_postgres::connect(&format!("postgres://{addr}/public"), NoTls)
        .await
        .unwrap();

    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        connection.await.unwrap();
        tx.send(()).unwrap();
    });

    let validate_datestyle = |client: Client, datestyle: &str, is_valid: bool| {
        let datestyle = datestyle.to_string();
        async move {
            assert_eq!(
                client
                    .simple_query(format!("SET DATESTYLE={}", datestyle).as_str())
                    .await
                    .is_ok(),
                is_valid
            );
            client
        }
    };

    // style followed by order is valid
    let client = validate_datestyle(client, "'ISO,MDY'", true).await;

    // Mix of string and ident is valid
    let client = validate_datestyle(client, "'ISO',MDY", true).await;

    // list of string that didn't corrupt is valid
    let client = validate_datestyle(client, "'ISO,MDY','ISO,MDY'", true).await;

    // corrupted style
    let client = validate_datestyle(client, "'ISO,German'", false).await;

    // corrupted order
    let client = validate_datestyle(client, "'ISO,DMY','ISO,MDY'", false).await;

    // as long as the value is not corrupted, it's valid
    let client = validate_datestyle(client, "ISO,ISO,ISO,ISO,ISO,MDY,MDY,MDY,MDY", true).await;

    let _ = client
        .simple_query("CREATE TABLE ts_test(ts TIMESTAMP TIME INDEX)")
        .await
        .expect("CREATE TABLE ts_test ERROR");
    let _ = client
        .simple_query("CREATE TABLE date_test(d date, ts TIMESTAMP TIME INDEX)")
        .await
        .expect("CREATE TABLE date_test ERROR");

    let _ = client
        .simple_query("CREATE TABLE dt_test(dt datetime, ts TIMESTAMP TIME INDEX)")
        .await
        .expect("CREATE TABLE dt_test ERROR");

    let _ = client
        .simple_query("INSERT INTO ts_test VALUES('1997-12-17 07:37:16.123')")
        .await
        .expect("INSERT INTO ts_test ERROR");

    let _ = client
        .simple_query("INSERT INTO date_test VALUES('1997-12-17', '1997-12-17 07:37:16.123')")
        .await
        .expect("INSERT INTO date_test ERROR");

    let _ = client
        .simple_query(
            "INSERT INTO dt_test VALUES('1997-12-17 07:37:16.123', '1997-12-17 07:37:16.123')",
        )
        .await
        .expect("INSERT INTO dt_test ERROR");

    let get_row = |mess: Vec<SimpleQueryMessage>| -> String {
        match &mess[1] {
            SimpleQueryMessage::Row(row) => row.get(0).unwrap().to_string(),
            _ => unreachable!("Unexpected messages: {:?}", mess),
        }
    };

    let date = "DATE";
    let datetime = "TIMESTAMP";
    let timestamp = "TIMESTAMP";

    let iso = "ISO";
    let sql = "SQL";
    let postgres = "Postgres";
    let german = "German";

    let expected_set: HashMap<&str, HashMap<&str, HashMap<&str, &str>>> = HashMap::from([
        (
            date,
            HashMap::from([
                (
                    iso,
                    HashMap::from([
                        ("MDY", "1997-12-17"),
                        ("DMY", "1997-12-17"),
                        ("YMD", "1997-12-17"),
                    ]),
                ),
                (
                    sql,
                    HashMap::from([
                        ("MDY", "12/17/1997"),
                        ("DMY", "17/12/1997"),
                        ("YMD", "12/17/1997"),
                    ]),
                ),
                (
                    postgres,
                    HashMap::from([
                        ("MDY", "12-17-1997"),
                        ("DMY", "17-12-1997"),
                        ("YMD", "12-17-1997"),
                    ]),
                ),
                (
                    german,
                    HashMap::from([
                        ("MDY", "17.12.1997"),
                        ("DMY", "17.12.1997"),
                        ("YMD", "17.12.1997"),
                    ]),
                ),
            ]),
        ),
        (
            timestamp,
            HashMap::from([
                (
                    iso,
                    HashMap::from([
                        ("MDY", "1997-12-17 07:37:16.123000"),
                        ("DMY", "1997-12-17 07:37:16.123000"),
                        ("YMD", "1997-12-17 07:37:16.123000"),
                    ]),
                ),
                (
                    sql,
                    HashMap::from([
                        ("MDY", "12/17/1997 07:37:16.123000"),
                        ("DMY", "17/12/1997 07:37:16.123000"),
                        ("YMD", "12/17/1997 07:37:16.123000"),
                    ]),
                ),
                (
                    postgres,
                    HashMap::from([
                        ("MDY", "Wed Dec 17 07:37:16.123000 1997"),
                        ("DMY", "Wed 17 Dec 07:37:16.123000 1997"),
                        ("YMD", "Wed Dec 17 07:37:16.123000 1997"),
                    ]),
                ),
                (
                    german,
                    HashMap::from([
                        ("MDY", "17.12.1997 07:37:16.123000"),
                        ("DMY", "17.12.1997 07:37:16.123000"),
                        ("YMD", "17.12.1997 07:37:16.123000"),
                    ]),
                ),
            ]),
        ),
    ]);

    let get_expected = |ty: &str, style: &str, order: &str| {
        expected_set
            .get(ty)
            .and_then(|m| m.get(style))
            .and_then(|m2| m2.get(order))
            .unwrap()
            .to_string()
    };

    for style in ["ISO", "SQL", "Postgres", "German"] {
        for order in ["MDY", "DMY", "YMD"] {
            let _ = client
                .simple_query(&format!("SET DATESTYLE='{}', '{}'", style, order))
                .await
                .expect("SET DATESTYLE ERROR");

            let r = client.simple_query("SELECT ts FROM ts_test").await.unwrap();
            let ts = get_row(r);
            assert_eq!(
                ts,
                get_expected(timestamp, style, order),
                "style: {}, order: {}",
                style,
                order
            );

            let r = client
                .simple_query("SELECT d FROM date_test")
                .await
                .unwrap();
            let d = get_row(r);
            assert_eq!(
                d,
                get_expected(date, style, order),
                "style: {}, order: {}",
                style,
                order
            );

            let r = client.simple_query("SELECT dt FROM dt_test").await.unwrap();
            let dt = get_row(r);
            assert_eq!(
                dt,
                get_expected(datetime, style, order),
                "style: {}, order: {}",
                style,
                order
            );
        }
    }

    drop(client);
    rx.await.unwrap();

    let _ = fe_pg_server.shutdown().await;
    guard.remove_all().await;
}

pub async fn test_postgres_timezone(store_type: StorageType) {
    let (addr, mut guard, fe_pg_server) = setup_pg_server(store_type, "sql_inference").await;

    let (client, connection) = tokio_postgres::connect(&format!("postgres://{addr}/public"), NoTls)
        .await
        .unwrap();

    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        connection.await.unwrap();
        tx.send(()).unwrap();
    });

    let get_row = |mess: Vec<SimpleQueryMessage>| -> String {
        match &mess[1] {
            SimpleQueryMessage::Row(row) => row.get(0).unwrap().to_string(),
            _ => unreachable!(),
        }
    };

    let _ = client.simple_query("SET time_zone = 'UTC'").await.unwrap();
    let timezone = get_row(
        client
            .simple_query("SHOW VARIABLES time_zone")
            .await
            .unwrap(),
    );
    assert_eq!(timezone, "UTC");
    let timezone = get_row(
        client
            .simple_query("SHOW VARIABLES system_time_zone")
            .await
            .unwrap(),
    );
    assert_eq!(timezone, "UTC");
    let _ = client
        .simple_query("SET time_zone = 'Asia/Shanghai'")
        .await
        .unwrap();
    let timezone = get_row(
        client
            .simple_query("SHOW VARIABLES time_zone")
            .await
            .unwrap(),
    );
    assert_eq!(timezone, "Asia/Shanghai");
    let timezone = get_row(
        client
            .simple_query("SHOW VARIABLES system_time_zone")
            .await
            .unwrap(),
    );
    assert_eq!(timezone, "UTC");

    drop(client);
    rx.await.unwrap();

    let _ = fe_pg_server.shutdown().await;
    guard.remove_all().await;
}

pub async fn test_postgres_parameter_inference(store_type: StorageType) {
    let (addr, mut guard, fe_pg_server) = setup_pg_server(store_type, "sql_inference").await;

    let (client, connection) = tokio_postgres::connect(&format!("postgres://{addr}/public"), NoTls)
        .await
        .unwrap();

    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        connection.await.unwrap();
        tx.send(()).unwrap();
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

    // Shutdown the client.
    drop(client);
    rx.await.unwrap();

    let _ = fe_pg_server.shutdown().await;
    guard.remove_all().await;
}

pub async fn test_mysql_async_timestamp(store_type: StorageType) {
    use mysql_async::prelude::*;
    use time::PrimitiveDateTime;

    #[derive(Debug)]
    struct CpuMetric {
        hostname: String,
        environment: String,
        usage_user: f64,
        usage_system: f64,
        usage_idle: f64,
        ts: i64,
    }

    impl CpuMetric {
        fn new(
            hostname: String,
            environment: String,
            usage_user: f64,
            usage_system: f64,
            usage_idle: f64,
            ts: i64,
        ) -> Self {
            Self {
                hostname,
                environment,
                usage_user,
                usage_system,
                usage_idle,
                ts,
            }
        }
    }
    common_telemetry::init_default_ut_logging();

    let (addr, mut guard, fe_mysql_server) = setup_mysql_server(store_type, "sql_timestamp").await;
    let url = format!("mysql://{addr}/public");
    let opts = mysql_async::Opts::from_url(&url).unwrap();
    let mut conn = mysql_async::Conn::new(opts)
        .await
        .expect("create connection failure");

    r"CREATE TABLE IF NOT EXISTS cpu_metrics (
    hostname STRING,
    environment STRING,
    usage_user DOUBLE,
    usage_system DOUBLE,
    usage_idle DOUBLE,
    ts TIMESTAMP,
    TIME INDEX(ts),
    PRIMARY KEY(hostname, environment)
);"
    .ignore(&mut conn)
    .await
    .expect("create table failure");

    let metrics = vec![
        CpuMetric::new(
            "host0".into(),
            "test".into(),
            32f64,
            3f64,
            4f64,
            1680307200050,
        ),
        CpuMetric::new(
            "host1".into(),
            "test".into(),
            29f64,
            32f64,
            50f64,
            1680307200050,
        ),
        CpuMetric::new(
            "host0".into(),
            "test".into(),
            32f64,
            3f64,
            4f64,
            1680307260050,
        ),
        CpuMetric::new(
            "host1".into(),
            "test".into(),
            29f64,
            32f64,
            50f64,
            1680307260050,
        ),
        CpuMetric::new(
            "host0".into(),
            "test".into(),
            32f64,
            3f64,
            4f64,
            1680307320050,
        ),
        CpuMetric::new(
            "host1".into(),
            "test".into(),
            29f64,
            32f64,
            50f64,
            1680307320050,
        ),
    ];

    r"INSERT INTO cpu_metrics (hostname, environment, usage_user, usage_system, usage_idle, ts)
      VALUES (:hostname, :environment, :usage_user, :usage_system, :usage_idle, :ts)"
        .with(metrics.iter().map(|metric| {
            params! {
                "hostname" => &metric.hostname,
                "environment" => &metric.environment,
                "usage_user" => metric.usage_user,
                "usage_system" => metric.usage_system,
                "usage_idle" => metric.usage_idle,
                "ts" => metric.ts,
            }
        }))
        .batch(&mut conn)
        .await
        .expect("insert data failure");

    // query data
    let loaded_metrics = "SELECT * FROM cpu_metrics"
        .with(())
        .map(
            &mut conn,
            |(hostname, environment, usage_user, usage_system, usage_idle, raw_ts): (
                String,
                String,
                f64,
                f64,
                f64,
                PrimitiveDateTime,
            )| {
                let ts = raw_ts.assume_utc().unix_timestamp() * 1000;
                CpuMetric::new(
                    hostname,
                    environment,
                    usage_user,
                    usage_system,
                    usage_idle,
                    ts,
                )
            },
        )
        .await
        .expect("query data failure");
    assert_eq!(loaded_metrics.len(), 6);

    let _ = fe_mysql_server.shutdown().await;
    guard.remove_all().await;
}

pub async fn test_mysql_prepare_stmt_insert_timestamp(store_type: StorageType) {
    let (addr, mut guard, server) =
        setup_mysql_server(store_type, "test_mysql_prepare_stmt_insert_timestamp").await;

    let pool = MySqlPoolOptions::new()
        .max_connections(2)
        .connect(&format!("mysql://{addr}/public"))
        .await
        .unwrap();

    sqlx::query("create table demo(i bigint, ts timestamp time index)")
        .execute(&pool)
        .await
        .unwrap();

    // Valid timestamp binary encoding: https://mariadb.com/kb/en/resultset-row/#timestamp-binary-encoding

    // Timestamp data length = 4, year-month-day(ymd) only:
    sqlx::query("insert into demo values(?, ?)")
        .bind(0)
        .bind(
            NaiveDate::from_ymd_opt(2023, 12, 19)
                // Though hour, minute and second are provided, `sqlx` will not encode them if they are all zeroes,
                // which is just what we desire here.
                // See https://github.com/launchbadge/sqlx/blob/bb064e3789d68ad4e9affe7cba34944abb000f72/sqlx-core/src/mysql/types/chrono.rs#L186C22-L186C22
                .and_then(|x| x.and_hms_opt(0, 0, 0))
                .unwrap(),
        )
        .execute(&pool)
        .await
        .unwrap();

    // Timestamp data length = 7, ymd and hour-minute-second(hms):
    sqlx::query("insert into demo values(?, ?)")
        .bind(1)
        .bind(
            NaiveDate::from_ymd_opt(2023, 12, 19)
                .and_then(|x| x.and_hms_opt(13, 19, 1))
                .unwrap(),
        )
        .execute(&pool)
        .await
        .unwrap();

    // Timestamp data length = 11, ymd, hms and microseconds:
    sqlx::query("insert into demo values(?, ?)")
        .bind(2)
        .bind(
            NaiveDate::from_ymd_opt(2023, 12, 19)
                .and_then(|x| x.and_hms_micro_opt(13, 20, 1, 123456))
                .unwrap(),
        )
        .execute(&pool)
        .await
        .unwrap();

    let rows = sqlx::query("select i, ts from demo order by i")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);

    let x: DateTime<Utc> = rows[0].get("ts");
    assert_eq!(x.to_string(), "2023-12-19 00:00:00 UTC");

    let x: DateTime<Utc> = rows[1].get("ts");
    assert_eq!(x.to_string(), "2023-12-19 13:19:01 UTC");

    let x: DateTime<Utc> = rows[2].get("ts");
    assert_eq!(x.to_string(), "2023-12-19 13:20:01.123 UTC");

    let _ = server.shutdown().await;
    guard.remove_all().await;
}
