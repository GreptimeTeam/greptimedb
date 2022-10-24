use client::{Client, Database, Select};
use tracing::{event, Level};

fn main() {
    tracing::subscriber::set_global_default(tracing_subscriber::FmtSubscriber::builder().finish())
        .unwrap();

    run();
}

#[tokio::main]
async fn run() {
    let mut db = Database::new("greptime", Client::new());
    db.start(&["127.0.0.1:3001"]);

    let sql = Select::Sql("select * from demo".to_string());
    let result = db.select(sql).await.unwrap();

    event!(Level::INFO, "result: {:#?}", result);
}
