use api::v1::{select_expr, SelectExpr};
use client::{Client, Database};
use tracing::{event, Level};

fn main() {
    tracing::subscriber::set_global_default(tracing_subscriber::FmtSubscriber::builder().finish())
        .unwrap();

    run();
}

#[tokio::main]
async fn run() {
    let client = Client::connect("http://127.0.0.1:3001").await.unwrap();
    let db = Database::new("greptime", client);

    let select_expr = SelectExpr {
        expr: Some(select_expr::Expr::Sql("select * from demo".to_string())),
    };
    let (header, body) = db.select(select_expr).await.unwrap();

    event!(Level::INFO, "response header: {:#?}", header);
    event!(Level::INFO, "response body: {:#?}", body);
}
