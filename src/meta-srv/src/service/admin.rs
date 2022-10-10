mod health;

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use tonic::body::BoxBody;
use tonic::codegen::empty_body;
use tonic::codegen::http;
use tonic::codegen::BoxFuture;
use tonic::codegen::Service;
use tonic::transport::NamedService;

use crate::metasrv::MetaSrv;

pub fn make_admin_service(_: MetaSrv) -> Admin {
    let router = Router::new().route("/health", health::HealthHandler);

    let router = Router::nest("/admin", router);

    Admin::new(router)
}

#[async_trait::async_trait]
pub trait HttpHandler: Send + Sync {
    async fn handle(
        &self,
        path: &str,
        params: &HashMap<String, String>,
    ) -> crate::Result<http::Response<String>>;
}

pub struct Admin
where
    Self: Send,
{
    router: Arc<Router>,
}

impl Admin {
    pub fn new(router: Router) -> Self {
        Self {
            router: Arc::new(router),
        }
    }
}

impl NamedService for Admin {
    const NAME: &'static str = "admin";
}

impl Clone for Admin {
    fn clone(&self) -> Self {
        Self {
            router: self.router.clone(),
        }
    }
}

impl<T> Service<http::Request<T>> for Admin
where
    T: Send,
{
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<T>) -> Self::Future {
        let router = self.router.clone();
        let query_params = req
            .uri()
            .query()
            .map(|q| {
                url::form_urlencoded::parse(q.as_bytes())
                    .into_owned()
                    .collect()
            })
            .unwrap_or_else(HashMap::new);
        let path = req.uri().path().to_owned();
        Box::pin(async move { router.call(&path, query_params).await })
    }
}

#[derive(Default)]
pub struct Router {
    handlers: HashMap<String, Box<dyn HttpHandler>>,
}

impl Router {
    pub fn new() -> Self {
        Self {
            handlers: HashMap::default(),
        }
    }

    pub fn nest(path: &str, router: Router) -> Self {
        if path.is_empty() || !path.starts_with('/') {
            panic!("paths must start with a `/`")
        }

        let handlers = router
            .handlers
            .into_iter()
            .map(|(url, handler)| (format!("{path}{url}"), handler))
            .collect();

        Self { handlers }
    }

    pub fn route(mut self, path: &str, handler: impl HttpHandler + 'static) -> Self {
        if path.is_empty() || !path.starts_with('/') {
            panic!("paths must start with a `/`")
        }

        self.handlers.insert(path.to_owned(), Box::new(handler));

        self
    }

    pub async fn call(
        &self,
        path: &str,
        params: HashMap<String, String>,
    ) -> Result<http::Response<BoxBody>, Infallible> {
        let handler = match self.handlers.get(path) {
            Some(handler) => handler,
            None => {
                return Ok(http::Response::builder()
                    .status(http::StatusCode::NOT_FOUND)
                    .body(empty_body())
                    .unwrap())
            }
        };

        let res = match handler.handle(path, &params).await {
            Ok(res) => res.map(boxed),
            Err(e) => http::Response::builder()
                .status(http::StatusCode::INTERNAL_SERVER_ERROR)
                .body(boxed(e.to_string()))
                .unwrap(),
        };

        Ok(res)
    }
}

fn boxed(body: String) -> BoxBody {
    use http_body::Body;

    body.map_err(|_| panic!("")).boxed_unsync()
}
