use sqlness::interceptor::{Interceptor, InterceptorFactory, InterceptorRef};
use sqlness::SqlnessError;

pub const PROTOCOL_KEY: &str = "protocol";

pub const POSTGRES: &str = "postgres";
pub const MYSQL: &str = "mysql";

pub const PREFIX: &str = "PROTOCOL";
pub struct BeginProtocolInterceptor {
    protocol: String,
}
pub struct BeginProtocolInterceptorFactory;
impl Interceptor for BeginProtocolInterceptor {
    fn before_execute(&self, _: &mut Vec<String>, context: &mut sqlness::QueryContext) {
        context
            .context
            .insert(PROTOCOL_KEY.to_string(), self.protocol.clone());
    }
}
impl InterceptorFactory for BeginProtocolInterceptorFactory {
    fn try_new(&self, ctx: &str) -> Result<InterceptorRef, SqlnessError> {
        match ctx.to_lowercase().as_str() {
            POSTGRES | MYSQL => Ok(Box::new(BeginProtocolInterceptor {
                protocol: ctx.to_string(),
            })),
            _ => Err(SqlnessError::InvalidContext {
                prefix: PREFIX.to_string(),
                msg: format!("Unsupported protocol: {}", ctx),
            }),
        }
    }
}
