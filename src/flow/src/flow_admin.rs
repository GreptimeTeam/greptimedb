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

//! Dispatch of the generic flow-admin route.
//!
//! `ADMIN <function>(...)` reaches a flownode as one request naming the
//! function and carrying that function's own string arguments, so the wire
//! format knows no function and no argument layout. Which functions a
//! deployment serves, and who answers them, is decided here: a deployment
//! installs a [`FlowAdminRegistry`] of [`FlowAdminHandler`]s, and the flownode
//! looks the requested name up. Adding a function is one handler plus its
//! registration; neither the route nor the request body changes.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use crate::Result;
use crate::error::{UnexpectedSnafu, UnsupportedSnafu};

/// The arguments of one flow-admin call.
///
/// The flow the call is about is carried by the route itself (`flow_id` and its
/// `catalog`/`flow` names), so a handler never resolves which flow it was asked
/// about; it only decides what its own `args` mean.
#[derive(Debug, Clone, Copy)]
pub struct FlowAdminRequest<'a> {
    /// The flow id the request was routed with, already resolved by the caller.
    pub flow_id: u32,
    /// The catalog of the flow the request names.
    pub catalog: &'a str,
    /// The flow name the request names.
    pub flow: &'a str,
    /// The arguments of the function, unchanged and unparsed by the route.
    pub args: &'a [String],
}

/// One flow-admin function a deployment serves.
///
/// A handler owns the argument validation and the answer of exactly one
/// function, so a function's meaning lives with its implementation instead of
/// in the route or in the wire format.
#[async_trait::async_trait]
pub trait FlowAdminHandler: Send + Sync + 'static {
    /// The function name `ADMIN <name>(...)` routes with.
    fn name(&self) -> &'static str;

    /// Serves one call of [`Self::name`].
    ///
    /// The answer is the string the caller of the admin function receives.
    async fn handle(&self, request: FlowAdminRequest<'_>) -> Result<String>;
}

/// The flow-admin functions one deployment serves, by name.
pub struct FlowAdminRegistry {
    handlers: BTreeMap<&'static str, Arc<dyn FlowAdminHandler>>,
}

/// Shared handle to the [`FlowAdminRegistry`] a deployment installs.
pub type FlowAdminRegistryRef = Arc<FlowAdminRegistry>;

impl fmt::Debug for FlowAdminRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlowAdminRegistry")
            .field("functions", &self.names().collect::<Vec<_>>())
            .finish()
    }
}

impl FlowAdminRegistry {
    /// Builds a registry from `handlers`.
    ///
    /// A function name registered twice is refused here rather than at call
    /// time: two answers for one name would make the route depend on
    /// registration order.
    pub fn try_new(handlers: Vec<Arc<dyn FlowAdminHandler>>) -> Result<Self> {
        let mut by_name: BTreeMap<&'static str, Arc<dyn FlowAdminHandler>> = BTreeMap::new();
        for handler in handlers {
            let name = handler.name();
            if by_name.insert(name, handler).is_some() {
                return UnexpectedSnafu {
                    reason: format!("flow admin function `{name}` is registered more than once"),
                }
                .fail();
            }
        }
        Ok(Self { handlers: by_name })
    }

    /// The function names this registry serves, in name order.
    pub fn names(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.handlers.keys().copied()
    }

    /// Serves one call of `function`.
    ///
    /// A name this deployment does not serve is refused with the one
    /// [`crate::Error::Unsupported`] of the route, naming the requested
    /// function: it is not a flow that is missing, not a range that is invalid
    /// and not an argument that failed to parse.
    pub async fn dispatch(&self, function: &str, request: FlowAdminRequest<'_>) -> Result<String> {
        let Some(handler) = self.handlers.get(function) else {
            return UnsupportedSnafu {
                reason: format!(
                    "flow admin function `{function}` is not supported by this flownode; \
                     it serves {}",
                    self.names().collect::<Vec<_>>().join(", ")
                ),
            }
            .fail();
        };

        handler.handle(request).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{FlowAdminHandler, FlowAdminRegistry, FlowAdminRequest};
    use crate::error::Error;

    /// A handler of one function that echoes what it was asked and counts how
    /// often it was entered.
    struct EchoHandler {
        name: &'static str,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl FlowAdminHandler for EchoHandler {
        fn name(&self) -> &'static str {
            self.name
        }

        async fn handle(&self, request: FlowAdminRequest<'_>) -> crate::Result<String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(format!(
                "{}:{}.{}#{}[{}]",
                self.name,
                request.catalog,
                request.flow,
                request.flow_id,
                request.args.join(",")
            ))
        }
    }

    fn echo(name: &'static str, calls: &Arc<AtomicUsize>) -> Arc<dyn FlowAdminHandler> {
        Arc::new(EchoHandler {
            name,
            calls: calls.clone(),
        })
    }

    #[test]
    fn registry_refuses_a_duplicated_function_name() {
        let calls = Arc::new(AtomicUsize::new(0));
        let err =
            FlowAdminRegistry::try_new(vec![echo("dup", &calls), echo("dup", &calls)]).unwrap_err();

        assert!(matches!(err, Error::Unexpected { .. }), "{err}");
        assert!(err.to_string().contains("dup"), "{err}");
        assert!(err.to_string().contains("more than once"), "{err}");
    }

    #[tokio::test]
    async fn registry_refuses_an_unknown_function_by_name() {
        let calls = Arc::new(AtomicUsize::new(0));
        let registry =
            FlowAdminRegistry::try_new(vec![echo("known", &calls), echo("other", &calls)]).unwrap();

        let err = registry
            .dispatch(
                "missing",
                FlowAdminRequest {
                    flow_id: 7,
                    catalog: "greptime",
                    flow: "demo",
                    args: &[],
                },
            )
            .await
            .unwrap_err();

        // One centralized refusal: it names the requested function and what the
        // deployment does serve, and no handler is entered.
        assert!(matches!(err, Error::Unsupported { .. }), "{err}");
        assert!(err.to_string().contains("`missing`"), "{err}");
        assert!(err.to_string().contains("known"), "{err}");
        assert!(err.to_string().contains("other"), "{err}");
        assert_eq!(0, calls.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn registry_dispatches_the_named_function_with_the_whole_request() {
        let calls = Arc::new(AtomicUsize::new(0));
        let registry =
            FlowAdminRegistry::try_new(vec![echo("first", &calls), echo("second", &calls)])
                .unwrap();
        let args = vec!["0".to_string(), "5000".to_string()];

        let answer = registry
            .dispatch(
                "second",
                FlowAdminRequest {
                    flow_id: 9,
                    catalog: "greptime",
                    flow: "demo",
                    args: &args,
                },
            )
            .await
            .unwrap();

        // The route resolves the name and hands over the request unchanged: the
        // flow id, both names and every argument reach the handler that owns the
        // name, and only that one.
        assert_eq!("second:greptime.demo#9[0,5000]", answer);
        assert_eq!(1, calls.load(Ordering::SeqCst));
    }

    #[test]
    fn registry_lists_its_function_names_in_order() {
        let calls = Arc::new(AtomicUsize::new(0));
        let registry =
            FlowAdminRegistry::try_new(vec![echo("zeta", &calls), echo("alpha", &calls)]).unwrap();

        assert_eq!(vec!["alpha", "zeta"], registry.names().collect::<Vec<_>>());
    }
}
