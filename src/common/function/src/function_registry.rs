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

//! functions registry
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, LazyLock, RwLock};

use datafusion::catalog::TableFunction;
use datafusion_expr::expr_rewriter::FunctionRewrite;
use datafusion_expr::{AggregateUDF, WindowUDF};

use crate::admin::AdminFunction;
use crate::aggrs::aggr_wrapper::StateMergeHelper;
use crate::aggrs::approximate::ApproximateFunction;
use crate::aggrs::count_hash::CountHash;
use crate::aggrs::vector::VectorFunction as VectorAggrFunction;
use crate::function::{Function, FunctionRef};
use crate::function_factory::ScalarFunctionFactory;
use crate::scalars::anomaly::AnomalyFunction;
use crate::scalars::date::DateFunction;
use crate::scalars::expression::ExpressionFunction;
use crate::scalars::hll_count::HllCalcFunction;
use crate::scalars::ip::IpFunctions;
use crate::scalars::json::JsonFunction;
use crate::scalars::matches::MatchesFunction;
use crate::scalars::matches_term::MatchesTermFunction;
use crate::scalars::math::MathFunction;
use crate::scalars::primary_key::DecodePrimaryKeyFunction;
use crate::scalars::string::register_string_functions;
use crate::scalars::timestamp::TimestampFunction;
use crate::scalars::uddsketch_calc::{UddSketchCalcFunction, UddSketchMergeStateFunction};
use crate::scalars::vector::VectorFunction as VectorScalarFunction;
use crate::system::SystemFunction;

#[derive(Default)]
pub struct FunctionRegistry {
    functions: RwLock<HashMap<String, ScalarFunctionFactory>>,
    aggregate_functions: RwLock<HashMap<String, AggregateUDF>>,
    table_functions: RwLock<HashMap<String, Arc<TableFunction>>>,
    function_rewrites: RwLock<Vec<Arc<dyn FunctionRewrite + Send + Sync>>>,
    window_functions: RwLock<HashMap<String, WindowUDF>>,
}

/// The result of registering a function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionRegistrationResult {
    /// The function was newly registered.
    Registered,
    /// A function with the same name was already registered and was kept.
    AlreadyExists,
}

impl FunctionRegistry {
    /// Register a function in the registry by converting it into a `ScalarFunctionFactory`.
    ///
    /// # Arguments
    ///
    /// * `func` - An object that can be converted into a `ScalarFunctionFactory`.
    ///
    /// The function is inserted into the internal function map, keyed by its name.
    /// If a function with the same name already exists, it will be replaced.
    pub fn register(&self, func: impl Into<ScalarFunctionFactory>) {
        let func = func.into();
        let _ = self
            .functions
            .write()
            .unwrap()
            .insert(func.name().to_string(), func);
    }

    /// Register a function only if no function with the same name exists.
    ///
    /// The duplicate check and the insert happen atomically under the same
    /// write lock of the functions map. If a function with the same name
    /// already exists, it is kept unchanged and
    /// [`FunctionRegistrationResult::AlreadyExists`] is returned; otherwise the
    /// function is registered and [`FunctionRegistrationResult::Registered`] is
    /// returned.
    pub fn register_if_absent(
        &self,
        func: impl Into<ScalarFunctionFactory>,
    ) -> FunctionRegistrationResult {
        let func = func.into();
        let mut functions = self.functions.write().unwrap();
        match functions.entry(func.name().to_string()) {
            Entry::Occupied(_) => FunctionRegistrationResult::AlreadyExists,
            Entry::Vacant(entry) => {
                entry.insert(func);
                FunctionRegistrationResult::Registered
            }
        }
    }

    /// Register a scalar function in the registry.
    pub fn register_scalar(&self, func: impl Function + 'static) {
        let func = Arc::new(func) as FunctionRef;

        for alias in func.aliases() {
            let func: ScalarFunctionFactory = func.clone().into();
            let alias = ScalarFunctionFactory {
                name: alias.clone(),
                ..func
            };
            self.register(alias);
        }

        self.register(func)
    }

    /// Register an aggregate function in the registry.
    pub fn register_aggr(&self, func: AggregateUDF) {
        let _ = self
            .aggregate_functions
            .write()
            .unwrap()
            .insert(func.name().to_string(), func);
    }

    /// Register a table function
    pub fn register_table_function(&self, func: TableFunction) {
        let _ = self
            .table_functions
            .write()
            .unwrap()
            .insert(func.name().to_string(), Arc::new(func));
    }

    /// Register a function rewrite rule.
    pub fn register_function_rewrite(&self, func: impl FunctionRewrite + Send + Sync + 'static) {
        self.function_rewrites.write().unwrap().push(Arc::new(func));
    }

    /// Register a window function (UDWF).
    pub fn register_window(&self, func: WindowUDF) {
        let _ = self
            .window_functions
            .write()
            .unwrap()
            .insert(func.name().to_string(), func);
    }

    pub fn get_function(&self, name: &str) -> Option<ScalarFunctionFactory> {
        self.functions.read().unwrap().get(name).cloned()
    }

    /// Returns a list of all scalar functions registered in the registry.
    pub fn scalar_functions(&self) -> Vec<ScalarFunctionFactory> {
        self.functions.read().unwrap().values().cloned().collect()
    }

    /// Returns a list of all aggregate functions registered in the registry.
    pub fn aggregate_functions(&self) -> Vec<AggregateUDF> {
        self.aggregate_functions
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }

    pub fn table_functions(&self) -> Vec<Arc<TableFunction>> {
        self.table_functions
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }

    /// Returns a list of all window functions registered in the registry.
    pub fn window_functions(&self) -> Vec<WindowUDF> {
        self.window_functions
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }

    /// Returns true if an aggregate function with the given name exists in the registry.
    pub fn is_aggr_func_exist(&self, name: &str) -> bool {
        self.aggregate_functions.read().unwrap().contains_key(name)
    }

    /// Returns a list of all function rewrite rules registered in the registry.
    pub fn function_rewrites(&self) -> Vec<Arc<dyn FunctionRewrite + Send + Sync>> {
        self.function_rewrites.read().unwrap().clone()
    }
}

pub static FUNCTION_REGISTRY: LazyLock<Arc<FunctionRegistry>> = LazyLock::new(|| {
    let function_registry = FunctionRegistry::default();

    // Utility functions
    MathFunction::register(&function_registry);
    TimestampFunction::register(&function_registry);
    DateFunction::register(&function_registry);
    ExpressionFunction::register(&function_registry);
    UddSketchCalcFunction::register(&function_registry);
    UddSketchMergeStateFunction::register(&function_registry);
    HllCalcFunction::register(&function_registry);
    DecodePrimaryKeyFunction::register(&function_registry);

    // Full text search function
    MatchesFunction::register(&function_registry);
    MatchesTermFunction::register(&function_registry);

    // System and administration functions
    SystemFunction::register(&function_registry);
    AdminFunction::register(&function_registry);

    // Json related functions
    JsonFunction::register(&function_registry);

    // String related functions
    register_string_functions(&function_registry);

    // Vector related functions
    VectorScalarFunction::register(&function_registry);
    VectorAggrFunction::register(&function_registry);

    // Geo functions
    #[cfg(feature = "geo")]
    crate::scalars::geo::GeoFunctions::register(&function_registry);
    #[cfg(feature = "geo")]
    crate::aggrs::geo::GeoFunction::register(&function_registry);

    // Ip functions
    IpFunctions::register(&function_registry);

    // Approximate functions
    ApproximateFunction::register(&function_registry);

    // CountHash function
    CountHash::register(&function_registry);

    // state function of supported aggregate functions
    StateMergeHelper::register(&function_registry);

    // Anomaly detection window functions
    AnomalyFunction::register(&function_registry);

    Arc::new(function_registry)
});

static ADMIN_FUNCTION_REGISTRY: LazyLock<FunctionRegistry> = LazyLock::new(|| {
    let registry = FunctionRegistry::default();
    AdminFunction::register_admin_only(&registry);
    registry
});

/// Returns a function that is only available to the ADMIN statement executor.
pub fn get_admin_function(name: &str) -> Option<ScalarFunctionFactory> {
    ADMIN_FUNCTION_REGISTRY.get_function(name)
}

/// Register a function that is only available to the ADMIN statement executor.
///
/// If a function with the same name is already registered in the ADMIN
/// registry, the existing one is kept and
/// [`FunctionRegistrationResult::AlreadyExists`] is returned. A name that
/// already exists in the normal [`FUNCTION_REGISTRY`] when this call
/// linearizes is also rejected: the ADMIN executor resolves admin-only
/// functions before falling back to the normal registry, so inserting such a
/// name here would shadow the built-in. Otherwise the function is registered
/// and [`FunctionRegistrationResult::Registered`] is returned.
///
/// The enforced contract is one-way: it only guards the ADMIN registration
/// against names already present in the normal registry. A later ordinary
/// [`FunctionRegistry::register`] may still install the same name in the
/// normal registry because the normal registry keeps its legacy replace
/// semantics.
pub fn register_admin_function(
    func: impl Into<ScalarFunctionFactory>,
) -> FunctionRegistrationResult {
    register_admin_function_in(&ADMIN_FUNCTION_REGISTRY, &FUNCTION_REGISTRY, func)
}

/// Core implementation of [`register_admin_function`] against a pair of
/// registries, parameterized so tests can exercise it with local registries.
///
/// Locking: the ADMIN-registry write lock is acquired first, then a read lock
/// on the normal registry, and the normal-registry guard (bound to
/// `normal_functions`) is kept alive through both the normal-name check and
/// the ADMIN insertion below. This is the only code path that holds both
/// registries' locks, so the ADMIN -> FUNCTION acquisition order is
/// consistent and a concurrent normal-registry registration cannot slip in
/// between the check and the ADMIN insert and be shadowed.
///
/// The enforced contract is one-way: it only guards the ADMIN registration
/// against names already present in the normal registry. A later ordinary
/// [`FunctionRegistry::register`] may still install the same name in the
/// normal registry because the normal registry keeps its legacy replace
/// semantics.
fn register_admin_function_in(
    admin_registry: &FunctionRegistry,
    normal_registry: &FunctionRegistry,
    func: impl Into<ScalarFunctionFactory>,
) -> FunctionRegistrationResult {
    let func = func.into();
    let mut admin_functions = admin_registry.functions.write().unwrap();
    // The normal-registry guard is a read lock: it is held across the
    // normal-name check and the ADMIN insertion below, and while it is alive
    // no writer can acquire the normal-registry write lock, so a concurrent
    // normal-registry registration cannot slip in between the check and the
    // ADMIN insert and be shadowed.
    let normal_functions = normal_registry.functions.read().unwrap();
    if normal_functions.contains_key(func.name()) {
        drop(normal_functions);
        return FunctionRegistrationResult::AlreadyExists;
    }
    let result = match admin_functions.entry(func.name().to_string()) {
        Entry::Occupied(_) => FunctionRegistrationResult::AlreadyExists,
        Entry::Vacant(entry) => {
            entry.insert(func);
            FunctionRegistrationResult::Registered
        }
    };
    // Drop the read guard only after the ADMIN insertion, so writers to the
    // normal registry stay blocked until the check-and-insert is complete.
    drop(normal_functions);
    result
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};
    use std::thread;

    use super::*;
    use crate::scalars::test::TestAndFunction;
    use crate::scalars::udf::create_udf;

    /// Creates a [`ScalarFunctionFactory`] with the given name. Each call
    /// allocates a distinct factory closure, so factories can be told apart by
    /// [`Arc::ptr_eq`] on their `factory` field even when names are identical.
    fn named_factory(name: &str) -> ScalarFunctionFactory {
        ScalarFunctionFactory {
            name: name.to_string(),
            factory: Arc::new(|_ctx| create_udf(Arc::new(TestAndFunction::default()))),
        }
    }

    #[test]
    fn test_function_registry() {
        let registry = FunctionRegistry::default();

        assert!(registry.get_function("test_and").is_none());
        assert!(registry.scalar_functions().is_empty());
        registry.register_scalar(TestAndFunction::default());
        let _ = registry.get_function("test_and").unwrap();
        assert_eq!(1, registry.scalar_functions().len());
    }

    #[test]
    fn test_register_if_absent_registers_new_function() {
        let registry = FunctionRegistry::default();
        let name = "pr3_register_if_absent_new";
        let factory = named_factory(name);

        assert_eq!(
            registry.register_if_absent(factory.clone()),
            FunctionRegistrationResult::Registered
        );
        let registered = registry
            .get_function(name)
            .expect("function should be registered");
        assert!(Arc::ptr_eq(&registered.factory, &factory.factory));
    }

    #[test]
    fn test_register_if_absent_first_registration_wins() {
        let registry = FunctionRegistry::default();
        let name = "pr3_register_if_absent_duplicate";
        let first = named_factory(name);
        let second = named_factory(name);

        assert_eq!(
            registry.register_if_absent(first.clone()),
            FunctionRegistrationResult::Registered
        );
        assert_eq!(
            registry.register_if_absent(second.clone()),
            FunctionRegistrationResult::AlreadyExists
        );

        let stored = registry
            .get_function(name)
            .expect("function should be registered");
        assert!(Arc::ptr_eq(&stored.factory, &first.factory));
        assert!(!Arc::ptr_eq(&stored.factory, &second.factory));
    }

    #[test]
    fn test_register_replaces_existing_function() {
        // Regression test: `register` keeps its replace semantics.
        let registry = FunctionRegistry::default();
        let name = "pr3_register_replaces";
        let first = named_factory(name);
        let second = named_factory(name);

        registry.register(first.clone());
        registry.register(second.clone());

        let stored = registry
            .get_function(name)
            .expect("function should be registered");
        assert!(Arc::ptr_eq(&stored.factory, &second.factory));
        assert!(!Arc::ptr_eq(&stored.factory, &first.factory));
    }

    #[test]
    fn test_concurrent_register_if_absent_same_name() {
        const THREADS: usize = 8;
        let registry = Arc::new(FunctionRegistry::default());
        let name = "pr3_concurrent_same_name";
        let barrier = Arc::new(Barrier::new(THREADS));

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let registry = Arc::clone(&registry);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    let factory = named_factory(name);
                    // Synchronize so every thread attempts registration at the
                    // same time; only one may win the write lock.
                    barrier.wait();
                    let result = registry.register_if_absent(factory.clone());
                    (result, factory)
                })
            })
            .collect();

        let mut results: Vec<(FunctionRegistrationResult, ScalarFunctionFactory)> =
            Vec::with_capacity(THREADS);
        for handle in handles {
            results.push(handle.join().expect("thread should not panic"));
        }

        let registered = results
            .iter()
            .filter(|(result, _)| *result == FunctionRegistrationResult::Registered)
            .count();
        let already_exists = results
            .iter()
            .filter(|(result, _)| *result == FunctionRegistrationResult::AlreadyExists)
            .count();
        assert_eq!(registered, 1);
        assert_eq!(already_exists, THREADS - 1);

        let winner = results
            .iter()
            .find(|(result, _)| *result == FunctionRegistrationResult::Registered)
            .map(|(_, factory)| factory)
            .expect("exactly one registration must win");

        let stored = registry
            .get_function(name)
            .expect("function should be registered");
        assert!(
            Arc::ptr_eq(&stored.factory, &winner.factory),
            "the stored factory must be the factory of the winning registration"
        );
    }

    #[test]
    fn test_register_admin_function_first_wins() {
        // Tests touching the global registry must use unique names.
        let name = "pr3_admin_runtime_register";
        let first = named_factory(name);
        let second = named_factory(name);

        assert_eq!(
            register_admin_function(first.clone()),
            FunctionRegistrationResult::Registered
        );
        assert_eq!(
            register_admin_function(second.clone()),
            FunctionRegistrationResult::AlreadyExists
        );

        let stored = get_admin_function(name).expect("admin function should be queryable");
        assert!(Arc::ptr_eq(&stored.factory, &first.factory));
        assert!(!Arc::ptr_eq(&stored.factory, &second.factory));
    }

    #[test]
    fn test_register_admin_function_duplicate_same_factory() {
        // The exact same factory clone registered twice: the first registration
        // wins, the duplicate is rejected, and the stored factory is
        // pointer-equal to the original. Tests touching the global registry
        // must use unique names.
        let name = "pr3_admin_runtime_register_same_factory";
        let factory = named_factory(name);

        assert_eq!(
            register_admin_function(factory.clone()),
            FunctionRegistrationResult::Registered
        );
        assert_eq!(
            register_admin_function(factory.clone()),
            FunctionRegistrationResult::AlreadyExists
        );

        let stored = get_admin_function(name).expect("admin function should be queryable");
        assert!(Arc::ptr_eq(&stored.factory, &factory.factory));
    }

    #[test]
    fn test_register_admin_function_in_is_one_way_later_normal_register_allowed() {
        // The guard is one-way: after the ADMIN registration completes, an
        // ordinary normal-registry registration of the same name still
        // succeeds because the normal registry keeps its legacy replace
        // semantics.
        let admin = FunctionRegistry::default();
        let normal = FunctionRegistry::default();
        let name = "pr3_admin_in_one_way";
        let admin_factory = named_factory(name);
        let normal_factory = named_factory(name);

        assert_eq!(
            register_admin_function_in(&admin, &normal, admin_factory.clone()),
            FunctionRegistrationResult::Registered
        );

        normal.register(normal_factory.clone());

        let stored_admin = admin
            .get_function(name)
            .expect("the ADMIN registration must be kept");
        assert!(Arc::ptr_eq(&stored_admin.factory, &admin_factory.factory));
        let stored_normal = normal
            .get_function(name)
            .expect("the later normal registration must succeed");
        assert!(Arc::ptr_eq(&stored_normal.factory, &normal_factory.factory));
    }

    #[test]
    fn test_register_admin_function_rejects_normal_registry_builtin_name() {
        // Regression test: the ADMIN executor resolves `get_admin_function`
        // before falling back to `FUNCTION_REGISTRY`, so registering a function
        // whose name already exists in the normal registry would shadow the
        // ADMIN-invocable built-in (e.g. `flush_table`). Such registrations
        // must be rejected with
        // [`FunctionRegistrationResult::AlreadyExists`] and must not be
        // inserted into the ADMIN registry.
        let factory = named_factory("flush_table");

        assert_eq!(
            register_admin_function(factory.clone()),
            FunctionRegistrationResult::AlreadyExists
        );
        assert!(
            get_admin_function("flush_table").is_none(),
            "a normal-registry built-in must not be shadowed into the ADMIN registry"
        );
        assert!(
            FUNCTION_REGISTRY.get_function("flush_table").is_some(),
            "the normal-registry built-in must remain registered"
        );
    }

    #[test]
    fn test_builtin_admin_functions_remain_queryable() {
        // Built-in admin-only functions registered at startup stay queryable
        // through the same global registry used for runtime registrations.
        #[cfg(feature = "enterprise")]
        {
            assert!(get_admin_function("purge_table").is_some());
        }
        #[cfg(not(feature = "enterprise"))]
        {
            assert!(get_admin_function("purge_table").is_none());
        }
    }
}
