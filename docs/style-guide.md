# GreptimeDB Style Guide

This style guide is intended to help contributors to GreptimeDB write code that is consistent with the rest of the codebase. It is a living document and will be updated as the codebase evolves.

It's mainly an complement to the [Rust Style Guide](https://pingcap.github.io/style-guide/rust/).

## Table of Contents

- Formatting
- Naming
- Imports
- Modules
- Structs and functions
- Comments
- Cargo.toml
- Unsafe

## Formatting

- Place all `mod` declaration before any `use`.
- Use `unimplemented!()` instead of `todo!()` for things that aren't likely to be implemented.
- Add an empty line before and after declaration blocks.
- Place comment before attributes (`#[]`) and derive (`#[derive]`).

## Naming

- Before adding a struct, function, or module, inspect the closest implementation
  in the same crate. Reuse its domain terminology, suffixes, placement, and
  construction pattern rather than inventing a new convention.
- Treat uppercase acronyms as one word in project-defined names: `Grpc`,
  `Http`, `Otlp`, `Sst`, `Wal`, and `Sql`, not `GRPC`, `HTTP`, or one letter per
  word. Keep externally defined names and product branding unchanged.
- Use `-` for Cargo package names and standalone crate directories (for example,
  `src/meta-client`). For nested crates, follow the nearby workspace layout
  (for example, `src/common/meta`), and use `_` for crate-internal module and
  directory names.
- Name a type after its responsibility. Established suffixes include `Config`
  or `Options` for configuration, `Builder` for staged construction, `Context`
  for an explicit operation context, `Request`/`Response` for protocol payloads,
  and `Ref` for shared-reference aliases. Use `Impl` only for a concrete
  implementation of an existing trait.
- Use verbs for operations: `create_*`, `build_*`, `parse_*`, `list_*`,
  `is_*`, and `has_*`. Reserve `*_inner` for an implementation behind a
  corresponding higher-level API.

## Imports

- Prefer crate-rooted imports (`use crate::...`) over `self::` or `super::` in
  production code. Tests may use `use super::*`; re-exports may use relative
  paths such as `pub use self::foo::bar`.
- Prefer importing a function or constant's module and qualifying the use at
  the call site. Do not import enum variants except for narrowly established
  cases such as `Some` and `Err`.
- Avoid wildcard imports except for a prelude or `use super::*` in tests.

## Modules

- Use the file with same name instead of `mod.rs` to define a module. E.g.:

```
.
├── cache
│  ├── cache_size.rs
│  └── write_cache.rs
└── cache.rs
```

- Keep items private by default. Do not introduce `pub(super)` or `pub(in ...)`.
  When an item must be used outside its defining module, use `pub(crate)` or
  restructure the module so it can remain private.
- Name test modules `tests`. Put public structs, enums, and functions before
  private items, and order related items top-down from their public entry point
  to implementation details.

## Structs and functions

- Give each struct one cohesive responsibility. Split independent state,
  I/O, transformation, and orchestration into focused types and compose them.
  Do not create catch-all `Helper`, `Utils`, `Manager`, or `Context` types to
  avoid defining ownership boundaries.
- Follow the closest same-crate constructor pattern. Use `new` for a small set
  of required inputs. For optional, chainable configuration, use the established
  fluent API in that module (`with_*` or field-named builder methods). Use a
  `Builder` when the construction has many optional inputs or requires staged
  validation. Use `Default` when the type has a meaningful default state.
- Keep a function focused on one operation. Move reusable transformations or
  stateful sub-operations into their own type instead of coupling them to an
  unrelated orchestrator.
- Before adding a feature, search the workspace for an existing module, type,
  or utility that already owns the same responsibility. Reuse it when it fits;
  if a small cohesive refactor makes it fit, prefer that refactor over a
  parallel implementation. Do not duplicate a shared abstraction merely to
  avoid touching its current boundary.

## Comments

- Add comments for public functions and structs.
- Prefer document comment (`///`) over normal comment (`//`) for structs, fields, functions etc.
- Add link (`[]`) to struct, method, or any other reference. And make sure that link works.
- Write descriptive comments, not imperative comments: `Opens the file`, not
  `Open the file`.
- Write pending work as `TODO(name): explanation` (or `TODO(name)` when no
  explanation is needed). Write bug workarounds as `FIXME(#1234): explanation`.

## Cargo.toml

- Keep dependency entries alphabetically ordered. Add shared dependencies to
  the root `[workspace.dependencies]` and reuse them from member crates.
- Do not weaken an existing exact version or compatibility pin merely to follow
  a general version-format preference.

## Error handling

- Define a custom error type for the module if needed.
- Use `context()` for cheap context selectors. Its argument is evaluated even when the
  operation succeeds.
- Use `with_context()` when constructing the context requires work such as `format!`,
  allocation, or cloning, so that work only happens on the error path. For example:

```rust
value.with_context(|| InvalidValueSnafu {
    reason: format!("invalid value: {value}"),
})?;
```

- Use `error!()` or `warn!()` macros in the `common_telemetry` crate to log errors. E.g.:

```rust
error!(e; "Failed to do something");
```

## Unsafe

- Avoid manual `unsafe impl Send` or `unsafe impl Sync`. Prefer expressing the
  required bounds in trait declarations or redesigning ownership so Rust can
  derive the auto traits safely.
