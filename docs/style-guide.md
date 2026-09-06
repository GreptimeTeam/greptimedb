# GreptimeDB Style Guide

This style guide is intended to help contributors to GreptimeDB write code that is consistent with the rest of the codebase. It is a living document and will be updated as the codebase evolves.

It's mainly an complement to the [Rust Style Guide](https://pingcap.github.io/style-guide/rust/).

## Table of Contents

- Formatting
- Modules
- Comments

## Formatting

- Place all `mod` declaration before any `use`.
- Use `unimplemented!()` instead of `todo!()` for things that aren't likely to be implemented.
- Add an empty line before and after declaration blocks.
- Place comment before attributes (`#[]`) and derive (`#[derive]`).

## Modules

- Use the file with same name instead of `mod.rs` to define a module. E.g.:

```
.
├── cache
│  ├── cache_size.rs
│  └── write_cache.rs
└── cache.rs
```

## Comments

- Add comments for public functions and structs.
- Prefer document comment (`///`) over normal comment (`//`) for structs, fields, functions etc.
- Add link (`[]`) to struct, method, or any other reference. And make sure that link works.

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
