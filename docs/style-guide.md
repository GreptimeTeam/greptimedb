# GreptimeDB Style Guide

This style guide is intended to help contributors to GreptimeDB write code that is consistent with the rest of the codebase. It is a living document and will be updated as the codebase evolves.

It's mainly an complement to the [Rust Style Guide](https://pingcap.github.io/style-guide/rust/).

## Table of Contents

- Formatting
- Modules

## Formatting

- Place all `mod` declaration before any `use`.
- Use `unimplemented!()` instead of `todo!()` for things that aren't likely to be implemented.

## Modules

- Use the file with same name instead of `mod.rs` to define a module. E.g.:

```
.
├── cache
│  ├── cache_size.rs
│  └── write_cache.rs
└── cache.rs
```