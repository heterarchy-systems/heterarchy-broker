# Repository xtask

This directory contains the repository's sole executable verification harness. It is materialized from the Alexandria Rust Engineering Harness v2 and adapted to the repository-local profile.

Run it through the root Makefile or Cargo alias:

```text
make rules|fmt|check|test|msrv|ci|doctor|extended|deps|perf
cargo xtask rules|fmt|check|test|msrv|ci|doctor|extended|deps|perf
```

The xtask parses TOML and Cargo metadata structurally. It verifies the complete activated rule inventory, workspace and public-policy MSRVs, lint inheritance, Harness manifest references, declared toolchain availability, and source inventory shape. Cargo child commands that can resolve the dependency graph use `--locked`; the declared `rust-toolchain.toml` channel is used through `rustup run`. `SOURCES.json` hashes are recorded provenance; byte-for-byte hash recalculation remains an external materialization check.

`msrv` runs the public policy crate's all-target and doctest suites under Rust 1.92.0. `deps` fails when `deny.toml` is absent. `extended` runs dependency policy only when `HARNESS.toml` enables it. `perf` fails explicitly unless Cargo metadata declares a benchmark target. A benchmark process succeeding is measurement evidence, not proof that a performance budget passed.
