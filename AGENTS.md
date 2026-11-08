# AGENTS.md — heterarchy-broker

Thin repository entrypoint for coding agents. It routes to the repository-local Rust harness; it does not duplicate the full Engineering Harness.

## Read order before changing code

1. `AGENTS.md`
2. `.agents/rust_dev_harness/PROJECT_PROFILE.md` — semantic project authority
3. `.agents/rust_dev_harness/HARNESS.toml` — machine-verifiable projection
4. `.agents/rust_dev_harness/HARNESS_MANIFEST.toml` — Harness identity/provenance
5. `.agents/rust_dev_harness/rules/README.md` and the active rule families (`normal_dev_rules`, `type_dev_rules` required; `async_dev_rules` enabled for this runtime)
6. `.agents/rust_dev_harness/skills/rust-engineering/SKILL.md`
7. Actual `Cargo.toml`, `rust-toolchain.toml`, source, tests, `xtask`, and CI

## Canonical product boundary

**Agent Broker owns distributed task claim/lease/fencing authority** for the Agent platform: the versioned wire protocol, the typed client, Raft consensus and term fencing, durable task storage, and the `agentbrokerd` server lifecycle.

- One-node and multi-node deployments share the same authoritative state-machine model.
- Consensus state and management/read-model state remain separate.
- Provider identity and transport details stay outside Broker domain state except narrow opaque routing metadata.
- The retired Python Broker and ChatToCodex application code are not production authorities; external runtimes integrate only through the versioned client protocol and SDK boundary.

## Required verification order

Use focused tests first, then the canonical repository gates:

```text
make rules
make fmt
make check
make test
make ci
```

The Makefile delegates to the repository `xtask` crate with `cargo run --locked -p xtask -- <command>`. Release-readiness targets (`make broker_ci`, `make cutover`, `make perf_check`, `make broker_doctor`) run the Broker e2e/performance/doctor chain and are required before release packaging.

If a gate does not yet exist, do not fabricate a PASS. Record the missing verifier and use the strongest real Cargo checks available.

## Execution discipline

- Read the exact current source and state before proposing edits; preserve unrelated dirty work. Never use reset/clean/stash/checkout--/broad revert as a shortcut.
- Make ownership, boundedness, cancellation, failure semantics, and evidence explicit. The client, router, and daemon surfaces are loopback-only by policy today; changing that is a Broker security decision, not a local edit.
- Multi-node safety claims require leader election, quorum commit, and stale-leader rejection tests with exact term/epoch/revision fencing.
- After an unknown outcome, read durable state before retrying a mutation.
