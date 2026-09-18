# Release validation

The [2026-09-18 results](RELEASE_RESULTS.md) record the completed first-release
qualification and its limits.

Run from the repository root with Python 3.11+, Rust 1.96.1, and the dependencies
in the committed lockfiles available in Cargo's cache:

```sh
python3 tests/qualification/facade.py --live
python3 tests/qualification/release_archives.py
```

These are local checks; neither command publishes a crate. The first command
runs the [facade, macro, package, example and documentation checks](FACADE.md),
then creates a disposable Docker Compose example environment. Without `--live`,
it runs the offline stages only. The second command builds the actual release
archives. Run Cargo-heavy checks sequentially on smaller machines; both respect
`CARGO_TARGET_DIR` and otherwise use the repository's `target/` directory.

## Actual archive verification

`release_archives.py` runs **`cargo package` with verification enabled**, rather
than treating `cargo package --list` or `--no-verify` as a successful build.
Cargo 1.96.1 packages the workspace dependencies together, stages their registry
entries locally, and compiles each package from its extracted archive. This
allows the first release to be qualified before its dependencies are published.
No temporary `[patch]` entries are added to the workspace and no archive
manifests are rewritten by the checker.

Two supported profiles are selected without `--all-features`:

| Profile | Coverage |
| --- | --- |
| `default` | All 39 packages with their default features |
| `documentation` | The explicit docs.rs features of 38 packages, including AsyncAPI, both inbox backends, and factory APIs; implicit defaults are disabled |

`lily_log` depends on ClickHouse's default singleton mode. It is verified in the
default profile and excluded from the factory composition to avoid enabling
mutually exclusive `single` and `factory` features together. Its individual
docs.rs build remains part of the documentation gate. These are archive build
profiles, not claims that all dependency-injection modes compose in one process.

Both commands use `--locked --offline`. The archive checker permits a dirty
working tree while recording that state in its report and each archive's VCS
metadata. It fails if Cargo reports a yanked locked dependency. The source
lockfiles are not changed by the checker. This is not a vulnerability audit and
offline metadata cannot detect registry changes newer than the local index.

After Cargo has successfully verified exactly the expected packages, the checker
opens each real `.crate` and checks:

- normalized package metadata, explicit `0.1.0`, Rust `1.96.1`, and edition 2024;
- registry dependency versions with no local `path`, Git or workspace overrides;
- unchanged feature definitions and internal registry identities in `Cargo.lock`;
- the original manifest, README, both license texts, and every other source file
  against the current checkout's bytes;
- duplicate/unexpected archive members and excluded development artifacts.

Reports, command logs, archive SHA-256 values, and copies of the tested archives
are retained under `target/release-archives/<run>/`. Select a profile explicitly
when diagnosing a failure:

```sh
python3 tests/qualification/release_archives.py --profile default
python3 tests/qualification/release_archives.py --profile documentation
```

## Additional host checks

Several existing tests are ignored during ordinary package tests because they
need host signals, loopback sockets, or the pinned OpenTelemetry Collector.
On an appropriate Linux host, run these explicitly:

```sh
cargo +1.96.1 test -p lily_shutdown --test signal_subprocess --locked --offline -- --ignored
cargo +1.96.1 test -p lily_websocket_client --test integration_test --locked --offline -- --ignored
cargo +1.96.1 test -p lily_trace --test live_otlp_qualification --locked --offline -- --ignored
cargo +1.96.1 test -p lily_injection --test resolution_exporters --locked --offline -- --ignored
cargo +1.96.1 test -p lily_websocket --test telemetry_collector --test build_cancellation_deadline --locked --offline -- --ignored
```

Collector tests create their own containers; they do not silently skip missing
Docker access or a missing image. See the
[live OTLP setup](../../crates/integrations/lily_trace/LIVE_OTLP_QUALIFICATION.md)
for the pinned image. Ordinary ignored counts and separately executed host tests
must both be reported; a successful default harness does not mean its ignored
tests ran.

## Boundaries

The default package suites, optional-feature fixtures, docs.rs builds, actual
archives and live examples cover different contracts. Preserve all of them.
Do not replace per-package tests with `cargo test --workspace`: workspace-wide
feature unification changes the transport-free DI test environment, as described
in [the facade guide](FACADE.md).

Linux qualification does not establish Windows/macOS compatibility. The docs.rs
target is a documentation setting, not a platform-support restriction. Dedicated
database/service-version, TLS/mTLS, fault-injection, transactional-inbox, fuzz,
load and performance suites also remain distinct from this gate. ClickHouse is
compiled and contract-tested but is not part of the live example stack.

Successful local packaging does not validate crates.io name ownership, account
permissions, index propagation or the hosted docs.rs build. Publication remains
a separate step against the final committed release candidate.

Cargo's archive and verification behavior is documented in
[cargo package](https://doc.rust-lang.org/cargo/commands/cargo-package.html).
