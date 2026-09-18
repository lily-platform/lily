# Release package and documentation checks

Every publishable crate has its own docs.rs profile and package-content policy.
Run the audit from the repository root with Python 3.11+ and Rust 1.96.1:

```sh
python3 tests/qualification/release_packages.py
python3 tests/qualification/release_packages.py --docs
```

The first command checks all 39 packages with `cargo package --list`. The second
also builds each package independently using its actual
`package.metadata.docs.rs` features. Neither command publishes packages. Cargo
runs with `--locked --offline`; fetch the workspace dependencies beforehand on
a new machine. Existing `CARGO_TARGET_DIR` is respected.

## Documentation profiles

All profiles disable implicit default features and build only
`x86_64-unknown-linux-gnu`. This target selection controls documentation builds,
not the platform support of application code. It avoids implying qualification
of additional documentation targets in the initial release.

| Packages | Documentation features |
| --- | --- |
| `lily_consumer`, `lily_queue` | `asyncapi`, both PostgreSQL and MongoDB transactional-inbox **factory** profiles |
| `lily_clickhouse`, `lily_mongodb`, `lily_postgresql`, `lily_queue_client`, `lily_redis`, `lily_websocket_client` | `factory` |
| `lily_clickhouse_derive`, `lily_mongodb_derive` | `factory` |
| `lily_config` | Both transactional-inbox configuration schemas |
| `lily_queue_derive`, `lily_queue_registry` | `asyncapi` |
| `lily_error` | `mongodb-driver` conversions |
| `lily_trace` | `console` |
| `lilyrs` | All component modules, factory composition, AsyncAPI and both transactional-inbox backends |
| Remaining packages | No optional features |

Factory documentation includes the ordinary database/client service types and
the named factory types. It does not change a crate's default application
features. Each docs.rs build selects one valid composition: `single` and
`factory`, or the corresponding transactional-inbox alternatives, are never
combined. Internal `fuzzing` and `test-support` features are not selected for API
documentation. `lilyrs/websocket-redis` still does not enable `websocket`; the
documentation profile selects both explicitly.

The runner checks generated API index entries for factory and transaction types,
in addition to Cargo's exit status. Broken intra-doc links fail the build. It
does not combine workspace defaults: that could hide missing features or enable
conflicting DI registrations. The existing facade qualification also checks the
separate combined single and factory application profiles.

The metadata follows the [docs.rs configuration contract](https://docs.rs/about/metadata).
Local checks use the supported Rust 1.96.1 compiler, `DOCS_RS=1` and the `docsrs`
rustdoc configuration. The hosted service uses its own nightly compiler and
sandbox, so these local results are not evidence of a completed hosted build.
See [docs.rs build behavior](https://docs.rs/about/builds).

## Archive contents

Each package explicitly excludes root `.vscode/`, `wip/` and `fuzz/` development
directories. Existing intentional exclusions, such as DI's repository-only test
and example directories, remain in place. Source, package-local tests and their
required inputs are retained. Every archive must contain its own README and
byte-identical copies of the root MIT and Apache-2.0 license texts.

WebSocket's 39 curated fuzz seeds have one shared home:
`crates/framework/lily_websocket/tests/fixtures/fuzz_corpus/`. Unit regressions
and fuzz runs use those same files. They are outside the nested fuzz workspace,
which [Cargo excludes from package archives](https://doc.rust-lang.org/cargo/reference/manifest.html#the-exclude-and-include-fields).
The fuzz guide copies these seeds into temporary working corpora before mutation.

WebSocket's Collector integration tests use their package-local
`tests/support/collector.rs` and `collector.yaml`. The audit compares both files
byte-for-byte with the shared implementation in `lily_trace/tests/support`;
updates must keep both copies synchronized. This preserves the same test logic
without requiring a sibling source checkout to compile the published package.

Literal `include_*` and `#[path]` references that resolve in the checkout are
checked against Cargo's file list. Files outside the package or excluded from
the archive are rejected. Rust's conditional/module path resolution still
requires compilation of the affected profiles; this scan is not a Rust parser.

## Reproduce checks without excluded source files

```sh
python3 tests/qualification/release_packages.py --snapshot
```

This adds a `package-source/` workspace to the evidence directory using only
Cargo-listed files plus the root workspace manifest and lockfile. It intentionally
does not copy the excluded fuzz workspaces, editor state or unrelated sibling
fixtures. The snapshot retains local path dependencies for pre-publication
compilation; it is not a substitute for a normalized `.crate` archive, registry
resolution or the later package verification gate.

For example, using the snapshot path printed in `report.json`:

```sh
cargo +1.96.1 test --manifest-path "$package_snapshot/Cargo.toml" \
  -p lily_websocket --lib --features fuzzing --locked --offline __fuzzing::tests
cargo +1.96.1 test --manifest-path "$package_snapshot/Cargo.toml" \
  -p lily_websocket --test build_cancellation_deadline \
  --test telemetry_collector --locked --offline
```

The opt-in live Collector cases remain explicitly ignored by ordinary test runs;
running them requires the pinned Docker image and a separate live qualification.

## Evidence and targeted reruns

Every audit writes `target/release-packages/<timestamp>-<id>/report.json` and
command logs, including commit, dirty-tree state, compiler, selected features,
file-list hashes, exit codes and durations. A failure returns a nonzero exit code.

```sh
python3 tests/qualification/release_packages.py --docs --package lily_consumer
```

`--package` is repeatable. Source snapshots require a complete workspace and
therefore cannot be combined with that filter. No lockfiles or registry state are
modified by the checker.

For builds from real normalized `.crate` archives, run
`python3 tests/qualification/release_archives.py` as described in the
[release validation guide](RELEASE_VALIDATION.md). This is a separate gate from
the file-list audit and source snapshot above.
