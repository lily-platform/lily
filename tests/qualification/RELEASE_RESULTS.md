# Release candidate qualification — 2026-09-18

The release validation gates passed on Linux x86-64 with Rust 1.96.1, using
commit `1fc21a0` plus the lockfile and qualification changes described below.
All 39 publishable packages remain version `0.1.0`. No crate was published.
See [release validation](RELEASE_VALIDATION.md) for reproduction commands and
the distinction between source tests, documentation and real archive builds.

## Completed checks

| Area | Result |
| --- | --- |
| Publication graph | 39 packages, 167 internal dependencies; path + version and no cycles |
| Umbrella feature graph | Empty facade and all 54 public features passed |
| Umbrella builds | 57 profiles: empty, 54 individual features, combined single and factory |
| Umbrella consumers | 30 canonical/renamed/profile runs; 94 Rust tests passed |
| Negative facade contracts | All 10 intended compiler diagnostics matched |
| Component facades | 18 runs; 28 Rust tests passed |
| DI facades and public boundaries | 11 runs; 15 Rust tests passed |
| Macro contracts and additional macro features | 9 commands; 66 Rust tests passed, 11 ignored doctests |
| Existing downstream, derive and golden applications | All 16 compilation/execution checks passed |
| Root packages, selected independently | All 39 packages passed; **2,568 tests passed, 0 failed, 122 ignored** |
| Examples and client | 4 unit tests passed; all six packages checked; example/client formatting passed |
| Documentation | All 39 docs.rs profiles and both combined facade profiles compiled; the per-package gate denied broken intra-doc links |
| Actual default archives | All 39 `.crate` files were packaged, extracted and compiled by Cargo |
| Actual optional-feature archives | 38 archives compiled with their documentation/factory profiles; `lily_log` was covered in the default profile |
| Additional Linux host/Collector tests | All 12 explicitly selected tests passed, none skipped |
| Live example stack | HTTP, WebSocket, Consumer, PostgreSQL, MongoDB, Redis and RabbitMQ verification passed |

The HTTP package contributed 437 passing tests and 14 ignored tests, WebSocket
563 passing and 3 ignored, and Consumer 97 passing and 9 ignored. Root-package
counts and the separate unpublished macro fixtures are reported separately;
the relocated macro/runtime tests still run in the macro gate. Its 11 ignored
doctests come from the additional MongoDB factory and Queue AsyncAPI unit/doc
commands, not the isolated runtime contract fixtures.

Counts are Rust harness executions, including doctests. The same contract can
run in more than one feature profile; these are not counts of unique test
functions, UI inputs or individual assertions. No assertion, timeout, expected
diagnostic or ignore annotation was weakened during this qualification.

## Archive evidence

The checker ran `cargo +1.96.1 package --workspace` with verification enabled.
The optional-feature pass disabled implicit defaults and used each package's
explicit docs.rs features. `lily_log` depends on ClickHouse's singleton default
and therefore does not join that factory composition.

All 39 real normalized manifests contained registry versions rather than local
dependency paths. Package metadata, feature definitions, original manifests,
README files, both license texts and every archived source file matched the
checkout. The default archives contained 1,055 files and totaled 3,145,960
compressed bytes. All 38 archives common to the two profiles had identical
SHA-256 values. Cargo's feature selection changed what was compiled without
changing those archive bytes.

## Live evidence

The disposable Compose project was `lily-facade-q-3afa799862fa`. The client
completed the HTTP and WebSocket contracts and submitted the same job twice.
Both deliveries were acknowledged; PostgreSQL contained exactly one job row
and one processed-event row. Redis reported a TTL of 59 seconds, and the deleted
MongoDB note had zero remaining rows.

Trace validation checked 216 HTTP, 58 WebSocket and 42 Consumer JSONL records,
including method lifecycle pairs, numeric duration fields, trace/span identity,
HTTP-to-queue propagation and application rejection classification. The HTTP
and WebSocket background workers stopped cooperatively. All three applications
exited with code 0. The runner removed only its own Compose containers, network
and volumes; the pre-existing MongoDB container remained running.

The extra host qualification executed one first/second-signal shutdown test,
six real loopback WebSocket client tests, two live trace/console Collector tests,
one DI Collector test, and two WebSocket Collector tests. Those 12 cases remain
ignored during ordinary root-package runs and were executed explicitly in the
separate host run; they are not silently included in the 2,568 count above.

## Findings and changes

- Packaging detected the yanked transitive dependency `chacha20 0.10.1` in the
  lockfiles. It was updated precisely to `0.10.2` in all 11 affected tracked
  lockfiles. No other external package resolution changed. The
  [upstream patch](https://github.com/RustCrypto/stream-ciphers/releases/tag/chacha20-v0.10.2)
  fixes an SSE4.1 intrinsic used in the SSE2 RNG/legacy implementation. Both
  final package verification profiles completed without a yanked warning.
- Cargo also reconciled stale internal macro dependencies in the Consumer fuzz
  lockfile with the already migrated macro manifests. Locked offline metadata
  resolution passed for that fuzz workspace; this was not a fuzz campaign.
- Added `release_archives.py` to retain real archive build results, normalized
  manifest/source checks, hashes and command logs. The reproduction guide links
  it with the existing facade and documentation gates.
- Existing package warnings remain: DI intentionally excludes its repository
  tests/example from the archive, and WebSocket has three dead-code warnings.
  These did not fail the package builds. No new runtime or macro behavior
  failure was found in the executed checks.

The exploratory packaging command that found the yanked version used
`--no-verify`; it is not counted as a build result. The first facade run was
interrupted to apply the dependency patch and is not counted as completed.
The successful full run used the updated lockfiles from the start.

## Local reports and boundaries

Raw evidence is retained in ignored artifact directories:

- `target/facade-qualification/20260918T094520Z-2d8ec138/`: all eight source gates,
  live stack, commands, test counts, image identities and cleanup;
- `target/facade-qualification/20260918T094935Z-2b066243/`: the 12 additional
  Linux host and real Collector tests;
- `target/release-archives/20260918T094648Z-6c1d8d01/`: both verified profiles,
  hashes, normalized/source audits and copies of the tested `.crate` files;
- `target/release-packages/20260918T102115Z-1af4edcc/`: all 39 package-content
  and individual docs.rs profile checks;
- `examples/artifacts/verify-20260918-132510-f3f4be/`: response, storage,
  shutdown and trace evidence;
- `target/facade-qualification/20260918T094102Z-c9561d80/`: the earlier,
  deliberately interrupted run before the dependency patch.

To keep disk space available, only build caches/executables created by this
session and belonging to completed checks were removed. The affected run
directories contain cleanup inventories. Source files, verified archives and
test reports were retained.

This is Linux qualification. Windows/macOS and hosted docs.rs were not run.
The remaining dedicated TLS/mTLS, service-version, fault-injection,
transactional-inbox, PostgreSQL context stress, fuzz and performance profiles
were not executed by this gate. ClickHouse remains a compilation/contract
check, outside the live stack. The live examples exercise ordinary queue
delivery with application idempotency, not the transactional-inbox profile.

The successful gates do not turn ignored tests into passes, establish a
vulnerability audit, or validate crates.io ownership and publication permissions.
The final committed candidate still needs the separate publication step.
