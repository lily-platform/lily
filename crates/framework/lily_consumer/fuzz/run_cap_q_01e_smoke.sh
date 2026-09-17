#!/usr/bin/env bash

set +e

CAPQ01E_FUZZ_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)" || {
  printf 'CAP-Q-01E fuzz bootstrap failed: cannot resolve fuzz workspace\n' >&2
  exit 1
}
CAPQ01E_EVIDENCE_DIR="$(mktemp -d /tmp/cap-q-01e-fuzz.XXXXXX)" || {
  printf 'CAP-Q-01E fuzz bootstrap failed: cannot create evidence directory\n' >&2
  exit 1
}
CAPQ01E_LOG="$CAPQ01E_EVIDENCE_DIR/cap-q-01e-fuzz.log"
CAPQ01E_FAILURES=0

: > "$CAPQ01E_LOG" || {
  printf 'CAP-Q-01E fuzz bootstrap failed: cannot create log file in %s\n' \
    "$CAPQ01E_EVIDENCE_DIR" >&2
  exit 1
}
cp -R "$CAPQ01E_FUZZ_DIR/corpus" "$CAPQ01E_EVIDENCE_DIR/corpus" || {
  printf 'CAP-Q-01E fuzz bootstrap failed: cannot copy tracked corpus\n' \
    | tee -a "$CAPQ01E_LOG" >&2
  exit 1
}

CAPQ01E_FUZZ_VERSION="$(cargo +nightly-2026-08-15 fuzz --version 2>&1)"
if [ "$?" -ne 0 ] || [ "$CAPQ01E_FUZZ_VERSION" != "cargo-fuzz 0.13.2" ]; then
  printf 'CAP-Q-01E fuzz bootstrap failed: expected cargo-fuzz 0.13.2, got %s\n' \
    "$CAPQ01E_FUZZ_VERSION" | tee -a "$CAPQ01E_LOG" >&2
  exit 1
fi

printf 'CAP-Q-01E evidence directory: %s\n' "$CAPQ01E_EVIDENCE_DIR" | tee -a "$CAPQ01E_LOG"

run_capq01e_target() {
  local target="$1"
  local max_len="$2"
  local corpus="$CAPQ01E_EVIDENCE_DIR/corpus/$target"
  local artifacts="$CAPQ01E_EVIDENCE_DIR/artifacts/$target"

  if ! mkdir -p "$artifacts"; then
    printf 'FAIL: %s (cannot create artifact directory)\n' "$target" \
      | tee -a "$CAPQ01E_LOG"
    CAPQ01E_FAILURES=$((CAPQ01E_FAILURES + 1))
    return
  fi
  printf '\n===== %s =====\n' "$target" | tee -a "$CAPQ01E_LOG"
  (
    cd "$CAPQ01E_FUZZ_DIR" || exit 1
    cargo +nightly-2026-08-15 fuzz run "$target" "$corpus" -- \
      -runs=10000 \
      -max_len="$max_len" \
      -timeout=2 \
      -artifact_prefix="$artifacts/"
  ) 2>&1 | tee -a "$CAPQ01E_LOG"
  local status=${PIPESTATUS[0]}

  if [ "$status" -eq 0 ]; then
    printf 'PASS: %s\n' "$target" | tee -a "$CAPQ01E_LOG"
  else
    printf 'FAIL: %s (exit code: %s)\n' "$target" "$status" | tee -a "$CAPQ01E_LOG"
    CAPQ01E_FAILURES=$((CAPQ01E_FAILURES + 1))
  fi
}

run_capq01e_target queue_delivery_envelope 65536
run_capq01e_target queue_amqp_metadata 65536
run_capq01e_target queue_extractor_plan 65536
run_capq01e_target queue_settlement_state 256

printf '\n===== CAP-Q-01E FUZZ SUMMARY =====\n' | tee -a "$CAPQ01E_LOG"
printf 'Failure count: %s\n' "$CAPQ01E_FAILURES" | tee -a "$CAPQ01E_LOG"
printf 'Evidence directory: %s\n' "$CAPQ01E_EVIDENCE_DIR" | tee -a "$CAPQ01E_LOG"
printf 'Log file: %s\n' "$CAPQ01E_LOG" | tee -a "$CAPQ01E_LOG"

if [ "$CAPQ01E_FAILURES" -eq 0 ]; then
  printf 'CAP-Q-01E fuzz sonucu: PASS\n' | tee -a "$CAPQ01E_LOG"
  exit 0
fi

printf 'CAP-Q-01E fuzz sonucu: FAIL\n' | tee -a "$CAPQ01E_LOG"
exit 1
