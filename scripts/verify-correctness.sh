#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
mode="${1:-}"

if [[ "$mode" != "fast" && "$mode" != "full" ]]; then
  printf 'Usage: %s {fast|full}\n' "$0" >&2
  exit 2
fi

cd "$repo_root"
start_seconds=$SECONDS

section() {
  printf '\n== %s ==\n' "$1"
}

run() {
  local label="$1"
  shift
  section "$label"
  "$@"
}

run "Rust formatting" cargo fmt --all -- --check
run "Rust workspace" cargo test --workspace --no-fail-fast

for formal_test in \
  formal:test \
  formal:group:test \
  formal:segments:test \
  formal:collection:test \
  formal:wal:test \
  formal:publication:test \
  formal:checkpoint:test \
  formal:e2e:test
do
  run "Quint model test: $formal_test" npm run "$formal_test"
done

for formal_typecheck in \
  formal:typecheck \
  formal:group:typecheck \
  formal:segments:typecheck \
  formal:collection:typecheck \
  formal:wal:typecheck \
  formal:publication:typecheck \
  formal:checkpoint:typecheck \
  formal:e2e:typecheck
do
  run "Quint typecheck: $formal_typecheck" npm run "$formal_typecheck"
done

if [[ "$mode" == "full" ]]; then
  run "Rust correspondence" cargo test -p dodb-testkit --test formal_correspondence -- --nocapture
  run "Persisted-prefix durability DST" cargo test -p dodb-testkit --test durability_dst -- --nocapture

  for formal_tlc in \
    formal:verify:tlc \
    formal:group:verify:tlc \
    formal:segments:verify:tlc \
    formal:collection:verify:tlc \
    formal:wal:verify:tlc \
    formal:publication:verify:tlc \
    formal:checkpoint:verify:tlc \
    formal:e2e:verify:tlc
  do
    run "Quint TLC: $formal_tlc" npm run "$formal_tlc"
  done

  for formal_apalache in \
    formal:verify:apalache \
    formal:group:verify:apalache \
    formal:segments:verify:apalache \
    formal:collection:verify:apalache \
    formal:wal:verify:apalache \
    formal:publication:verify:apalache \
    formal:checkpoint:verify:apalache \
    formal:e2e:verify:apalache
  do
    run "Quint Apalache: $formal_apalache" npm run "$formal_apalache"
  done

  run "Final whitespace check" git diff --check
fi

section "Correctness gate complete"
printf 'mode=%s elapsed_seconds=%s\n' "$mode" "$((SECONDS - start_seconds))"
