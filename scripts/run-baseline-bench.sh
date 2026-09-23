#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"

allowed_cpus="${DODB_ALLOWED_CPUS:-0,1,6,7}"
memory_high="${DODB_MEMORY_HIGH:-6G}"
memory_max="${DODB_MEMORY_MAX:-8G}"
memory_swap_max="${DODB_MEMORY_SWAP_MAX:-0}"
bench_root="${DODB_BENCH_ROOT:-$repo_dir/target/dodb-bench/work}"
git_sha="$(git rev-parse HEAD)"
output="${DODB_BENCH_OUTPUT:-$repo_dir/target/dodb-bench/baseline-$git_sha.json}"

mkdir -p "$bench_root" "$(dirname "$output")"
CARGO_BUILD_JOBS=4 cargo build --release -p dodb-bench

systemd-run --user --scope --quiet \
    -p "AllowedCPUs=$allowed_cpus" \
    -p "MemoryHigh=$memory_high" \
    -p "MemoryMax=$memory_max" \
    -p "MemorySwapMax=$memory_swap_max" \
    -- taskset -c "$allowed_cpus" env \
    DODB_ALLOWED_CPUS="$allowed_cpus" \
    DODB_MEMORY_HIGH="$memory_high" \
    DODB_MEMORY_MAX="$memory_max" \
    DODB_MEMORY_SWAP_MAX="$memory_swap_max" \
    DODB_BENCH_ROOT="$bench_root" \
    CARGO_BUILD_JOBS=4 \
    target/release/dodb-bench --output "$output"

printf 'benchmark artifact: %s\n' "$output"
