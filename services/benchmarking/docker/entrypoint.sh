#!/usr/bin/env sh
set -eu

export BENCHMARK_EXECUTION_MODE="${BENCHMARK_EXECUTION_MODE:-docker}"
exec "$@"
