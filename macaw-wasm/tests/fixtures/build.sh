#!/usr/bin/env bash
set -euo pipefail

fixtures="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
guest="$fixtures/guest"
output="$fixtures/http-auth-plugin.wasm"

cargo build \
  --manifest-path "$guest/Cargo.toml" \
  --target wasm32-unknown-unknown \
  --release
wasm-tools component new \
  "$guest/target/wasm32-unknown-unknown/release/macaw_http_auth_test_plugin.wasm" \
  -o "$output"
wasm-tools validate "$output"
